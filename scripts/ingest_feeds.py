import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import requests

# Optional dependency: feedparser (RSS/Atom). If not installed, we fallback to simple HTML scrape.
try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None

GH_API = "https://api.github.com"

DEFAULT_EXISTING_ISSUE_PAGES = int(os.environ.get("INGEST_EXISTING_ISSUE_PAGES", "10"))
REQUEST_TIMEOUT = int(os.environ.get("INGEST_REQUEST_TIMEOUT", "30"))


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def gh_headers():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("Missing GITHUB_TOKEN in Actions environment.")

    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "voodoo-feed-ingest",
    }


def gh_get(url, params=None):
    r = requests.get(url, headers=gh_headers(), params=params, timeout=REQUEST_TIMEOUT)

    if r.status_code == 403:
        # GitHub sometimes returns Search/secondary-rate-limit style 403s.
        # Repo-listing calls should rarely hit this, but include diagnostics.
        remaining = r.headers.get("X-RateLimit-Remaining")
        reset = r.headers.get("X-RateLimit-Reset")
        raise RuntimeError(
            f"GitHub GET forbidden/limited: {url} params={params} "
            f"remaining={remaining} reset={reset} body={r.text[:500]}"
        )

    r.raise_for_status()
    return r.json()


def gh_post(url, payload):
    r = requests.post(url, headers=gh_headers(), json=payload, timeout=REQUEST_TIMEOUT)

    # Helpful diagnostics if labels are wrong etc.
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub POST failed {r.status_code}: {r.text[:500]}")

    return r.json()


def list_all_labels(owner, repo):
    labels = set()
    page = 1

    while True:
        data = gh_get(
            f"{GH_API}/repos/{owner}/{repo}/labels",
            params={"per_page": 100, "page": page},
        )

        if not data:
            break

        for x in data:
            name = x.get("name")
            if name:
                labels.add(name)

        page += 1
        if page > 20:
            break

    return labels


def normalize_url(url):
    raw = str(url or "").strip()
    if not raw:
        return ""

    try:
        parts = urlsplit(raw)
        scheme = (parts.scheme or "https").lower()
        netloc = parts.netloc.lower()

        # Remove common tracking parameters.
        query_items = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            k = key.lower()
            if k.startswith("utm_") or k in {
                "fbclid",
                "gclid",
                "mc_cid",
                "mc_eid",
                "igshid",
                "ref",
                "ref_src",
            }:
                continue
            query_items.append((key, value))

        query = urlencode(query_items, doseq=True)
        path = parts.path.rstrip("/") or parts.path

        return urlunsplit((scheme, netloc, path, query, "")).strip().lower()
    except Exception:
        return raw.rstrip("/").lower()


def extract_urls(text):
    urls = re.findall(r"https?://[^\s<>'\")]+", str(text or ""))
    return {normalize_url(u) for u in urls if normalize_url(u)}


def list_existing_issue_links(owner, repo, pages=DEFAULT_EXISTING_ISSUE_PAGES):
    """
    Avoid GitHub Search API for dedupe.

    The old script queried /search/issues once per candidate link. That can hit
    GitHub Search rate limits or secondary abuse limits and fail with HTTP 403.
    This version loads existing issues in pages and dedupes locally by normalized URL.
    """
    links = set()
    ingest_ids = set()

    for page in range(1, pages + 1):
        data = gh_get(
            f"{GH_API}/repos/{owner}/{repo}/issues",
            params={
                "state": "all",
                "per_page": 100,
                "page": page,
                "sort": "created",
                "direction": "desc",
            },
        )

        if not data:
            break

        for issue in data:
            if issue.get("pull_request"):
                continue

            title = issue.get("title") or ""
            body = issue.get("body") or ""
            links.update(extract_urls(title))
            links.update(extract_urls(body))

            for m in re.finditer(r"VOODOO_FEED_INGEST:\s*([a-f0-9]{8,64})", body, re.I):
                ingest_ids.add(m.group(1).lower())

        if len(data) < 100:
            break

    return links, ingest_ids


def link_seen(link, existing_links):
    return normalize_url(link) in existing_links


def make_ingest_id(feed_name, link, title):
    base = f"{feed_name}\n{normalize_url(link)}\n{str(title or '').strip()}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()[:16]


def safe_labels(wanted, existing):
    # Keep only labels that exist; avoid 422 errors.
    out = []
    for label in wanted:
        if label in existing:
            out.append(label)
    return sorted(set(out))


REG_RULES = [
    (re.compile(r"\b(north sea|nordsee|german bight|deutsche bucht)\b", re.I), "REG:NORTH_SEA"),
    (re.compile(r"\b(baltic|ostsee|kiel bight|fehmarn)\b", re.I), "REG:BALTIC_SEA"),
    (re.compile(r"\b(channel|english channel|la manche|ärmelkanal)\b", re.I), "REG:ATLANTIC_NE"),
    (re.compile(r"\b(mediterranean|mittelmeer)\b", re.I), "REG:MED"),
    (re.compile(r"\b(wilhelmshaven)\b", re.I), "REG:WILHELMSHAVEN"),
    (re.compile(r"\b(hamburg)\b", re.I), "REG:HAMBURG"),
]

DOMAIN_RULES = [
    (re.compile(r"\b(drone|uas|uav)\b", re.I), "D:DRONE_UAS"),
    (re.compile(r"\b(gnss|jamming|spoof)\b", re.I), "D:RF_SIGNAL"),
    (re.compile(r"\b(cyber|hack|ransom|ot|scada)\b", re.I), "D:CYBER_OT"),
    (re.compile(r"\b(piracy|attack|boarding|smuggling|stowaway)\b", re.I), "D:SECURITY_CRIME"),
    (re.compile(r"\b(collision|fire|grounding|engine failure|detention)\b", re.I), "D:INCIDENT"),
    (re.compile(r"\b(cable|pipeline|wind farm|windfarm|port)\b", re.I), "D:INFRA_CI"),
]

PAT_RULES = [
    (re.compile(r"\bloiter|loitering\b", re.I), "PAT:LOITERING"),
    (re.compile(r"\bAIS gap|ais off|dark\b", re.I), "PAT:AIS_GAP"),
    (re.compile(r"\bspoof\b", re.I), "PAT:GNSS_SPOOF"),
    (re.compile(r"\bjamm\b", re.I), "PAT:GNSS_JAM"),
    (re.compile(r"\bsurvey\b|\bseabed\b|\bhydrographic\b", re.I), "PAT:SURVEYING"),
    (re.compile(r"\bSTS\b|\bship-to-ship\b", re.I), "PAT:STS_SUSPECT"),
]


def infer_labels(text, base_labels):
    labels = set(base_labels)

    # Region.
    for rx, reg in REG_RULES:
        if rx.search(text):
            labels.add(reg)
            break

    # Domain (override NEWS_INTEL if strong signal).
    for rx, dom in DOMAIN_RULES:
        if rx.search(text):
            labels.add(dom)
            break

    # Patterns.
    for rx, pat in PAT_RULES:
        if rx.search(text):
            labels.add(pat)

    # Phase Zero suspicion: conservative trigger words.
    if (
        re.search(r"\b(sabotage|hybrid|proxy|unknown actor|covert)\b", text, re.I)
        or (
            re.search(r"\b(cable|pipeline)\b", text, re.I)
            and re.search(r"\b(damage|cut|rupture)\b", text, re.I)
        )
        or re.search(r"\b(jamming|spoof)\b", text, re.I)
    ):
        labels.add("P0:SUSPECT")

    return labels


def extract_latlon(text):
    m = re.search(r"(-?\d{1,2}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)", text)
    if m:
        return float(m.group(1)), float(m.group(2))
    return None, None


def parse_feed_items(feed):
    url = feed["url"]
    name = feed.get("name", url)
    items = []

    # 1) RSS/Atom via feedparser if possible.
    if feedparser is not None:
        try:
            parsed = feedparser.parse(url)
            entries = getattr(parsed, "entries", []) or []
            if entries:
                for entry in entries[:40]:
                    link = getattr(entry, "link", None) or getattr(entry, "id", None)
                    title = getattr(entry, "title", None) or "(no title)"
                    summary = getattr(entry, "summary", None) or getattr(entry, "description", "") or ""
                    published = getattr(entry, "published", None) or getattr(entry, "updated", None) or ""

                    if link:
                        link = urljoin(url, str(link).strip())

                    items.append(
                        {
                            "title": str(title).strip(),
                            "link": link,
                            "summary": str(summary).strip(),
                            "published": str(published).strip(),
                        }
                    )

                return items
        except Exception as exc:
            print(f"WARN: feedparser failed for {name}: {exc}")

    # 2) Fallback: light scrape (for pages like UKMTO recent-incidents).
    try:
        res = requests.get(url, headers={"User-Agent": "voodoo-feed-ingest"}, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        html = res.text
    except Exception as exc:
        print(f"WARN: fetch failed for feed {name}: {exc}")
        return []

    # Very simple: grab first N hrefs that look like detail pages.
    links = re.findall(r'href=["\']([^"\']+)["\']', html)
    seen = set()

    for href in links:
        link = urljoin(url, href)
        if not link.startswith("http"):
            continue
        if "ukmto.org" in url and "ukmto.org" not in link:
            continue

        normalized = normalize_url(link)
        if normalized in seen:
            continue
        seen.add(normalized)

        # Crude title from slug.
        title = link.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ").strip()
        if len(title) < 8:
            continue

        items.append(
            {
                "title": f"{name}: {title[:120]}",
                "link": link,
                "summary": "",
                "published": "",
            }
        )

        if len(items) >= 25:
            break

    return items


def load_feed_config():
    with open("config/feeds.json", "r", encoding="utf-8") as f:
        return json.load(f)


def main():
    repo_full = os.environ.get("GITHUB_REPOSITORY")  # "owner/repo"
    if not repo_full or "/" not in repo_full:
        raise RuntimeError("Missing GITHUB_REPOSITORY.")

    owner, repo = repo_full.split("/", 1)
    cfg = load_feed_config()

    max_new = int(cfg.get("max_new_per_run", 10))
    feeds = cfg.get("feeds", [])

    existing_labels = list_all_labels(owner, repo)
    existing_links, existing_ingest_ids = list_existing_issue_links(owner, repo)

    print(f"Existing issue URLs indexed: {len(existing_links)}")
    print(f"Existing ingest IDs indexed: {len(existing_ingest_ids)}")

    created = 0
    skipped_existing = 0
    skipped_no_link = 0
    failed_feeds = 0

    for feed in feeds:
        if created >= max_new:
            break

        feed_name = feed.get("name", feed.get("url", "(unknown)"))
        base_labels = feed.get("base_labels", ["D:NEWS_INTEL", "CONF:LOW", "SEV:1", "SRC:OSINT"])

        try:
            items = parse_feed_items(feed)
        except Exception as exc:
            failed_feeds += 1
            print(f"WARN: feed skipped after parser error: {feed_name}: {exc}")
            continue

        print(f"Feed: {feed_name} items={len(items)}")

        for item in items:
            if created >= max_new:
                break

            link = item.get("link")
            if not link:
                skipped_no_link += 1
                continue

            title = item.get("title") or "(no title)"
            summary = (item.get("summary") or "").strip()
            published = item.get("published") or ""
            ingest_id = make_ingest_id(feed_name, link, title)

            if link_seen(link, existing_links) or ingest_id in existing_ingest_ids:
                skipped_existing += 1
                continue

            blob = f"{title}\n{summary}\n{link}"
            inferred = infer_labels(blob, base_labels)
            labels = safe_labels(inferred, existing_labels)

            lat, lon = extract_latlon(blob)
            geo_line = ""
            if lat is not None and lon is not None:
                geo_line = f"\n\nGeo: {lat:.4f}, {lon:.4f}\n"

            body = (
                f"### Source\n{feed_name}\n\n"
                f"### Link\n{link}\n\n"
                f"### Published\n{published if published else '(unknown)'}\n\n"
                f"### Auto-Labels\n" + ", ".join(labels) + "\n"
                f"{geo_line}\n"
                f"### Summary\n{summary[:4000] if summary else '(no summary)'}\n\n"
                f"---\n"
                f"_Ingested at {now_iso()} UTC_\n\n"
                f"<!-- VOODOO_FEED_INGEST: {ingest_id} -->"
            )

            gh_post(
                f"{GH_API}/repos/{owner}/{repo}/issues",
                {"title": f"[AUTO] {title[:180]}", "body": body, "labels": labels},
            )

            created += 1
            existing_links.add(normalize_url(link))
            existing_ingest_ids.add(ingest_id)
            time.sleep(0.7)  # Be gentle to API.

    print(f"Created issues: {created}")
    print(f"Skipped existing: {skipped_existing}")
    print(f"Skipped no link: {skipped_no_link}")
    print(f"Failed feeds: {failed_feeds}")


if __name__ == "__main__":
    main()
