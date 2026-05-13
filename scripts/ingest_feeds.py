import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
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
DEFAULT_LOOKBACK_HOURS = int(os.environ.get("INGEST_DEFAULT_LOOKBACK_HOURS", "168"))
DEFAULT_MAX_ITEMS_PER_SOURCE = int(os.environ.get("INGEST_DEFAULT_MAX_ITEMS_PER_SOURCE", "20"))


# -------------------- time helpers --------------------
def now_utc():
    return datetime.now(timezone.utc)


def now_iso():
    return now_utc().isoformat()


def iso_z(dt):
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_source_datetime(value):
    """
    Parses common RSS/Atom datetime values into timezone-aware UTC datetime.
    Returns None if parsing fails.
    """
    if not value:
        return None

    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text or text.lower() in {"unknown", "(unknown)", "none", "null"}:
            return None

        try:
            dt = parsedate_to_datetime(text)
        except Exception:
            try:
                text = text.replace("Z", "+00:00")
                dt = datetime.fromisoformat(text)
            except Exception:
                return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def entry_get(entry, key, default=None):
    """
    Works for normal dicts and feedparser FeedParserDict objects.
    """
    try:
        if isinstance(entry, dict):
            return entry.get(key, default)
        if hasattr(entry, "get"):
            return entry.get(key, default)
        return getattr(entry, key, default)
    except Exception:
        return default


def entry_source_datetime(entry):
    """
    Gets the best available source publication timestamp from a feed entry.
    """
    candidates = [
        entry_get(entry, "published"),
        entry_get(entry, "updated"),
        entry_get(entry, "created"),
        entry_get(entry, "pubDate"),
        entry_get(entry, "date"),
        entry_get(entry, "published_at"),
        entry_get(entry, "time"),
        entry_get(entry, "timestamp"),
    ]

    for value in candidates:
        dt = parse_source_datetime(value)
        if dt:
            return dt

    # feedparser sometimes has *_parsed structs
    for key in ["published_parsed", "updated_parsed", "created_parsed"]:
        value = entry_get(entry, key)
        if value:
            try:
                return datetime(*value[:6], tzinfo=timezone.utc)
            except Exception:
                pass

    return None


def source_lookback_hours(source, default_hours=DEFAULT_LOOKBACK_HOURS):
    try:
        return int(source.get("lookback_hours", default_hours))
    except Exception:
        return default_hours


def source_allows_backfill(source):
    return bool(source.get("allow_backfill", False))


def max_items_for_source(source, default_items=DEFAULT_MAX_ITEMS_PER_SOURCE):
    try:
        return int(source.get("max_items", default_items))
    except Exception:
        return default_items


def should_keep_entry_for_source(entry, source):
    """
    Drops old RSS/social items unless allow_backfill is true.

    This is the important anti-backfill filter:
    Old RSS posts are not allowed to become "fresh" just because they were
    ingested today.
    """
    if source_allows_backfill(source):
        return True

    dt = entry_source_datetime(entry)
    if not dt:
        # Keep undated items for now. They are later visibly marked as unknown.
        return True

    cutoff = now_utc() - timedelta(hours=source_lookback_hours(source))
    return dt >= cutoff


# -------------------- social / signal hygiene --------------------
MARITIME_HARD_KEYWORDS = [
    "shadow fleet", "dark fleet", "sanctions", "sanctioned", "lng", "oil tanker", "tanker",
    "ais", "gnss", "gps jamming", "jamming", "spoofing", "subsea", "seabed",
    "cable", "pipeline", "offshore", "wind farm", "windfarm", "port", "harbour", "harbor",
    "navy", "naval", "warship", "frigate", "corvette", "patrol vessel",
    "drone", "uav", "uas", "usv", "maritime", "baltic", "north sea", "german bight",
    "ostsee", "nordsee", "deutsche bucht", "kritische infrastruktur", "kritis", "markritis",
    "sabotage", "russia", "russian", "hybrid", "phase zero", "grey zone", "gray zone",
    "piracy", "smuggling", "container", "vessel", "ship", "shipping",
]


def social_text_is_useful(text):
    """
    Prevents very small social fragments from creating issues.
    A short post can still pass if it has a link or a hard maritime keyword.
    """
    t = str(text or "").strip()
    low = t.lower()

    if len(t) >= 80:
        return True

    if "http://" in low or "https://" in low or "bsky.app/" in low or "x.com/" in low:
        return True

    return any(k in low for k in MARITIME_HARD_KEYWORDS)


def source_is_social(source, platform=None):
    raw = str(source.get("type") or source.get("platform") or platform or "").strip().lower()
    return raw in {"social", "x", "bsky", "social_x", "social_bsky"}


# -------------------- GitHub helpers --------------------
def gh_headers():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("Missing GITHUB_TOKEN in Actions environment.")

    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "magic-paws-feed-ingest",
    }


def gh_get(url, params=None):
    r = requests.get(url, headers=gh_headers(), params=params, timeout=REQUEST_TIMEOUT)

    if r.status_code == 403:
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


# -------------------- URL / dedupe helpers --------------------
def normalize_url(url):
    raw = str(url or "").strip()
    if not raw:
        return ""

    try:
        parts = urlsplit(raw)
        scheme = (parts.scheme or "https").lower()
        netloc = parts.netloc.lower()

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

    Loads existing issues in pages and dedupes locally by normalized URL and
    ingest marker. This avoids GitHub Search API rate limits.
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

            # Compatibility with old and new markers.
            for m in re.finditer(
                r"(?:VOODOO_FEED_INGEST|VOODOO_INGEST|MAGIC_PAWS_FEED_INGEST):\s*([A-Za-z0-9+/=_-]{8,64})",
                body,
                re.I,
            ):
                ingest_ids.add(m.group(1).lower())

        if len(data) < 100:
            break

    return links, ingest_ids


def link_seen(link, existing_links):
    return normalize_url(link) in existing_links


def make_ingest_id(source_id, link, title):
    base = f"{source_id}\n{normalize_url(link)}\n{str(title or '').strip()}"
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()[:16]


def safe_labels(wanted, existing):
    """
    Keep only labels that exist in GitHub to avoid 422 errors.
    """
    out = []
    for label in wanted:
        if label in existing:
            out.append(label)
    return sorted(set(out))


# -------------------- source / platform helpers --------------------
def is_rss_like_url(url):
    u = str(url or "").lower()
    return (
        u.endswith(".rss")
        or u.endswith(".xml")
        or u.endswith("/feed/")
        or "feedburner" in u
        or "rss" in u
        or "alt=rss" in u
    )


def feed_platform(feed):
    explicit = str(feed.get("platform") or feed.get("type") or "").strip().lower()

    if explicit in {"rss", "atom"}:
        return "rss"
    if explicit in {"social", "x", "social_x"}:
        return "x"
    if explicit in {"bsky", "social_bsky"}:
        return "bsky"
    if explicit == "html":
        return "html"

    url = feed.get("url", "")
    return "rss" if is_rss_like_url(url) else "html"


# -------------------- label inference --------------------
REG_RULES = [
    (re.compile(r"\b(north sea|nordsee|german bight|deutsche bucht)\b", re.I), "REG:NORTH_SEA"),
    (re.compile(r"\b(baltic sea|baltic|ostsee|kiel bight|fehmarn)\b", re.I), "REG:BALTIC"),
    (re.compile(r"\b(channel|english channel|la manche|ärmelkanal|dover strait)\b", re.I), "REG:CHANNEL"),
    (re.compile(r"\b(mediterranean|mittelmeer)\b", re.I), "REG:MEDITERRANEAN"),
    (re.compile(r"\b(wilhelmshaven|jade weser|jadeweser)\b", re.I), "REG:GER_BIGHT"),
    (re.compile(r"\b(hamburg|bremerhaven|bremen|emden|cuxhaven)\b", re.I), "REG:GER_BIGHT"),
]

DOMAIN_RULES = [
    (re.compile(r"\b(drone|uas|uav|usv)\b", re.I), "D:DRONE_UAS"),
    (re.compile(r"\b(gnss|gps|jamming|spoof)\b", re.I), "D:RF_SIGNAL"),
    (re.compile(r"\b(cyber|hack|ransom|ot|scada)\b", re.I), "D:CYBER_OT"),
    (re.compile(r"\b(piracy|attack|boarding|smuggling|stowaway|cocaine|drug)\b", re.I), "D:SECURITY_CRIME"),
    (re.compile(r"\b(collision|fire|grounding|engine failure|detention)\b", re.I), "D:INCIDENT"),
    (re.compile(r"\b(cable|pipeline|wind farm|windfarm|port|offshore|subsea|seabed)\b", re.I), "D:INFRA_CI"),
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

    for rx, reg in REG_RULES:
        if rx.search(text):
            labels.add(reg)
            break

    for rx, dom in DOMAIN_RULES:
        if rx.search(text):
            labels.add(dom)
            break

    for rx, pat in PAT_RULES:
        if rx.search(text):
            labels.add(pat)

    if (
        re.search(r"\b(sabotage|hybrid|proxy|unknown actor|covert|phase zero|grey zone|gray zone)\b", text, re.I)
        or (
            re.search(r"\b(cable|pipeline|subsea|seabed)\b", text, re.I)
            and re.search(r"\b(damage|cut|rupture|interference|incident)\b", text, re.I)
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


# -------------------- parsing feeds --------------------
def clean_summary(s):
    text = str(s or "")
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
    )
    return re.sub(r"\s+", " ", text).strip()


def parse_feed_items(feed):
    """
    Returns normalized items from RSS/Atom or a light HTML fallback.

    Important:
    - Old entries are filtered here using lookback_hours / allow_backfill.
    - Per-source max_items is applied after the old-entry filter.
    """
    url = feed.get("url")
    name = feed.get("name", url or feed.get("id", "(unknown)"))
    items = []

    if not url:
        print(f"WARN: source has no url and cannot be parsed by ingest_feeds.py: {name}")
        return []

    max_items = max_items_for_source(feed)

    # 1) RSS/Atom via feedparser if possible.
    if feedparser is not None:
        try:
            parsed = feedparser.parse(url)
            entries = getattr(parsed, "entries", []) or []

            if entries:
                kept_count = 0

                for entry in entries:
                    if not should_keep_entry_for_source(entry, feed):
                        continue

                    if kept_count >= max_items:
                        break

                    link = entry_get(entry, "link") or entry_get(entry, "id")
                    title = entry_get(entry, "title") or "(no title)"
                    summary = entry_get(entry, "summary") or entry_get(entry, "description") or ""
                    published_raw = entry_get(entry, "published") or entry_get(entry, "updated") or ""
                    source_dt = entry_source_datetime(entry)
                    published = iso_z(source_dt) if source_dt else str(published_raw or "").strip()

                    if link:
                        link = urljoin(url, str(link).strip())

                    items.append(
                        {
                            "title": str(title).strip(),
                            "link": link,
                            "summary": clean_summary(summary),
                            "published": published,
                        }
                    )
                    kept_count += 1

                return items
        except Exception as exc:
            print(f"WARN: feedparser failed for {name}: {exc}")

    # 2) Fallback: light scrape for simple HTML pages.
    try:
        res = requests.get(url, headers={"User-Agent": "magic-paws-feed-ingest"}, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        html = res.text
    except Exception as exc:
        print(f"WARN: fetch failed for feed {name}: {exc}")
        return []

    links = re.findall(r'href=["\']([^"\']+)["\']', html)
    seen = set()
    kept_count = 0

    for href in links:
        if kept_count >= max_items:
            break

        link = urljoin(url, href)
        if not link.startswith("http"):
            continue
        if "ukmto.org" in url and "ukmto.org" not in link:
            continue

        normalized = normalize_url(link)
        if normalized in seen:
            continue
        seen.add(normalized)

        title = link.rstrip("/").split("/")[-1].replace("-", " ").replace("_", " ").strip()
        if len(title) < 8:
            continue

        item = {
            "title": f"{name}: {title[:120]}",
            "link": link,
            "summary": "",
            "published": "",
        }

        # HTML fallback usually has no date; should_keep_entry_for_source keeps undated items.
        if not should_keep_entry_for_source(item, feed):
            continue

        items.append(item)
        kept_count += 1

    return items


# -------------------- config loading --------------------
def load_feed_config():
    """
    Legacy Python ingest reads config/feeds.json.

    If your active workflow uses config/sources.yml, it is probably running
    ingest.mjs instead. In that case, this Python file will not affect the
    current source pipeline.
    """
    with open("config/feeds.json", "r", encoding="utf-8") as f:
        return json.load(f)


def source_base_labels(feed):
    labels = (
        feed.get("base_labels")
        or feed.get("labels")
        or ["D:NEWS_INTEL", "CONF:LOW", "SEV:1", "SRC:OSINT"]
    )

    out = list(labels)

    region_hint = feed.get("region_hint")
    if region_hint:
        out.append(region_hint)

    return out


# -------------------- main --------------------
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
    print(f"Feeds configured: {len(feeds)}")
    print(f"Max new issues this run: {max_new}")

    created = 0
    skipped_existing = 0
    skipped_no_link = 0
    skipped_low_signal = 0
    failed_feeds = 0

    for feed in feeds:
        if created >= max_new:
            break

        feed_name = feed.get("name", feed.get("url", "(unknown)"))
        source_id = feed.get("id", feed_name)
        platform = feed_platform(feed)
        base_labels = source_base_labels(feed)

        try:
            items = parse_feed_items(feed)
        except Exception as exc:
            failed_feeds += 1
            print(f"WARN: feed skipped after parser error: {feed_name}: {exc}")
            continue

        print(
            f"Feed: {feed_name} platform={platform} items={len(items)} "
            f"lookback_hours={source_lookback_hours(feed)} "
            f"allow_backfill={source_allows_backfill(feed)} "
            f"max_items={max_items_for_source(feed)}"
        )

        for item in items:
            if created >= max_new:
                break

            link = item.get("link")
            if not link:
                skipped_no_link += 1
                continue

            title = item.get("title") or "(no title)"
            summary = (item.get("summary") or "").strip()
            published_raw = item.get("published") or ""

            # Extra guard: if parse_feed_items returned a manually built item,
            # apply the lookback filter one more time here.
            if not should_keep_entry_for_source(item, feed):
                continue

            if source_is_social(feed, platform):
                social_blob = f"{title}\n{summary}\n{link}"
                if not social_text_is_useful(social_blob):
                    skipped_low_signal += 1
                    continue

            ingest_id = make_ingest_id(source_id, link, title)

            if link_seen(link, existing_links) or ingest_id in existing_ingest_ids:
                skipped_existing += 1
                continue

            blob = f"{title}\n{summary}\n{link}"
            inferred = infer_labels(blob, base_labels)
            labels = safe_labels(inferred, existing_labels)

            source_dt = parse_source_datetime(published_raw)
            source_time_utc = iso_z(source_dt) if source_dt else "(unknown)"

            lat, lon = extract_latlon(blob)
            geo_line = ""
            if lat is not None and lon is not None:
                geo_line = f"\n\nGeo: {lat:.4f}, {lon:.4f}\n"

            body = (
                f"### Quelle\n{feed_name}\n\n"
                f"### Source\n{feed_name}\n\n"
                f"### Plattform\n{platform}\n\n"
                f"### Link\n{link}\n\n"
                f"### Zeit (UTC)\n{source_time_utc}\n\n"
                f"### Published\n{published_raw if published_raw else '(unknown)'}\n\n"
                f"### Auto-Labels\n" + ", ".join(labels) + "\n"
                f"{geo_line}\n"
                f"### Summary\n{summary[:4000] if summary else '(no summary)'}\n\n"
                f"---\n"
                f"_Ingested at {now_iso()} UTC_\n\n"
                f"<!-- VOODOO_FEED_INGEST: {ingest_id} -->"
            )

            title_prefix = "[RSS]" if platform == "rss" else "[AUTO]"
            gh_post(
                f"{GH_API}/repos/{owner}/{repo}/issues",
                {
                    "title": f"{title_prefix} {title[:180]}",
                    "body": body,
                    "labels": labels,
                },
            )

            created += 1
            existing_links.add(normalize_url(link))
            existing_ingest_ids.add(ingest_id)
            time.sleep(0.7)  # Be gentle to API.

    print(f"Created issues: {created}")
    print(f"Skipped existing: {skipped_existing}")
    print(f"Skipped no link: {skipped_no_link}")
    print(f"Skipped low-signal social: {skipped_low_signal}")
    print(f"Failed feeds: {failed_feeds}")


if __name__ == "__main__":
    main()
