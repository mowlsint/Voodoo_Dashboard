import os, json, re, hashlib, time
from datetime import datetime, timezone
import requests

# Optional dependency: feedparser (RSS/Atom). If not installed, we fallback to simple HTML scrape.
try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None

GH_API = "https://api.github.com"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def gh_headers():
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        raise RuntimeError("Missing GITHUB_TOKEN in Actions environment.")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "voodoo-feed-ingest"
    }

def gh_get(url, params=None):
    r = requests.get(url, headers=gh_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def gh_post(url, payload):
    r = requests.post(url, headers=gh_headers(), json=payload, timeout=30)
    # Helpful diagnostics if labels are wrong etc.
    if r.status_code >= 400:
        raise RuntimeError(f"GitHub POST failed {r.status_code}: {r.text[:500]}")
    return r.json()

def list_all_labels(owner, repo):
    labels = set()
    page = 1
    while True:
        data = gh_get(f"{GH_API}/repos/{owner}/{repo}/labels", params={"per_page": 100, "page": page})
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

def search_issue_by_link(owner, repo, link):
    # Search in existing issues for the URL
    q = f'repo:{owner}/{repo} in:body "{link}" type:issue'
    data = gh_get(f"{GH_API}/search/issues", params={"q": q, "per_page": 1})
    items = data.get("items", [])
    return len(items) > 0

def safe_labels(wanted, existing):
    # Keep only labels that exist; avoid 422 errors
    out = []
    for l in wanted:
        if l in existing:
            out.append(l)
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

    # Region
    for rx, reg in REG_RULES:
        if rx.search(text):
            labels.add(reg)
            break

    # Domain (override NEWS_INTEL if strong signal)
    for rx, dom in DOMAIN_RULES:
        if rx.search(text):
            labels.add(dom)
            break

    # Patterns
    for rx, pat in PAT_RULES:
        if rx.search(text):
            labels.add(pat)

    # Phase Zero suspicion: conservative trigger words
    if re.search(r"\b(sabotage|hybrid|proxy|unknown actor|covert)\b", text, re.I) or \
       re.search(r"\b(cable|pipeline)\b", text, re.I) and re.search(r"\b(damage|cut|rupture)\b", text, re.I) or \
       re.search(r"\b(jamming|spoof)\b", text, re.I):
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

    # 1) RSS/Atom via feedparser if possible
    if feedparser is not None and (url.endswith(".rss") or url.endswith("/feed/") or "feedburner" in url or url.endswith(".xml")):
        d = feedparser.parse(url)
        items = []
        for e in d.entries[:40]:
            link = getattr(e, "link", None) or getattr(e, "id", None)
            title = getattr(e, "title", None) or "(no title)"
            summary = getattr(e, "summary", None) or getattr(e, "description", "") or ""
            published = getattr(e, "published", None) or getattr(e, "updated", None) or ""
            items.append({"title": title.strip(), "link": link, "summary": summary, "published": published})
        return items

    # 2) Fallback: light scrape (for pages like UKMTO recent-incidents)
    html = requests.get(url, headers={"User-Agent":"voodoo-feed-ingest"}, timeout=30).text
    # very simple: grab first N hrefs that look like detail pages
    links = re.findall(r'href="([^"]+)"', html)
    items = []
    seen = set()
    for l in links:
        if l.startswith("/"):
            l = url.rstrip("/") + l
        if not l.startswith("http"):
            continue
        if "ukmto.org" in url and "ukmto.org" not in l:
            continue
        if l in seen:
            continue
        seen.add(l)
        # crude title from slug
        t = l.split("/")[-1].replace("-", " ").strip()
        if len(t) < 8:
            continue
        items.append({"title": f"{name}: {t[:120]}", "link": l, "summary": "", "published": ""})
        if len(items) >= 25:
            break
    return items

def main():
    repo_full = os.environ.get("GITHUB_REPOSITORY")  # "owner/repo"
    if not repo_full or "/" not in repo_full:
        raise RuntimeError("Missing GITHUB_REPOSITORY.")
    owner, repo = repo_full.split("/", 1)

    with open("config/feeds.json", "r", encoding="utf-8") as f:
        cfg = json.load(f)

    max_new = int(cfg.get("max_new_per_run", 10))
    feeds = cfg.get("feeds", [])

    existing_labels = list_all_labels(owner, repo)

    created = 0
    for feed in feeds:
        base_labels = feed.get("base_labels", ["D:NEWS_INTEL","CONF:LOW","SEV:1","SRC:OSINT"])
        items = parse_feed_items(feed)

        for it in items:
            if created >= max_new:
                break

            link = it.get("link")
            if not link:
                continue

            # dedupe
            if search_issue_by_link(owner, repo, link):
                continue

            title = it.get("title") or "(no title)"
            summary = (it.get("summary") or "").strip()
            published = it.get("published") or ""

            blob = f"{title}\n{summary}\n{link}"
            inferred = infer_labels(blob, base_labels)

            # keep only known labels
            labels = safe_labels(inferred, existing_labels)

            lat, lon = extract_latlon(blob)
            geo_line = ""
            if lat is not None and lon is not None:
                geo_line = f"\n\nGeo: {lat:.4f}, {lon:.4f}\n"

            body = (
                f"### Source\n{feed.get('name','(unknown)')}\n\n"
                f"### Link\n{link}\n\n"
                f"### Published\n{published if published else '(unknown)'}\n\n"
                f"### Auto-Labels\n" + ", ".join(labels) + "\n"
                f"{geo_line}\n"
                f"### Summary\n{summary[:4000] if summary else '(no summary)'}\n\n"
                f"---\n"
                f"_Ingested at {now_iso()} UTC_"
            )

            gh_post(
                f"{GH_API}/repos/{owner}/{repo}/issues",
                {"title": f"[AUTO] {title[:180]}", "body": body, "labels": labels}
            )

            created += 1
            time.sleep(0.7)  # be gentle to API

    print(f"Created issues: {created}")

if __name__ == "__main__":
    main()
