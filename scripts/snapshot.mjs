import fs from "fs";
import path from "path";
import YAML from "yaml";

const SNAPSHOT_PATH = "data/snapshots/voodoo_sensor_snapshots.ndjson";
const LATEST_PATH = "data/snapshots/voodoo_sensor_latest.json";
const KEYWORD_CFG_PATH = "config/keyword_barometer.yml";

const DEFAULT_OWNER = "mowlsint";
const DEFAULT_REPO = "Voodoo_Dashboard";
const SNAPSHOT_KEEP_DAYS = Number(process.env.SNAPSHOT_KEEP_DAYS || 100);
const ISSUE_PAGES = Number(process.env.SNAPSHOT_ISSUE_PAGES || 10);
const BUCKET_HOURS = Number(process.env.SNAPSHOT_BUCKET_HOURS || 3);

function norm(s) {
  return String(s ?? "").trim();
}

function nowIso() {
  return new Date().toISOString();
}

function parseDate(v) {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d;
}

function hoursAgo(date, now = new Date()) {
  return (now.getTime() - date.getTime()) / (1000 * 60 * 60);
}

function clamp(n, min = 0, max = 100) {
  return Math.max(min, Math.min(max, n));
}

function uniq(arr) {
  return Array.from(new Set((arr || []).filter(Boolean)));
}

function firstLabel(labels, prefix) {
  return (labels || []).find((l) => typeof l === "string" && l.startsWith(prefix)) || null;
}

function hasAny(labels, wanted) {
  return wanted.some((w) => labels.includes(w));
}

function anyLabelStarts(labels, prefixes) {
  return labels.some((l) => prefixes.some((p) => l.startsWith(p)));
}

function parseRepo() {
  const full = process.env.GITHUB_REPOSITORY || "";
  if (full.includes("/")) {
    const [owner, repo] = full.split("/", 2);
    return { owner, repo };
  }
  return { owner: DEFAULT_OWNER, repo: DEFAULT_REPO };
}

async function ghRequest(pathname) {
  const token = process.env.GH_TOKEN || process.env.GITHUB_TOKEN;
  if (!token) {
    throw new Error("Missing GH_TOKEN / GITHUB_TOKEN.");
  }

  const res = await fetch(`https://api.github.com${pathname}`, {
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "User-Agent": "voodoo-snapshot",
    },
  });

  const txt = await res.text().catch(() => "");
  if (!res.ok) {
    throw new Error(`GitHub API ${res.status} GET ${pathname}: ${txt.slice(0, 500)}`);
  }

  return txt ? JSON.parse(txt) : null;
}

async function listOpenIssues(owner, repo, pages = ISSUE_PAGES) {
  const out = [];

  for (let p = 1; p <= pages; p++) {
    const items = await ghRequest(`/repos/${owner}/${repo}/issues?state=open&per_page=100&page=${p}`);
    for (const it of items || []) {
      if (!it.pull_request) out.push(it);
    }
    if (!items || items.length < 100) break;
  }

  return out;
}

function issueToEvent(issue) {
  const labels = (issue.labels || [])
    .map((l) => (typeof l === "string" ? l : l?.name))
    .filter(Boolean);

  const body = issue.body || "";
  const bodyTimeMatch = body.match(/### Zeit \(UTC\)\s*\n([^\n]+)/i);
  const sourceMatch = body.match(/### Quelle\s*\n([^\n]+)/i);
  const platformMatch = body.match(/### Plattform\s*\n([^\n]+)/i);
  const linkMatch = body.match(/### Link\s*\n([^\n]+)/i);

  const bodyTime = bodyTimeMatch ? parseDate(bodyTimeMatch[1]) : null;
  const issueTime = parseDate(issue.created_at);
  const ts = bodyTime || issueTime || new Date();

  const sourceLine = sourceMatch ? norm(sourceMatch[1]) : "";
  const sourceIdMatch = sourceLine.match(/\(([^()]+)\)\s*$/);
  const sourceId = sourceIdMatch ? sourceIdMatch[1] : null;

  const latLonMatch =
    body.match(/Geo\s*[:=]\s*(-?\d{1,2}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)/i) ||
    body.match(/(-?\d{1,2}\.\d+)\s*,\s*(-?\d{1,3}\.\d+)/);

  const geo =
    latLonMatch
      ? { lat: Number(latLonMatch[1]), lon: Number(latLonMatch[2]) }
      : null;

  return {
    id: String(issue.id),
    number: issue.number,
    title: issue.title || "",
    body,
    url: issue.html_url || "",
    link: linkMatch ? norm(linkMatch[1]) : "",
    source_line: sourceLine,
    source_id: sourceId,
    platform: platformMatch ? norm(platformMatch[1]) : "",
    ts: ts.toISOString(),
    labels,
    category: firstLabel(labels, "D:") || "D:UNKNOWN",
    region: firstLabel(labels, "REG:") || null,
    severity: firstLabel(labels, "SEV:") || "SEV:1",
    confidence: firstLabel(labels, "CONF:") || "CONF:LOW",
    phase0_suspect: labels.includes("P0:SUSPECT"),
    phase0_level: labels.find((l) => l === "P0:LOW" || l === "P0:MED" || l === "P0:HIGH") || null,
    geo,
  };
}

function bucketStart(date = new Date(), bucketHours = BUCKET_HOURS) {
  const d = new Date(date);
  d.setUTCMinutes(0, 0, 0);
  const h = d.getUTCHours();
  d.setUTCHours(Math.floor(h / bucketHours) * bucketHours);
  return d;
}

function bucketEnd(start, bucketHours = BUCKET_HOURS) {
  return new Date(start.getTime() + bucketHours * 60 * 60 * 1000);
}

function loadKeywordConfig() {
  if (!fs.existsSync(KEYWORD_CFG_PATH)) {
    return {
      loaded: false,
      tiers: {},
      genericSingleTerms: [],
      contextAnchors: [],
    };
  }

  const raw = fs.readFileSync(KEYWORD_CFG_PATH, "utf8");
  const cfg = YAML.parse(raw);

  const tiers = {};
  for (const tierName of ["A", "B", "C", "D"]) {
    const t = cfg?.tiers?.[tierName];
    if (!t) continue;
    tiers[tierName] = {
      weight: Number(t.weight ?? cfg?.scoring?.tier_weights?.[tierName] ?? 0),
      requireContext: Boolean(t.require_context),
      terms: Array.isArray(t.terms) ? t.terms.map(String) : [],
    };
  }

  const genericSingleTerms = cfg?.scoring?.noise_dampening?.generic_single_terms || [];
  const contextAnchors = []
    .concat(cfg?.context_anchors?.maritime || [])
    .concat(cfg?.context_anchors?.regions || [])
    .concat(cfg?.context_anchors?.infrastructure || [])
    .concat(cfg?.context_anchors?.vessel || [])
    .concat(cfg?.context_anchors?.authority_activity || [])
    .map(String);

  return {
    loaded: true,
    version: cfg?.version ?? null,
    tiers,
    genericSingleTerms,
    contextAnchors,
    genericFactor: Number(cfg?.scoring?.noise_dampening?.single_generic_term_factor ?? 0.25),
  };
}

function includesTerm(textLower, term) {
  const t = String(term || "").toLowerCase().trim();
  if (!t) return false;

  // For short uppercase-ish abbreviations such as AIS, UAS, USV, EW, use boundary logic.
  if (/^[a-z0-9-]{2,6}$/.test(t)) {
    const escaped = t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    return new RegExp(`(^|[^a-z0-9])${escaped}([^a-z0-9]|$)`, "i").test(textLower);
  }

  return textLower.includes(t);
}

function hasContext(textLower, anchors) {
  return anchors.some((a) => includesTerm(textLower, a));
}

function scoreKeywordBarometer(events, kwCfg) {
  const sourceScoped = events.filter((e) => {
    const labels = e.labels || [];
    return labels.includes("SRC:SOCIAL") || labels.includes("SRC:MEDIA") ||
      e.platform === "rss" || e.platform === "x" || e.platform === "bsky";
  });

  if (!kwCfg.loaded) {
    return {
      score: 0,
      weighted_hits: 0,
      raw_hits: 0,
      deduped_events: sourceScoped.length,
      top_terms: [],
      config_loaded: false,
    };
  }

  const termStats = new Map();
  let weighted = 0;
  let rawHits = 0;

  for (const e of sourceScoped) {
    const text = `${e.title}\n${e.body}`.toLowerCase();
    const context = hasContext(text, kwCfg.contextAnchors);

    for (const [tierName, tier] of Object.entries(kwCfg.tiers)) {
      for (const term of tier.terms) {
        if (!includesTerm(text, term)) continue;

        const generic = kwCfg.genericSingleTerms
          .map((x) => String(x).toLowerCase())
          .includes(String(term).toLowerCase());

        if (tier.requireContext && !context) continue;

        let w = tier.weight || 0;
        if (generic && !context) w *= kwCfg.genericFactor || 0.25;

        rawHits += 1;
        weighted += w;

        const key = `${tierName}|${term}`;
        const cur = termStats.get(key) || { term, tier: tierName, count: 0, weighted: 0 };
        cur.count += 1;
        cur.weighted += w;
        termStats.set(key, cur);
      }
    }
  }

  const topTerms = Array.from(termStats.values())
    .sort((a, b) => b.weighted - a.weighted || b.count - a.count)
    .slice(0, 12)
    .map((x) => ({
      term: x.term,
      tier: x.tier,
      count: x.count,
      weighted: Number(x.weighted.toFixed(2)),
    }));

  // Cold-start score: not a real baseline yet, just capped weighted density.
  const score = clamp(Math.round((weighted / 20) * 100));

  return {
    score,
    weighted_hits: Number(weighted.toFixed(2)),
    raw_hits: rawHits,
    deduped_events: sourceScoped.length,
    top_terms: topTerms,
    config_loaded: true,
    config_version: kwCfg.version,
  };
}

function scoreHybridSeismograph(events) {
  let points = 0;

  for (const e of events) {
    const labels = e.labels || [];
    const sev = e.severity || "SEV:1";
    const conf = e.confidence || "CONF:LOW";

    const sevW = sev === "SEV:4" ? 9 : sev === "SEV:3" ? 6 : sev === "SEV:2" ? 3 : 1;
    const confW = conf === "CONF:HIGH" ? 1.2 : conf === "CONF:MED" ? 1.0 : 0.8;

    const ci = hasAny(labels, ["OBJ:CABLE", "OBJ:PIPELINE", "OBJ:WINDFARM", "OBJ:PORT", "OBJ:VTS_WSV"]) ||
      labels.includes("D:INFRA_CI");

    const patterns = hasAny(labels, [
      "PAT:LOITERING",
      "PAT:STS_SUSPECT",
      "PAT:AIS_GAP",
      "PAT:DARK_ACTIVITY",
      "PAT:SURVEYING",
      "PAT:ROUTE_DEVIATION",
      "PAT:GNSS_JAM",
      "PAT:GNSS_SPOOF",
      "PAT:RF_BURST",
    ]);

    const vesselHot = hasAny(labels, ["V:SHADOW_FLEET", "V:RUS_RESEARCH", "V:RUS_WARSHIP", "V:SANCTIONS_EVASION"]);
    const rfCyber = labels.includes("D:RF_SIGNAL") || labels.includes("D:CYBER_OT") || anyLabelStarts(labels, ["RF:"]);

    let evPts = sevW * confW;
    if (e.phase0_suspect) evPts *= 1.6;
    if (ci) evPts *= 1.25;
    if (patterns) evPts *= 1.2;
    if (vesselHot) evPts *= 1.15;
    if (rfCyber) evPts *= 1.1;

    points += evPts;
  }

  return {
    score: clamp(Math.round((points / 120) * 100)),
    weighted_points: Number(points.toFixed(2)),
  };
}

function eventInRegion(e, regionId) {
  const labels = e.labels || [];
  const hay = `${e.title}\n${e.body}`.toLowerCase();

  if (regionId === "north_sea") {
    return labels.includes("REG:NORTH_SEA") ||
      labels.includes("REG:GER_BIGHT") ||
      hay.includes("north sea") ||
      hay.includes("nordsee") ||
      hay.includes("german bight") ||
      hay.includes("deutsche bucht");
  }

  if (regionId === "baltic_sea") {
    return labels.includes("REG:BALTIC_SEA") ||
      labels.includes("REG:BALTIC") ||
      hay.includes("baltic") ||
      hay.includes("ostsee") ||
      hay.includes("kattegat") ||
      hay.includes("skagerrak") ||
      hay.includes("bornholm") ||
      hay.includes("gotland");
  }

  return false;
}

function scoreGovernmentWeirdness(events, regionId) {
  const regional = events.filter((e) => eventInRegion(e, regionId));
  let points = 0;

  for (const e of regional) {
    const labels = e.labels || [];
    const sev = e.severity || "SEV:1";
    const conf = e.confidence || "CONF:LOW";

    const official = labels.includes("SRC:OFFICIAL");
    const social = labels.includes("SRC:SOCIAL");
    const govVessel = hasAny(labels, [
      "V:AUTH_COAST_GUARD",
      "V:AUTH_CUSTOMS",
      "V:AUTH_POLICE",
      "V:AUTH_NAVY",
      "V:SAR_UNIT",
      "V:RUS_WARSHIP",
      "V:RUS_AUXILIARY",
      "V:RUS_RESEARCH",
      "V:RUS_GOV",
    ]);

    const authorityDomain = hasAny(labels, ["D:AIR_ACTIVITY", "D:SAR", "D:RF_SIGNAL"]) ||
      anyLabelStarts(labels, ["RF:"]) ||
      labels.includes("RF:NAVWARN") ||
      labels.includes("RF:NAVTEX");

    const ci = hasAny(labels, ["OBJ:PORT", "OBJ:VTS_WSV", "OBJ:CABLE", "OBJ:PIPELINE", "OBJ:WINDFARM"]) ||
      labels.includes("D:INFRA_CI");

    const pattern = hasAny(labels, [
      "PAT:RENDEZVOUS",
      "PAT:LOITERING",
      "PAT:SURVEYING",
      "PAT:ROUTE_DEVIATION",
      "PAT:GNSS_JAM",
      "PAT:GNSS_SPOOF",
      "PAT:RF_BURST",
    ]);

    const sevW = sev === "SEV:4" ? 6 : sev === "SEV:3" ? 4 : sev === "SEV:2" ? 2 : 1;
    const confW = conf === "CONF:HIGH" ? 1.0 : conf === "CONF:MED" ? 1.1 : 1.2;

    let evPts = sevW * confW;

    if (official) evPts *= 1.2;
    if (social) evPts *= 0.9;
    if (govVessel) evPts *= 1.4;
    if (authorityDomain) evPts *= 1.3;
    if (ci) evPts *= 1.15;
    if (pattern) evPts *= 1.2;
    if (e.phase0_suspect) evPts *= 1.1;

    points += evPts;
  }

  return {
    score: clamp(Math.round((points / 45) * 100)),
    weighted_points: Number(points.toFixed(2)),
    regional_events: regional.length,
  };
}

function topSources(events) {
  const m = new Map();

  for (const e of events) {
    const key = e.source_id || e.source_line || "unknown";
    m.set(key, (m.get(key) || 0) + 1);
  }

  return Array.from(m.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([source_id, count]) => ({ source_id, count }));
}

function domainCounts(events) {
  const m = new Map();

  for (const e of events) {
    const key = e.category || "D:UNKNOWN";
    m.set(key, (m.get(key) || 0) + 1);
  }

  return Object.fromEntries(Array.from(m.entries()).sort((a, b) => a[0].localeCompare(b[0])));
}

function regionCounts(events) {
  const m = new Map();

  for (const e of events) {
    const key = e.region || "REG:UNKNOWN";
    m.set(key, (m.get(key) || 0) + 1);
  }

  return Object.fromEntries(Array.from(m.entries()).sort((a, b) => a[0].localeCompare(b[0])));
}

function mapToSortedObject(m) {
  return Object.fromEntries(
    Array.from(m.entries()).sort((a, b) => String(a[0]).localeCompare(String(b[0])))
  );
}

function normalizeUrlForDedupe(url) {
  const raw = norm(url);
  if (!raw) return "";

  try {
    const u = new URL(raw);
    u.hash = "";

    for (const p of [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_term",
      "utm_content",
      "fbclid",
      "gclid",
      "mc_cid",
      "mc_eid",
    ]) {
      u.searchParams.delete(p);
    }

    return u.toString().replace(/\/+$/, "").toLowerCase();
  } catch {
    return raw.replace(/\/+$/, "").toLowerCase();
  }
}

function simpleHash(s) {
  let h = 2166136261;
  const str = String(s || "");
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return (h >>> 0).toString(16);
}

function extractTextSection(body) {
  const b = String(body || "");
  const m = b.match(/###\s*Text\s*\n([\s\S]*?)(?:\n\n###|\n###\s*Extra|<!--|$)/i);
  return (m ? m[1] : b)
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 2500);
}

function sourceKey(e) {
  return e.source_id || e.source_line || "unknown";
}

function sourceKind(e) {
  const labels = e.labels || [];
  const p = String(e.platform || "").toLowerCase();
  const title = String(e.title || "");

  if (labels.includes("SRC:SOCIAL") || p === "x" || p === "bsky" || p === "mastodon") {
    return "social";
  }

  if (p === "rss" || /^\[RSS\]/i.test(title)) {
    return "rss";
  }

  if (labels.includes("SRC:OFFICIAL")) {
    return "official";
  }

  if (labels.includes("SRC:MEDIA")) {
    return "media";
  }

  if (labels.includes("SRC:OSINT")) {
    return "osint";
  }

  return "unknown";
}

function isSocialOrRss(e) {
  const k = sourceKind(e);
  return k === "social" || k === "rss";
}

function eventFingerprint(e) {
  const link = normalizeUrlForDedupe(e.link || "");
  if (link) return `link:${link}`;

  const ingest = String(e.body || "").match(/VOODOO_INGEST:\s*([a-f0-9]+)\s+SOURCE=([A-Za-z0-9_-]+)/i);
  if (ingest) return `ingest:${ingest[2]}:${ingest[1]}`;

  const text = `${e.title}\n${extractTextSection(e.body)}`
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();

  return `text:${simpleHash(text)}`;
}

function dedupeEventList(events) {
  const seen = new Set();
  const kept = [];
  const duplicateBySource = new Map();

  for (const e of events || []) {
    const fp = eventFingerprint(e);
    if (seen.has(fp)) {
      const src = sourceKey(e);
      duplicateBySource.set(src, (duplicateBySource.get(src) || 0) + 1);
      continue;
    }

    seen.add(fp);
    kept.push(e);
  }

  return {
    events: kept,
    duplicate_count: Math.max(0, (events || []).length - kept.length),
    duplicates_by_source: mapToSortedObject(duplicateBySource),
  };
}

function sourceKindCounts(events) {
  const m = new Map();

  for (const e of events || []) {
    const key = sourceKind(e);
    m.set(key, (m.get(key) || 0) + 1);
  }

  return mapToSortedObject(m);
}

function sourceDomainCounts(events) {
  const m = new Map();

  for (const e of events || []) {
    const key = `${sourceKey(e)}|${e.category || "D:UNKNOWN"}`;
    m.set(key, (m.get(key) || 0) + 1);
  }

  return mapToSortedObject(m);
}

function sourceRegionCounts(events) {
  const m = new Map();

  for (const e of events || []) {
    const key = `${sourceKey(e)}|${e.region || "REG:UNKNOWN"}`;
    m.set(key, (m.get(key) || 0) + 1);
  }

  return mapToSortedObject(m);
}

function readExistingSnapshots(filePath) {
  if (!fs.existsSync(filePath)) return [];

  const raw = fs.readFileSync(filePath, "utf8");
  return raw
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function writeSnapshots(filePath, snapshots) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const out = snapshots.map((x) => JSON.stringify(x)).join("\n") + "\n";
  fs.writeFileSync(filePath, out, "utf8");
}

function writeLatest(filePath, snapshot) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(snapshot, null, 2) + "\n", "utf8");
}

async function main() {
  const now = new Date();
  const start = bucketStart(now, BUCKET_HOURS);
  const end = bucketEnd(start, BUCKET_HOURS);

  const { owner, repo } = parseRepo();
  console.log(`VOODOO snapshot start: ${nowIso()}`);
  console.log(`Repo: ${owner}/${repo}`);
  console.log(`Bucket: ${start.toISOString()} -> ${end.toISOString()}`);

  const issues = await listOpenIssues(owner, repo, ISSUE_PAGES);
  const events = issues.map(issueToEvent);

  const recent72 = events.filter((e) => {
    const d = parseDate(e.ts);
    return d && hoursAgo(d, now) <= 72;
  });

  const currentBucket = events.filter((e) => {
    const d = parseDate(e.ts);
    return d && d >= start && d < end;
  });

  const dedupeAll = dedupeEventList(events);
  const dedupe72 = dedupeEventList(recent72);
  const dedupeBucket = dedupeEventList(currentBucket);

  const socialRssBucket = currentBucket.filter(isSocialOrRss);
  const socialRss72 = recent72.filter(isSocialOrRss);

  const keywordCfg = loadKeywordConfig();

  const hybrid = scoreHybridSeismograph(recent72);
  const keyword = scoreKeywordBarometer(currentBucket, keywordCfg);
  const govNorth = scoreGovernmentWeirdness(recent72, "north_sea");
  const govBaltic = scoreGovernmentWeirdness(recent72, "baltic_sea");

  const snapshot = {
    schema_version: 2,
    ts: now.toISOString(),
    bucket_start_utc: start.toISOString(),
    bucket_end_utc: end.toISOString(),
    bucket_hours: BUCKET_HOURS,
    source: {
      type: "github_issues",
      owner,
      repo,
      state: "open",
    },
    counts: {
      open_events_total: events.length,
      open_events_deduped_total: dedupeAll.events.length,
      recent_72h_events: recent72.length,
      recent_72h_deduped_events: dedupe72.events.length,
      current_bucket_events: currentBucket.length,
      current_bucket_deduped_events: dedupeBucket.events.length,
      duplicate_events_total: dedupeAll.duplicate_count,
      duplicate_events_72h: dedupe72.duplicate_count,
      duplicate_events_bucket: dedupeBucket.duplicate_count,
      social_rss_events_72h: socialRss72.length,
      social_rss_events_bucket: socialRssBucket.length,
      p0_suspect_72h: recent72.filter((e) => e.phase0_suspect).length,

      // Achtung: snapshot.mjs erkennt derzeit nur explizite Koordinaten im Issue-Text.
      // Gazetteer-/Text-Geocoding läuft im Worker und wird in Punkt 5 harmonisiert.
      geo_total: events.filter((e) => !!e.geo).length,
      geo_72h: recent72.filter((e) => !!e.geo).length,
      geo_count_method: "explicit_coordinates_only",
    },
    sensors: {
      hybrid_seismograph_pct: hybrid.score,
      keyword_barometer_pct: keyword.score,
      government_weirdness: {
        north_sea_pct: govNorth.score,
        baltic_sea_pct: govBaltic.score,
      },
    },
    diagnostics: {
      hybrid_weighted_points_72h: hybrid.weighted_points,
      keyword_weighted_hits_bucket: keyword.weighted_hits,
      keyword_raw_hits_bucket: keyword.raw_hits,
      keyword_deduped_events_bucket: keyword.deduped_events,
      government_weirdness_north_sea_points: govNorth.weighted_points,
      government_weirdness_baltic_sea_points: govBaltic.weighted_points,
      government_weirdness_north_sea_events: govNorth.regional_events,
      government_weirdness_baltic_sea_events: govBaltic.regional_events,
      keyword_config_loaded: keyword.config_loaded,
      keyword_config_version: keyword.config_version ?? null,
      cold_start_note: "Scores are cold-start density values until a 90-day baseline exists.",
    },
    top_terms: keyword.top_terms,
    top_sources_72h: topSources(recent72),
    domain_counts_72h: domainCounts(recent72),
    region_counts_72h: regionCounts(recent72),

    baseline_observations: {
      note: "Raw baseline observations for later 90-day normalization. Scores are not yet baseline-normalized.",
      source_kind_counts_bucket: sourceKindCounts(currentBucket),
      source_kind_counts_72h: sourceKindCounts(recent72),
      source_domain_counts_bucket: sourceDomainCounts(currentBucket),
      source_domain_counts_72h: sourceDomainCounts(recent72),
      source_region_counts_72h: sourceRegionCounts(recent72),
      duplicate_sources_bucket: dedupeBucket.duplicates_by_source,
      duplicate_sources_72h: dedupe72.duplicates_by_source,
    },
  };

  const cutoff = new Date(now.getTime() - SNAPSHOT_KEEP_DAYS * 24 * 60 * 60 * 1000);
  const existing = readExistingSnapshots(SNAPSHOT_PATH)
    .filter((x) => {
      const d = parseDate(x.ts || x.bucket_start_utc);
      return d && d >= cutoff;
    })
    .filter((x) => x.bucket_start_utc !== snapshot.bucket_start_utc);

  existing.push(snapshot);
  existing.sort((a, b) => String(a.bucket_start_utc).localeCompare(String(b.bucket_start_utc)));

  writeSnapshots(SNAPSHOT_PATH, existing);
  writeLatest(LATEST_PATH, snapshot);

  console.log(`Snapshots stored: ${existing.length}`);
  console.log(`Latest written: ${LATEST_PATH}`);
  console.log(`NDJSON written: ${SNAPSHOT_PATH}`);
  console.log(`Hybrid: ${snapshot.sensors.hybrid_seismograph_pct}%`);
  console.log(`Keyword: ${snapshot.sensors.keyword_barometer_pct}%`);
  console.log(`Government-Weirdness North Sea: ${snapshot.sensors.government_weirdness.north_sea_pct}%`);
  console.log(`Government-Weirdness Baltic Sea: ${snapshot.sensors.government_weirdness.baltic_sea_pct}%`);
}

main().catch((err) => {
  console.error("FATAL snapshot error:");
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
