#!/usr/bin/env node
/**
 * MAGIC PAWS – Log/Archive/Summary Builder
 *
 * Runs in GitHub Actions every 6 hours.
 * - Fetches the Worker bundle (/api/bundle?fresh=1 by default)
 * - Merges new events with existing repository archive files
 * - Writes:
 *   data/logs/magicpaws_events_20d.json
 *   data/archive/magicpaws_YYYY-MM.jsonl
 *   data/archive/magicpaws_archive_manifest.json
 *   data/snapshots/magicpaws_daily_summary.json
 *   data/snapshots/magicpaws_sensor_latest.json
 *   data/snapshots/magicpaws_sensor_snapshots.ndjson
 *
 * No npm dependencies required. Node 20+ recommended.
 */

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";

const ROOT = process.cwd();
const WORKER_BASE = process.env.MAGIC_PAWS_WORKER_BASE || "https://voodoo-hybrid-api.mowlsint.workers.dev";
const BUNDLE_URL = process.env.MAGIC_PAWS_BUNDLE_URL || `${WORKER_BASE}/api/bundle?fresh=1`;
const ACTIVE_WINDOW_DAYS = Number(process.env.MAGIC_PAWS_ACTIVE_WINDOW_DAYS || 20);
const DAILY_SUMMARY_DAYS = Number(process.env.MAGIC_PAWS_DAILY_SUMMARY_DAYS || 365);
const SNAPSHOT_KEEP_LINES = Number(process.env.MAGIC_PAWS_SNAPSHOT_KEEP_LINES || 12000);

const OUT = {
  logsDir: path.join(ROOT, "data", "logs"),
  archiveDir: path.join(ROOT, "data", "archive"),
  snapshotsDir: path.join(ROOT, "data", "snapshots"),
  active20d: path.join(ROOT, "data", "logs", "magicpaws_events_20d.json"),
  manifest: path.join(ROOT, "data", "archive", "magicpaws_archive_manifest.json"),
  dailySummary: path.join(ROOT, "data", "snapshots", "magicpaws_daily_summary.json"),
  sensorLatest: path.join(ROOT, "data", "snapshots", "magicpaws_sensor_latest.json"),
  sensorHistory: path.join(ROOT, "data", "snapshots", "magicpaws_sensor_snapshots.ndjson")
};

const KEYWORD_TIERS = {
  hard: [
    "sabotage", "sabotageverdacht", "диверсия", "диверсія", "sabotaż", "sabotaje", "sabotageactie",
    "espionage", "spionage", "spy ship", "spionageschiff", "reconnaissance", "surveillance", "разведка", "розвідка",
    "gnss jamming", "gnss spoofing", "gps jamming", "gps spoofing", "jamming", "spoofing", "глушение", "глушіння",
    "cable cut", "cable damage", "subsea cable", "seekabel", "kabelschaden", "kabelbruch", "pipeline damage", "pipeline leak", "pipe rupture",
    "underwater drone", "uuv", "auv", "seabed warfare", "seabed", "subsea", "unterwasser", "meeresboden",
    "ais spoof", "ais gap", "ais off", "dark vessel", "dark activity", "loitering", "sts", "ship-to-ship",
    "shadow fleet", "dark fleet", "sanctions evasion", "sanctions avoidance", "sanktionsumgehung", "sanktionsflotte"
  ],
  soft: [
    "unusual activity", "suspicious", "auffällig", "ungewöhnlich", "verdächtig", "anomalous", "incident", "disturbance", "disruption",
    "navwarn", "navtex", "navigational warning", "security zone", "exclusion zone", "übungsgebiet", "military exercise", "coast guard", "navy", "patrol",
    "drone", "uas", "uav", "drohne", "helicopter", "mpa", "isr", "sar", "rendezvous", "anchoring", "route deviation",
    "port disruption", "terminal outage", "cyber", "ransomware", "ot", "scada", "vts"
  ]
};

function normalizeText(s) {
  return String(s ?? "").toLowerCase().normalize("NFKD").replace(/[\u0300-\u036f]/g, "");
}

function shortText(s, n = 1200) {
  const value = String(s ?? "").trim();
  return value.length > n ? `${value.slice(0, n - 1)}…` : value;
}

function cleanUrl(raw) {
  return String(raw || "").trim().replace(/^<|>$/g, "").replace(/[),.;]+$/g, "");
}

function isGithubIssueUrl(url) {
  return /github\.com\/[^/]+\/[^/]+\/issues\/\d+/i.test(String(url || ""));
}

function parseDateMs(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const d = new Date(raw);
  const ms = d.getTime();
  return Number.isFinite(ms) ? ms : null;
}

function firstDateMs(...values) {
  for (const value of values) {
    const ms = parseDateMs(value);
    if (ms !== null) return ms;
  }
  return null;
}

function firstIsoDate(...values) {
  const ms = firstDateMs(...values);
  return ms === null ? null : new Date(ms).toISOString();
}

function summaryField(ev, field) {
  const txt = String(ev?.summary ?? "");
  const re = new RegExp(`${field}\\s*:\\s*([^\\n\\r]+)`, "i");
  return (txt.match(re) || [])[1]?.trim() || null;
}

function summaryZeitHeaderDate(ev) {
  const txt = String(ev?.summary ?? "");
  const re = /###\s*Zeit\s*\(UTC\)\s*\n\s*([^\n\r]+)/i;
  const m = txt.match(re);
  if (!m) return null;
  const value = m[1].trim();
  if (/^[a-z0-9_ -]+\s*:/i.test(value)) return null;
  return value;
}

function originalUrlFromEvent(ev) {
  const direct = [ev?.original_url, ev?.source_url, ev?.link]
    .map(cleanUrl)
    .find(u => /^https?:\/\//i.test(u) && !isGithubIssueUrl(u));
  if (direct) return direct;

  const txt = String(ev?.summary || "");
  const patterns = [
    /###\s*Link\s*\n\s*(https?:\/\/[^\s<>"']+)/i,
    /###\s*URL\s*\n\s*(https?:\/\/[^\s<>"']+)/i,
    /(?:^|\n)\s*Link\s*:\s*(https?:\/\/[^\s<>"']+)/i,
    /(?:^|\n)\s*Original(?:meldung|quelle| source)?\s*:\s*(https?:\/\/[^\s<>"']+)/i
  ];
  for (const re of patterns) {
    const m = txt.match(re);
    if (m?.[1]) {
      const u = cleanUrl(m[1]);
      if (/^https?:\/\//i.test(u) && !isGithubIssueUrl(u)) return u;
    }
  }

  const urls = txt.match(/https?:\/\/[^\s<>"']+/gi) || [];
  for (const raw of urls) {
    const u = cleanUrl(raw);
    if (/^https?:\/\//i.test(u) && !isGithubIssueUrl(u)) return u;
  }
  return null;
}

function getEventChronologyMs(ev) {
  return firstDateMs(
    ev?.chronology_ts,
    ev?.source_published_at,
    ev?.sourcePublishedAt,
    ev?.published_at,
    ev?.publishedAt,
    ev?.source_hint?.source_published_at,
    ev?.source_hint?.sourcePublishedAt,
    summaryField(ev, "source_published_at"),
    summaryField(ev, "published_at"),
    summaryField(ev, "published"),
    summaryZeitHeaderDate(ev),
    ev?.ts
  ) ?? 0;
}

function inferSourceHint(ev) {
  const s = String(ev?.summary ?? "");
  const existing = ev?.source_hint ?? {};
  const source = s.match(/###\s*Quelle\s*\n([^\n\r]+)/i);
  const platform = s.match(/###\s*Plattform\s*\n([^\n\r]+)/i);
  const link = s.match(/###\s*Link\s*\n([^\n\r]+)/i);
  const sourcePublishedAt = firstIsoDate(
    existing.source_published_at,
    existing.sourcePublishedAt,
    ev?.source_published_at,
    ev?.sourcePublishedAt,
    summaryField(ev, "source_published_at"),
    summaryField(ev, "published_at"),
    summaryZeitHeaderDate(ev)
  );
  const ingestedAt = firstIsoDate(
    existing.ingested_at,
    existing.ingestedAt,
    ev?.ingested_at,
    ev?.ingestedAt,
    summaryField(ev, "ingested_at"),
    summaryField(ev, "ingest_run"),
    ev?.ts
  );
  return {
    source: existing.source ?? (source ? source[1].trim() : null),
    platform: existing.platform ?? (platform ? platform[1].trim() : null),
    link: existing.link ?? (link ? link[1].trim() : null),
    source_published_at: sourcePublishedAt,
    ingested_at: ingestedAt
  };
}

function inferReportCategory(ev) {
  const labels = ev?.labels ?? [];
  const cat = ev?.category ?? "";
  if (cat === "D:CYBER_OT") return "Hafen/KRITIS/Cyber/operative Störungen";
  if (cat === "D:INFRA_CI") return "Maritime KRITIS / Offshore-Infrastruktur";
  if (cat === "D:RF_SIGNAL" || labels.some(l => String(l).startsWith("RF:"))) return "RF/GNSS/NAVWARN";
  if (cat === "D:DRONE_UAS") return "Drohnen / UxV";
  if (cat === "D:AIR_ACTIVITY") return "Luftaktivität / ISR / SAR";
  if (cat === "D:AIS_TRACK") return "AIS / Schiffsmuster / Schattenflotte";
  if (cat === "D:SECURITY_CRIME") return "Maritime Crime / Security";
  if (cat === "D:INCIDENT") return "Zwischenfälle / Safety / Störungen";
  if (cat === "D:SATELLITE") return "Satellit / Fernerkundung";
  return "News / Intelligence Hinweise";
}

function inferGreybookRubric(ev) {
  const labels = ev?.labels ?? [];
  const cat = ev?.category ?? "";
  const text = `${ev?.title ?? ""} ${ev?.summary ?? ""} ${labels.join(" ")}`;
  if (cat === "D:DRONE_UAS" || /\b(drone|drohne|uas|uav|usv|uuv|auv)\b/i.test(text)) return "Drohnen";
  if (cat === "D:CYBER_OT" || /\b(cyber|ransomware|scada|ot|vts|terminal operating system)\b/i.test(text)) return "Cyber- und IT-Sicherheit";
  if (labels.includes("V:SHADOW_FLEET") || labels.includes("V:SANCTIONS_EVASION") || /shadow fleet|dark fleet|sanktionsflotte|sanctions/i.test(text)) return "Schattenflotte/Sanktionsflotte";
  if (cat === "D:RF_SIGNAL" || cat === "D:AIR_ACTIVITY" || cat === "D:INFRA_CI" || labels.includes("P0:SUSPECT")) return "Maritime Security/Hybride Bedrohungen";
  if (/russia|russisch|china|iran|black sea|schwarzes meer|nato|navy|kriegsschiff|warship/i.test(text)) return "maritime Geopolitik";
  return "Unreleased but self-confirmed";
}

function eventArchiveKey(ev) {
  const directUrl = cleanUrl(ev?.url || ev?.original_url || ev?.source_url || ev?.link || originalUrlFromEvent(ev) || "");
  if (directUrl) return `url:${directUrl.toLowerCase()}`;
  if (ev?.id) return `id:${ev.id}`;
  if (ev?.number) return `issue:${ev.number}`;
  const ms = getEventChronologyMs(ev) || parseDateMs(ev?.ts) || 0;
  return `txt:${normalizeText((ev?.title || "").slice(0, 180))}:${ms}`;
}

function compactEvent(ev) {
  const labels = Array.isArray(ev?.labels) ? ev.labels : [];
  const sourceHint = inferSourceHint(ev);
  const chronologyMs = getEventChronologyMs(ev);
  return {
    archive_key: ev?.archive_key || eventArchiveKey(ev),
    number: ev?.number ?? null,
    id: ev?.id ?? null,
    ts: ev?.ts ?? null,
    chronology_ts: chronologyMs ? new Date(chronologyMs).toISOString() : null,
    issue_created_at: ev?.issue_created_at ?? ev?.ts ?? null,
    source_published_at: sourceHint.source_published_at ?? null,
    ingested_at: sourceHint.ingested_at ?? null,
    title: ev?.title ?? "",
    url: ev?.url ?? null,
    original_url: originalUrlFromEvent(ev) || ev?.original_url || ev?.source_url || ev?.link || null,
    labels,
    category: ev?.category ?? null,
    region: ev?.region ?? null,
    severity: ev?.severity ?? null,
    confidence: ev?.confidence ?? null,
    phase0: ev?.phase0 ?? null,
    geo: ev?.geo ?? null,
    source_hint: sourceHint,
    report_category: inferReportCategory(ev),
    greybook_rubric: inferGreybookRubric(ev),
    summary: shortText(ev?.summary ?? "", 1200)
  };
}

function scoreEventForHybrid(e) {
  const labels = Array.isArray(e?.labels) ? e.labels : [];
  const sev = e?.severity ?? "SEV:1";
  const conf = e?.confidence ?? "CONF:LOW";
  const sevW = sev === "SEV:4" ? 9 : sev === "SEV:3" ? 6 : sev === "SEV:2" ? 3 : 1;
  const confW = conf === "CONF:HIGH" ? 1.2 : conf === "CONF:MED" ? 1.0 : 0.8;
  let v = sevW * confW;
  if (e?.phase0?.suspect || labels.includes("P0:SUSPECT")) v *= 1.6;
  if (["OBJ:CABLE", "OBJ:PIPELINE", "OBJ:WINDFARM", "OBJ:PORT", "OBJ:VTS_WSV"].some(x => labels.includes(x))) v *= 1.25;
  if (["PAT:LOITERING", "PAT:STS_SUSPECT", "PAT:AIS_GAP", "PAT:DARK_ACTIVITY", "PAT:SURVEYING", "PAT:ROUTE_DEVIATION", "PAT:GNSS_JAM", "PAT:GNSS_SPOOF", "RF:GNSS_JAM", "RF:GNSS_SPOOF"].some(x => labels.includes(x))) v *= 1.20;
  if (["V:SHADOW_FLEET", "V:RUS_RESEARCH", "V:RUS_WARSHIP"].some(x => labels.includes(x))) v *= 1.15;
  return v;
}

function clamp(n, a, b) {
  return Math.max(a, Math.min(b, n));
}

function daysAgoMs(ms, now = Date.now()) {
  return (now - ms) / (1000 * 60 * 60 * 24);
}

function scorePhaseZero(events, now = Date.now()) {
  const recent = events.filter(e => {
    const ms = getEventChronologyMs(e);
    return ms && daysAgoMs(ms, now) <= 7;
  });
  const points = recent.reduce((sum, ev) => sum + scoreEventForHybrid(ev), 0);
  return clamp(Math.round((points / 90) * 100), 0, 100);
}

function scoreAuthorityWeirdness(events, regionMode, now = Date.now()) {
  const regionLabels = regionMode === "north" ? ["REG:NORTH_SEA", "REG:GER_BIGHT"] : ["REG:BALTIC_SEA", "REG:BALTIC"];
  const recent = events.filter(e => {
    const ms = getEventChronologyMs(e);
    const labels = Array.isArray(e?.labels) ? e.labels : [];
    const inWindow = ms && daysAgoMs(ms, now) <= 7;
    const regionHit = regionLabels.includes(e?.region) || labels.some(l => regionLabels.includes(l));
    return inWindow && regionHit;
  });
  let w = 0;
  const seen = new Set();
  for (const e of recent) {
    const labels = Array.isArray(e?.labels) ? e.labels : [];
    const key = e?.url || e?.id || e?.number || e?.archive_key || e?.title;
    if (seen.has(key)) continue;
    seen.add(key);
    const sev = e?.severity ?? "SEV:1";
    const conf = e?.confidence ?? "CONF:LOW";
    const sevW = sev === "SEV:4" ? 7 : sev === "SEV:3" ? 5 : sev === "SEV:2" ? 2.5 : 1;
    const confW = conf === "CONF:HIGH" ? 1.1 : conf === "CONF:MED" ? 1.0 : 0.85;
    const official = labels.includes("SRC:OFFICIAL");
    const authorityVessel = labels.some(l => String(l).startsWith("V:AUTH_") || ["V:SAR_UNIT", "V:RUS_GOV", "V:RUS_WARSHIP", "V:RUS_AUXILIARY", "V:RUS_RESEARCH"].includes(l));
    const air = labels.includes("D:AIR_ACTIVITY");
    const sar = labels.includes("D:SAR") || labels.includes("V:SAR_UNIT");
    const navwarn = labels.includes("RF:NAVWARN") || labels.includes("RF:NAVTEX");
    const rf = labels.includes("D:RF_SIGNAL") || labels.some(l => String(l).startsWith("RF:"));
    const ci = ["OBJ:PORT", "OBJ:VTS_WSV", "OBJ:CABLE", "OBJ:PIPELINE", "OBJ:WINDFARM"].some(x => labels.includes(x));
    let evW = sevW * confW;
    if (official) evW *= 1.20;
    if (authorityVessel) evW *= 1.55;
    if (air) evW *= 1.35;
    if (sar) evW *= 1.20;
    if (navwarn) evW *= 0.85;
    if (rf) evW *= 1.20;
    if (ci) evW *= 1.15;
    w += evW;
  }
  return clamp(Math.round((w / 45) * 100), 0, 100);
}

function countBy(values) {
  const out = {};
  for (const v of values.filter(Boolean)) out[v] = (out[v] || 0) + 1;
  return Object.fromEntries(Object.entries(out).sort((a, b) => b[1] - a[1]));
}

function topKeywordsForEvents(events, max = 10) {
  const scores = new Map();
  const keywords = [...KEYWORD_TIERS.hard, ...KEYWORD_TIERS.soft];
  for (const ev of events) {
    const text = normalizeText(`${ev?.title || ""} ${ev?.summary || ""} ${(ev?.labels || []).join(" ")}`);
    for (const kw of keywords) {
      const nk = normalizeText(kw);
      if (nk && text.includes(nk)) scores.set(kw, (scores.get(kw) || 0) + 1);
    }
  }
  return [...scores.entries()].sort((a, b) => b[1] - a[1]).slice(0, max).map(([keyword, count]) => ({ keyword, count }));
}

function buildDailySummaries(events, days = DAILY_SUMMARY_DAYS, now = Date.now()) {
  const cutoff = now - days * 24 * 60 * 60 * 1000;
  const groups = new Map();
  for (const ev of events) {
    const ms = getEventChronologyMs(ev);
    if (!ms || ms < cutoff) continue;
    const day = new Date(ms).toISOString().slice(0, 10);
    if (!groups.has(day)) groups.set(day, []);
    groups.get(day).push(ev);
  }

  return [...groups.entries()].sort((a, b) => b[0].localeCompare(a[0])).map(([date, dayEvents]) => {
    const hybridRaw = dayEvents.reduce((sum, ev) => sum + scoreEventForHybrid(ev), 0);
    const northEvents = dayEvents.filter(ev => {
      const labels = ev.labels || [];
      return ev.region === "REG:NORTH_SEA" || ev.region === "REG:GER_BIGHT" || labels.includes("REG:NORTH_SEA") || labels.includes("REG:GER_BIGHT");
    });
    const balticEvents = dayEvents.filter(ev => {
      const labels = ev.labels || [];
      return ev.region === "REG:BALTIC_SEA" || ev.region === "REG:BALTIC" || labels.includes("REG:BALTIC_SEA") || labels.includes("REG:BALTIC");
    });
    return {
      date,
      events_total: dayEvents.length,
      p0_suspect: dayEvents.filter(ev => ev.phase0?.suspect || (ev.labels || []).includes("P0:SUSPECT")).length,
      geo_events: dayEvents.filter(ev => !!ev.geo).length,
      top_domains: countBy(dayEvents.map(ev => ev.category)),
      top_regions: countBy(dayEvents.map(ev => ev.region)),
      severity: countBy(dayEvents.map(ev => ev.severity)),
      hybrid_index_proxy: clamp(Math.round((hybridRaw / 70) * 100), 0, 100),
      government_weirdness_proxy: {
        north_sea: clamp(Math.round((northEvents.reduce((sum, ev) => sum + scoreEventForHybrid(ev), 0) / 35) * 100), 0, 100),
        baltic_sea: clamp(Math.round((balticEvents.reduce((sum, ev) => sum + scoreEventForHybrid(ev), 0) / 35) * 100), 0, 100)
      },
      top_keywords: topKeywordsForEvents(dayEvents, 8)
    };
  });
}

function isSocialOrRss(e) {
  const labels = e?.labels ?? [];
  const text = normalizeText(`${e?.summary ?? ""} ${e?.title ?? ""}`);
  return labels.includes("SRC:SOCIAL") || labels.includes("SRC:MEDIA") || text.includes("### plattform\nrss") || text.includes("platform: rss");
}

function keywordHits(text) {
  const t = normalizeText(text);
  let hard = 0;
  let soft = 0;
  for (const kw of KEYWORD_TIERS.hard) if (t.includes(normalizeText(kw))) hard++;
  for (const kw of KEYWORD_TIERS.soft) if (t.includes(normalizeText(kw))) soft++;
  return { hard, soft, score: hard * 3 + soft };
}

function buildKeywordBuckets(events, now = Date.now()) {
  const bucketMs = 3 * 60 * 60 * 1000;
  const bucketCount = 24;
  const buckets = Array.from({ length: bucketCount }, (_, i) => ({ idx: i, score: 0, hits: 0, sources: new Set() }));
  const dedupe = new Set();
  for (const e of events) {
    if (!isSocialOrRss(e)) continue;
    const eventMs = getEventChronologyMs(e);
    if (!eventMs) continue;
    const age = now - eventMs;
    if (age < 0 || age > bucketMs * bucketCount) continue;
    const key = e.url || e.id || e.number || e.archive_key || `${e.title}${e.ts || ""}`;
    if (dedupe.has(key)) continue;
    dedupe.add(key);
    const b = bucketCount - 1 - Math.floor(age / bucketMs);
    if (b < 0 || b >= bucketCount) continue;
    const kh = keywordHits(`${e.title ?? ""} ${e.summary ?? ""} ${(e.labels ?? []).join(" ")}`);
    if (kh.score <= 0) continue;
    buckets[b].score += kh.score;
    buckets[b].hits += kh.hard + kh.soft;
    buckets[b].sources.add((e.labels ?? []).includes("SRC:SOCIAL") ? "social" : "rss/media");
  }
  return buckets.map(b => ({ ...b, sources: b.sources.size }));
}

function buildSensorSnapshot(events, generatedAt = new Date()) {
  const now = generatedAt.getTime();
  const keywordBuckets = buildKeywordBuckets(events, now);
  const keywordScore = keywordBuckets.reduce((sum, b) => sum + b.score, 0);
  const keywordHitsTotal = keywordBuckets.reduce((sum, b) => sum + b.hits, 0);
  const keywordBarometerPct = clamp(Math.round((keywordScore / 130) * 100), 0, 100);
  const events72h = events.filter(e => {
    const ms = getEventChronologyMs(e);
    return ms && now - ms >= 0 && now - ms <= 72 * 60 * 60 * 1000;
  });
  const snapshotKeyDate = new Date(generatedAt);
  snapshotKeyDate.setUTCMinutes(0, 0, 0);
  snapshotKeyDate.setUTCHours(Math.floor(snapshotKeyDate.getUTCHours() / 6) * 6);
  return {
    snapshot_schema: "MAGIC_PAWS_SENSOR_SNAPSHOT_v1",
    snapshot_key: snapshotKeyDate.toISOString(),
    generated_at: generatedAt.toISOString(),
    source: {
      worker_base: WORKER_BASE,
      bundle_url: BUNDLE_URL
    },
    counts: {
      events_total_archive: events.length,
      events_72h: events72h.length,
      p0_suspect_72h: events72h.filter(e => e?.phase0?.suspect || (e?.labels || []).includes("P0:SUSPECT")).length,
      geo_72h: events72h.filter(e => !!e.geo).length
    },
    sensors: {
      hybrid_index_pct: scorePhaseZero(events, now),
      government_weirdness: {
        north_sea_pct: scoreAuthorityWeirdness(events, "north", now),
        baltic_sea_pct: scoreAuthorityWeirdness(events, "baltic", now)
      },
      keyword_barometer_pct: keywordBarometerPct,
      keyword_hits_72h: keywordHitsTotal
    },
    density: {
      domains_72h: countBy(events72h.map(e => e.category)),
      regions_72h: countBy(events72h.map(e => e.region)),
      top_keywords_72h: topKeywordsForEvents(events72h, 12)
    }
  };
}

async function ensureDirs() {
  await Promise.all([OUT.logsDir, OUT.archiveDir, OUT.snapshotsDir].map(dir => fs.mkdir(dir, { recursive: true })));
}

async function readJsonIfExists(file, fallback = null) {
  try {
    return JSON.parse(await fs.readFile(file, "utf8"));
  } catch (err) {
    if (err.code === "ENOENT") return fallback;
    throw err;
  }
}

async function readJsonlIfExists(file) {
  try {
    const text = await fs.readFile(file, "utf8");
    return text.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
  } catch (err) {
    if (err.code === "ENOENT") return [];
    throw err;
  }
}

async function readExistingArchiveEvents() {
  const events = [];

  const active = await readJsonIfExists(OUT.active20d, null);
  if (Array.isArray(active?.events)) events.push(...active.events);

  try {
    const names = await fs.readdir(OUT.archiveDir);
    for (const name of names) {
      if (!/^magicpaws_\d{4}-\d{2}\.jsonl$/.test(name)) continue;
      const fileEvents = await readJsonlIfExists(path.join(OUT.archiveDir, name));
      events.push(...fileEvents);
    }
  } catch (err) {
    if (err.code !== "ENOENT") throw err;
  }

  return events;
}

async function fetchBundle() {
  const headers = { "User-Agent": "MagicPaws-LogArchive-GitHubAction/1.0" };
  if (process.env.MAGIC_PAWS_BUNDLE_BEARER_TOKEN) headers.Authorization = `Bearer ${process.env.MAGIC_PAWS_BUNDLE_BEARER_TOKEN}`;
  if (process.env.CF_ACCESS_CLIENT_ID && process.env.CF_ACCESS_CLIENT_SECRET) {
    headers["CF-Access-Client-Id"] = process.env.CF_ACCESS_CLIENT_ID;
    headers["CF-Access-Client-Secret"] = process.env.CF_ACCESS_CLIENT_SECRET;
  }

  const res = await fetch(BUNDLE_URL, { headers, cache: "no-store" });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Bundle fetch failed: HTTP ${res.status} ${res.statusText}: ${text.slice(0, 1000)}`);
  }
  let data;
  try {
    data = JSON.parse(text);
  } catch (err) {
    throw new Error(`Bundle response was not valid JSON: ${err.message}; head=${text.slice(0, 300)}`);
  }
  if (data?.ok === false) throw new Error(data.error || data.message || "bundle returned ok=false");
  return data;
}

function mergeEvents(...eventLists) {
  const byKey = new Map();
  for (const list of eventLists) {
    for (const raw of Array.isArray(list) ? list : []) {
      const c = compactEvent(raw);
      const key = c.archive_key || eventArchiveKey(c);
      const previous = byKey.get(key);
      if (!previous || getEventChronologyMs(c) >= getEventChronologyMs(previous)) {
        byKey.set(key, c);
      }
    }
  }
  return [...byKey.values()].sort((a, b) => getEventChronologyMs(b) - getEventChronologyMs(a));
}

function groupByMonth(events) {
  const groups = new Map();
  for (const ev of events) {
    const ms = getEventChronologyMs(ev) || Date.now();
    const month = new Date(ms).toISOString().slice(0, 7);
    if (!groups.has(month)) groups.set(month, []);
    groups.get(month).push(ev);
  }
  return [...groups.entries()].sort((a, b) => a[0].localeCompare(b[0]));
}

async function writeJson(file, obj) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}

async function writeJsonl(file, events) {
  await fs.mkdir(path.dirname(file), { recursive: true });
  const lines = events
    .sort((a, b) => getEventChronologyMs(b) - getEventChronologyMs(a))
    .map(ev => JSON.stringify(ev));
  await fs.writeFile(file, `${lines.join("\n")}${lines.length ? "\n" : ""}`, "utf8");
}

async function updateSensorHistory(snapshot) {
  let lines = [];
  try {
    lines = (await fs.readFile(OUT.sensorHistory, "utf8")).split(/\r?\n/).filter(Boolean);
  } catch (err) {
    if (err.code !== "ENOENT") throw err;
  }

  const byKey = new Map();
  for (const line of lines) {
    try {
      const obj = JSON.parse(line);
      byKey.set(obj.snapshot_key || obj.generated_at || String(byKey.size), obj);
    } catch (_err) {
      // Drop corrupt lines rather than poisoning the archive forever.
    }
  }
  byKey.set(snapshot.snapshot_key, snapshot);
  const next = [...byKey.values()]
    .sort((a, b) => String(a.snapshot_key || a.generated_at).localeCompare(String(b.snapshot_key || b.generated_at)))
    .slice(-SNAPSHOT_KEEP_LINES)
    .map(obj => JSON.stringify(obj));
  await fs.writeFile(OUT.sensorHistory, `${next.join("\n")}${next.length ? "\n" : ""}`, "utf8");
}

async function main() {
  await ensureDirs();
  const generatedAt = new Date();
  console.log(`[magicpaws] Fetching bundle: ${BUNDLE_URL}`);
  const bundle = await fetchBundle();
  const liveEvents = Array.isArray(bundle.events) ? bundle.events : [];
  console.log(`[magicpaws] Live events from Worker: ${liveEvents.length}`);

  const existingEvents = await readExistingArchiveEvents();
  console.log(`[magicpaws] Existing repository events: ${existingEvents.length}`);

  const merged = mergeEvents(existingEvents, liveEvents);
  const activeCutoff = generatedAt.getTime() - ACTIVE_WINDOW_DAYS * 24 * 60 * 60 * 1000;
  const activeEvents = merged.filter(ev => {
    const ms = getEventChronologyMs(ev);
    return ms && ms >= activeCutoff;
  });
  const dailySummary = buildDailySummaries(merged, DAILY_SUMMARY_DAYS, generatedAt.getTime());
  const sensorSnapshot = buildSensorSnapshot(merged, generatedAt);

  await writeJson(OUT.active20d, {
    schema: "MAGIC_PAWS_EVENTS_20D_v1",
    generated_at: generatedAt.toISOString(),
    worker_base: WORKER_BASE,
    source: bundle.source ?? null,
    bundle_generated_at: bundle.generated_at ?? null,
    window: {
      days: ACTIVE_WINDOW_DAYS,
      cutoff_utc: new Date(activeCutoff).toISOString()
    },
    counts: {
      live_events_from_worker: liveEvents.length,
      archive_events_total: merged.length,
      active_events: activeEvents.length
    },
    events: activeEvents
  });

  const manifestMonths = [];
  for (const [month, events] of groupByMonth(merged)) {
    const filename = `magicpaws_${month}.jsonl`;
    const file = path.join(OUT.archiveDir, filename);
    await writeJsonl(file, events);
    const sortedMs = events.map(getEventChronologyMs).filter(Boolean).sort((a, b) => a - b);
    manifestMonths.push({
      month,
      file: `data/archive/${filename}`,
      count: events.length,
      first_utc: sortedMs.length ? new Date(sortedMs[0]).toISOString() : null,
      last_utc: sortedMs.length ? new Date(sortedMs[sortedMs.length - 1]).toISOString() : null
    });
  }

  await writeJson(OUT.manifest, {
    schema: "MAGIC_PAWS_ARCHIVE_MANIFEST_v1",
    generated_at: generatedAt.toISOString(),
    worker_base: WORKER_BASE,
    total_events: merged.length,
    active_window_days: ACTIVE_WINDOW_DAYS,
    daily_summary_days: DAILY_SUMMARY_DAYS,
    months: manifestMonths.sort((a, b) => b.month.localeCompare(a.month))
  });

  await writeJson(OUT.dailySummary, {
    schema: "MAGIC_PAWS_DAILY_SUMMARY_v1",
    generated_at: generatedAt.toISOString(),
    worker_base: WORKER_BASE,
    days: dailySummary.length,
    summary_window_days: DAILY_SUMMARY_DAYS,
    daily_summary: dailySummary
  });

  await writeJson(OUT.sensorLatest, sensorSnapshot);
  await updateSensorHistory(sensorSnapshot);

  console.log(`[magicpaws] Wrote active=${activeEvents.length}, archive_total=${merged.length}, daily_days=${dailySummary.length}`);
  console.log(`[magicpaws] Latest sensor snapshot: ${sensorSnapshot.snapshot_key}`);
}

main().catch(err => {
  console.error("[magicpaws] FAILED", err);
  process.exit(1);
});
