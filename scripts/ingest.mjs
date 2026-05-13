import fs from "fs";
import YAML from "yaml";
import { XMLParser } from "fast-xml-parser";

const PRODUCT = "MAGIC PAWS // MARITIME PHASE ZERO";
const CFG_PATH = process.env.SOURCES_CONFIG_PATH || "config/sources.yml";
const DEFAULT_OWNER = process.env.DEFAULT_OWNER || "mowlsint";
const DEFAULT_REPO = process.env.DEFAULT_REPO || "MagicPaws";

const MAX_CREATED_PER_RUN = Number(process.env.MAX_CREATED_PER_RUN || 35);
const MAX_ITEMS_PER_SOURCE = Number(process.env.MAX_ITEMS_PER_SOURCE || 8);
const RECENT_ISSUE_PAGES = Number(process.env.RECENT_ISSUE_PAGES || 8);
const SOCIAL_FETCH_LIMIT = Number(process.env.SOCIAL_FETCH_LIMIT || 40);
const HTML_LINK_LIMIT = Number(process.env.HTML_LINK_LIMIT || 90);
const FETCH_TIMEOUT_MS = Number(process.env.FETCH_TIMEOUT_MS || 20000);
const AUTO_CREATE_LABELS = String(process.env.AUTO_CREATE_LABELS || "1") !== "0";

// Keep the legacy VOODOO_INGEST marker for compatibility with the existing
// Worker/dashboard cleanup logic. The visible product naming is MAGIC PAWS.
const INGEST_MARKER = "VOODOO_INGEST";

// -------------------- small helpers --------------------

function must(v, msg) {
  if (!v) throw new Error(msg);
  return v;
}

function norm(s) {
  return String(s ?? "").trim();
}

function isoNow() {
  return new Date().toISOString();
}

function safeDateIso(v) {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function short(s, n = 280) {
  s = String(s ?? "");
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

function clampNumber(v, fallback, min = 0, max = 999999) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function stripHtml(s) {
  s = String(s ?? "");
  s = s.replace(/<script[\s\S]*?<\/script>/gi, "");
  s = s.replace(/<style[\s\S]*?<\/style>/gi, "");
  s = s.replace(/<br\s*\/?>/gi, "\n");
  s = s.replace(/<\/(p|div|li|tr|h1|h2|h3|h4|h5|h6)>/gi, "\n");
  s = s.replace(/<[^>]+>/g, "");
  return decodeHtmlEntities(s)
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/[ \t]{2,}/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function decodeHtmlEntities(s) {
  return String(s ?? "")
    .replace(/&#x([0-9a-f]+);/gi, (_, hex) => {
      const cp = parseInt(hex, 16);
      return Number.isFinite(cp) ? String.fromCodePoint(cp) : _;
    })
    .replace(/&#(\d+);/g, (_, dec) => {
      const cp = parseInt(dec, 10);
      return Number.isFinite(cp) ? String.fromCodePoint(cp) : _;
    })
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, "\"")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&deg;/g, "°");
}

function ensureLabels(arr) {
  return Array.from(
    new Set(
      (arr || [])
        .flatMap((x) => Array.isArray(x) ? x : [x])
        .map(String)
        .map((s) => s.trim())
        .filter(Boolean)
    )
  );
}

function pickFirstLabel(labels, prefix) {
  return (labels || []).find((l) => typeof l === "string" && l.startsWith(prefix)) || null;
}

function hasLabel(labels, label) {
  return ensureLabels(labels).includes(label);
}

function hashKey(s) {
  const input = String(s ?? "");
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

function normalizeUrl(url) {
  const raw = norm(url);
  if (!raw) return "";
  try {
    const u = new URL(raw);
    u.hash = "";

    const removeParams = new Set([
      "fbclid", "gclid", "mc_cid", "mc_eid", "igshid", "ref", "ref_src",
      "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"
    ]);

    for (const key of Array.from(u.searchParams.keys())) {
      if (removeParams.has(key.toLowerCase()) || key.toLowerCase().startsWith("utm_")) {
        u.searchParams.delete(key);
      }
    }

    if (u.pathname.length > 1) u.pathname = u.pathname.replace(/\/+$/, "");
    return u.toString().toLowerCase();
  } catch {
    return raw.replace(/#.*$/, "").replace(/\/+$/, "").toLowerCase();
  }
}

function makeAbsoluteUrl(baseUrl, maybeUrl) {
  const raw = norm(maybeUrl);
  if (!raw) return "";
  if (/^(mailto:|tel:|javascript:|#)/i.test(raw)) return "";
  try {
    return new URL(raw, baseUrl).toString();
  } catch {
    return raw;
  }
}

function parseRepo() {
  const full = process.env.GITHUB_REPOSITORY || "";
  if (full.includes("/")) {
    const [owner, repo] = full.split("/", 2);
    return { owner, repo };
  }
  return { owner: DEFAULT_OWNER, repo: DEFAULT_REPO };
}

function sourcePriority(s) {
  return clampNumber(s?.priority, 50, 0, 1000);
}

function sourceItemLimit(s) {
  return clampNumber(s?.max_items ?? s?.max_items_per_source, MAX_ITEMS_PER_SOURCE, 1, 50);
}

function sourceLookbackHours(s) {
  if (s?.lookback_hours != null) return clampNumber(s.lookback_hours, 72, 1, 24 * 60);

  const labels = ensureLabels(s?.labels || []);
  const type = norm(s?.type);

  if (labels.includes("DECAY:SLOW") || labels.includes("SRC:THINKTANK") || labels.includes("SRC:ANALYSIS")) return 24 * 14;
  if (type === "social_x" || type === "social_bsky") return 72;
  if (type === "html" || type === "json") return 96;
  return 96;
}

function allowBackfill(s) {
  if (s?.allow_backfill != null) return Boolean(s.allow_backfill);

  const labels = ensureLabels(s?.labels || []);
  if (labels.includes("DECAY:SLOW") || labels.includes("SRC:THINKTANK") || labels.includes("SRC:ANALYSIS")) return false;
  return false;
}

function ageHours(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return (Date.now() - t) / 3600000;
}

function passesLookback(item, source) {
  const iso = safeDateIso(item?.published_at || item?.updated_at || item?.date);
  const h = ageHours(iso);
  const lookback = sourceLookbackHours(source);

  // Undated HTML/RSS links are allowed, because many official pages do not expose
  // dates in list views. Dedupe prevents repeated issue creation.
  if (h == null) return true;
  if (h <= lookback) return true;
  return allowBackfill(source);
}

function splitKeywords(v) {
  if (!v) return [];
  if (Array.isArray(v)) return v.map(String).map(x => x.trim()).filter(Boolean);
  return String(v).split(/[,;\n]/).map(x => x.trim()).filter(Boolean);
}

const DEFAULT_INCLUDE_KEYWORDS = [
  // hybrid / phase zero / sabotage / KRITIS
  "hybrid", "phase zero", "phase-zero", "grey zone", "gray zone", "sabotage", "espionage",
  "critical infrastructure", "kritische infrastruktur", "kritis", "marKritis", "maritime kritis",
  "subsea", "undersea", "cable", "pipeline", "wind farm", "offshore", "energy infrastructure",
  "telecom", "data cable", "anchor damage", "seabed",

  // GNSS / AIS / RF / cyber
  "gnss", "gps", "jamming", "spoofing", "ais", "dark fleet", "shadow fleet", "sanctions",
  "sanktions", "schattenflotte", "tanker", "oil tanker", "rf", "radio", "cyber", "ot security",

  // maritime incidents / operations
  "naval", "warship", "coast guard", "coastguard", "navy", "maritime security", "shipping",
  "port", "harbour", "harbor", "container", "vessel", "ship", "cargo", "ferry", "fishing",
  "mine", "minesweeping", "exercise", "drone", "uav", "usv", "unmanned", "survey vessel",

  // North/Baltic/Europe anchors
  "baltic", "ostsee", "north sea", "nordsee", "german bight", "deutsche bucht", "skagerrak",
  "kattegat", "øresund", "oresund", "denmark", "sweden", "finland", "norway", "poland",
  "estonia", "latvia", "lithuania", "germany", "netherlands", "belgium", "uk", "russia",
  "kaliningrad", "st. petersburg", "murmansk", "black sea", "schwarzes meer"
];

const DEFAULT_EXCLUDE_KEYWORDS = [
  "celebrity", "football", "soccer", "basketball", "horoscope", "recipe", "lottery", "fashion"
];

function keywordScore(text, source) {
  const hay = String(text || "").toLowerCase();
  const include = [...DEFAULT_INCLUDE_KEYWORDS, ...splitKeywords(source?.include_keywords)];
  const exclude = [...DEFAULT_EXCLUDE_KEYWORDS, ...splitKeywords(source?.exclude_keywords)];

  let score = 0;
  const hits = [];

  for (const kw of include) {
    const k = kw.toLowerCase();
    if (!k) continue;
    if (hay.includes(k)) {
      score += k.length >= 9 ? 2 : 1;
      hits.push(kw);
    }
  }

  for (const kw of exclude) {
    const k = kw.toLowerCase();
    if (!k) continue;
    if (hay.includes(k)) score -= 4;
  }

  const labels = ensureLabels(source?.labels || []);
  if (labels.includes("SRC:OFFICIAL")) score += 2;
  if (labels.includes("SRC:THINKTANK")) score += 2;
  if (labels.includes("SRC:ANALYSIS")) score += 1;
  if (labels.includes("SRC:SOCIAL")) score += 0;
  if (labels.includes("RF:NAVWARN")) score += 2;
  if (labels.includes("MAP:NO")) score += 1;

  return { score, hits: Array.from(new Set(hits)).slice(0, 12) };
}

function minScoreForSource(source) {
  if (source?.min_score != null) return Number(source.min_score);
  const labels = ensureLabels(source?.labels || []);
  const type = norm(source?.type);

  if (labels.includes("SRC:OFFICIAL") || labels.includes("RF:NAVWARN")) return 1;
  if (labels.includes("SRC:THINKTANK") || labels.includes("SRC:ANALYSIS")) return 1;
  if (type === "html") return 2;
  if (type === "social_x" || type === "social_bsky") return 1;
  return 1;
}

function passesRelevance(item, source) {
  if (source?.disable_relevance_filter === true) return { ok: true, score: 99, hits: ["filter_disabled"] };

  const text = [item?.title, item?.summary, item?.text, item?.link].filter(Boolean).join("\n");
  const scored = keywordScore(text, source);
  return { ok: scored.score >= minScoreForSource(source), ...scored };
}

// -------------------- fetch helpers --------------------

async function fetchText(url, headers = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      headers,
      signal: ac.signal,
    });
    const txt = await res.text().catch(() => "");
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} for ${url}: ${txt.slice(0, 300)}`);
    }
    return txt;
  } finally {
    clearTimeout(t);
  }
}

async function fetchJson(url, headers = {}, timeoutMs = FETCH_TIMEOUT_MS) {
  const txt = await fetchText(url, headers, timeoutMs);
  return txt ? JSON.parse(txt) : null;
}

// -------------------- GitHub API --------------------

async function ghRequest(path, method = "GET", body = null) {
  const token = process.env.GH_TOKEN || process.env.GITHUB_TOKEN;
  must(token, "Missing GH_TOKEN / GITHUB_TOKEN. In GitHub Actions use: GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}");

  const url = `https://api.github.com${path}`;

  const res = await fetch(url, {
    method,
    headers: {
      Accept: "application/vnd.github+json",
      Authorization: `Bearer ${token}`,
      "User-Agent": "magic-paws-ingest",
      ...(body ? { "Content-Type": "application/json" } : {}),
    },
    body: body ? JSON.stringify(body) : null,
  });

  const txt = await res.text().catch(() => "");
  if (!res.ok) {
    throw new Error(`GitHub API ${res.status} ${method} ${path}: ${txt.slice(0, 500)}`);
  }

  return txt ? JSON.parse(txt) : null;
}

async function listRecentIssues(owner, repo, pages = RECENT_ISSUE_PAGES) {
  const out = [];

  for (let p = 1; p <= pages; p++) {
    const items = await ghRequest(`/repos/${owner}/${repo}/issues?state=all&per_page=100&page=${p}`);
    for (const it of items || []) {
      if (!it.pull_request) out.push(it);
    }
    if (!items || items.length < 100) break;
  }

  return out;
}

async function listAllLabels(owner, repo) {
  const labels = new Set();

  for (let p = 1; p <= 20; p++) {
    const items = await ghRequest(`/repos/${owner}/${repo}/labels?per_page=100&page=${p}`);
    for (const it of items || []) {
      if (it?.name) labels.add(it.name);
    }
    if (!items || items.length < 100) break;
  }

  return labels;
}

function colorForLabel(label) {
  const l = String(label || "");
  if (l.startsWith("SRC:")) return "8b949e";
  if (l.startsWith("D:")) return "0e8a16";
  if (l.startsWith("REG:")) return "1d76db";
  if (l.startsWith("CONF:")) return "fbca04";
  if (l.startsWith("SEV:")) return "fef2c0";
  if (l.startsWith("V:")) return "d93f0b";
  if (l.startsWith("PAT:")) return "5319e7";
  if (l.startsWith("OBJ:")) return "006b75";
  if (l.startsWith("RF:")) return "b60205";
  if (l.startsWith("SCORE:")) return "c5def5";
  if (l.startsWith("MAP:")) return "eeeeee";
  if (l.startsWith("DECAY:")) return "bfdadc";
  return "ededed";
}

async function ensureLabel(owner, repo, label, existingLabels) {
  if (!label || existingLabels.has(label)) return true;
  if (!AUTO_CREATE_LABELS) return false;

  try {
    await ghRequest(`/repos/${owner}/${repo}/labels`, "POST", {
      name: label,
      color: colorForLabel(label),
      description: `Auto-created by ${PRODUCT} ingest`,
    });
    existingLabels.add(label);
    console.log(`Created missing label: ${label}`);
    return true;
  } catch (e) {
    const msg = e?.message || String(e);
    if (/already_exists|already exists|Validation Failed/i.test(msg)) {
      existingLabels.add(label);
      return true;
    }
    console.log(`Could not create label ${label}: ${msg.slice(0, 300)}`);
    return false;
  }
}

async function safeLabels(owner, repo, wanted, existingLabels, sourceId) {
  const out = [];
  const missing = [];

  for (const label of ensureLabels(wanted)) {
    const ok = await ensureLabel(owner, repo, label, existingLabels);
    if (ok && existingLabels.has(label)) out.push(label);
    else missing.push(label);
  }

  if (missing.length) {
    console.log(`Missing labels for ${sourceId}: ${missing.join(", ")} — skipped for this issue`);
  }

  return out;
}

async function createIssue(owner, repo, title, body, labels) {
  return await ghRequest(`/repos/${owner}/${repo}/issues`, "POST", {
    title: short(title, 240),
    body,
    labels,
  });
}

// -------------------- RSS / Atom / JSON parsing --------------------

function textFromNode(v) {
  if (v == null) return "";
  if (typeof v === "string" || typeof v === "number") return norm(v);
  if (typeof v === "object") {
    if (v["#text"] != null) return norm(v["#text"]);
    if (v._text != null) return norm(v._text);
    if (v.value != null) return norm(v.value);
  }
  return norm(v);
}

function linkFromAtomEntry(entry) {
  const linkNode = entry?.link;

  if (!linkNode) return "";
  if (typeof linkNode === "string") return norm(linkNode);

  if (Array.isArray(linkNode)) {
    const preferred =
      linkNode.find((x) => x?.["@_rel"] === "alternate") ||
      linkNode.find((x) => x?.["@_rel"] !== "self") ||
      linkNode[0];

    return norm(preferred?.["@_href"] || preferred?.href || preferred);
  }

  if (typeof linkNode === "object") {
    return norm(linkNode["@_href"] || linkNode.href || linkNode["#text"] || "");
  }

  return "";
}

function parseRss(xml, baseUrl = "") {
  let data;
  try {
    const parser = new XMLParser({
      ignoreAttributes: false,
      attributeNamePrefix: "@_",
      trimValues: true,
    });
    data = parser.parse(xml);
  } catch {
    return [];
  }

  const items = [];

  const rssItems = data?.rss?.channel?.item;
  if (rssItems) {
    const arr = Array.isArray(rssItems) ? rssItems : [rssItems];

    for (const it of arr) {
      const title = textFromNode(it?.title);
      const rawLink = textFromNode(it?.link) || textFromNode(it?.guid);
      const description = textFromNode(it?.description || it?.["content:encoded"] || it?.summary);

      items.push({
        title: stripHtml(title),
        link: makeAbsoluteUrl(baseUrl, rawLink),
        published_at: safeDateIso(it?.pubDate || it?.published || it?.updated || it?.["dc:date"]),
        summary: stripHtml(description),
        parser: "rss",
      });
    }

    return items.filter((x) => x.title || x.link).slice(0, 100);
  }

  const atomEntries = data?.feed?.entry;
  if (atomEntries) {
    const arr = Array.isArray(atomEntries) ? atomEntries : [atomEntries];

    for (const e of arr) {
      const rawTitle = textFromNode(e?.title);
      const rawLink = linkFromAtomEntry(e) || textFromNode(e?.id);
      const rawSummary = textFromNode(e?.summary || e?.content);

      items.push({
        title: stripHtml(rawTitle),
        link: makeAbsoluteUrl(baseUrl, rawLink),
        published_at: safeDateIso(e?.published || e?.updated),
        summary: stripHtml(rawSummary),
        parser: "atom",
      });
    }

    return items.filter((x) => x.title || x.link).slice(0, 100);
  }

  return [];
}

function parseJsonItems(data, baseUrl = "") {
  const candidates =
    Array.isArray(data) ? data :
    Array.isArray(data?.items) ? data.items :
    Array.isArray(data?.data) ? data.data :
    Array.isArray(data?.results) ? data.results :
    [];

  const out = [];
  for (const it of candidates) {
    if (!it || typeof it !== "object") continue;
    const title = norm(it.title || it.name || it.headline || it.summary || it.text || "");
    const link = makeAbsoluteUrl(baseUrl, it.url || it.link || it.href || it.external_url || "");
    const published_at = safeDateIso(it.published_at || it.published || it.date || it.created_at || it.updated_at);
    const summary = stripHtml(it.summary || it.description || it.text || it.body || title);
    out.push({ title: stripHtml(title), link, published_at, summary, parser: "json" });
  }
  return out.filter((x) => x.title || x.link).slice(0, 100);
}

// -------------------- generic HTML parsing --------------------

function pageTitleFromHtml(html) {
  const m = String(html || "").match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return stripHtml(m?.[1] || "");
}

function isProbablyArticleUrl(link) {
  const u = String(link || "").toLowerCase();
  if (!u) return false;
  if (/\/(tag|tags|category|author|about|contact|privacy|impressum|login|account|search|feed|rss|newsletter)(\/|$|\?)/i.test(u)) return false;
  if (/\.(jpg|jpeg|png|gif|webp|svg|css|js|ico|pdf)(\?|$)/i.test(u)) return false;
  if (/\b(share|facebook|twitter|x\.com\/intent|linkedin\.com\/share)\b/i.test(u)) return false;

  return /\/(news|article|articles|publication|publications|commentary|analysis|reports?|blog|post|press|security|maritime|shipping|offshore|cyber|sanctions|russia|ukraine|baltic|north-sea|ports?)\b/i.test(u) ||
    /\b(20\d{2}[\/-][01]?\d[\/-][0-3]?\d)\b/.test(u);
}

function parseHtmlLinks(html, baseUrl = "") {
  const out = [];
  const seen = new Set();
  const pageTitle = pageTitleFromHtml(html);
  const anchorRe = /<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>([\s\S]*?)<\/a>/gi;
  let m;

  while ((m = anchorRe.exec(String(html || ""))) && out.length < HTML_LINK_LIMIT) {
    const link = makeAbsoluteUrl(baseUrl, decodeHtmlEntities(m[1]));
    if (!link) continue;

    const key = normalizeUrl(link);
    if (!key || seen.has(key)) continue;
    seen.add(key);

    const title = stripHtml(m[2]).replace(/\s+/g, " ").trim();
    if (!title || title.length < 8) continue;
    if (!isProbablyArticleUrl(link) && title.length < 35) continue;

    out.push({
      title,
      link,
      published_at: null,
      summary: `${title}${pageTitle ? ` — ${pageTitle}` : ""}`,
      parser: "html_anchor",
    });
  }

  return out;
}

async function fetchSourceItems(source, socialBase) {
  const type = norm(source?.type);
  const sourceId = norm(source?.id);
  const headers = { "User-Agent": "magic-paws-ingest" };

  if (type === "rss") {
    const url = must(norm(source?.url), `RSS source without url: ${sourceId}`);
    const raw = await fetchText(url, headers);
    return parseRss(raw, url);
  }

  if (type === "json") {
    const url = must(norm(source?.url), `JSON source without url: ${sourceId}`);
    const data = await fetchJson(url, headers);
    return parseJsonItems(data, url);
  }

  if (type === "html") {
    const url = must(norm(source?.url), `HTML source without url: ${sourceId}`);
    const raw = await fetchText(url, headers);

    // Some config entries are marked html but actually return RSS/Atom/XML.
    const feedItems = parseRss(raw, url);
    if (feedItems.length) return feedItems;

    try {
      const maybeJson = JSON.parse(raw);
      const jsonItems = parseJsonItems(maybeJson, url);
      if (jsonItems.length) return jsonItems;
    } catch {
      // not JSON
    }

    return parseHtmlLinks(raw, url);
  }

  if (type === "social_x" || type === "social_bsky") {
    const bridgePath = must(norm(source?.bridge_path), `Social source without bridge_path: ${sourceId}`);
    const limit = clampNumber(source?.fetch_limit, SOCIAL_FETCH_LIMIT, 1, 100);
    const url = socialBase.replace(/\/+$/, "") + bridgePath + "?limit=" + limit;
    const data = await fetchJson(url, headers, 25000);
    if (data?.ok === false) throw new Error(data?.error || "social bridge returned ok=false");

    const rawItems = Array.isArray(data?.items) ? data.items : [];
    return rawItems.map((it) => {
      const text = stripHtml(norm(it.text || it.title || ""));
      return {
        title: short(text || it.link || source.name || sourceId, 120),
        link: norm(it.link),
        published_at: safeDateIso(it.published_at || it.created_at || it.date),
        summary: text,
        platform: it.platform || (type === "social_x" ? "x" : "bsky"),
        parser: type,
      };
    });
  }

  console.log(`Unknown source type skipped: ${sourceId} (${type})`);
  return [];
}

// -------------------- Issue body / dedupe --------------------

function buildIssueBody({ source, platform, link, published_at, text, extra, itemScore, keywordHits }) {
  const sourceId = norm(source?.id);
  const sourceName = norm(source?.name || sourceId || "unknown_source");
  const cleanText = stripHtml(text || "");
  const cleanLink = norm(link);
  const normalizedLink = normalizeUrl(cleanLink);
  const keyMaterial = normalizedLink ? `url:${normalizedLink}` : `text:${sourceId}:${cleanText.slice(0, 360).toLowerCase()}`;
  const key = hashKey(keyMaterial);

  const lines = [];
  lines.push("### Quelle");
  lines.push(`${sourceName} (${sourceId})`);
  lines.push("");

  if (platform) {
    lines.push("### Plattform");
    lines.push(platform);
    lines.push("");
  }

  if (published_at) {
    lines.push("### Zeit (UTC)");
    lines.push(published_at);
    lines.push("");
  }

  if (cleanLink) {
    lines.push("### Link");
    lines.push(cleanLink);
    lines.push("");
  }

  lines.push("### Text");
  lines.push(cleanText || cleanLink || sourceName);
  lines.push("");

  lines.push("### MAGIC PAWS Meta");
  lines.push(`product: ${PRODUCT}`);
  lines.push(`source_id: ${sourceId}`);
  lines.push(`source_priority: ${sourcePriority(source)}`);
  lines.push(`item_score: ${itemScore ?? "n/a"}`);
  lines.push(`keyword_hits: ${(keywordHits || []).join(", ") || "none"}`);
  lines.push(`region_hint: ${source?.region_hint || ""}`);
  lines.push(`lookback_hours: ${sourceLookbackHours(source)}`);
  if (source?.map_weight != null) lines.push(`map_weight: ${source.map_weight}`);
  if (source?.score_weight != null) lines.push(`score_weight: ${source.score_weight}`);
  lines.push("");

  if (extra) {
    lines.push("### Extra");
    lines.push(extra);
    lines.push("");
  }

  lines.push(`<!-- ${INGEST_MARKER}: ${key} SOURCE=${sourceId} -->`);

  return {
    body: lines.join("\n"),
    key,
    normalizedLink,
  };
}

function extractUrls(text) {
  const urls = new Set();
  const re = /https?:\/\/[^\s<>'")]+/gi;
  let m;
  while ((m = re.exec(String(text || "")))) {
    const u = normalizeUrl(m[0]);
    if (u) urls.add(u);
  }
  return urls;
}

function existingDedupe(issues) {
  const keys = new Set();
  const urls = new Set();

  for (const it of issues || []) {
    const b = it?.body || "";

    const markerRe = /(VOODOO_INGEST|MAGIC_PAWS_INGEST):\s*([A-Za-z0-9+/=_-]+)/g;
    let m;
    while ((m = markerRe.exec(b))) {
      if (m[2]) keys.add(m[2]);
    }

    for (const u of extractUrls(b)) urls.add(u);
    if (it?.html_url) urls.add(normalizeUrl(it.html_url));
  }

  return { keys, urls };
}

function labelsForSource(cfg, source) {
  const baseLabels = ensureLabels([
    ...(cfg?.defaults?.base_labels || []),
    ...(source?.labels || []),
    source?.region_hint || "",
  ]);

  const dom = pickFirstLabel(baseLabels, "D:") || cfg?.defaults?.domain_fallback || "D:NEWS_INTEL";
  const conf = pickFirstLabel(baseLabels, "CONF:") || cfg?.defaults?.confidence_fallback || "CONF:LOW";
  const sev = pickFirstLabel(baseLabels, "SEV:") || cfg?.defaults?.severity_fallback || "SEV:1";

  return ensureLabels([...baseLabels, dom, conf, sev]);
}

function prefixForType(type) {
  if (type === "rss") return "RSS";
  if (type === "html") return "HTML";
  if (type === "json") return "JSON";
  if (type === "social_x" || type === "social_bsky") return "SOCIAL";
  return "INGEST";
}

// -------------------- main ingest --------------------

async function main() {
  console.log(`${PRODUCT} ingest start:`, isoNow());

  if (!fs.existsSync(CFG_PATH)) {
    throw new Error(`Missing ${CFG_PATH}`);
  }

  const raw = fs.readFileSync(CFG_PATH, "utf8");
  const cfg = YAML.parse(raw);

  const { owner, repo } = parseRepo();

  const socialBase = must(
    cfg?.endpoints?.social_bridge_base,
    "endpoints.social_bridge_base missing in config/sources.yml"
  );

  const sourcesRaw = Array.isArray(cfg?.sources) ? cfg.sources : [];
  if (!sourcesRaw.length) throw new Error("No sources[] in config/sources.yml");

  const sources = sourcesRaw
    .filter((s) => s && s.enabled !== false)
    .sort((a, b) => sourcePriority(b) - sourcePriority(a));

  console.log(`Repo: ${owner}/${repo}`);
  console.log(`Sources enabled: ${sources.length}/${sourcesRaw.length}`);
  console.log(`Social bridge: ${socialBase}`);
  console.log(`Max created per run: ${MAX_CREATED_PER_RUN}`);
  console.log(`Default max items per source: ${MAX_ITEMS_PER_SOURCE}`);
  console.log(`Auto-create labels: ${AUTO_CREATE_LABELS ? "yes" : "no"}`);

  const existingLabels = await listAllLabels(owner, repo);
  console.log(`GitHub labels available: ${existingLabels.size}`);

  const recent = await listRecentIssues(owner, repo, RECENT_ISSUE_PAGES);
  const seen = existingDedupe(recent);
  console.log(`Recent issues scanned: ${recent.length}`);
  console.log(`Known dedupe keys: ${seen.keys.size}`);
  console.log(`Known issue URLs: ${seen.urls.size}`);

  let created = 0;
  let checkedSources = 0;
  let failedSources = 0;
  let skippedOld = 0;
  let skippedLowScore = 0;
  let skippedDuplicate = 0;

  const runAt = isoNow();

  for (const source of sources) {
    if (created >= MAX_CREATED_PER_RUN) break;

    const type = norm(source?.type);
    const sourceId = norm(source?.id);
    const sourceName = norm(source?.name || source?.id || "unknown_source");

    if (!sourceId) {
      console.log("Skipping source without id");
      continue;
    }

    checkedSources++;

    try {
      console.log(`SOURCE: ${sourceId} type=${type} priority=${sourcePriority(source)} lookback=${sourceLookbackHours(source)}h`);
      const fetched = await fetchSourceItems(source, socialBase);
      const limit = sourceItemLimit(source);
      const candidates = fetched.slice(0, Math.max(limit * 4, limit));
      console.log(`Items fetched for ${sourceId}: ${fetched.length}; candidates checked: ${candidates.length}; create limit: ${limit}`);

      let createdForSource = 0;
      const wantedLabels = labelsForSource(cfg, source);
      const labels = await safeLabels(owner, repo, wantedLabels, existingLabels, sourceId);

      for (const item of candidates) {
        if (created >= MAX_CREATED_PER_RUN) break;
        if (createdForSource >= limit) break;

        const link = norm(item.link);
        const normalizedLink = normalizeUrl(link);
        const title = norm(item.title) || link || sourceName;
        const text = stripHtml(item.summary || item.text || title || link);
        const published_at = safeDateIso(item.published_at || item.updated_at || item.date);

        if (!link && !text) continue;

        if (!passesLookback({ ...item, published_at }, source)) {
          skippedOld++;
          continue;
        }

        const relevance = passesRelevance({ ...item, title, summary: text, link }, source);
        if (!relevance.ok) {
          skippedLowScore++;
          continue;
        }

        const { body, key } = buildIssueBody({
          source,
          platform: item.platform || item.parser || type,
          link,
          published_at,
          text: short(text || link, 1800),
          extra: `Ingest run: ${runAt}`,
          itemScore: relevance.score,
          keywordHits: relevance.hits,
        });

        if ((key && seen.keys.has(key)) || (normalizedLink && seen.urls.has(normalizedLink))) {
          skippedDuplicate++;
          continue;
        }

        const issueTitle = `[${prefixForType(type)}] ${short(title, 105)}`;
        await createIssue(owner, repo, issueTitle, body, labels);

        if (key) seen.keys.add(key);
        if (normalizedLink) seen.urls.add(normalizedLink);

        created++;
        createdForSource++;
        console.log(`Created issue ${created}: ${sourceId} | score=${relevance.score} | ${short(title, 90)}`);
      }

      console.log(`Created for ${sourceId}: ${createdForSource}`);
    } catch (e) {
      failedSources++;
      console.log(`Source failed: ${sourceId} (${type})`);
      console.log(e?.stack || e?.message || String(e));
      continue;
    }
  }

  console.log(`${PRODUCT} ingest done.`);
  console.log(`Sources checked: ${checkedSources}`);
  console.log(`Sources failed: ${failedSources}`);
  console.log(`Issues created: ${created}`);
  console.log(`Skipped duplicate: ${skippedDuplicate}`);
  console.log(`Skipped old: ${skippedOld}`);
  console.log(`Skipped low score: ${skippedLowScore}`);
}

main().catch((err) => {
  console.error("FATAL ingest error:");
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
