/**
 * MAGIC PAWS // MARITIME PHASE ZERO
 * ingest.mjs
 *
 * Reads config/sources.yml and creates GitHub issues from RSS/Atom feeds
 * and optional social bridge sources.
 *
 * Requirements in GitHub Action:
 *   npm install yaml fast-xml-parser
 *
 * Required env:
 *   GITHUB_TOKEN
 *
 * Optional env:
 *   GITHUB_REPOSITORY      normally set by GitHub Actions, e.g. mowlsint/MagicPaws
 *   SOURCES_FILE           default: config/sources.yml
 *   SOCIAL_BRIDGE_BASE     fallback for social_x bridge_path
 *   MAX_ITEMS_PER_SOURCE   fallback default: 15
 *   MAX_EXISTING_ISSUES    fallback default: 1000
 *   DRY_RUN                "1" = do not create issues
 */

import fs from "node:fs/promises";
import crypto from "node:crypto";
import YAML from "yaml";
import { XMLParser } from "fast-xml-parser";

const VERSION = 'MAGIC PAWS ingest v1.0 "Source Published Time + Social Bridge"';

const SOURCES_FILE = process.env.SOURCES_FILE || "config/sources.yml";
const DEFAULT_SOCIAL_BRIDGE_BASE =
  process.env.SOCIAL_BRIDGE_BASE ||
  "https://voodoo-social-bridge.mowlsint.workers.dev";

const MAX_ITEMS_PER_SOURCE = Number(process.env.MAX_ITEMS_PER_SOURCE || 15);
const MAX_EXISTING_ISSUES = Number(process.env.MAX_EXISTING_ISSUES || 1000);
const DRY_RUN = process.env.DRY_RUN === "1";

const GITHUB_TOKEN = process.env.GITHUB_TOKEN || "";
const GITHUB_REPOSITORY = process.env.GITHUB_REPOSITORY || "mowlsint/MagicPaws";
const [OWNER, REPO] = GITHUB_REPOSITORY.split("/");

if (!OWNER || !REPO) {
  throw new Error(`Invalid GITHUB_REPOSITORY: ${GITHUB_REPOSITORY}`);
}

if (!GITHUB_TOKEN && !DRY_RUN) {
  throw new Error("GITHUB_TOKEN missing. Set it in the GitHub Action env.");
}

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
  textNodeName: "#text",
  cdataPropName: "#cdata",
  trimValues: true,
  parseTagValue: false,
  parseAttributeValue: false,
  removeNSPrefix: false
});

// -----------------------------------------------------------------------------
// Generic helpers
// -----------------------------------------------------------------------------

function nowIso() {
  return new Date().toISOString();
}

function sha256Short(value, len = 16) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex").slice(0, len);
}

function asArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

function asPlainText(value) {
  if (value == null) return "";

  if (typeof value === "string") return value.trim();
  if (typeof value === "number") return String(value);
  if (typeof value === "boolean") return String(value);

  if (Array.isArray(value)) {
    return value.map(asPlainText).filter(Boolean).join(" ").trim();
  }

  if (typeof value === "object") {
    return String(
      value["#cdata"] ??
      value["#text"] ??
      value._text ??
      value.text ??
      value.value ??
      value.href ??
      ""
    ).trim();
  }

  return String(value).trim();
}

function stripHtml(value) {
  return asPlainText(value)
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>|<\/div>|<\/li>|<\/tr>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/\s+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function truncate(value, max = 4000) {
  const text = String(value || "");
  if (text.length <= max) return text;
  return text.slice(0, max - 20).trimEnd() + "\n\n[…gekürzt]";
}

function normalizeUrl(url) {
  const raw = asPlainText(url);
  if (!raw) return "";

  try {
    const u = new URL(raw);

    const removeParams = [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_term",
      "utm_content",
      "utm_id",
      "fbclid",
      "gclid",
      "mc_cid",
      "mc_eid",
      "igshid"
    ];

    for (const p of removeParams) u.searchParams.delete(p);

    u.hash = "";
    return u.toString();
  } catch {
    return raw.trim();
  }
}

function toIsoDateOrNull(value) {
  const raw = asPlainText(value);
  if (!raw) return null;

  const d = new Date(raw);
  if (!Number.isFinite(d.getTime())) return null;

  return d.toISOString();
}

function pickSourcePublishedAt(item) {
  return (
    toIsoDateOrNull(item?.source_published_at) ||
    toIsoDateOrNull(item?.published_at) ||
    toIsoDateOrNull(item?.publishedAt) ||
    toIsoDateOrNull(item?.pubDate) ||
    toIsoDateOrNull(item?.pubdate) ||
    toIsoDateOrNull(item?.isoDate) ||
    toIsoDateOrNull(item?.published) ||
    toIsoDateOrNull(item?.updated) ||
    toIsoDateOrNull(item?.date) ||
    toIsoDateOrNull(item?.created) ||
    toIsoDateOrNull(item?.created_at) ||
    toIsoDateOrNull(item?.timestamp) ||
    toIsoDateOrNull(item?.ts) ||
    toIsoDateOrNull(item?.["dc:date"]) ||
    toIsoDateOrNull(item?.["atom:updated"]) ||
    null
  );
}

function yamlScalarArray(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value.map(String).map(x => x.trim()).filter(Boolean);
  return [String(value).trim()].filter(Boolean);
}

function uniqueStrings(values) {
  return Array.from(
    new Set(
      (values || [])
        .map(x => String(x || "").trim())
        .filter(Boolean)
    )
  );
}

function safeIssueTitle(title, prefix = "") {
  const t = stripHtml(title).replace(/\s+/g, " ").trim() || "Untitled maritime source item";
  const full = `${prefix}${t}`.trim();
  return full.length <= 240 ? full : full.slice(0, 237).trimEnd() + "…";
}

function labelColorFor(label) {
  if (label.startsWith("SRC:")) return "7057ff";
  if (label.startsWith("D:")) return "d93f0b";
  if (label.startsWith("REG:")) return "0e8a16";
  if (label.startsWith("CONF:")) return "5319e7";
  if (label.startsWith("SEV:")) return "fbca04";
  if (label.startsWith("MAP:")) return "cfd3d7";
  if (label.startsWith("DECAY:")) return "bfdadc";
  if (label.startsWith("SCORE:")) return "1d76db";
  if (label.startsWith("V:")) return "0052cc";
  if (label.startsWith("PAT:")) return "006b75";
  return "ededed";
}

function normalizeLabels(source, item = {}) {
  const sourceLabels = yamlScalarArray(source.labels);
  const itemLabels = yamlScalarArray(item.labels);

  const labels = [
    ...sourceLabels,
    ...itemLabels
  ];

  if (source.region_hint && !labels.some(l => l.startsWith("REG:"))) {
    labels.push(String(source.region_hint));
  }

  if (!labels.some(l => l.startsWith("SRC:"))) labels.push("SRC:OSINT");
  if (!labels.some(l => l.startsWith("D:"))) labels.push("D:NEWS_INTEL");
  if (!labels.some(l => l.startsWith("CONF:"))) labels.push("CONF:LOW");
  if (!labels.some(l => l.startsWith("SEV:"))) labels.push("SEV:1");

  return uniqueStrings(labels);
}

function makeIngestId(source, item) {
  const stable = [
    source.id || source.name || "source",
    normalizeUrl(item.link || item.url || item.guid || item.id || ""),
    asPlainText(item.guid || item.id || ""),
    asPlainText(item.title || ""),
    pickSourcePublishedAt(item) || ""
  ].join("|");

  return `${source.id || "source"}:${sha256Short(stable, 20)}`;
}

function makeBody({ source, item, link, title, text, labels, ingestId, sourcePublishedAt, ingestedAt }) {
  const platform =
    source.platform ||
    source.type ||
    item.platform ||
    "unknown";

  const sourceLine = `${source.name || source.id || "Unknown source"} (${source.id || "no_id"})`;

  return [
    `<!-- MAGIC_PAWS_INGEST_ID:${ingestId} -->`,
    `<!-- VOODOO_INGEST_ID:${ingestId} -->`,
    "",
    "### Text",
    truncate(text || title || "", 5000),
    "",
    "### Zeit (UTC)",
    `source_published_at: ${sourcePublishedAt || ""}`,
    `ingested_at: ${ingestedAt}`,
    "",
    "### Quelle",
    sourceLine,
    "",
    "### Plattform",
    String(platform),
    "",
    "### Link",
    link || "",
    "",
    "### Auto-Labels",
    labels.join(", "),
    "",
    "---",
    `ingest_version: ${VERSION}`
  ].join("\n");
}

// -----------------------------------------------------------------------------
// Config loading
// -----------------------------------------------------------------------------

async function loadSourcesConfig() {
  const raw = await fs.readFile(SOURCES_FILE, "utf8");
  const parsed = YAML.parse(raw);

  if (Array.isArray(parsed)) {
    return {
      endpoints: {},
      sources: parsed
    };
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error(`${SOURCES_FILE} is empty or invalid.`);
  }

  const sources =
    Array.isArray(parsed.sources) ? parsed.sources :
    Array.isArray(parsed.feeds) ? parsed.feeds :
    [];

  return {
    ...parsed,
    endpoints: parsed.endpoints || {},
    sources
  };
}

function enabledSources(config) {
  return (config.sources || [])
    .filter(s => s && s.enabled !== false)
    .filter(s => s.type && (s.url || s.bridge_path || s.handle))
    .sort((a, b) => Number(b.priority || 0) - Number(a.priority || 0));
}

// -----------------------------------------------------------------------------
// GitHub API
// -----------------------------------------------------------------------------

function ghHeaders(extra = {}) {
  const h = {
    "Accept": "application/vnd.github+json",
    "User-Agent": "magic-paws-ingest",
    ...extra
  };

  if (GITHUB_TOKEN) {
    h.Authorization = `Bearer ${GITHUB_TOKEN}`;
  }

  return h;
}

async function ghRequest(path, options = {}) {
  const url = `https://api.github.com/repos/${OWNER}/${REPO}${path}`;
  const res = await fetch(url, {
    ...options,
    headers: ghHeaders(options.headers || {})
  });

  const text = await res.text().catch(() => "");
  let data = null;

  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = text;
  }

  if (!res.ok) {
    const msg = typeof data === "string" ? data : JSON.stringify(data);
    throw new Error(`GitHub ${res.status} ${res.statusText}: ${msg.slice(0, 500)}`);
  }

  return data;
}

async function fetchExistingIssues(maxItems = MAX_EXISTING_ISSUES) {
  const all = [];
  const perPage = 100;
  const maxPages = Math.ceil(maxItems / perPage);

  for (let page = 1; page <= maxPages; page++) {
    const batch = await ghRequest(
      `/issues?state=all&per_page=${perPage}&page=${page}&sort=created&direction=desc`
    );

    if (!Array.isArray(batch) || batch.length === 0) break;

    for (const issue of batch) {
      if (!issue.pull_request) all.push(issue);
    }

    if (batch.length < perPage) break;
    if (all.length >= maxItems) break;
  }

  return all.slice(0, maxItems);
}

function buildExistingIndex(issues) {
  const urls = new Set();
  const ingestIds = new Set();

  for (const issue of issues || []) {
    const body = String(issue.body || "");

    const idMatches = body.matchAll(/(?:MAGIC_PAWS_INGEST_ID|VOODOO_INGEST_ID|INGEST_ID)\s*:?\s*([A-Za-z0-9:_./-]+)/g);
    for (const m of idMatches) {
      if (m[1]) ingestIds.add(m[1].trim());
    }

    const urlMatches = body.matchAll(/https?:\/\/[^\s<>"')]+/g);
    for (const m of urlMatches) {
      urls.add(normalizeUrl(m[0]));
    }
  }

  return { urls, ingestIds };
}

async function ensureLabel(label) {
  if (DRY_RUN) return;

  try {
    await ghRequest(`/labels/${encodeURIComponent(label)}`);
    return;
  } catch {
    // create below
  }

  try {
    await ghRequest("/labels", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        name: label,
        color: labelColorFor(label),
        description: "Created by MAGIC PAWS ingest"
      })
    });
  } catch (e) {
    console.warn(`Could not ensure label "${label}": ${e.message}`);
  }
}

async function createIssue({ title, body, labels }) {
  if (DRY_RUN) {
    console.log(`[DRY_RUN] Would create issue: ${title}`);
    return { dry_run: true, title };
  }

  for (const label of labels) {
    await ensureLabel(label);
  }

  try {
    return await ghRequest("/issues", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title, body, labels })
    });
  } catch (e) {
    console.warn(`Issue creation with labels failed, retrying without labels: ${e.message}`);

    return await ghRequest("/issues", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title, body })
    });
  }
}

// -----------------------------------------------------------------------------
// RSS / Atom parsing
// -----------------------------------------------------------------------------

function extractAtomLink(entry) {
  const links = asArray(entry.link);
  if (!links.length) return "";

  for (const l of links) {
    if (typeof l === "string") return l;
    if (l?.["@_rel"] === "alternate" && l?.["@_href"]) return l["@_href"];
  }

  const first = links[0];
  return asPlainText(first?.["@_href"] || first?.href || first);
}

function normalizeRssItem(raw, source) {
  const item = raw || {};

  const title =
    asPlainText(item.title) ||
    asPlainText(item["media:title"]) ||
    asPlainText(item.guid) ||
    "Untitled RSS item";

  const link =
    normalizeUrl(
      item.link ||
      item.guid?.["#text"] ||
      item.guid ||
      item.id ||
      extractAtomLink(item)
    );

  const description =
    stripHtml(
      item.description ||
      item.summary ||
      item.content ||
      item["content:encoded"] ||
      item["media:description"] ||
      item.title ||
      ""
    );

  const sourcePublishedAt = pickSourcePublishedAt(item);

  return {
    raw: item,
    source_id: source.id,
    platform: "rss",
    title,
    link,
    url: link,
    guid: asPlainText(item.guid || item.id || link),
    text: description,
    source_published_at: sourcePublishedAt
  };
}

function extractFeedItems(parsed) {
  if (parsed?.rss?.channel?.item) {
    return asArray(parsed.rss.channel.item);
  }

  if (parsed?.feed?.entry) {
    return asArray(parsed.feed.entry);
  }

  if (parsed?.rdf?.item) {
    return asArray(parsed.rdf.item);
  }

  return [];
}

async function fetchRssSource(source) {
  const res = await fetch(source.url, {
    headers: {
      "Accept": "application/rss+xml, application/atom+xml, application/xml, text/xml, */*",
      "User-Agent": "MAGIC-PAWS-RSS-Ingest/1.0"
    }
  });

  const text = await res.text().catch(() => "");

  if (!res.ok) {
    throw new Error(`RSS fetch failed ${res.status}: ${text.slice(0, 300)}`);
  }

  const parsed = xmlParser.parse(text);
  const rawItems = extractFeedItems(parsed);

  return rawItems.map(item => normalizeRssItem(item, source));
}

// -----------------------------------------------------------------------------
// Social bridge parsing
// -----------------------------------------------------------------------------

function socialBridgeBase(config) {
  return (
    config?.endpoints?.social_bridge_base ||
    config?.social_bridge_base ||
    DEFAULT_SOCIAL_BRIDGE_BASE
  ).replace(/\/+$/, "");
}

function socialBridgeUrl(source, config) {
  const base = socialBridgeBase(config);

  if (source.bridge_url) return source.bridge_url;

  if (source.bridge_path) {
    const path = String(source.bridge_path).startsWith("/")
      ? String(source.bridge_path)
      : `/${source.bridge_path}`;
    return `${base}${path}`;
  }

  if (source.handle) {
    return `${base}/x/${encodeURIComponent(String(source.handle).replace(/^@/, ""))}`;
  }

  throw new Error(`social_x source ${source.id} has no bridge_path, bridge_url or handle.`);
}

function extractSocialItems(payload) {
  if (Array.isArray(payload)) return payload;

  return (
    asArray(payload?.items).length ? asArray(payload.items) :
    asArray(payload?.posts).length ? asArray(payload.posts) :
    asArray(payload?.events).length ? asArray(payload.events) :
    asArray(payload?.data).length ? asArray(payload.data) :
    []
  );
}

function normalizeSocialItem(raw, source) {
  const item = raw || {};

  const handle =
    item.handle ||
    item.user ||
    item.username ||
    source.handle ||
    source.name ||
    source.id;

  const title =
    asPlainText(item.title) ||
    asPlainText(item.text).slice(0, 120) ||
    asPlainText(item.body).slice(0, 120) ||
    `Social item ${handle}`;

  const link =
    normalizeUrl(
      item.url ||
      item.link ||
      item.permalink ||
      item.tweet_url ||
      item.post_url ||
      ""
    );

  const text =
    stripHtml(
      item.text ||
      item.body ||
      item.content ||
      item.description ||
      item.summary ||
      title
    );

  const sourcePublishedAt = pickSourcePublishedAt(item);

  return {
    raw: item,
    source_id: source.id,
    platform: "social_x",
    handle,
    title,
    link,
    url: link,
    guid: asPlainText(item.id || item.guid || item.post_id || link || title),
    text,
    source_published_at: sourcePublishedAt,
    labels: item.labels
  };
}

async function fetchSocialSource(source, config) {
  const url = socialBridgeUrl(source, config);

  const res = await fetch(url, {
    headers: {
      "Accept": "application/json",
      "User-Agent": "MAGIC-PAWS-Social-Ingest/1.0"
    }
  });

  const text = await res.text().catch(() => "");

  if (!res.ok) {
    throw new Error(`Social bridge fetch failed ${res.status}: ${text.slice(0, 300)}`);
  }

  let payload;
  try {
    payload = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`Social bridge returned non-JSON response: ${text.slice(0, 300)}`);
  }

  return extractSocialItems(payload).map(item => normalizeSocialItem(item, source));
}

// -----------------------------------------------------------------------------
// Source processing
// -----------------------------------------------------------------------------

async function fetchSourceItems(source, config) {
  const type = String(source.type || "").toLowerCase();

  if (type === "rss" || type === "atom" || type === "feed") {
    return fetchRssSource(source);
  }

  if (type === "social_x" || type === "x" || type === "twitter") {
    return fetchSocialSource(source, config);
  }

  console.warn(`Skipping unsupported source type "${source.type}" for ${source.id || source.name}`);
  return [];
}

function itemPassesBasicQuality(item) {
  const title = asPlainText(item.title);
  const text = asPlainText(item.text);
  const link = normalizeUrl(item.link || item.url || "");

  if (!title && !text) return false;

  // Linkless social items may still be useful, but RSS items should normally have links.
  if (item.platform === "rss" && !link) return false;

  return true;
}

async function processSource(source, config, existingIndex) {
  const sourceId = source.id || source.name || "unknown_source";
  const maxItems = Number(source.max_items || source.max_items_per_run || MAX_ITEMS_PER_SOURCE);

  const stats = {
    source_id: sourceId,
    source_name: source.name || sourceId,
    type: source.type,
    fetched: 0,
    considered: 0,
    created: 0,
    skipped_duplicate: 0,
    skipped_quality: 0,
    errors: []
  };

  console.log(`\n=== Source: ${sourceId} (${source.type}) ===`);

  let items = [];

  try {
    items = await fetchSourceItems(source, config);
  } catch (e) {
    stats.errors.push(e.message);
    console.error(`Fetch failed for ${sourceId}: ${e.message}`);
    return stats;
  }

  stats.fetched = items.length;

  const limited = items.slice(0, maxItems);
  stats.considered = limited.length;

  for (const item of limited) {
    try {
      if (!itemPassesBasicQuality(item)) {
        stats.skipped_quality++;
        continue;
      }

      const link = normalizeUrl(item.link || item.url || "");
      const ingestId = makeIngestId(source, item);

      if (existingIndex.ingestIds.has(ingestId) || (link && existingIndex.urls.has(link))) {
        stats.skipped_duplicate++;
        continue;
      }

      const labels = normalizeLabels(source, item);
      const sourcePublishedAt = pickSourcePublishedAt(item);
      const ingestedAt = nowIso();

      const prefix =
        item.platform === "social_x" ? "[X]" :
        item.platform === "rss" ? "[RSS]" :
        "[OSINT]";

      const title = safeIssueTitle(item.title, `${prefix} `);

      const body = makeBody({
        source,
        item,
        link,
        title,
        text: item.text || item.title,
        labels,
        ingestId,
        sourcePublishedAt,
        ingestedAt
      });

      const created = await createIssue({ title, body, labels });

      stats.created++;

      existingIndex.ingestIds.add(ingestId);
      if (link) existingIndex.urls.add(link);

      const issueNo = created?.number ? `#${created.number}` : "";
      console.log(`Created ${issueNo}: ${title}`);
    } catch (e) {
      stats.errors.push(e.message);
      console.error(`Item failed in ${sourceId}: ${e.message}`);
    }
  }

  console.log(
    `Done ${sourceId}: fetched=${stats.fetched}, considered=${stats.considered}, created=${stats.created}, duplicate=${stats.skipped_duplicate}, quality_skip=${stats.skipped_quality}`
  );

  return stats;
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

async function main() {
  console.log(`${VERSION}`);
  console.log(`Repository: ${OWNER}/${REPO}`);
  console.log(`Sources file: ${SOURCES_FILE}`);
  console.log(`Dry run: ${DRY_RUN ? "yes" : "no"}`);

  const config = await loadSourcesConfig();
  const sources = enabledSources(config);

  console.log(`Enabled sources: ${sources.length}`);

  const existingIssues = DRY_RUN ? [] : await fetchExistingIssues(MAX_EXISTING_ISSUES);
  const existingIndex = buildExistingIndex(existingIssues);

  console.log(`Existing issues indexed: ${existingIssues.length}`);
  console.log(`Existing issue URLs indexed: ${existingIndex.urls.size}`);
  console.log(`Existing ingest IDs indexed: ${existingIndex.ingestIds.size}`);

  const results = [];

  for (const source of sources) {
    const result = await processSource(source, config, existingIndex);
    results.push(result);
  }

  const summary = {
    ok: true,
    version: VERSION,
    repository: `${OWNER}/${REPO}`,
    sources_total: sources.length,
    fetched_total: results.reduce((a, x) => a + x.fetched, 0),
    considered_total: results.reduce((a, x) => a + x.considered, 0),
    created_total: results.reduce((a, x) => a + x.created, 0),
    skipped_duplicate_total: results.reduce((a, x) => a + x.skipped_duplicate, 0),
    skipped_quality_total: results.reduce((a, x) => a + x.skipped_quality, 0),
    errors_total: results.reduce((a, x) => a + x.errors.length, 0),
    results
  };

  console.log("\n=== INGEST SUMMARY ===");
  console.log(JSON.stringify(summary, null, 2));

  if (summary.errors_total > 0) {
    process.exitCode = 1;
  }
}

main().catch(err => {
  console.error("Fatal ingest error:", err);
  process.exitCode = 1;
});
