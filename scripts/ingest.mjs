import fs from "fs";
import YAML from "yaml";
import { XMLParser } from "fast-xml-parser";

const CFG_PATH = "config/sources.yml";
const DEFAULT_OWNER = "mowlsint";
const DEFAULT_REPO = "Voodoo_Dashboard";

const MAX_CREATED_PER_RUN = Number(process.env.MAX_CREATED_PER_RUN || 25);
const MAX_ITEMS_PER_SOURCE = Number(process.env.MAX_ITEMS_PER_SOURCE || 10);
const RECENT_ISSUE_PAGES = Number(process.env.RECENT_ISSUE_PAGES || 5);

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

function stripHtml(s) {
  s = String(s ?? "");
  s = s.replace(/<script[\s\S]*?<\/script>/gi, "");
  s = s.replace(/<style[\s\S]*?<\/style>/gi, "");
  s = s.replace(/<br\s*\/?>/gi, "\n");
  s = s.replace(/<\/(p|div|li|tr|h1|h2|h3|h4|h5|h6)>/gi, "\n");
  s = s.replace(/<[^>]+>/g, "");
  s = s.replace(/&nbsp;/g, " ");
  s = s.replace(/&amp;/g, "&");
  s = s.replace(/&quot;/g, "\"");
  s = s.replace(/&#39;/g, "'");
  s = s.replace(/&apos;/g, "'");
  s = s.replace(/\r/g, "");
  s = s.replace(/[ \t]+\n/g, "\n");
  s = s.replace(/\n{2,}/g, "\n");
  return s.trim();
}

function ensureLabels(arr) {
  return Array.from(
    new Set(
      (arr || [])
        .map(String)
        .map((s) => s.trim())
        .filter(Boolean)
    )
  );
}

function pickFirstLabel(labels, prefix) {
  return (labels || []).find((l) => typeof l === "string" && l.startsWith(prefix)) || null;
}

function hashKey(s) {
  // simple stable dedupe key, safe for GitHub issue body marker
  const input = String(s ?? "");
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(16).padStart(8, "0");
}

function makeAbsoluteUrl(baseUrl, maybeUrl) {
  const raw = norm(maybeUrl);
  if (!raw) return "";
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

// -------------------- fetch helpers --------------------

async function fetchText(url, headers = {}, timeoutMs = 15000) {
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

async function fetchJson(url, headers = {}, timeoutMs = 15000) {
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
      "User-Agent": "voodoo-ingest",
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

  for (let p = 1; p <= 10; p++) {
    const items = await ghRequest(`/repos/${owner}/${repo}/labels?per_page=100&page=${p}`);
    for (const it of items || []) {
      if (it?.name) labels.add(it.name);
    }
    if (!items || items.length < 100) break;
  }

  return labels;
}

function safeLabels(wanted, existingLabels, sourceId) {
  const out = [];
  const missing = [];

  for (const label of ensureLabels(wanted)) {
    if (existingLabels.has(label)) out.push(label);
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

// -------------------- RSS / Atom parsing --------------------

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
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "@_",
    trimValues: true,
  });

  const data = parser.parse(xml);
  const items = [];

  // RSS 2.0
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
        published_at: safeDateIso(it?.pubDate || it?.published || it?.updated),
        summary: stripHtml(description),
      });
    }

    return items.filter((x) => x.title || x.link).slice(0, 50);
  }

  // Atom
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
      });
    }

    return items.filter((x) => x.title || x.link).slice(0, 50);
  }

  return [];
}

// -------------------- Issue body / dedupe --------------------

function buildIssueBody({ sourceId, sourceName, platform, link, published_at, text, extra }) {
  const cleanText = stripHtml(text || "");
  const cleanLink = norm(link);

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
  lines.push(cleanText);
  lines.push("");

  if (extra) {
    lines.push("### Extra");
    lines.push(extra);
    lines.push("");
  }

  const keyMaterial = [
    cleanLink,
    cleanText.slice(0, 300),
    sourceId,
  ].join("|");

  const key = hashKey(keyMaterial);
  lines.push(`<!-- VOODOO_INGEST: ${key} SOURCE=${sourceId} -->`);

  return {
    body: lines.join("\n"),
    key,
  };
}

function existingDedupeSet(issues) {
  const seen = new Set();

  for (const it of issues || []) {
    const b = it?.body || "";
    const m = b.match(/VOODOO_INGEST:\s*([A-Za-z0-9+/=_-]+)/);
    if (m) seen.add(m[1]);
  }

  return seen;
}

// -------------------- main ingest --------------------

async function main() {
  console.log("VOODOO ingest start:", isoNow());

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

  const sources = Array.isArray(cfg?.sources) ? cfg.sources : [];
  if (!sources.length) throw new Error("No sources[] in config/sources.yml");

  console.log(`Repo: ${owner}/${repo}`);
  console.log(`Sources: ${sources.length}`);
  console.log(`Social bridge: ${socialBase}`);
  console.log(`Max created per run: ${MAX_CREATED_PER_RUN}`);
  console.log(`Max items per source: ${MAX_ITEMS_PER_SOURCE}`);

  const existingLabels = await listAllLabels(owner, repo);
  console.log(`GitHub labels available: ${existingLabels.size}`);

  const recent = await listRecentIssues(owner, repo, RECENT_ISSUE_PAGES);
  const seen = existingDedupeSet(recent);
  console.log(`Recent issues scanned: ${recent.length}`);
  console.log(`Known dedupe keys: ${seen.size}`);

  let created = 0;
  let checkedSources = 0;
  let failedSources = 0;

  const runAt = isoNow();

  for (const s of sources) {
    if (created >= MAX_CREATED_PER_RUN) break;

    const type = norm(s?.type);
    const sourceId = norm(s?.id);
    const sourceName = norm(s?.name || s?.id || "unknown_source");

    if (!sourceId) {
      console.log("Skipping source without id");
      continue;
    }

    checkedSources++;

    const baseLabels = ensureLabels([
      ...(cfg?.defaults?.base_labels || []),
      ...(s?.labels || []),
      s?.region_hint || "",
    ]);

    const dom = pickFirstLabel(baseLabels, "D:") || cfg?.defaults?.domain_fallback || "D:NEWS_INTEL";
    const conf = pickFirstLabel(baseLabels, "CONF:") || cfg?.defaults?.confidence_fallback || "CONF:LOW";
    const sev = pickFirstLabel(baseLabels, "SEV:") || cfg?.defaults?.severity_fallback || "SEV:1";

    const wantedLabels = ensureLabels([...baseLabels, dom, conf, sev]);
    const labels = safeLabels(wantedLabels, existingLabels, sourceId);

    try {
      // RSS / Atom sources
      if (type === "rss") {
        const url = norm(s?.url);
        if (!url) {
          console.log(`Skipping RSS source without url: ${sourceId}`);
          continue;
        }

        console.log(`RSS: ${sourceId} -> ${url}`);

        const xml = await fetchText(url, { "User-Agent": "voodoo-ingest" }, 15000);
        const items = parseRss(xml, url).slice(0, MAX_ITEMS_PER_SOURCE);

        console.log(`RSS items parsed for ${sourceId}: ${items.length}`);

        for (const it of items) {
          if (created >= MAX_CREATED_PER_RUN) break;

          const link = norm(it.link);
          const title = norm(it.title) || link || sourceName;
          const text = stripHtml(it.summary || title || link);

          if (!link && !text) continue;

          const { body, key } = buildIssueBody({
            sourceId,
            sourceName,
            platform: "rss",
            link,
            published_at: it.published_at,
            text: short(text, 1500),
            extra: `Ingest run: ${runAt}`,
          });

          if (key && seen.has(key)) continue;

          await createIssue(owner, repo, `[RSS] ${short(title, 90)}`, body, labels);

          if (key) seen.add(key);
          created++;
          console.log(`Created RSS issue ${created}: ${sourceId} | ${short(title, 80)}`);
        }

        continue;
      }

      // Social bridge sources: X/Nitter + Bluesky
      if (type === "social_x" || type === "social_bsky") {
        const bridgePath = norm(s?.bridge_path);
        if (!bridgePath) {
          console.log(`Skipping social source without bridge_path: ${sourceId}`);
          continue;
        }

        const limit = 30;
        const url = socialBase.replace(/\/+$/, "") + bridgePath + "?limit=" + limit;

        console.log(`SOCIAL: ${sourceId} -> ${url}`);

        const data = await fetchJson(url, { "User-Agent": "voodoo-ingest" }, 20000);
        if (data?.ok === false) {
          throw new Error(data?.error || "social bridge returned ok=false");
        }

        const items = Array.isArray(data?.items) ? data.items.slice(0, MAX_ITEMS_PER_SOURCE) : [];
        console.log(`Social items parsed for ${sourceId}: ${items.length}`);

        for (const it of items) {
          if (created >= MAX_CREATED_PER_RUN) break;

          const link = norm(it.link);
          const text = stripHtml(norm(it.text || it.title || ""));
          const published_at = safeDateIso(it.published_at);

          if (!link && !text) continue;

          const platform = it.platform || (type === "social_x" ? "x" : "bsky");

          const { body, key } = buildIssueBody({
            sourceId,
            sourceName,
            platform,
            link,
            published_at,
            text: short(text || link, 1500),
            extra: `Ingest run: ${runAt}`,
          });

          if (key && seen.has(key)) continue;

          const issueTitle = `[SOCIAL] ${short(text || link || sourceName, 90)}`;
          await createIssue(owner, repo, issueTitle, body, labels);

          if (key) seen.add(key);
          created++;
          console.log(`Created SOCIAL issue ${created}: ${sourceId} | ${short(text || link, 80)}`);
        }

        continue;
      }

      // HTML / JSON currently intentionally not auto-ingested.
      // Reason: raw HTML pages are too noisy and should become explicit parsers later.
      if (type === "html" || type === "json") {
        console.log(`Skipping ${type} source for now: ${sourceId}`);
        continue;
      }

      console.log(`Unknown source type skipped: ${sourceId} (${type})`);
    } catch (e) {
      failedSources++;
      console.log(`Source failed: ${sourceId} (${type})`);
      console.log(e?.stack || e?.message || String(e));
      // Important: do not fail the whole hourly run because one feed/bridge/source is down.
      continue;
    }
  }

  console.log("VOODOO ingest done.");
  console.log(`Sources checked: ${checkedSources}`);
  console.log(`Sources failed: ${failedSources}`);
  console.log(`Issues created: ${created}`);
}

main().catch((err) => {
  console.error("FATAL ingest error:");
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
