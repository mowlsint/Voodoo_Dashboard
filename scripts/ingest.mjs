import fs from "fs";
import YAML from "yaml";
import { XMLParser } from "fast-xml-parser";

const CFG_PATH = "config/sources.yml";

function must(v, msg) { if (!v) throw new Error(msg); return v; }
function norm(s) { return String(s ?? "").trim(); }
function isoNow() { return new Date().toISOString(); }

function pickFirstLabel(labels, prefix) {
  return (labels || []).find(l => typeof l === "string" && l.startsWith(prefix)) || null;
}

function ensureLabels(arr) {
  return Array.from(new Set((arr || []).map(String).map(s => s.trim()).filter(Boolean)));
}

function short(s, n = 280) {
  s = String(s ?? "");
  return s.length > n ? (s.slice(0, n) + "…") : s;
}

function hashKey(s) {
  // stable-ish marker key
  return Buffer.from(String(s)).toString("base64").replace(/=+$/g, "");
}

async function ghRequest(path, method = "GET", body = null) {
  const token = process.env.GH_TOKEN;
  must(token, "Missing GH_TOKEN (actions should provide secrets.GITHUB_TOKEN)");
  const url = `https://api.github.com${path}`;
  const res = await fetch(url, {
    method,
    headers: {
      "Accept": "application/vnd.github+json",
      "Authorization": `Bearer ${token}`,
      "User-Agent": "voodoo-ingest",
      ...(body ? { "Content-Type": "application/json" } : {})
    },
    body: body ? JSON.stringify(body) : null
  });
  const txt = await res.text().catch(() => "");
  if (!res.ok) throw new Error(`GitHub API ${res.status} ${method} ${path}: ${txt.slice(0, 300)}`);
  return txt ? JSON.parse(txt) : null;
}

async function listRecentIssues(owner, repo, pages = 3) {
  // Pull a few pages (covers most setups). Increase if you have thousands.
  const out = [];
  for (let p = 1; p <= pages; p++) {
    const items = await ghRequest(`/repos/${owner}/${repo}/issues?state=all&per_page=100&page=${p}`);
    for (const it of items) if (!it.pull_request) out.push(it);
    if (!items || items.length < 100) break;
  }
  return out;
}

function parseRss(xml) {
  const parser = new XMLParser({ ignoreAttributes: false, attributeNamePrefix: "@_" });
  const data = parser.parse(xml);

  // Support RSS2 and Atom
  const items = [];
  const rssItems = data?.rss?.channel?.item;
  const atomEntries = data?.feed?.entry;

  if (rssItems) {
    const arr = Array.isArray(rssItems) ? rssItems : [rssItems];
    for (const it of arr) {
      items.push({
        title: norm(it?.title),
        link: norm(it?.link),
        published_at: it?.pubDate ? new Date(it.pubDate).toISOString() : null,
        summary: norm(it?.description || it?.["content:encoded"] || "")
      });
    }
  } else if (atomEntries) {
    const arr = Array.isArray(atomEntries) ? atomEntries : [atomEntries];
    for (const e of arr) {
      const linkObj = Array.isArray(e?.link) ? e.link.find(x => x?.["@_rel"] !== "self") : e?.link;
      const link = linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || linkObj?.["@_href"] || e?.id;
      items.push({
        title: norm(e?.title?.["#text"] ?? e?.title),
        link: norm(link),
        published_at: e?.updated ? new Date(e.updated).toISOString() : null,
        summary: norm(e?.summary?.["#text"] ?? e?.summary ?? e?.content?.["#text"] ?? e?.content ?? "")
      });
    }
  }

  // sanitize
  return items.filter(x => x.title || x.link).slice(0, 50);
}

async function fetchWithTimeout(url, ms = 15000, headers = {}) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(), ms);
  try {
    const res = await fetch(url, { headers, signal: ac.signal });
    const txt = await res.text().catch(() => "");
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return txt;
  } finally {
    clearTimeout(t);
  }
}

function buildIssueBody({ sourceId, sourceName, platform, link, published_at, text, extra }) {
  const lines = [];
  lines.push(`### Quelle`);
  lines.push(`${sourceName} (${sourceId})`);
  lines.push(``);
  if (platform) { lines.push(`### Plattform`); lines.push(platform); lines.push(""); }
  if (published_at) { lines.push(`### Zeit (UTC)`); lines.push(published_at); lines.push(""); }
  if (link) { lines.push(`### Link`); lines.push(link); lines.push(""); }
  lines.push(`### Text`);
  lines.push(text || "");
  lines.push("");
  if (extra) {
    lines.push(`### Extra`);
    lines.push(extra);
    lines.push("");
  }
  // hidden dedup marker
  const key = hashKey((link || "") + "|" + (text || "").slice(0, 120) + "|" + sourceId);
  lines.push(`<!-- VOODOO_INGEST: ${key} SOURCE=${sourceId} -->`);
  return lines.join("\n");
}

async function createIssue(owner, repo, title, body, labels) {
  return await ghRequest(`/repos/${owner}/${repo}/issues`, "POST", { title, body, labels });
}

async function main() {
  const raw = fs.readFileSync(CFG_PATH, "utf8");
  const cfg = YAML.parse(raw);

  const owner = must(cfg?.endpoints?.hybrid_api_base ? "mowlsint" : "mowlsint", "Owner missing"); // fixed for your repo
  const repo = "Voodoo_Dashboard";

  const socialBase = must(cfg?.endpoints?.social_bridge_base, "endpoints.social_bridge_base missing in sources.yml");
  const sources = Array.isArray(cfg?.sources) ? cfg.sources : [];
  if (!sources.length) throw new Error("No sources[] in config/sources.yml");

  // Build dedup set from recent issues
  const recent = await listRecentIssues(owner, repo, 5);
  const seen = new Set();
  for (const it of recent) {
    const b = it?.body || "";
    const m = b.match(/VOODOO_INGEST:\s*([A-Za-z0-9+/=_-]+)/);
    if (m) seen.add(m[1]);
  }

  let created = 0;
  const runAt = isoNow();

  for (const s of sources) {
    const type = norm(s?.type);
    const sourceId = norm(s?.id);
    const sourceName = norm(s?.name || s?.id);
    const baseLabels = ensureLabels([...(cfg?.defaults?.base_labels || []), ...(s?.labels || [])]);

    // ensure we always have a domain label
    const dom = pickFirstLabel(baseLabels, "D:") || (cfg?.defaults?.domain_fallback || "D:NEWS_INTEL");
    const conf = pickFirstLabel(baseLabels, "CONF:") || (cfg?.defaults?.confidence_fallback || "CONF:LOW");
    const sev = pickFirstLabel(baseLabels, "SEV:") || (cfg?.defaults?.severity_fallback || "SEV:1");

    let labels = ensureLabels([...baseLabels, dom, conf, sev]);

    try {
      if (type === "rss") {
        const url = norm(s?.url);
        if (!url) continue;
        const xml = await fetchWithTimeout(url, 15000, { "User-Agent": "voodoo-ingest" });
        const items = parseRss(xml).slice(0, 10);

        for (const it of items) {
          const link = norm(it.link);
          const title = norm(it.title) || (link ? link : sourceName);
          const body = buildIssueBody({
            sourceId,
            sourceName,
            platform: "rss",
            link,
            published_at: it.published_at,
            text: short(stripHtml(it.summary || ""), 1200),
            extra: `Ingest run: ${runAt}`
          });

          const key = (body.match(/VOODOO_INGEST:\s*([A-Za-z0-9+/=_-]+)/) || [])[1];
          if (key && seen.has(key)) continue;

          await createIssue(owner, repo, `[RSS] ${short(title, 90)}`, body, labels);
          if (key) seen.add(key);
          created++;
          if (created >= 25) break; // guard
        }
      }

      if (type === "social_x" || type === "social_bsky") {
        const bridgePath = norm(s?.bridge_path);
        if (!bridgePath) continue;

        const limit = 30;
        const url = socialBase.replace(/\/+$/, "") + bridgePath + "?limit=" + limit;
        const data = await fetchJson(url, { "User-Agent": "voodoo-ingest" });

        const items = Array.isArray(data?.items) ? data.items : [];
        for (const it of items.slice(0, 10)) {
          const link = norm(it.link);
          const text = norm(it.text || it.title || "");
          const published_at = it.published_at || null;

          const body = buildIssueBody({
            sourceId,
            sourceName,
            platform: it.platform || (type === "social_x" ? "x" : "bsky"),
            link,
            published_at,
            text: short(text, 1500),
            extra: `Ingest run: ${runAt}`
          });

          const key = (body.match(/VOODOO_INGEST:\s*([A-Za-z0-9+/=_-]+)/) || [])[1];
          if (key && seen.has(key)) continue;

          await createIssue(owner, repo, `[SOCIAL] ${short(text || link || sourceName, 90)}`, body, labels);
          if (key) seen.add(key);
          created++;
          if (created >= 25) break;
        }
      }

      // html sources: for now we do NOT auto-create issues (too noisy)
      // They are still useful via your /api/navwarn and /api/weather worker windows.
    } catch (e) {
      // optional: create a low-sev issue once? for now just log
      console.log("Source failed:", sourceId, e?.message || String(e));
    }

    if (created >= 25) break;
  }

  console.log("Done. Created:", created);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
