/**
 * MAGIC PAWS // MARITIME PHASE ZERO
 * Visual Snapshot Renderer
 *
 * Creates PNG, PDF and a small metadata JSON from snapshot.html.
 * Intended for GitHub Actions, but also works locally:
 *   npm install playwright
 *   npx playwright install chromium
 *   node scripts/render_magicpaws_snapshot.mjs --hours=36
 */

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { chromium } from "playwright";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");

function argValue(name, fallback = null) {
  const prefix = `--${name}=`;
  const hit = process.argv.find(x => x.startsWith(prefix));
  return hit ? hit.slice(prefix.length) : fallback;
}

function utcStamp(d = new Date()) {
  const p = n => String(n).padStart(2, "0");
  return `${d.getUTCFullYear()}${p(d.getUTCMonth() + 1)}${p(d.getUTCDate())}_${p(d.getUTCHours())}${p(d.getUTCMinutes())}Z`;
}

const hours = Number(argValue("hours", process.env.SNAPSHOT_HOURS || "36"));
const fresh = String(argValue("fresh", process.env.SNAPSHOT_FRESH || "0"));
const outDir = path.resolve(repoRoot, argValue("out", process.env.SNAPSHOT_OUT || "snapshots/visual"));
const stamp = utcStamp();
const baseName = `${stamp}_MAGIC_PAWS_daily_snapshot_${hours}h`;

await fs.mkdir(outDir, { recursive: true });

const localHtml = path.resolve(repoRoot, "snapshot.html");
const url = `${pathToFileURL(localHtml).href}?hours=${encodeURIComponent(String(hours))}&fresh=${encodeURIComponent(fresh)}&auto=1`;

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1600, height: 900 }, deviceScaleFactor: 1 });

page.on("console", msg => console.log(`[browser:${msg.type()}] ${msg.text()}`));
page.on("pageerror", err => console.error(`[browser:pageerror] ${err.message}`));

await page.goto(url, { waitUntil: "domcontentloaded", timeout: 120_000 });
await page.waitForFunction(() => window.__MAGIC_PAWS_SNAPSHOT_READY__ === true, null, { timeout: 120_000 });
await page.waitForTimeout(1500);

const pngPath = path.join(outDir, `${baseName}.png`);
const pdfPath = path.join(outDir, `${baseName}.pdf`);
const metaPath = path.join(outDir, `${baseName}.json`);

await page.screenshot({ path: pngPath, fullPage: false });
await page.pdf({ path: pdfPath, width: "1600px", height: "900px", printBackground: true, margin: { top: "0", right: "0", bottom: "0", left: "0" } });

const meta = {
  ok: true,
  created_at: new Date().toISOString(),
  hours,
  fresh: fresh === "1" || fresh === "true",
  url,
  outputs: {
    png: path.relative(repoRoot, pngPath).replaceAll(path.sep, "/"),
    pdf: path.relative(repoRoot, pdfPath).replaceAll(path.sep, "/")
  }
};
await fs.writeFile(metaPath, JSON.stringify(meta, null, 2) + "\n", "utf8");

await browser.close();
console.log(JSON.stringify(meta, null, 2));
