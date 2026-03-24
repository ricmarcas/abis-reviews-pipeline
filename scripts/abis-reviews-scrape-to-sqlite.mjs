import fs from "fs/promises";
import path from "path";
import { parseRelativeDate, scrapeGoogleMapsReviews } from "../lib/abis/modules/sentiment/scraper.js";
import { ensureSqliteDir, getSqlitePath, openDb } from "./abis-reviews-sqlite.mjs";

function deriveNameFromUrl(url) {
  try {
    const parsed = new URL(url);
    const cleaned = decodeURIComponent(parsed.pathname || "")
      .replace(/^\/maps\/place\//, "")
      .replace(/^\/place\//, "")
      .replace(/\+/g, " ")
      .split("/")[0]
      .trim();
    return cleaned || parsed.hostname;
  } catch {
    return "Ubicación";
  }
}

function normalizeGroup(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "client";
  if (raw === "cliente" || raw === "client") return "client";
  return raw;
}

function normalizeList(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => ({
      name: String(item?.name || "").trim(),
      location_url: String(item?.location_url || "").trim(),
      group: normalizeGroup(item?.group),
      max_comment_age_days:
        Number.isFinite(Number(item?.max_comment_age_days)) && Number(item?.max_comment_age_days) > 0
          ? Number(item.max_comment_age_days)
          : null,
      fallback_limit:
        Number.isFinite(Number(item?.fallback_limit)) && Number(item?.fallback_limit) > 0
          ? Number(item.fallback_limit)
          : null,
      scroll_cycles:
        Number.isFinite(Number(item?.scroll_cycles)) && Number(item?.scroll_cycles) > 0
          ? Number(item.scroll_cycles)
          : null,
    }))
    .filter((item) => item.name && item.location_url && item.group);
}

function parseLineBasedLocations(rawText) {
  const lines = String(rawText || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));

  const locations = [];
  for (const line of lines) {
    const parts = line.split("|").map((part) => part.trim());

    if (parts.length === 1 && /^https?:\/\//i.test(parts[0])) {
      const locationUrl = parts[0];
      locations.push({
        project: "default",
        group: "client",
        name: deriveNameFromUrl(locationUrl),
        location_url: locationUrl,
        max_comment_age_days: null,
        fallback_limit: null,
        scroll_cycles: null,
      });
      continue;
    }

    // 3-col format: group|name|url|...
    if (parts.length >= 3 && /^https?:\/\//i.test(parts[2])) {
      locations.push({
        project: "default",
        group: normalizeGroup(parts[0]),
        name: parts[1] || deriveNameFromUrl(parts[2]),
        location_url: parts[2],
        max_comment_age_days:
          Number.isFinite(Number(parts[3])) && Number(parts[3]) > 0 ? Number(parts[3]) : null,
        fallback_limit:
          Number.isFinite(Number(parts[4])) && Number(parts[4]) > 0 ? Number(parts[4]) : null,
        scroll_cycles:
          Number.isFinite(Number(parts[5])) && Number(parts[5]) > 0 ? Number(parts[5]) : null,
      });
      continue;
    }

    // 4-col format: project|group|name|url|...
    if (parts.length >= 4 && /^https?:\/\//i.test(parts[3])) {
      locations.push({
        project: parts[0] || "default",
        group: normalizeGroup(parts[1]),
        name: parts[2] || deriveNameFromUrl(parts[3]),
        location_url: parts[3],
        max_comment_age_days:
          Number.isFinite(Number(parts[4])) && Number(parts[4]) > 0 ? Number(parts[4]) : null,
        fallback_limit:
          Number.isFinite(Number(parts[5])) && Number(parts[5]) > 0 ? Number(parts[5]) : null,
        scroll_cycles:
          Number.isFinite(Number(parts[6])) && Number(parts[6]) > 0 ? Number(parts[6]) : null,
      });
    }
  }

  return locations;
}

async function main() {
  const configArg = process.argv[2] || "./data/reviews-locations.lines.txt";
  const dbArg = process.argv[3] || "./data/abis_reviews.sqlite";

  let configPath = path.isAbsolute(configArg)
    ? configArg
    : path.resolve(process.cwd(), configArg);
  try {
    await fs.access(configPath);
  } catch {
    const fallbackPath = path.resolve(process.cwd(), "./data/reviews-locations.lines.example.txt");
    await fs.access(fallbackPath);
    configPath = fallbackPath;
  }
  const dbPath = getSqlitePath(dbArg);

  const raw = await fs.readFile(configPath, "utf-8");
  const isJson = configPath.toLowerCase().endsWith(".json");
  const config = isJson ? JSON.parse(raw) : {};
  const client = isJson ? String(config?.client || "unknown-client").trim() : "unknown-client";
  const locations = isJson ? normalizeList(config?.locations) : parseLineBasedLocations(raw);
  const defaultMaxDays = isJson
    ? Number.isFinite(Number(config?.analysis?.max_comment_age_days)) &&
      Number(config.analysis.max_comment_age_days) > 0
      ? Number(config.analysis.max_comment_age_days)
      : 30
    : 30;
  const defaultFallbackLimit = isJson
    ? Number.isFinite(Number(config?.analysis?.fallback_limit)) &&
      Number(config.analysis.fallback_limit) > 0
      ? Number(config.analysis.fallback_limit)
      : 25
    : 25;
  const defaultScrollCycles = isJson
    ? Number.isFinite(Number(config?.analysis?.scroll_cycles)) &&
      Number(config.analysis.scroll_cycles) > 0
      ? Number(config.analysis.scroll_cycles)
      : 12
    : 12;
  const runId = isJson
    ? String(config?.analysis?.run_id || "").trim() ||
      `run-${new Date().toISOString().replace(/[:.]/g, "-")}`
    : `run-${new Date().toISOString().replace(/[:.]/g, "-")}`;

  if (locations.length === 0) {
    throw new Error(
      "No locations found. Use JSON (locations[]) or line file with: project|group|name|url"
    );
  }

  process.env.MAPS_USE_PERSISTENT_CONTEXT = process.env.MAPS_USE_PERSISTENT_CONTEXT || "true";
  process.env.MAPS_HEADLESS = process.env.MAPS_HEADLESS || "false";

  await ensureSqliteDir(dbPath);
  const db = openDb(dbPath);
  const upsert = db.prepare(`
    INSERT INTO reviews_staging (
      id, user_name, rating, comment, date_text, location_url,
      project_name, client, group_name, location_name, scraped_at, run_id, review_age_days, synced
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    ON CONFLICT(id) DO UPDATE SET
      user_name=excluded.user_name,
      rating=excluded.rating,
      comment=excluded.comment,
      date_text=excluded.date_text,
      location_url=excluded.location_url,
      project_name=excluded.project_name,
      client=excluded.client,
      group_name=excluded.group_name,
      location_name=excluded.location_name,
      scraped_at=excluded.scraped_at,
      run_id=excluded.run_id,
      review_age_days=excluded.review_age_days
  `);

  const summary = [];
  let totalStored = 0;

  for (const location of locations) {
    console.log(`\n[ABIS] Scraping: ${location.name} (${location.group})`);
    try {
      const maxDays = location.max_comment_age_days ?? defaultMaxDays;
      const fallbackLimit = location.fallback_limit ?? defaultFallbackLimit;
      const scrollCycles = location.scroll_cycles ?? defaultScrollCycles;
      const result = await scrapeGoogleMapsReviews(location.location_url, {
        maxDays,
        fallbackLimit,
        scrollCycles,
      });
      const now = new Date().toISOString();

      for (const review of result.reviews) {
        const ageDays = parseRelativeDate(review.date_text);
        upsert.run(
          review.id,
          review.user_name || "",
          Number(review.rating || 0),
          review.comment || "",
          review.date_text || "",
          review.location_url || location.location_url,
          location.project || "default",
          client,
          location.group,
          location.name,
          now,
          runId,
          Number.isFinite(ageDays) ? ageDays : null
        );
      }

      totalStored += result.reviews.length;
      summary.push({
        project: location.project || "default",
        location: location.name,
        group: location.group,
        extracted: result.total_extracted,
        stored: result.reviews.length,
        mode: result.filter_mode,
        max_days: maxDays,
        fallback_limit: fallbackLimit,
      });
      console.log(
        `[ABIS] Done: extracted=${result.total_extracted}, stored=${result.reviews.length}, mode=${result.filter_mode}`
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unexpected error";
      summary.push({
        location: location.name,
        group: location.group,
        extracted: 0,
        stored: 0,
        mode: "error",
        error: message,
      });
      console.error(`[ABIS] Error on ${location.name}: ${message}`);
    }
  }

  console.log("\n[ABIS] Batch summary (SQLite staging):");
  console.table(summary);
  console.log(`[ABIS] Run ID: ${runId}`);
  console.log(`[ABIS] Total reviews stored in SQLite: ${totalStored}`);
  db.close();
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
