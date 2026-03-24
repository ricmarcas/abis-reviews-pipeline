import fs from "fs/promises";
import path from "path";
import { getSqlitePath, openDb } from "./abis-reviews-sqlite.mjs";

function getOutputPath(outputArg = "./data/reviews-sync-payload.json") {
  return path.isAbsolute(outputArg) ? outputArg : path.resolve(process.cwd(), outputArg);
}

async function ensureOutputDir(outputPath) {
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
}

function normalizeReview(row) {
  return {
    id: String(row.id || "").trim(),
    user_name: String(row.user_name || "").trim(),
    rating: Number(row.rating || 0),
    comment: String(row.comment || "").trim(),
    date_text: String(row.date_text || "").trim(),
    location_url: String(row.location_url || "").trim(),
    project_name: String(row.project_name || "").trim(),
    client: String(row.client || "").trim(),
    group_name: String(row.group_name || "").trim(),
    location_name: String(row.location_name || "").trim(),
    scraped_at: String(row.scraped_at || "").trim(),
  };
}

async function main() {
  const dbArg = process.argv[2] || "./data/abis_reviews.sqlite";
  const outputArg = process.argv[3] || "./data/reviews-sync-payload.json";

  const dbPath = getSqlitePath(dbArg);
  const outputPath = getOutputPath(outputArg);
  const db = openDb(dbPath);

  const rows = db
    .prepare(
      `
      SELECT id, user_name, rating, comment, date_text, location_url, project_name, client, group_name, location_name, scraped_at
      FROM reviews_staging
      WHERE synced = 0
      ORDER BY scraped_at DESC
    `
    )
    .all();

  const seen = new Set();
  const reviews = [];
  for (const row of rows) {
    const normalized = normalizeReview(row);
    if (!normalized.id || seen.has(normalized.id)) {
      continue;
    }
    seen.add(normalized.id);
    reviews.push(normalized);
  }

  const payload = {
    generated_at: new Date().toISOString(),
    source: "sqlite-staging",
    reviews,
  };

  await ensureOutputDir(outputPath);
  await fs.writeFile(outputPath, `${JSON.stringify(payload, null, 2)}\n`, "utf-8");
  db.close();

  console.log(`[ABIS] Payload generated: ${outputPath}`);
  console.log(`[ABIS] Reviews included: ${reviews.length}`);
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
