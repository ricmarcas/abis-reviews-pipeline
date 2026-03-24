import fs from "fs/promises";
import path from "path";
import { getSqlitePath, openDb } from "./abis-reviews-sqlite.mjs";

const DEFAULT_BUCKETS = [7, 30, 90, 180];

function getOutputPath(outputArg = "./data/reviews-analysis-report.json") {
  return path.isAbsolute(outputArg) ? outputArg : path.resolve(process.cwd(), outputArg);
}

function normalizeBuckets(value) {
  if (!Array.isArray(value)) return DEFAULT_BUCKETS;
  const buckets = value
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v) && v > 0)
    .sort((a, b) => a - b);
  return buckets.length > 0 ? Array.from(new Set(buckets)) : DEFAULT_BUCKETS;
}

function calcBucketLabel(age, buckets) {
  if (!Number.isFinite(age) || age < 0) return "sin_fecha_relativa";
  for (const bucket of buckets) {
    if (age <= bucket) return `0-${bucket}d`;
  }
  return `>${buckets[buckets.length - 1]}d`;
}

function avg(values) {
  if (values.length === 0) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function topWords(comments, limit = 20) {
  const stopwords = new Set([
    "de",
    "la",
    "el",
    "los",
    "las",
    "y",
    "en",
    "muy",
    "con",
    "para",
    "que",
    "por",
    "del",
    "una",
    "un",
    "es",
    "lo",
    "al",
    "se",
    "no",
    "le",
    "me",
    "todo",
    "pero",
  ]);

  const counts = new Map();
  for (const rawComment of comments) {
    const words = String(rawComment || "")
      .toLowerCase()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/[^a-z0-9\s]/g, " ")
      .split(/\s+/)
      .filter((word) => word.length >= 4 && !stopwords.has(word));

    for (const word of words) {
      counts.set(word, (counts.get(word) || 0) + 1);
    }
  }

  return Array.from(counts.entries())
    .sort((a, b) => b[1] - a[1])
    .slice(0, limit)
    .map(([word, count]) => ({ word, count }));
}

function toSummary(rows, buckets) {
  const ratings = rows.map((row) => Number(row.rating || 0)).filter((value) => value > 0);
  const ageBucketCounts = {};
  for (const row of rows) {
    const label = calcBucketLabel(Number(row.review_age_days), buckets);
    ageBucketCounts[label] = (ageBucketCounts[label] || 0) + 1;
  }

  return {
    total_reviews: rows.length,
    average_rating: Number(avg(ratings).toFixed(3)),
    ratings_distribution: {
      "1": rows.filter((row) => Math.round(Number(row.rating || 0)) === 1).length,
      "2": rows.filter((row) => Math.round(Number(row.rating || 0)) === 2).length,
      "3": rows.filter((row) => Math.round(Number(row.rating || 0)) === 3).length,
      "4": rows.filter((row) => Math.round(Number(row.rating || 0)) === 4).length,
      "5": rows.filter((row) => Math.round(Number(row.rating || 0)) === 5).length,
    },
    age_buckets: ageBucketCounts,
    top_words: topWords(rows.map((row) => row.comment)),
  };
}

function toDetailedReviews(rows, limit = 25) {
  return [...rows]
    .sort((a, b) => {
      const ageA = Number.isFinite(Number(a.review_age_days)) ? Number(a.review_age_days) : Number.MAX_SAFE_INTEGER;
      const ageB = Number.isFinite(Number(b.review_age_days)) ? Number(b.review_age_days) : Number.MAX_SAFE_INTEGER;
      if (ageA !== ageB) return ageA - ageB;
      return String(b.scraped_at || "").localeCompare(String(a.scraped_at || ""));
    })
    .slice(0, limit)
    .map((row) => ({
      id: row.id,
      user_name: row.user_name,
      rating: Number(row.rating || 0),
      comment: row.comment,
      date_text: row.date_text,
      review_age_days:
        row.review_age_days === null || row.review_age_days === undefined || row.review_age_days === ""
          ? null
          : Number.isFinite(Number(row.review_age_days))
            ? Number(row.review_age_days)
            : null,
      location_url: row.location_url,
      scraped_at: row.scraped_at,
    }));
}

async function readConfig(configArg = "./data/reviews-locations.json") {
  const configPath = path.isAbsolute(configArg)
    ? configArg
    : path.resolve(process.cwd(), configArg);
  try {
    const raw = await fs.readFile(configPath, "utf-8");
    return JSON.parse(raw);
  } catch {
    return {};
  }
}

async function main() {
  const configArg = process.argv[2] || "./data/reviews-locations.json";
  const dbArg = process.argv[3] || "./data/abis_reviews.sqlite";
  const outputArg = process.argv[4] || "./data/reviews-analysis-report.json";

  const config = await readConfig(configArg);
  const dbPath = getSqlitePath(dbArg);
  const outputPath = getOutputPath(outputArg);
  const buckets = normalizeBuckets(config?.analysis?.age_buckets_days);
  const db = openDb(dbPath);

  const latestRun = db
    .prepare(
      `
      SELECT run_id
      FROM reviews_staging
      WHERE run_id IS NOT NULL AND run_id <> ''
      ORDER BY scraped_at DESC
      LIMIT 1
    `
    )
    .get();
  const hasNamedRun = Boolean(latestRun?.run_id);
  const rows = hasNamedRun
    ? db
        .prepare(
          `
      SELECT id, user_name, rating, comment, date_text, location_url, project_name, client, group_name, location_name, scraped_at, run_id, review_age_days
      FROM reviews_staging
          WHERE run_id = ?
          ORDER BY group_name, location_name, scraped_at DESC
        `
        )
        .all(latestRun.run_id)
    : db
        .prepare(
          `
          SELECT id, user_name, rating, comment, date_text, location_url, project_name, client, group_name, location_name, scraped_at, run_id, review_age_days
          FROM reviews_staging
          ORDER BY scraped_at DESC, group_name, location_name
        `
        )
        .all();
  db.close();

  if (rows.length === 0) {
    throw new Error("No hay reseñas en SQLite. Ejecuta primero el scraping local.");
  }

  const byGroup = {};
  const byProject = {};
  const byLocation = {};

  for (const row of rows) {
    if (!byProject[row.project_name || "default"]) byProject[row.project_name || "default"] = [];
    byProject[row.project_name || "default"].push(row);

    if (!byGroup[row.group_name]) byGroup[row.group_name] = [];
    byGroup[row.group_name].push(row);

    const locationKey = `${row.group_name}::${row.location_name}`;
    if (!byLocation[locationKey]) byLocation[locationKey] = [];
    byLocation[locationKey].push(row);
  }

  const report = {
    generated_at: new Date().toISOString(),
    run_id: hasNamedRun ? latestRun.run_id : "legacy-no-run-id",
    client: String(config?.client || "unknown-client"),
    analysis_settings: {
      max_comment_age_days: Number(config?.analysis?.max_comment_age_days || 30),
      fallback_limit: Number(config?.analysis?.fallback_limit || 25),
      age_buckets_days: buckets,
    },
    global: toSummary(rows, buckets),
    groups: Object.fromEntries(
      Object.entries(byGroup).map(([groupName, groupRows]) => [groupName, toSummary(groupRows, buckets)])
    ),
    projects: Object.fromEntries(
      Object.entries(byProject).map(([projectName, projectRows]) => [
        projectName,
        toSummary(projectRows, buckets),
      ])
    ),
    locations: Object.fromEntries(
      Object.entries(byLocation).map(([locationKey, locationRows]) => [
        locationKey,
        {
          group_name: locationRows[0]?.group_name || "",
          project_name: locationRows[0]?.project_name || "default",
          location_name: locationRows[0]?.location_name || "",
          total_reviews: locationRows.length,
          summary: toSummary(locationRows, buckets),
          latest_reviews: toDetailedReviews(locationRows, 25),
        },
      ])
    ),
  };

  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, `${JSON.stringify(report, null, 2)}\n`, "utf-8");
  console.log(`[ABIS] Analysis report generated: ${outputPath}`);
  console.log(`[ABIS] run_id: ${latestRun.run_id}`);
  console.log(`[ABIS] total_reviews: ${report.global.total_reviews}`);
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
