import fs from "fs/promises";
import path from "path";

function escapeCsv(value) {
  const text = String(value ?? "");
  if (text.includes(",") || text.includes('"') || text.includes("\n")) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function toCsv(headers, rows) {
  const lines = [headers.map(escapeCsv).join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => escapeCsv(row[header])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  const reportArg = process.argv[2] || "./data/reviews-analysis-report.json";
  const outputDirArg = process.argv[3] || "./data";

  const reportPath = path.isAbsolute(reportArg)
    ? reportArg
    : path.resolve(process.cwd(), reportArg);
  const outputDir = path.isAbsolute(outputDirArg)
    ? outputDirArg
    : path.resolve(process.cwd(), outputDirArg);

  const raw = await fs.readFile(reportPath, "utf-8");
  const report = JSON.parse(raw);

  const locationEntries = Object.entries(report?.locations || {});
  const summaryRows = [];
  const detailRows = [];

  for (const [locationKey, locationData] of locationEntries) {
    const summary = locationData.summary || {};
    summaryRows.push({
      run_id: report.run_id || "",
      project_name: locationData.project_name || "",
      group_name: locationData.group_name || "",
      location_name: locationData.location_name || "",
      location_key: locationKey,
      total_reviews: summary.total_reviews || 0,
      average_rating: summary.average_rating || 0,
      rating_1: summary.ratings_distribution?.["1"] || 0,
      rating_2: summary.ratings_distribution?.["2"] || 0,
      rating_3: summary.ratings_distribution?.["3"] || 0,
      rating_4: summary.ratings_distribution?.["4"] || 0,
      rating_5: summary.ratings_distribution?.["5"] || 0,
    });

    for (const review of locationData.latest_reviews || []) {
      detailRows.push({
        run_id: report.run_id || "",
        project_name: locationData.project_name || "",
        group_name: locationData.group_name || "",
        location_name: locationData.location_name || "",
        review_id: review.id || "",
        user_name: review.user_name || "",
        rating: review.rating ?? "",
        date_text: review.date_text || "",
        review_age_days: review.review_age_days ?? "",
        comment: review.comment || "",
        location_url: review.location_url || "",
        scraped_at: review.scraped_at || "",
      });
    }
  }

  summaryRows.sort((a, b) => b.total_reviews - a.total_reviews);
  detailRows.sort((a, b) => String(b.scraped_at).localeCompare(String(a.scraped_at)));

  const summaryCsv = toCsv(
    [
      "run_id",
      "project_name",
      "group_name",
      "location_name",
      "location_key",
      "total_reviews",
      "average_rating",
      "rating_1",
      "rating_2",
      "rating_3",
      "rating_4",
      "rating_5",
    ],
    summaryRows
  );

  const detailCsv = toCsv(
    [
      "run_id",
      "project_name",
      "group_name",
      "location_name",
      "review_id",
      "user_name",
      "rating",
      "date_text",
      "review_age_days",
      "comment",
      "location_url",
      "scraped_at",
    ],
    detailRows
  );

  const topLocationsMd = summaryRows
    .slice(0, 20)
    .map(
      (row) =>
        `- ${row.project_name} | ${row.group_name} | ${row.location_name}: ${row.total_reviews} reseñas, rating promedio ${row.average_rating}`
    )
    .join("\n");

  const markdown = `# ABIS Executive Reviews Report

- Generated at: ${report.generated_at}
- Run ID: ${report.run_id}
- Total reviews: ${report.global?.total_reviews || 0}
- Average rating: ${report.global?.average_rating || 0}

## Top Locations
${topLocationsMd || "- No data"}
`;

  await fs.mkdir(outputDir, { recursive: true });
  const summaryPath = path.join(outputDir, "reviews-executive-summary.csv");
  const detailsPath = path.join(outputDir, "reviews-executive-latest-reviews.csv");
  const markdownPath = path.join(outputDir, "reviews-executive-report.md");

  await fs.writeFile(summaryPath, summaryCsv, "utf-8");
  await fs.writeFile(detailsPath, detailCsv, "utf-8");
  await fs.writeFile(markdownPath, markdown, "utf-8");

  console.log(`[ABIS] Executive summary CSV: ${summaryPath}`);
  console.log(`[ABIS] Executive details CSV: ${detailsPath}`);
  console.log(`[ABIS] Executive report MD: ${markdownPath}`);
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
