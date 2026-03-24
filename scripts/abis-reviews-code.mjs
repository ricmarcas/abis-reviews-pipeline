import fs from "fs/promises";
import path from "path";
import { runReviewCodingPipeline } from "../lib/abis/modules/sentiment/coding-engine.js";

async function main() {
  const runId = process.argv[2] || undefined;
  const batchSize = Number(process.argv[3] || 8);
  const maxBatches = Number(process.argv[4] || 10);

  const result = await runReviewCodingPipeline({
    runId,
    batchSize: Number.isFinite(batchSize) ? batchSize : 8,
    maxBatches: Number.isFinite(maxBatches) ? maxBatches : 10,
  });

  const outputPath = path.resolve(process.cwd(), "./data/reviews-code-metrics.json");
  await fs.writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`, "utf-8");

  console.log(`[ABIS] Reviews processed: ${result.reviews_processed}`);
  console.log(`[ABIS] Codes created: ${result.codes_created}`);
  console.log(`[ABIS] Total codes: ${result.total_codes}`);
  console.log(`[ABIS] Metrics file: ${outputPath}`);
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
