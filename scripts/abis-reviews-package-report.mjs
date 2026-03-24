import fs from "fs/promises";
import path from "path";

async function main() {
  const dataDir = path.resolve(process.cwd(), "./data");
  const outDir = path.resolve(process.cwd(), "./out");
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const bundleDir = path.join(outDir, `reviews-report-${stamp}`);

  await fs.mkdir(bundleDir, { recursive: true });

  const files = [
    "reviews-analysis-report.json",
    "reviews-code-metrics.json",
    "reviews-executive-summary.csv",
    "reviews-executive-latest-reviews.csv",
    "reviews-executive-report.md",
    "reviews-sync-payload.json",
  ];

  for (const file of files) {
    const source = path.join(dataDir, file);
    try {
      await fs.access(source);
      await fs.copyFile(source, path.join(bundleDir, file));
    } catch {
      // Skip missing file.
    }
  }

  console.log(`[ABIS] Report bundle directory: ${bundleDir}`);
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
