import fs from "fs/promises";
import path from "path";
import { consolidateCodeDictionary } from "../lib/abis/modules/sentiment/coding-engine.js";

async function main() {
  const result = consolidateCodeDictionary();
  const outputPath = path.resolve(process.cwd(), "./data/reviews-code-consolidation.json");
  await fs.writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`, "utf-8");

  console.log(`[ABIS] Code groups merged: ${result.merged_groups}`);
  console.log(`[ABIS] Codes merged into masters: ${result.merged_codes}`);
  console.log(`[ABIS] Total codes after consolidation: ${result.total_codes_after}`);
  console.log(`[ABIS] Review-code rows after consolidation: ${result.review_code_rows_after}`);
  console.log(`[ABIS] Consolidation file: ${outputPath}`);
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
