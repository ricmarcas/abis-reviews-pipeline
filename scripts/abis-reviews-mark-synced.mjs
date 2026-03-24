import fs from "fs/promises";
import path from "path";
import { getSqlitePath, openDb } from "./abis-reviews-sqlite.mjs";

function getPayloadPath(payloadArg = "./data/reviews-sync-payload.json") {
  return path.isAbsolute(payloadArg) ? payloadArg : path.resolve(process.cwd(), payloadArg);
}

async function main() {
  const dbArg = process.argv[2] || "./data/abis_reviews.sqlite";
  const payloadArg = process.argv[3] || "./data/reviews-sync-payload.json";

  const dbPath = getSqlitePath(dbArg);
  const payloadPath = getPayloadPath(payloadArg);
  const raw = await fs.readFile(payloadPath, "utf-8");
  const payload = JSON.parse(raw);
  const reviews = Array.isArray(payload?.reviews) ? payload.reviews : [];
  const ids = reviews.map((item) => String(item?.id || "").trim()).filter(Boolean);

  if (ids.length === 0) {
    console.log("[ABIS] No review IDs found in payload. Nothing to mark as synced.");
    return;
  }

  const db = openDb(dbPath);
  const update = db.prepare("UPDATE reviews_staging SET synced = 1 WHERE id = ?");
  const transaction = db.transaction((reviewIds) => {
    for (const id of reviewIds) {
      update.run(id);
    }
  });
  transaction(ids);
  db.close();

  console.log(`[ABIS] Marked as synced in SQLite: ${ids.length}`);
}

main().catch((error) => {
  console.error("[ABIS] Fatal:", error instanceof Error ? error.message : error);
  process.exit(1);
});
