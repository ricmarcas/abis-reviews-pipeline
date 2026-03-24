import fs from "fs/promises";
import path from "path";
import { DatabaseSync } from "node:sqlite";

export function getSqlitePath(dbArg = "./data/abis_reviews.sqlite") {
  return path.isAbsolute(dbArg) ? dbArg : path.resolve(process.cwd(), dbArg);
}

export async function ensureSqliteDir(dbPath) {
  await fs.mkdir(path.dirname(dbPath), { recursive: true });
}

export function openDb(dbPath) {
  const db = new DatabaseSync(dbPath);
  db.exec(`
    CREATE TABLE IF NOT EXISTS reviews_staging (
      id TEXT PRIMARY KEY,
      user_name TEXT NOT NULL,
      rating REAL NOT NULL,
      comment TEXT NOT NULL,
      date_text TEXT NOT NULL,
      location_url TEXT NOT NULL,
      project_name TEXT NOT NULL DEFAULT 'default',
      client TEXT NOT NULL,
      group_name TEXT NOT NULL,
      location_name TEXT NOT NULL,
      scraped_at TEXT NOT NULL,
      run_id TEXT,
      review_age_days INTEGER,
      synced INTEGER NOT NULL DEFAULT 0
    );
  `);

  // Lightweight schema migration for existing local DBs.
  const alterStatements = [
    "ALTER TABLE reviews_staging ADD COLUMN project_name TEXT NOT NULL DEFAULT 'default'",
    "ALTER TABLE reviews_staging ADD COLUMN run_id TEXT",
    "ALTER TABLE reviews_staging ADD COLUMN review_age_days INTEGER",
  ];
  for (const statement of alterStatements) {
    try {
      db.exec(statement);
    } catch {
      // Column already exists in this SQLite file.
    }
  }

  return db;
}
