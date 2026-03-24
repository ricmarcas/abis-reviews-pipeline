import path from "path";
import { DatabaseSync } from "node:sqlite";

const OPENAI_API_URL = "https://api.openai.com/v1/responses";
const OPENAI_MODEL = "gpt-4.1-mini";

function getSqlitePath(dbArg = "./data/abis_reviews.sqlite") {
  return path.isAbsolute(dbArg) ? dbArg : path.resolve(process.cwd(), dbArg);
}

function openDb(dbPath) {
  return new DatabaseSync(dbPath);
}

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function toTitleCase(value) {
  return value
    .split(" ")
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

function asProjectId(value) {
  const normalized = normalizeText(value).replace(/\s+/g, "_");
  return normalized || "default";
}

function cleanIdea(value) {
  const text = String(value || "")
    .replace(/^[-*•\d.)\s]+/, "")
    .replace(/\s+/g, " ")
    .trim();
  if (text.length < 6) return "";
  if (text.split(" ").length < 2) return "";
  return text;
}

const STOPWORDS = new Set([
  "de",
  "la",
  "el",
  "los",
  "las",
  "y",
  "en",
  "con",
  "para",
  "que",
  "del",
  "una",
  "un",
  "es",
  "al",
  "se",
  "por",
]);

function toGeneralCodePhrase(value) {
  const words = normalizeText(value)
    .split(" ")
    .filter((word) => word.length > 2 && !STOPWORDS.has(word));

  if (words.length === 0) return "";
  if (words.length === 1) return `${words[0]} general`;
  if (words.length <= 8) return words.join(" ");
  return words.slice(0, 8).join(" ");
}

function normalizeCodeName(value) {
  const phrase = toGeneralCodePhrase(value);
  if (!phrase) return "";
  return toTitleCase(phrase);
}

function sanitizeNarrativeLanguage(value) {
  return String(value || "")
    .replace(/\b(cliente|clientes|yo|me|mi|nosotros|recomiendo|recomendar|recomendado)\b/gi, " ")
    .replace(
      /\b(bueno|buena|buen|malo|mala|excelente|deficiente|pesimo|pesima|amable|satisfecho|satisfecha|feliz|molesto|molesta)\b/gi,
      " "
    )
    .replace(/\s+/g, " ")
    .trim();
}

function looksSpanishEnough(value) {
  const text = normalizeText(value);
  if (!text) return false;
  const englishTokens = ["service", "price", "staff", "store", "good", "bad", "recommend"];
  const hasEnglish = englishTokens.some((token) => text.includes(token));
  return !hasEnglish;
}

function canonicalizeAnalyticalCategory(value) {
  const raw = sanitizeNarrativeLanguage(value);
  const text = normalizeText(raw);
  if (!text) return "";

  const has = (pattern) => pattern.test(text);
  if (has(/\b(atencion|servicio|personal)\b/)) return "Atencion al cliente";
  if (has(/\b(asesoria|orientacion|recomendacion|recomendar|elegir|eleccion)\b/))
    return "Asesoria al cliente";
  if (has(/\b(precio|precios|caro|costoso|accesible|barato)\b/)) return "Precios";
  if (has(/\b(variedad|surtido|inventario|oferta|todo)\b/)) return "Variedad de productos";
  if (has(/\b(estacionamiento)\b/)) return "Estacionamiento";
  if (has(/\b(acceso|ubicacion)\b/)) return "Acceso y ubicacion";
  if (has(/\b(calidad|producto|productos|ingredientes)\b/)) return "Calidad de productos";
  if (has(/\b(limpieza|higiene)\b/)) return "Limpieza e higiene";
  if (has(/\b(seguridad|medidas)\b/)) return "Seguridad en tienda";
  if (has(/\b(ambiente|espacio|presentacion|distribucion)\b/)) return "Ambiente de tienda";
  if (has(/\b(entrega|domicilio)\b/)) return "Entrega a domicilio";

  const normalized = normalizeCodeName(raw);
  return normalized || "Experiencia de compra";
}

function sentimentFromRating(rating) {
  const value = Number(rating || 0);
  if (value <= 2) return "negative";
  if (value === 3) return "neutral";
  return "positive";
}

function tokenize(value) {
  return normalizeText(value)
    .split(" ")
    .filter((token) => token.length > 2);
}

function tokenSimilarity(a, b) {
  const ta = tokenize(a);
  const tb = tokenize(b);
  if (ta.length === 0 || tb.length === 0) return 0;
  const setB = new Set(tb);
  let common = 0;
  for (const token of ta) {
    if (setB.has(token)) common += 1;
  }
  return common / Math.max(ta.length, tb.length);
}

function stripProjectPrefix(normalizedName) {
  const value = String(normalizedName || "");
  const idx = value.indexOf("::");
  return idx >= 0 ? value.slice(idx + 2) : value;
}

const GENERIC_TOKENS = new Set([
  "general",
  "muy",
  "buena",
  "bueno",
  "excelente",
  "rapida",
  "rapido",
  "poco",
  "alta",
  "baja",
]);

function intersectionCount(aTokens, bTokens) {
  const bSet = new Set(bTokens);
  let count = 0;
  for (const token of aTokens) {
    if (bSet.has(token)) count += 1;
  }
  return count;
}

function shouldMergeCodes(a, b) {
  if (!a || !b) return false;
  const aBase = stripProjectPrefix(a.normalized_name);
  const bBase = stripProjectPrefix(b.normalized_name);
  if (aBase === bBase) return true;

  const ta = tokenize(aBase);
  const tb = tokenize(bBase);
  if (ta.length === 0 || tb.length === 0) return false;

  const common = intersectionCount(ta, tb);
  if (common >= 2) return true;

  const samePrimary = ta[0] && tb[0] && ta[0] === tb[0];
  if (!samePrimary) return false;

  if (common >= 1) {
    const onlyGenericDiff = ta.concat(tb).every((token) => token === ta[0] || GENERIC_TOKENS.has(token));
    if (onlyGenericDiff) return true;
  }

  return tokenSimilarity(aBase, bBase) >= 0.8;
}

function parseIdeasFromText(text) {
  const rawText = String(text || "").trim();
  if (!rawText) return [];

  try {
    const maybeJson = JSON.parse(rawText);
    if (Array.isArray(maybeJson)) {
      return maybeJson.map(cleanIdea).filter(Boolean);
    }
  } catch {
    // fallback to line parsing
  }

  const lines = rawText
    .split(/\r?\n/)
    .map((line) => cleanIdea(line))
    .filter(Boolean);

  if (lines.length > 0) {
    return lines.slice(0, 5);
  }

  return rawText
    .split(/[.;]/)
    .map((part) => cleanIdea(part))
    .filter(Boolean)
    .slice(0, 5);
}

async function analyzeReview(comment) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is required for qualitative coding.");
  }

  const prompt = `Analiza el siguiente comentario de cliente.
Extrae entre 1 y 5 categorias analiticas reutilizables en espanol.
No uses lenguaje narrativo, personal ni descriptivo.
Devuelve exclusivamente un arreglo JSON de strings.

Comentario:
${comment}`;

  const response = await fetch(OPENAI_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      input: [
        {
          role: "system",
          content:
            "Eres un analista cualitativo. Extraes ideas de comentarios en español para codificación de research.",
        },
        {
          role: "user",
          content: prompt,
        },
      ],
      temperature: 0.2,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OpenAI error: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  const text = data?.output_text ?? data?.output?.[0]?.content?.[0]?.text ?? "";
  const ideas = Array.from(
    new Set(
      parseIdeasFromText(text)
        .map((idea) => toGeneralCodePhrase(idea))
        .filter(Boolean)
    )
  ).slice(0, 5);
  const normalized = ideas.map((idea) => canonicalizeAnalyticalCategory(idea)).filter(Boolean);
  return normalized.length > 0 ? Array.from(new Set(normalized)).slice(0, 5) : ["Experiencia de compra general"];
}

function parseCodingOutput(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return [];

  try {
    const parsed = JSON.parse(text);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((item) => ({
        code_name: canonicalizeAnalyticalCategory(item?.code_name || ""),
        is_new: Boolean(item?.is_new),
      }))
      .filter((item) => item.code_name);
  } catch {
    return [];
  }
}

async function classifyCommentWithCodebook({ comment, codebook }) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) {
    throw new Error("OPENAI_API_KEY is required for qualitative coding.");
  }

  const prompt = `Tienes un conjunto de códigos que se han generado dinámicamente a partir de comentarios anteriores.

Tu tarea es analizar un nuevo comentario y asignar sus ideas a códigos existentes o crear nuevos códigos solo cuando sea necesario.

IMPORTANTE:

No estás describiendo el comentario.
Estás clasificando ideas en dimensiones analíticas.

---

REGLAS PRINCIPALES

1. Reutiliza un código existente si la idea pertenece al mismo tema.
2. Crea un nuevo código SOLO si aparece un tema nuevo.
3. Evita duplicar temas con diferentes nombres.
4. Mantén un codebook coherente y reutilizable.

---

NEUTRALIDAD OBLIGATORIA (CRÍTICO)

Los códigos deben representar SOLO el TEMA.

NO incluir:

* calificadores (bueno, malo, excelente, deficiente)
* emociones
* juicios
* narrativa

---

FORMATO DE CÓDIGOS

Usar estructura:

→ sustantivo o dimensión clara

Ejemplos correctos:

* "Atención al cliente"
* "Precios"
* "Variedad de productos"
* "Acceso y ubicación"
* "Estacionamiento"
* "Calidad de productos"
* "Asesoría al cliente"

---

EJEMPLOS

Entrada:

"excelente atención", "muy mal servicio", "personal amable"

Salida:
"Atención al cliente"

---

Entrada:

"muy caro", "buen precio", "precios accesibles"

Salida:
"Precios"

---

Entrada:

"falta variedad", "tienen de todo"

Salida:
"Variedad de productos"

---

PROHIBICIONES

NO usar:

* "bueno", "malo", "excelente"
* "cliente satisfecho"
* "me gusta"
* frases narrativas
* combinaciones de tema + sentimiento

---

GRANULARIDAD

* nivel medio
* no demasiado general ("Servicio")
* no demasiado específico

---

SALIDA JSON

[
{
"code_name": "...",
"is_new": true/false
}
]

---

ENTRADA

Comentario:
${comment}

Códigos existentes:
${codebook.length > 0 ? codebook.join(", ") : "[]"}`;

  const response = await fetch(OPENAI_API_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: OPENAI_MODEL,
      input: [
        {
          role: "system",
          content:
            "Eres un analista cualitativo experto en research. Tu salida siempre es JSON valido y codigo en espanol.",
        },
        {
          role: "user",
          content: prompt,
        },
      ],
      temperature: 0.1,
    }),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`OpenAI error: ${response.status} ${errorText}`);
  }

  const data = await response.json();
  const text = data?.output_text ?? data?.output?.[0]?.content?.[0]?.text ?? "";
  const parsed = parseCodingOutput(text);
  if (parsed.length > 0) {
    const dedup = new Map();
    for (const item of parsed.slice(0, 5)) {
      const key = normalizeText(item.code_name);
      if (!key || dedup.has(key)) continue;
      dedup.set(key, {
        code_name: item.code_name,
        is_new: item.is_new,
      });
    }
    return Array.from(dedup.values());
  }

  // Fallback to idea extraction if model does not return expected JSON.
  const ideas = await analyzeReview(comment);
  return ideas.map((idea) => ({ code_name: normalizeCodeName(idea), is_new: true }));
}

function ensureCodingTables(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS codes (
      code_id INTEGER PRIMARY KEY AUTOINCREMENT,
      project_name TEXT NOT NULL DEFAULT 'default',
      code_name TEXT NOT NULL,
      normalized_name TEXT NOT NULL UNIQUE,
      created_at TEXT NOT NULL
    );
  `);

  db.exec(`
    CREATE TABLE IF NOT EXISTS review_codes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      project_name TEXT NOT NULL DEFAULT 'default',
      review_id TEXT NOT NULL,
      code_id INTEGER NOT NULL,
      sentiment TEXT NOT NULL,
      idea_text TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(review_id, code_id, idea_text)
    );
  `);

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_review_codes_review_id
    ON review_codes (review_id);
  `);

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_review_codes_code_id
    ON review_codes (code_id);
  `);

  const alterStatements = [
    "ALTER TABLE codes ADD COLUMN project_name TEXT NOT NULL DEFAULT 'default'",
    "ALTER TABLE review_codes ADD COLUMN project_name TEXT NOT NULL DEFAULT 'default'",
  ];
  for (const statement of alterStatements) {
    try {
      db.exec(statement);
    } catch {
      // ignore migration errors if column exists
    }
  }
}

function resolveReviewsTable(db) {
  const tables = db
    .prepare(
      `
      SELECT name
      FROM sqlite_master
      WHERE type = 'table'
      AND name IN ('reviews', 'reviews_staging')
    `
    )
    .all()
    .map((row) => row.name);

  if (tables.includes("reviews")) return "reviews";
  if (tables.includes("reviews_staging")) return "reviews_staging";
  throw new Error("No reviews source table found. Expected 'reviews' or 'reviews_staging'.");
}

function getCodeCache(db) {
  const rows = db
    .prepare("SELECT code_id, code_name, normalized_name, project_name FROM codes")
    .all();
  return rows.map((row) => ({
    code_id: Number(row.code_id),
    code_name: String(row.code_name),
    normalized_name: String(row.normalized_name),
    project_name: String(row.project_name || "default"),
  }));
}

function findOrCreateCode(db, cache, idea, nowIso, projectName) {
  const projectId = asProjectId(projectName);
  const canonical = canonicalizeAnalyticalCategory(idea);
  const normalizedIdea = normalizeText(canonical);
  if (!normalizedIdea) return null;
  const scopedNormalized = `${projectId}::${normalizedIdea}`;

  const exact = cache.find(
    (code) => code.project_name === projectId && code.normalized_name === scopedNormalized
  );
  if (exact) return exact.code_id;

  const similar = cache.find(
    (code) =>
      code.project_name === projectId &&
      tokenSimilarity(code.normalized_name.replace(`${projectId}::`, ""), normalizedIdea) >= 0.66
  );
  if (similar) return similar.code_id;

  const normalizedName = scopedNormalized;
  const codeName = normalizeCodeName(canonical);
  if (!looksSpanishEnough(codeName)) {
    return null;
  }
  const insert = db.prepare(
    "INSERT INTO codes (project_name, code_name, normalized_name, created_at) VALUES (?, ?, ?, ?)"
  );
  insert.run(projectId, codeName, normalizedName, nowIso);
  const row = db.prepare("SELECT last_insert_rowid() AS id").get();
  const codeId = Number(row.id);
  cache.push({
    code_id: codeId,
    code_name: codeName,
    normalized_name: normalizedName,
    project_name: projectId,
  });
  return codeId;
}

function buildUncodedQuery({ reviewsTable, hasRunIdFilter }) {
  const runIdClause = hasRunIdFilter ? "AND r.run_id = ?" : "";
  const projectExpr = "LOWER(REPLACE(TRIM(COALESCE(r.project_name, 'default')), ' ', '_'))";
  return `
    SELECT r.id, r.rating, r.comment, ${projectExpr} AS project_name
    FROM ${reviewsTable} r
    LEFT JOIN review_codes rc
      ON rc.review_id = r.id
      AND (
        rc.project_name = ${projectExpr}
        OR rc.project_name = 'default'
      )
    WHERE rc.review_id IS NULL
      AND TRIM(COALESCE(r.comment, '')) <> ''
      ${runIdClause}
    ORDER BY r.id
    LIMIT ?
  `;
}

function buildQueryParams(runId, batchSize) {
  return runId ? [runId, batchSize] : [batchSize];
}

async function processReviewBatch({
  db,
  uncodedRows,
  codeCacheByProject,
  insertReviewCode,
  nowIso,
}) {
  let codesCreated = 0;

  for (const review of uncodedRows) {
    const sentiment = sentimentFromRating(review.rating);
    const projectId = asProjectId(review.project_name || "default");
    const currentCodebook = (codeCacheByProject.get(projectId) || []).map((item) => item.code_name);
    const codeAssignments = await classifyCommentWithCodebook({
      comment: review.comment,
      codebook: currentCodebook,
    });
    const usedCodeIds = new Set();

    for (const assignment of codeAssignments) {
      const codeName = normalizeCodeName(assignment.code_name);
      if (!codeName) continue;
      const projectCache = codeCacheByProject.get(projectId) || [];
      const previousCodeCount = projectCache.length;

      const existingByName = projectCache.find((item) => item.code_name === codeName);
      let codeId = existingByName?.code_id || null;

      if (!codeId && assignment.is_new !== true) {
        const similar = projectCache.find(
          (item) => tokenSimilarity(normalizeText(item.code_name), normalizeText(codeName)) >= 0.66
        );
        if (similar) {
          codeId = similar.code_id;
        }
      }

      if (!codeId) {
        codeId = findOrCreateCode(
          db,
          projectCache,
          codeName,
          nowIso,
          projectId
        );
      }
      if (!codeId) continue;
      if (usedCodeIds.has(codeId)) continue;
      usedCodeIds.add(codeId);
      if (projectCache.length > previousCodeCount) {
        codesCreated += 1;
        codeCacheByProject.set(projectId, projectCache);
      }

      insertReviewCode.run(projectId, review.id, codeId, sentiment, codeName, nowIso);
    }
  }

  return {
    reviewsProcessed: uncodedRows.length,
    codesCreated,
  };
}

export function getCodeMetrics(db) {
  const rows = db
    .prepare(
      `
      SELECT
        c.project_name,
        c.code_id,
        c.code_name,
        COUNT(*) AS total_count,
        SUM(CASE WHEN rc.sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_count,
        SUM(CASE WHEN rc.sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_count,
        SUM(CASE WHEN rc.sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_count
      FROM review_codes rc
      INNER JOIN codes c ON c.code_id = rc.code_id
      GROUP BY c.project_name, c.code_id, c.code_name
      ORDER BY c.project_name ASC, total_count DESC, c.code_name ASC
    `
    )
    .all();

  return rows.map((row) => {
    const total = Number(row.total_count || 0);
    const positive = Number(row.positive_count || 0);
    const neutral = Number(row.neutral_count || 0);
    const negative = Number(row.negative_count || 0);
    const pct = (value) => (total > 0 ? Number(((value / total) * 100).toFixed(2)) : 0);

    return {
      project_name: String(row.project_name || "default"),
      code_id: Number(row.code_id),
      code_name: String(row.code_name),
      total_count: total,
      positive_count: positive,
      neutral_count: neutral,
      negative_count: negative,
      positive_pct: pct(positive),
      neutral_pct: pct(neutral),
      negative_pct: pct(negative),
    };
  });
}

export async function runReviewCodingPipeline({
  runId,
  batchSize = 8,
  maxBatches = 10,
  dbPath = "./data/abis_reviews.sqlite",
} = {}) {
  const absoluteDbPath = getSqlitePath(dbPath);
  const db = openDb(absoluteDbPath);
  ensureCodingTables(db);
  const reviewsTable = resolveReviewsTable(db);
  const hasRunIdFilter = Boolean(runId && reviewsTable === "reviews_staging");
  const uncodedQuery = buildUncodedQuery({ reviewsTable, hasRunIdFilter });
  const uncodedStatement = db.prepare(uncodedQuery);
  const insertReviewCode = db.prepare(
    `
    INSERT OR IGNORE INTO review_codes (project_name, review_id, code_id, sentiment, idea_text, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
  `
  );
  const rawCodeCache = getCodeCache(db);
  const codeCacheByProject = new Map();
  for (const code of rawCodeCache) {
    const pid = asProjectId(code.project_name);
    if (!codeCacheByProject.has(pid)) codeCacheByProject.set(pid, []);
    codeCacheByProject.get(pid).push(code);
  }
  const nowIso = new Date().toISOString();

  let processedReviews = 0;
  let createdCodes = 0;
  let batches = 0;

  while (batches < maxBatches) {
    const uncodedRows = uncodedStatement.all(
      ...buildQueryParams(hasRunIdFilter ? runId : null, batchSize)
    );
    if (uncodedRows.length === 0) {
      break;
    }

    const result = await processReviewBatch({
      db,
      uncodedRows,
      codeCacheByProject,
      insertReviewCode,
      nowIso,
    });
    processedReviews += result.reviewsProcessed;
    createdCodes += result.codesCreated;
    batches += 1;
  }

  if (processedReviews === 0) {
    const totalCodes = Number(db.prepare("SELECT COUNT(*) AS count FROM codes").get().count || 0);
    const metrics = getCodeMetrics(db);
    db.close();
    return {
      reviews_processed: 0,
      codes_created: 0,
      total_codes: totalCodes,
      code_metrics: metrics,
    };
  }

  const totalCodes = Number(db.prepare("SELECT COUNT(*) AS count FROM codes").get().count || 0);
  const metrics = getCodeMetrics(db);
  db.close();

  return {
    reviews_processed: processedReviews,
    codes_created: createdCodes,
    batches_processed: batches,
    total_codes: totalCodes,
    code_metrics: metrics,
  };
}

class DisjointSet {
  constructor(size) {
    this.parent = Array.from({ length: size }, (_, i) => i);
    this.rank = Array.from({ length: size }, () => 0);
  }
  find(x) {
    if (this.parent[x] !== x) {
      this.parent[x] = this.find(this.parent[x]);
    }
    return this.parent[x];
  }
  union(x, y) {
    const rx = this.find(x);
    const ry = this.find(y);
    if (rx === ry) return;
    if (this.rank[rx] < this.rank[ry]) this.parent[rx] = ry;
    else if (this.rank[rx] > this.rank[ry]) this.parent[ry] = rx;
    else {
      this.parent[ry] = rx;
      this.rank[rx] += 1;
    }
  }
}

export function consolidateCodeDictionary({
  dbPath = "./data/abis_reviews.sqlite",
} = {}) {
  const absoluteDbPath = getSqlitePath(dbPath);
  const db = openDb(absoluteDbPath);
  ensureCodingTables(db);

  const codes = db
    .prepare(
      `
      SELECT
        c.project_name,
        c.code_id,
        c.code_name,
        c.normalized_name,
        COUNT(rc.id) AS usage_count
      FROM codes c
      LEFT JOIN review_codes rc ON rc.code_id = c.code_id
      GROUP BY c.project_name, c.code_id, c.code_name, c.normalized_name
      ORDER BY c.project_name ASC, usage_count DESC, c.code_id ASC
    `
    )
    .all()
    .map((row) => ({
      code_id: Number(row.code_id),
      project_name: String(row.project_name || "default"),
      code_name: String(row.code_name),
      normalized_name: String(row.normalized_name),
      usage_count: Number(row.usage_count || 0),
    }));

  if (codes.length <= 1) {
    const totalCodes = Number(db.prepare("SELECT COUNT(*) AS c FROM codes").get().c || 0);
    const reviewRows = Number(
      db.prepare("SELECT COUNT(*) AS c FROM review_codes").get().c || 0
    );
    db.close();
    return {
      merged_groups: 0,
      merged_codes: 0,
      total_codes_after: totalCodes,
      review_code_rows_after: reviewRows,
    };
  }

  const ds = new DisjointSet(codes.length);
  for (let i = 0; i < codes.length; i += 1) {
    for (let j = i + 1; j < codes.length; j += 1) {
      if (shouldMergeCodes(codes[i], codes[j])) {
        if (codes[i].project_name !== codes[j].project_name) continue;
        ds.union(i, j);
      }
    }
  }

  const groups = new Map();
  for (let i = 0; i < codes.length; i += 1) {
    const root = ds.find(i);
    if (!groups.has(root)) groups.set(root, []);
    groups.get(root).push(codes[i]);
  }

  const updateCodeId = db.prepare("UPDATE review_codes SET code_id = ? WHERE code_id = ?");
  let mergedCodes = 0;
  let mergedGroups = 0;

  for (const [, group] of groups) {
    if (group.length <= 1) continue;
    mergedGroups += 1;

    const master = [...group].sort((a, b) => {
      if (b.usage_count !== a.usage_count) return b.usage_count - a.usage_count;
      return a.code_id - b.code_id;
    })[0];

    for (const code of group) {
      if (code.code_id === master.code_id) continue;
      updateCodeId.run(master.code_id, code.code_id);
      mergedCodes += 1;
    }
  }

  db.exec(`
    DELETE FROM review_codes
    WHERE id NOT IN (
      SELECT MIN(id)
      FROM review_codes
      GROUP BY review_id, code_id
    );
  `);

  db.exec(`
    DELETE FROM codes
    WHERE code_id NOT IN (
      SELECT DISTINCT code_id FROM review_codes
    );
  `);

  const totalCodesAfter = Number(db.prepare("SELECT COUNT(*) AS c FROM codes").get().c || 0);
  const reviewCodeRowsAfter = Number(
    db.prepare("SELECT COUNT(*) AS c FROM review_codes").get().c || 0
  );
  db.close();

  return {
    merged_groups: mergedGroups,
    merged_codes: mergedCodes,
    total_codes_after: totalCodesAfter,
    review_code_rows_after: reviewCodeRowsAfter,
  };
}
