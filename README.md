# ABIS Reviews Pipeline (Local)

Proyecto local separado para scraping y análisis de reseñas de Google Maps.

Este proyecto:
- corre en tu máquina (no en Vercel),
- guarda datos en SQLite,
- genera reportes ejecutivos (`csv`, `md`, `json`),
- opcionalmente prepara un payload para sincronización posterior.

## Requisitos

- Node.js 22+
- `OPENAI_API_KEY` en `.env.local` o variable de entorno del shell (solo para `reviews:code`)

## Estructura

- `scripts/`: procesos del pipeline
- `lib/abis/modules/sentiment/`: scraper + coding engine
- `data/`: SQLite, archivos de entrada y reportes

## Configuración de ubicaciones

1. Copia el ejemplo:

```bash
cp data/reviews-locations.lines.example.txt data/reviews-locations.lines.txt
```

2. Edita `data/reviews-locations.lines.txt` con formato:

```txt
proyecto|group|nombre_ubicacion|google_maps_url|max_comment_age_days|fallback_limit|scroll_cycles
La Europea|cliente|La Europea Interlomas|https://www.google.com/maps/place/...|30|25|12
La Europea|competitor_1|Competidor X|https://www.google.com/maps/place/...|30|25|12
```

## Ejecución

Instalar:

```bash
npm install
```

Correr todo el pipeline:

```bash
npm run reviews:run
```

O por etapas:

```bash
npm run reviews:scrape
npm run reviews:analyze
npm run reviews:code
npm run reviews:executive
npm run reviews:payload
```

Modo scraping recomendado (abre navegador y hace scroll visible):

```bash
npm run reviews:scrape:visual
```

Modo headless (más rápido, menor estabilidad en algunos layouts de Maps):

```bash
npm run reviews:scrape:headless
```

## Salidas principales

- `data/abis_reviews.sqlite`
- `data/reviews-analysis-report.json`
- `data/reviews-code-metrics.json`
- `data/reviews-executive-summary.csv`
- `data/reviews-executive-latest-reviews.csv`
- `data/reviews-executive-report.md`
- `data/reviews-sync-payload.json`
- `out/reviews-report-YYYY-MM-DD.../` (bundle para compartir/publicar)

## Publicación posterior

La recomendación es publicar solo el reporte final (`csv/md/json`) en `estrategiaretail.com` o sincronizar payload bajo demanda.
