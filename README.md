# Portfolio Project — TMDB Data Pipeline

## Objective

A batch pipeline that ingests movie data from the TMDB API, transforms it into layers, and delivers a dashboard with trends, popularity, ratings, and analytical insights across genres, budgets, box office performance, and regional film production.

The pipeline is designed around a key architectural decision: **separating hot data from cold data**. Most movie metadata is historical and static — it does not change after release. Only a small subset of fields (popularity, ratings, vote count) fluctuates over time. This separation drives the ingestion frequency strategy and avoids unnecessary API calls and processing costs.

Currently, the project is in the Minimum Viable Product (MVP) phase. The initial GCP infrastructure was manually provisioned to validate Python extraction, Airflow orchestration, and dbt transformations, ensuring the data flow works from end to end.

---

## Data Architecture (Data Lakehouse)

The data flow follows the Modern Data Stack pattern, replacing intermediate transactional databases with a Data Lake in Google Cloud Storage:

1. **Extraction:** Python scripts extract data from the TMDB API and save the raw files (`.json` format) in a GCS bucket (Landing Zone).
2. **Ingestion:** Apache Airflow orchestrates the transfer from GCS to the BigQuery Bronze layer using the native `GCSToBigQueryOperator`.
3. **Transformation:** dbt Core processes the data within BigQuery, promoting it to the Silver and Gold layers.
4. **Visualization:** Looker Studio directly consumes the Gold layer to feed the dashboards.

---

## Tech Stack

| Tool | Function |
|---|---|
| **TMDB API** | Free data source |
| **Python** | Extraction scripts and pagination handling |
| **Google Cloud Storage** | Data Lake / Landing Zone (Raw files) |
| **Apache Airflow** | Task orchestration (Local Docker) |
| **BigQuery** | Data Warehouse (GCP free tier) |
| **dbt Core** | Data transformation, quality, and documentation |
| **Looker Studio** | Final dashboard for consumption |
| **GitHub** | Code versioning and portfolio |

---

## Hot Data vs Cold Data

A core architectural decision in this project is distinguishing between data that changes frequently and data that is essentially static after a film's release.

### 🔥 Hot Data — changes regularly
| Field | Why it changes |
|---|---|
| `popularity` | Fluctuates daily based on searches and user interactions on TMDB |
| `vote_average` | Grows as more users submit ratings |
| `vote_count` | Increments continuously |

### 🧊 Cold Data — static after release
| Field | Why it is static |
|---|---|
| `budget`, `revenue` | Historical financial data, rarely corrected |
| `genres`, `runtime` | Fixed after film release |
| `production_countries`, `original_language` | Never changes |
| `credits` (cast, director) | Fixed after release |
| `release_date`, `title` | Immutable |

This distinction drives the ingestion frequency design: cold data is loaded once historically and refreshed only when TMDB reports a change. Hot data is updated on a daily or weekly cadence.

---

## Ingestion Strategy by Frequency

### 🗂️ One-time (Historical Setup)
- Download the **TMDB Daily ID Export** — a `.gz` file published daily at:
  ```
  http://files.tmdb.org/p/exports/movie_ids_MM_DD_YYYY.json.gz
  ```
  This file contains all movie IDs in the TMDB database (hundreds of thousands of entries).
- Filter by a minimum popularity threshold to avoid obscure entries with no analytical value.
- For each ID, call `/movie/{id}` and `/movie/{id}/credits` to build the full historical base.
- Save raw JSONs partitioned by ID range in GCS.

### 📅 Daily (Hot Data Refresh)
- `/trending/movie/day` — updates trending rankings
- `/movie/popular` — refreshes current popularity scores
- `/movie/now_playing` — tracks what is currently in theaters
- Target: top ~1,000 most relevant films updated daily for dashboard freshness

### 🗓️ Monthly (Incremental Cold Data)
- `/movie/changes` — returns only IDs that TMDB flagged as modified in the last X days
- For each changed ID, re-fetch `/movie/{id}` and `/movie/{id}/credits`
- Updates corrections to budget, revenue, credits, or metadata without reprocessing the full dataset

---

## TMDB Endpoints Used

| Endpoint | Description | Frequency | Data Type |
|---|---|---|---|
| TMDB Daily Export (`.gz`) | Full list of all movie IDs | One-time | Cold |
| `/movie/{id}` | Full details: genre, budget, revenue, runtime, release date, origin country, languages, companies | One-time + Monthly incremental | Cold |
| `/movie/{id}/credits` | Cast and director | One-time + Monthly incremental | Cold |
| `/genre/movie/list` | Full genre list with IDs | One-time | Cold |
| `/trending/movie/day` | Daily trending movies | Daily | Hot |
| `/movie/popular` | Currently popular movies | Daily | Hot |
| `/movie/now_playing` | Movies currently in theaters | Daily | Hot |
| `/movie/changes` | Recently changed IDs for incremental refresh | Monthly | Hot → triggers Cold re-fetch |

> **Note:** `/movie/{id}` is the richest endpoint. It returns `production_countries`, `original_language`, `budget`, `revenue`, `vote_average`, `vote_count`, `release_date`, `genres`, and `runtime` — covering the majority of dashboard questions.

---

## GCS Bucket Structure

The bucket is organized by data type, mirroring the endpoint that generated each file. Every folder receives data from a specific Python script and feeds a specific Bronze table in BigQuery.

```
gs://tmdb-data-pipeline/
│
├── raw/
│   │
│   ├── movie_ids/
│   │   └── 2026-07-02.json.gz         ← TMDB Daily Export (.gz)
│   │                                     Script: load_movie_ids.py
│   │                                     Frequency: one-time
│   │                                     BQ Bronze: raw_movie_ids
│   │
│   ├── movies/
│   │   └── 2026-07-02/
│   │       ├── 550.json               ← /movie/{id}
│   │       └── 551.json                  Script: load_movie_details.py
│   │                                     Frequency: one-time + monthly incremental
│   │                                     BQ Bronze: raw_movies
│   │
│   ├── credits/
│   │   └── 2026-07-02/
│   │       ├── 550.json               ← /movie/{id}/credits
│   │       └── 551.json                  Script: load_movie_details.py (same run)
│   │                                     Frequency: one-time + monthly incremental
│   │                                     BQ Bronze: raw_credits
│   │
│   ├── genres/
│   │   └── genres.json                ← /genre/movie/list
│   │                                     Script: load_genres.py
│   │                                     Frequency: one-time
│   │                                     BQ Bronze: raw_genres
│   │
│   ├── trending/
│   │   └── 2026-07-02.json            ← /trending/movie/day
│   │                                     Script: load_trending.py
│   │                                     Frequency: daily
│   │                                     BQ Bronze: raw_trending
│   │
│   ├── popular/
│   │   └── 2026-07-02.json            ← /movie/popular
│   │                                     Script: load_trending.py (same run)
│   │                                     Frequency: daily
│   │                                     BQ Bronze: raw_popular
│   │
│   └── now_playing/
│       └── 2026-07-02.json            ← /movie/now_playing
│                                         Script: load_trending.py (same run)
│                                         Frequency: daily
│                                         BQ Bronze: raw_now_playing
```

### Design Decisions
- **One file per date** — enables reprocessing a specific day without touching others
- **One folder per endpoint** — clear lineage from source to storage to BigQuery
- **Credits saved separately from movies** — different schema, different Bronze table, easier to load independently
- **`load_movie_details.py` handles both `/movie/{id}` and `/movie/{id}/credits`** — they are always fetched together in the same loop, so it makes sense to keep them in one script but save to separate folders
- **`load_trending.py` handles popular, trending and now_playing** — all three are hot data, daily, and follow the same pagination pattern

---

## Dashboard Metrics & Analytical Questions

### 🎬 Genre Analysis
- **Most popular genres by decade** — group by `release_date` decade and genre, rank by average `popularity` and `vote_average`
- **Genres with the highest volume of low-rated films** — filter `vote_average < 5` and `vote_count > 100`, group by genre
- **Genre trends over time** — count of releases per genre per year
- **ROI by genre** — `(revenue - budget) / budget` grouped by genre

### 💰 Budget & Box Office
- **Budget evolution over time** — average `budget` per year, adjusted for volume
- **Biggest box offices of all time** — rank by `revenue`, with budget comparison
- **Budget vs popularity outliers** — low-budget films with high `revenue` or `popularity` (scatter plot)

### 🎥 Directors
- **Worst directors by average rating** — aggregate `vote_average` per director (minimum film count filter to avoid noise, e.g., `>= 5 films`)
- **Best directors by ROI** — directors whose films consistently outperform budget expectations
- **Most prolific directors** — count of films per director over time

### 🌍 Regional & International Films
- **Non-American and non-European films with highest ratings and revenue** — filter `production_countries` excluding US, UK, FR, DE, IT, ES; rank by `vote_average` and `revenue`
- **Original language diversity** — distribution of `original_language` across the dataset
- **Emerging film markets** — countries with increasing production volume over decades

### 📊 Outliers & Interesting Correlations
- **Poor films with high box office** — `vote_average < 5` but `revenue` in top 20% (guilty pleasures)
- **Critically acclaimed films with low revenue** — `vote_average > 7.5` but `revenue` in bottom 20%
- **Runtime vs rating correlation** — do longer films tend to score higher?
- **Hidden gems** — high `vote_average` + low `vote_count` + low `revenue` (under the radar films)

---

## Data Modeling: Medallion Architecture

### 🥉 Bronze Layer (Raw)
- Raw data from the API, loaded in its original format.
- Tables partitioned by ingestion date for historical tracking.
- Main tables: `raw_movies`, `raw_credits`, `raw_genres`.

### 🥈 Silver Layer (Staging)
- Null data cleansing, type standardization, and entity separation.
- Explodes nested fields (e.g., `genres`, `production_countries`, `spoken_languages`) into separate relational tables.
- dbt models:
  - `stg_movies.sql` — core movie attributes
  - `stg_genres.sql` — genre dimension
  - `stg_credits.sql` — cast and crew
  - `stg_movie_genres.sql` — bridge table (movie ↔ genre)
  - `stg_production_countries.sql` — bridge table (movie ↔ country)

### 🥇 Gold Layer (Marts)
- Aggregated facts and dimensions ready for direct dashboard consumption.
- dbt models:
  - `mart_genre_by_decade.sql` — popularity and rating per genre per decade
  - `mart_box_office.sql` — top revenue films with budget comparison
  - `mart_budget_over_time.sql` — average budget evolution per year
  - `mart_director_ratings.sql` — director performance ranked by average rating
  - `mart_director_roi.sql` — director ROI ranking
  - `mart_international_films.sql` — non-US/EU films ranked by rating and revenue
  - `mart_low_rated_genres.sql` — genres with highest volume of bad films
  - `mart_outliers_budget_popularity.sql` — scatter data for budget vs popularity analysis
  - `mart_hidden_gems.sql` — high rating + low revenue + low vote count films
  - `mart_guilty_pleasures.sql` — low rating + high revenue films

---

## Implemented Data Engineering Best Practices

**Hot vs Cold Data Separation**
The pipeline distinguishes between fields that change frequently (popularity, ratings) and fields that are static after release (budget, credits, genres). This avoids full reloads and reduces unnecessary API calls, reflecting a production-grade architectural decision.

**Data Quality Tests**
Configured native dbt tests (`schema.yml`) in the Silver and Gold layers to ensure data reliability. Rules applied include uniqueness (`unique`) and non-null (`not_null`) tests for primary keys, plus value restrictions (e.g., `vote_average` between 0 and 10, `budget >= 0`, `revenue >= 0`).

**Vote Count Filtering**
All rating-based aggregations apply a minimum `vote_count` threshold (e.g., `>= 100 votes`) to avoid statistical noise from films with very few reviews.

**DAG Idempotency**
Airflow DAGs were designed to be idempotent. Date partitioning and the use of incremental materializations in dbt ensure that failure re-executions do not duplicate data in BigQuery.

**Resilience and Rate Limiting**
Implementation of Retry and Exponential Backoff strategies (via the Tenacity library) in Python scripts. This prevents pipeline failures when hitting API request limits (HTTP 429).

**Credential Security**
Strict use of `Connections` and `Variables` in Airflow, alongside `.env` files managed via `.gitignore`, eliminating any hardcoded API keys or cloud credentials in the public repository.

**Nested Field Handling**
TMDB returns arrays for fields like `genres`, `production_countries`, and `spoken_languages`. The Silver layer explodes these into proper bridge tables, enabling clean many-to-many joins in the Gold layer without data duplication.