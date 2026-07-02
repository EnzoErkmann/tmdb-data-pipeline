# Portfolio Project — TMDB Data Pipeline

## Objective
A daily batch pipeline that ingests movie data from the TMDB API, transforms it into layers, and delivers a dashboard with trends, popularity, and ratings. 

Currently, the project is in the Minimum Viable Product (MVP) phase. The initial GCP infrastructure was manually provisioned to validate Python extraction, Airflow orchestration, and dbt transformations, ensuring the data flow works from end to end.

---

## Data Architecture (Data Lakehouse)

The data flow follows the Modern Data Stack pattern, replacing intermediate transactional databases with a Data Lake in Google Cloud Storage:

1. **Extraction:** Python scripts extract data from the TMDB API and save the raw files (`.json` format) in a GCS bucket (Landing Zone).
2. **Ingestion:** Apache Airflow orchestrates the transfer from GCS to the BigQuery Bronze layer. For this, we use the native `GCSToBigQueryOperator`.
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

## TMDB Endpoints Used

| Endpoint | Description | Frequency |
|---|---|---|
| `/trending/movie/day` | Daily trending | Daily |
| `/movie/popular` | Currently popular movies | Daily |
| `/movie/now_playing` | Movies currently in theaters | Daily |
| `/movie/changes` | Recently changed IDs for incremental load | Weekly |
| `/movie/{id}` | Full details (genre, budget, revenue) | Weekly / One-time |
| `/movie/{id}/credits` | Cast and director | Weekly / One-time |

---

## Data Modeling: Medallion Architecture

### 🥉 Bronze Layer (Raw)
* Raw data from the API, loaded in its original format.
* Tables partitioned by ingestion date for historical tracking.
* Main tables: `raw_movies`, `raw_credits`, `raw_genres`.

### 🥈 Silver Layer (Staging)
* Null data cleansing, type standardization, and entity separation.
* dbt models: `stg_movies.sql`, `stg_genres.sql`, `stg_credits.sql`.

### 🥇 Gold Layer (Marts)
* Aggregated facts and dimensions, ready for direct dashboard consumption.
* dbt models: `mart_popular_movies.sql`, `mart_genre_ratings.sql`, `mart_revenue_vs_budget.sql`.

---

## Implemented Data Engineering Best Practices

**Data Quality Tests**
Configured native dbt tests (`schema.yml`) in the Silver and Gold layers to ensure data reliability. Rules applied include uniqueness (`unique`) and non-null (`not_null`) tests for primary keys, plus value restrictions (e.g., `vote_average` between 0 and 10).

**DAG Idempotency**
Airflow DAGs were designed to be idempotent. Date partitioning and the use of incremental materializations in dbt ensure that failure re-executions do not duplicate data in BigQuery.

**Resilience and Rate Limiting**
Implementation of Retry and Exponential Backoff strategies (via the Tenacity library) in Python scripts. This prevents pipeline failures when hitting API request limits (HTTP 429).

**Credential Security**
Strict use of `Connections` and `Variables` in Airflow, alongside `.env` files managed via `.gitignore`, eliminating any hardcoded API keys or cloud credentials in the public repository.