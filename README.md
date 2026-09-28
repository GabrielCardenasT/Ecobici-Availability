# ECOBICI Real-Time Rebalancing & Availability Pipeline

![Pipeline Status](https://github.com/GabrielCardenasT/Ecobici-Availability/actions/workflows/ingest.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.12-blue)
![BigQuery](https://img.shields.io/badge/BigQuery-Sandbox-orange)
![dbt](https://img.shields.io/badge/dbt-1.12-red)

> A production-grade data pipeline tracking real-time bikeshare availability
> across 677 stations in Mexico City, built entirely on free-tier infrastructure.

## 🔴 Live Dashboard
**[→ View on Looker Studio](https://datastudio.google.com/reporting/55f90a71-20f4-45bb-8e8f-5312a3e9ecbd)**

![Dashboard Preview](<img width="1464" height="885" alt="image" src="https://github.com/user-attachments/assets/4ddddc62-723c-4c1e-9c37-aea1f5108d0e" />

)

---

## The Problem

During CDMX morning rush hour, bikes drain from residential zones like
Roma/Condesa and overflow into corporate zones like Reforma/Polanco.
Rebalancing crews operate blind — no real-time intelligence, no priority signals.

## The Solution

An automated pipeline that polls the live ECOBICI GBFS API every 15 minutes,
computes depletion velocity per station, and surfaces rebalancing alerts to an
operations dashboard before stations run critically low.

## Pipeline Architecture
ECOBICI GBFS API (677 stations)
↓ every 15 min
GitHub Actions CI/CD
↓
Python ingestion script
(Pydantic validation)
↓
BigQuery Sandbox
(partitioned by day, clustered by station_id)
↓
dbt Core (5 models, 21 tests)
staging → intermediate → marts
↓
Looker Studio Dashboard


## Key Technical Decisions

- **GitHub Actions over Cloud Run** — BigQuery Sandbox blocks streaming inserts
  and DML, so the pipeline uses load jobs triggered by CI/CD instead of a
  serverless function. Same automation, zero cost.

- **Partitioned + clustered BigQuery tables** — raw table partitioned by
  `ingested_at_utc` (DAY) and clustered by `station_id`. Reduces bytes scanned
  by ~80% on per-station time-series queries.

- **Depletion velocity via LAG window** — dbt computes bikes-per-minute drain
  rate using `LAG() OVER (PARTITION BY station_id ORDER BY ingested_at_utc)`.
  NULL-guarded for gaps > 30 minutes to avoid false alerts during outages.

- **Terraform IaC** — full infrastructure defined as code (BigQuery datasets,
  GCS buckets, IAM, Cloud Scheduler). Not deployed due to Sandbox constraints
  but production-ready for a billing-enabled project.

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.12, Requests, Pydantic |
| Orchestration | GitHub Actions (cron every 15 min) |
| Warehouse | BigQuery Sandbox |
| Transformation | dbt Core 1.12 |
| Infrastructure | Terraform (IaC) |
| Dashboard | Looker Studio |

## dbt Models
stg_station_snapshots (view) — clean + filter raw snapshots
int_station_velocity (table) — LAG window, depletion velocity
int_neighborhood_clusters (table) — station → CDMX zone mapping
mart_rebalancing_alerts (table) — one row per active alert
mart_cluster_health (table) — zone-level aggregations


## Running Locally

```bash
# Clone and set up
git clone https://github.com/GabrielCardenasT/Ecobici-Availability.git
cd Ecobici-Availability
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1      # Windows
pip install -r requirements.txt

# Run live extraction
python -m ingestion.extract
```

## Project Structure
ingestion/ Python extraction + Pydantic schema validation
functions/ Cloud Run Function (production deployment)
infrastructure/ Terraform IaC — BigQuery, GCS, IAM, Scheduler
dbt/ Analytics models (staging → intermediate → marts)
.github/ GitHub Actions CI/CD workflow

## Data Source

ECOBICI GBFS feed — Mexico City's public bikeshare open data API.
Updated every ~5 minutes by Lyft/ECOBICI infrastructure.
`https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json`

