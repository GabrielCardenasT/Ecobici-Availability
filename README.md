
# ECOBICI Real-Time Rebalancing & Availability Pipeline

A production-grade data engineering pipeline tracking live bikeshare availability
across Mexico City's ECOBICI network, built entirely on GCP's Always Free tier.

## The Problem
During rush hours, bikes drain from residential zones (Roma/Condesa) and overflow
into corporate zones (Reforma/Polanco). Logistics crews rebalance manually with
no real-time intelligence.

## The Solution
A serverless pipeline that polls the live GBFS API every 5 minutes, computes
depletion velocity per station, and surfaces rebalancing alerts to an operations
dashboard before stations run critically low.

## Stack
- **Ingestion:** Python 3.12, Pydantic, Requests → Cloud Run Function (Gen 2)
- **Orchestration:** Cloud Scheduler (cron every 5 min)
- **Warehouse:** BigQuery (partitioned + clustered)
- **Transformation:** dbt Core (5 models, 45 tests, custom macros)
- **Infrastructure:** Terraform (IaC, Always Free tier)
- **Dashboard:** Looker Studio

## Pipeline Architecture

GBFS API → Cloud Run Function → BigQuery (raw)

                                     ↓
                                     
                              dbt Core models
                              
                                     ↓
                                     
                    ┌────────────────┴───────────────────┐
                    ↓                                    ↓
         mart_rebalancing_alerts            mart_cluster_health
                    ↓                                    ↓
                         Looker Studio Dashboard


## Project Structure

ingestion/       # Python extraction + schema validation
infrastructure/  # Terraform IaC (BigQuery, GCS, IAM, Scheduler)
functions/       # Cloud Run Function entrypoint
dbt/             # Analytics models (staging → intermediate → marts)


## Running Locally
bash
python -m venv .venv
.venv\Scripts\Activate.ps1     # Windows
pip install -r requirements.txt
python -m ingestion.extract
