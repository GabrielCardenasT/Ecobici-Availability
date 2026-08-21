# ecobici-rebalancing-pipeline

```
ecobici-rebalancing-pipeline/
│
├── .env.example                    # Template for secrets (never commit .env)
├── .gitignore
├── README.md
│
├── ingestion/                      # Phase 1 — Local extraction logic
│   ├── __init__.py
│   ├── gbfs_client.py              # API client: fetches + merges both feeds
│   ├── schema.py                   # Pydantic models / column contracts
│   └── extract.py                  # Entrypoint: runs extraction, saves snapshot
│
├── data/                           # Local snapshot output (git-ignored)
│   ├── raw/                        # Raw JSON responses (one file per poll)
│   └── processed/                  # Normalized Parquet/CSV snapshots
│
├── infrastructure/                 # Phase 2 — Terraform IaC
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── modules/
│       ├── bigquery/
│       └── cloud_scheduler/
│
├── functions/                      # Phase 3 — Cloud Run Function
│   ├── main.py                     # GCP function entrypoint
│   ├── requirements.txt
│   └── Dockerfile                  # Optional: for local Cloud Run emulation
│
├── dbt/                            # Phase 4 — Analytics engineering
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_station_snapshots.sql
│   │   ├── intermediate/
│   │   │   └── int_station_velocity.sql
│   │   └── marts/
│   │       ├── mart_rebalancing_alerts.sql
│   │       └── mart_cluster_health.sql
│   ├── tests/
│   │   └── assert_capacity_positive.sql
│   └── macros/
│       └── depletion_velocity.sql
│
├── notebooks/                      # EDA / one-off analysis (not production)
│   └── 01_schema_exploration.ipynb
│
└── requirements.txt                # Shared Python dependencies
```

## Key Design Decisions

- `ingestion/` is pure Python with no GCP dependencies.
  This lets you run and test locally before any cloud deployment.

- `data/` is .gitignored. Snapshots live in GCS/BigQuery in production.

- `infrastructure/` and `functions/` are completely decoupled.
  You can iterate on SQL models without touching IaC.

- `dbt/` models follow the staging → intermediate → mart pattern.
  Staging = raw cleaning. Intermediate = business logic. Marts = final outputs.
