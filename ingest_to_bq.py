"""
ingest_to_bq.py
---------------
Called by GitHub Actions. Fetches live ECOBICI snapshot and writes to BigQuery.
"""

import os
import sys
import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError
import pandas as pd

from ingestion.gbfs_client import get_enriched_snapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


def write_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str) -> int:
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Strip timezone info after converting to UTC — pyarrow needs naive datetimes
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

    logger.info("Writing %d rows to %s...", len(df), table_ref)
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info("Done. Rows written: %d", len(df))
    return len(df)


def main():
    project_id = os.environ.get("GCP_PROJECT_ID")
    dataset_id = os.environ.get("BQ_DATASET_ID")
    table_id   = os.environ.get("BQ_TABLE_ID")

    if not all([project_id, dataset_id, table_id]):
        logger.error("Missing environment variables. Need: GCP_PROJECT_ID, BQ_DATASET_ID, BQ_TABLE_ID")
        sys.exit(1)

    logger.info("Fetching ECOBICI snapshot...")
    snapshot_df, _ = get_enriched_snapshot()
    logger.info("Fetched %d stations.", len(snapshot_df))

    rows = write_to_bigquery(snapshot_df, project_id, dataset_id, table_id)
    logger.info("Pipeline complete. %d rows in BigQuery.", rows)


if __name__ == "__main__":
    main()