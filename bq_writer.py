"""
bq_writer.py — BigQuery write layer
-------------------------------------
Handles all interaction with the BigQuery API.

Write strategy: load jobs (load_table_from_dataframe)
  - Fully supported by BigQuery Sandbox (no billing account needed)
  - Rows are queryable within seconds of the job completing
  - No GCS staging required
  - Free tier: 10 GB storage / month, 1 TB queries / month
"""

import logging
import time
from typing import Any

import pandas as pd
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition

logger = logging.getLogger("ecobici.bq_writer")


class BigQueryWriteError(Exception):
    """Raised when a BigQuery write fails after all retries are exhausted."""


class BigQueryWriter:
    """
    Stateful BQ client wrapper. Initialized once and reused across
    invocations to avoid repeated authentication overhead.
    """

    def __init__(self, project_id: str, dataset_id: str, table_id: str) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id   = table_id
        self.table_ref  = f"{project_id}.{dataset_id}.{table_id}"

        self._client = bigquery.Client(project=project_id)
        logger.info("BigQuery client initialized — table: %s", self.table_ref)

    # -------------------------------------------------------------------------
    # Type normalization
    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize_value(val: Any) -> Any:
        """Converts pandas/numpy types to JSON-serializable Python natives."""
        import numpy as np
        from datetime import datetime

        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass

        if isinstance(val, pd.Timestamp):
            return val.isoformat()
        if isinstance(val, datetime):
            return val.isoformat()
        if isinstance(val, np.bool_):
            return bool(val)
        if isinstance(val, np.integer):
            return int(val)
        if isinstance(val, np.floating):
            return float(val)

        return val

    # -------------------------------------------------------------------------
    # Write
    # -------------------------------------------------------------------------

    def write_snapshot(
        self,
        df: pd.DataFrame,
        max_retries: int = 3,
        retry_delay_s: float = 2.0,
    ) -> int:
        """
        Writes a snapshot DataFrame to BigQuery via a load job.

        Returns the number of rows written.
        Raises BigQueryWriteError if all retries fail.
        """
        if df.empty:
            logger.warning("write_snapshot called with empty DataFrame — nothing to write.")
            return 0

        job_config = LoadJobConfig(
            write_disposition=WriteDisposition.WRITE_APPEND,
        )

        logger.info("Writing %d rows to %s...", len(df), self.table_ref)

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                job = self._client.load_table_from_dataframe(
                    df, self.table_ref, job_config=job_config
                )
                job.result()  # blocks until the load job completes

                logger.info(
                    "BigQuery write successful — %d rows written (attempt %d).",
                    len(df), attempt,
                )
                return len(df)

            except (GoogleAPICallError, RetryError) as exc:
                last_error = exc
                if attempt < max_retries:
                    delay = retry_delay_s * (2 ** (attempt - 1))
                    logger.warning(
                        "BigQuery error on attempt %d — retrying in %.0fs. Error: %s",
                        attempt, delay, exc,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "BigQuery write failed after %d attempts. Last error: %s",
                        attempt, exc,
                    )

        raise BigQueryWriteError(
            f"BigQuery write failed after {max_retries} attempts. "
            f"Last error: {last_error}"
        )
