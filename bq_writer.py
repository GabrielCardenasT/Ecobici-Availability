"""
bq_writer.py — BigQuery write layer
-------------------------------------
Handles all interaction with the BigQuery streaming insert API.
Kept separate from main.py so it can be unit-tested independently
and swapped for a batch-load strategy if cost becomes a concern.

Write strategy: streaming inserts (insertAll API)
  - Pro: rows appear in BQ within ~1 second of ingestion
  - Pro: no GCS staging required, lower operational complexity
  - Pro: at our scale (~500 rows × 288 polls/day) the cost is ~$0.04/month
  - Con: technically not in the Always Free tier (free tier covers load jobs)
  - Con: rows written via streaming are in a "streaming buffer" and cannot
         be updated or deleted for ~90 minutes after insertion.

  Alternative (batch load): use load_table_from_dataframe() with a GCS
  staging file — fully free, but adds ~10-30s latency and GCS dependency.
  Switch to this if the project scales beyond the free tier budget.

Type normalization contract:
  BigQuery's insertAll JSON API requires:
    - datetime objects → ISO-8601 strings  (e.g. "2026-05-18T22:46:00+00:00")
    - pandas Timestamp → same
    - numpy int64/float64 → Python int/float
    - numpy bool_ → Python bool
    - pandas NA / float NaN → None (JSON null)
  This module handles all of these conversions before the API call.
"""

import logging
import time
from typing import Any

import pandas as pd
from google.api_core.exceptions import GoogleAPICallError, RetryError
from google.cloud import bigquery

logger = logging.getLogger("ecobici.bq_writer")


class BigQueryWriteError(Exception):
    """Raised when a BigQuery write fails after all retries are exhausted."""


class BigQueryWriter:
    """
    Stateful BQ client wrapper. Initialized once at module level in main.py
    and reused across warm Cloud Run invocations — avoids per-request
    authentication overhead and connection setup.
    """

    def __init__(self, project_id: str, dataset_id: str, table_id: str) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id   = table_id
        self.table_ref  = f"{project_id}.{dataset_id}.{table_id}"

        # bigquery.Client uses Application Default Credentials automatically.
        # In Cloud Run: the function's service account (set in Terraform).
        # Locally with DRY_RUN=false: your gcloud auth application-default login.
        self._client = bigquery.Client(project=project_id)
        logger.info("BigQuery client initialized", extra={"table": self.table_ref})

    # -------------------------------------------------------------------------
    # Type normalization
    # -------------------------------------------------------------------------

    @staticmethod
    def _normalize_value(val: Any) -> Any:
        """
        Converts a single cell value to a JSON-serializable Python native type.
        Called on every cell before building the insertAll payload.
        """
        import numpy as np
        from datetime import datetime

        # pandas NA / numpy NaN → JSON null
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass  # isna() raises on non-scalar types; ignore

        # pandas Timestamp / datetime → ISO-8601 string
        if isinstance(val, pd.Timestamp):
            return val.isoformat()
        if isinstance(val, datetime):
            return val.isoformat()

        # numpy integer types → Python int
        if isinstance(val, (np.integer,)):
            return int(val)

        # numpy float types → Python float
        if isinstance(val, (np.floating,)):
            return float(val)

        # numpy bool_ → Python bool
        # IMPORTANT: must check before int — numpy bool_ is a subclass of np.integer
        if isinstance(val, (np.bool_,)):
            return bool(val)

        # Python native types pass through unchanged
        return val

    def _dataframe_to_rows(self, df: pd.DataFrame) -> list[dict]:
        """
        Converts a DataFrame to a list of dicts with all values normalized
        to JSON-serializable Python natives.
        Called once per snapshot batch.
        """
        rows = []
        for _, row in df.iterrows():
            rows.append({col: self._normalize_value(val) for col, val in row.items()})
        return rows

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
        Writes a snapshot DataFrame to BigQuery via streaming inserts.

        Args:
            df            : validated snapshot DataFrame (from main.py)
            max_retries   : number of attempts on transient API errors
            retry_delay_s : base delay between retries (doubles each attempt)

        Returns:
            Number of rows successfully written.

        Raises:
            BigQueryWriteError if all retries fail or a non-retryable error occurs.
        """
        if df.empty:
            logger.warning("write_snapshot called with empty DataFrame — nothing to write.")
            return 0

        rows = self._dataframe_to_rows(df)
        logger.info(
            "Preparing BigQuery write",
            extra={"table": self.table_ref, "row_count": len(rows)},
        )

        last_error = None
        for attempt in range(1, max_retries + 1):
            try:
                errors = self._client.insert_rows_json(
                    table=self.table_ref,
                    json_rows=rows,
                    # skip_invalid_rows=False: we want to know about every error.
                    # The caller (main.py) has already validated rows via Pydantic,
                    # so errors here indicate a schema drift or BQ-side issue.
                    skip_invalid_rows=False,
                    # ignore_unknown_values=False: fail loudly on unexpected columns.
                    ignore_unknown_values=False,
                )

                if not errors:
                    # Empty error list = complete success
                    logger.info(
                        "BigQuery write successful",
                        extra={
                            "table":        self.table_ref,
                            "rows_written": len(rows),
                            "attempt":      attempt,
                        },
                    )
                    return len(rows)

                # Partial failure — some rows rejected by BQ
                # Log each rejected row with its error details
                for err in errors:
                    logger.error(
                        "BigQuery row insert error",
                        extra={
                            "row_index":    err.get("index"),
                            "bq_errors":    err.get("errors"),
                            "table":        self.table_ref,
                        },
                    )
                # Partial failures are non-retryable (schema issues, not transient)
                raise BigQueryWriteError(
                    f"{len(errors)} rows rejected by BigQuery. "
                    "Check logs for bq_errors details."
                )

            except BigQueryWriteError:
                raise  # don't retry schema/validation errors

            except (GoogleAPICallError, RetryError) as exc:
                last_error = exc
                if attempt < max_retries:
                    delay = retry_delay_s * (2 ** (attempt - 1))  # exponential backoff
                    logger.warning(
                        "BigQuery API error, will retry",
                        extra={
                            "attempt":      attempt,
                            "max_retries":  max_retries,
                            "retry_in_s":   delay,
                            "error":        str(exc),
                        },
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "BigQuery write failed after all retries",
                        extra={"attempts": attempt, "error": str(exc)},
                    )

        raise BigQueryWriteError(
            f"BigQuery write failed after {max_retries} attempts. "
            f"Last error: {last_error}"
        )
