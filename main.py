"""
main.py — Cloud Run Function entrypoint
----------------------------------------
HTTP trigger consumed by Cloud Scheduler every 5 minutes.

Request contract (set in Terraform Scheduler module):
  Method  : POST
  Auth    : OIDC bearer token (validated automatically by Cloud Run)
  Body    : {"source": "cloud_scheduler", "feed": "station_status", "version": "1.0"}

Response contract:
  200 : ingestion succeeded — body contains execution stats JSON
  204 : health check (GET /) — empty body
  400 : malformed request body
  500 : ingestion failed — body contains error detail
  503 : upstream GBFS API unreachable — Scheduler will retry

Architecture notes:
  - Module-level globals (_info_cache, _bq_writer) survive across WARM
    Cloud Run invocations, eliminating the redundant station_information
    HTTP call and BQ client initialization on every poll.
  - Cold starts re-initialize everything from scratch.
  - DRY_RUN=true skips the BigQuery write entirely — used for local testing
    and CI pipelines that can't authenticate against a real GCP project.
"""

import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import flask
import functions_framework

from bq_writer import BigQueryWriter, BigQueryWriteError
from ingestion.gbfs_client import get_enriched_snapshot
from ingestion.schema import StationSnapshot

# =============================================================================
# Structured logging — outputs JSON to stdout, parsed by Cloud Logging
# =============================================================================

class _CloudRunJsonFormatter(logging.Formatter):
    """
    Formats log records as single-line JSON objects.
    Cloud Logging automatically indexes these fields:
      severity  → log level dropdown in the Console
      message   → primary searchable text
      timestamp → used for time-ordering
    All extra kwargs passed to logger.info(..., extra={...}) are included
    as top-level JSON fields, making logs queryable in Log Explorer.
    """

    SEVERITY_MAP = {
        "DEBUG":    "DEBUG",
        "INFO":     "INFO",
        "WARNING":  "WARNING",
        "ERROR":    "ERROR",
        "CRITICAL": "CRITICAL",
    }

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            "severity":  self.SEVERITY_MAP.get(record.levelname, "DEFAULT"),
            "message":   record.getMessage(),
            "logger":    record.name,
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
        }
        # Merge any extra fields (e.g. station_count, rows_written)
        for key, val in record.__dict__.items():
            if key not in (
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
            ):
                entry[key] = val

        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(entry, default=str)


def _configure_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Remove any default handlers before adding ours
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_CloudRunJsonFormatter())
    root.addHandler(handler)


_configure_logging()
logger = logging.getLogger("ecobici.function")


# =============================================================================
# Configuration — read once at module level (survives warm invocations)
# =============================================================================

class _Config:
    """Reads required environment variables at import time. Fails fast on startup
    if anything is missing, rather than failing silently at invocation time."""

    def __init__(self) -> None:
        self.project_id  = os.environ.get("GCP_PROJECT_ID", "")
        self.dataset_id  = os.environ.get("BQ_DATASET_ID",  "")
        self.table_id    = os.environ.get("BQ_TABLE_ID",    "")
        self.dry_run     = os.environ.get("DRY_RUN", "false").lower() == "true"

        if not self.dry_run:
            missing = [
                k for k, v in {
                    "GCP_PROJECT_ID": self.project_id,
                    "BQ_DATASET_ID":  self.dataset_id,
                    "BQ_TABLE_ID":    self.table_id,
                }.items() if not v
            ]
            if missing:
                raise RuntimeError(
                    f"Missing required environment variables: {missing}. "
                    "Set DRY_RUN=true to run without BigQuery."
                )


_config = _Config()


# =============================================================================
# Module-level warm-invocation cache
# =============================================================================

# station_information is nearly static — fetch once, reuse across invocations.
# On a cold start these are None and get populated on the first poll.
_station_info_cache = None    # pd.DataFrame | None
_bq_writer: BigQueryWriter | None = None


def _get_bq_writer() -> BigQueryWriter:
    """Lazy-initializes the BigQuery writer singleton."""
    global _bq_writer
    if _bq_writer is None:
        _bq_writer = BigQueryWriter(
            project_id=_config.project_id,
            dataset_id=_config.dataset_id,
            table_id=_config.table_id,
        )
    return _bq_writer


# =============================================================================
# Request parsing
# =============================================================================

def _parse_request(request: flask.Request) -> dict:
    """
    Parses and validates the incoming HTTP request body.
    Returns the parsed payload dict or raises ValueError with a clear message.
    """
    if not request.is_json:
        raise ValueError(
            f"Content-Type must be application/json, got: {request.content_type}"
        )
    try:
        payload = request.get_json(force=True, silent=False)
    except Exception as exc:
        raise ValueError(f"Failed to parse JSON body: {exc}") from exc

    if payload is None:
        raise ValueError("Request body is empty or not valid JSON.")

    return payload


# =============================================================================
# Pydantic row validation — same logic as local extract.py
# =============================================================================

def _validate_dataframe(df):
    """
    Validates every row. Returns (valid_df, error_count).
    We log errors but don't fail the invocation — partial writes are
    better than dropping the entire batch over one malformed station.
    """
    import pandas as pd
    valid_rows = []
    error_count = 0

    for _, row in df.iterrows():
        try:
            StationSnapshot(**row.to_dict())
            valid_rows.append(row)
        except Exception as exc:
            error_count += 1
            logger.warning(
                "Row validation failed",
                extra={
                    "station_id": row.get("station_id"),
                    "validation_error": str(exc),
                },
            )

    valid_df = pd.DataFrame(valid_rows) if valid_rows else df.__class__()
    return valid_df, error_count


# =============================================================================
# Core ingestion handler
# =============================================================================

def _run_ingestion() -> dict:
    """
    Executes the full ingestion cycle:
      1. Fetch enriched snapshot from GBFS
      2. Validate rows
      3. Write to BigQuery (or log-only in DRY_RUN mode)

    Returns a stats dict that becomes the HTTP response body.
    Raises on unrecoverable errors so the caller can return the right HTTP code.
    """
    global _station_info_cache

    t_start = time.monotonic()

    # ── Step 1: Fetch ──────────────────────────────────────────────────────────
    logger.info("Fetching GBFS snapshot", extra={"cached_info": _station_info_cache is not None})

    try:
        snapshot_df, fresh_info_df = get_enriched_snapshot(
            cached_info_df=_station_info_cache
        )
        _station_info_cache = fresh_info_df  # update cache on every successful fetch
    except Exception as exc:
        # Upstream API failure — return 503 so Scheduler retries
        raise RuntimeError(f"GBFS fetch failed: {exc}") from exc

    t_fetch = time.monotonic() - t_start
    logger.info(
        "GBFS fetch complete",
        extra={"station_count": len(snapshot_df), "fetch_seconds": round(t_fetch, 2)},
    )

    # ── Step 2: Validate ───────────────────────────────────────────────────────
    valid_df, validation_errors = _validate_dataframe(snapshot_df)

    critical_bike_alerts = int(valid_df["is_critically_low_bikes"].sum())
    critical_dock_alerts = int(valid_df["is_critically_low_docks"].sum())

    if critical_bike_alerts or critical_dock_alerts:
        logger.warning(
            "Critical availability alerts detected",
            extra={
                "critical_bike_stations": critical_bike_alerts,
                "critical_dock_stations": critical_dock_alerts,
                # Emit station IDs so alerts are queryable in Log Explorer
                "bike_alert_stations": valid_df[valid_df["is_critically_low_bikes"]]["station_id"].tolist(),
                "dock_alert_stations": valid_df[valid_df["is_critically_low_docks"]]["station_id"].tolist(),
            },
        )

    # ── Step 3: Write ──────────────────────────────────────────────────────────
    rows_written = 0

    if _config.dry_run:
        logger.info(
            "DRY_RUN=true — skipping BigQuery write",
            extra={"would_write_rows": len(valid_df)},
        )
        rows_written = len(valid_df)  # pretend we wrote them
    else:
        writer = _get_bq_writer()
        try:
            rows_written = writer.write_snapshot(valid_df)
        except BigQueryWriteError as exc:
            # BQ write failure — return 500 so Scheduler retries
            raise RuntimeError(f"BigQuery write failed: {exc}") from exc

    t_total = time.monotonic() - t_start

    stats = {
        "status":                "ok",
        "ingested_at_utc":       datetime.now(timezone.utc).isoformat(),
        "station_count":         len(snapshot_df),
        "valid_rows":            len(valid_df),
        "validation_errors":     validation_errors,
        "rows_written_to_bq":    rows_written,
        "critical_bike_alerts":  critical_bike_alerts,
        "critical_dock_alerts":  critical_dock_alerts,
        "dry_run":               _config.dry_run,
        "duration_seconds":      round(t_total, 3),
    }

    logger.info("Ingestion cycle complete", extra=stats)
    return stats


# =============================================================================
# HTTP entrypoint — decorated by functions-framework
# =============================================================================

@functions_framework.http
def ingest(request: flask.Request) -> flask.Response:
    """
    Cloud Run Function HTTP trigger.
    Registered as the entrypoint in Terraform: entry_point = "ingest"
    """

    # ── Health check (Cloud Run probes / with GET) ─────────────────────────
    if request.method == "GET" and request.path in ("/", "/health"):
        return flask.Response(
            json.dumps({"status": "healthy", "service": "ecobici-ingestion"}),
            status=200,
            mimetype="application/json",
        )

    # ── Reject non-POST ────────────────────────────────────────────────────
    if request.method != "POST":
        return flask.Response(
            json.dumps({"error": f"Method {request.method} not allowed"}),
            status=405,
            mimetype="application/json",
        )

    # ── Parse request ──────────────────────────────────────────────────────
    try:
        payload = _parse_request(request)
    except ValueError as exc:
        logger.warning("Bad request", extra={"error": str(exc)})
        return flask.Response(
            json.dumps({"error": str(exc)}),
            status=400,
            mimetype="application/json",
        )

    logger.info(
        "Ingestion request received",
        extra={
            "source":  payload.get("source", "unknown"),
            "feed":    payload.get("feed",   "unknown"),
            "version": payload.get("version","unknown"),
        },
    )

    # ── Run ingestion ──────────────────────────────────────────────────────
    try:
        stats = _run_ingestion()
        return flask.Response(
            json.dumps(stats),
            status=200,
            mimetype="application/json",
        )
    except RuntimeError as exc:
        error_msg = str(exc)
        logger.error(
            "Ingestion failed",
            extra={"error": error_msg, "traceback": traceback.format_exc()},
        )
        # 503 for upstream failures → Scheduler retries
        # 500 for internal failures → Scheduler retries
        status_code = 503 if "GBFS fetch failed" in error_msg else 500
        return flask.Response(
            json.dumps({"status": "error", "error": error_msg}),
            status=status_code,
            mimetype="application/json",
        )
