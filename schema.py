"""
schema.py
---------
Single source of truth for the BigQuery landing table schema.

Using Pydantic here serves two purposes:
  1. Validates each row before it touches BigQuery (cheap, local check).
  2. Generates the BigQuery schema JSON automatically — no manual schema
     drift between Python types and BQ column types.

In Phase 3 (Cloud Functions), we'll import BIGQUERY_SCHEMA from here
to pass directly to the BigQuery client's load_table_from_dataframe().
"""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class StationSnapshot(BaseModel):
    """
    Represents a single station's state at a single point in time.
    One row in the BigQuery raw snapshot table.
    """

    # --- Identity ---
    station_id: str
    short_name: str
    name: str

    # --- Geography ---
    lat: float = Field(..., ge=-90.0, le=90.0)
    lon: float = Field(..., ge=-180.0, le=180.0)

    # --- Capacity (raw from feed) ---
    capacity: int = Field(..., ge=0)
    num_bikes_available: int = Field(..., ge=0)
    num_bikes_disabled: int = Field(..., ge=0)
    num_docks_available: int = Field(..., ge=0)
    num_docks_disabled: int = Field(..., ge=0)

    # --- Derived metrics ---
    total_operational_capacity: int = Field(..., ge=0)
    availability_pct: Optional[float] = Field(None, ge=0.0, le=1.0)
    dock_pct: Optional[float] = Field(None, ge=0.0, le=1.0)

    # --- Alert flags ---
    is_critically_low_bikes: bool
    is_critically_low_docks: bool

    # --- Operational status ---
    is_installed: bool
    is_renting: bool
    is_returning: bool

    # --- Timestamps ---
    station_last_reported_utc: datetime
    feed_last_updated_utc: datetime
    ingested_at_utc: datetime

    @field_validator("availability_pct", "dock_pct", mode="before")
    @classmethod
    def allow_none_for_zero_capacity(cls, v):
        """
        Stations under full maintenance have 0 operational capacity.
        We store NULL rather than a division-by-zero artifact.
        """
        if v is None or (isinstance(v, float) and v != v):  # NaN check
            return None
        return v


# ---------------------------------------------------------------------------
# BigQuery schema — maps Python/Pydantic types to BQ column definitions
# Imported by the Cloud Function in Phase 3.
# ---------------------------------------------------------------------------

BIGQUERY_SCHEMA = [
    {"name": "station_id",                  "type": "STRING",    "mode": "REQUIRED"},
    {"name": "short_name",                  "type": "STRING",    "mode": "REQUIRED"},
    {"name": "name",                        "type": "STRING",    "mode": "REQUIRED"},
    {"name": "lat",                         "type": "FLOAT64",   "mode": "REQUIRED"},
    {"name": "lon",                         "type": "FLOAT64",   "mode": "REQUIRED"},
    {"name": "capacity",                    "type": "INT64",     "mode": "REQUIRED"},
    {"name": "num_bikes_available",         "type": "INT64",     "mode": "REQUIRED"},
    {"name": "num_bikes_disabled",          "type": "INT64",     "mode": "REQUIRED"},
    {"name": "num_docks_available",         "type": "INT64",     "mode": "REQUIRED"},
    {"name": "num_docks_disabled",          "type": "INT64",     "mode": "REQUIRED"},
    {"name": "total_operational_capacity",  "type": "INT64",     "mode": "REQUIRED"},
    {"name": "availability_pct",            "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "dock_pct",                    "type": "FLOAT64",   "mode": "NULLABLE"},
    {"name": "is_critically_low_bikes",     "type": "BOOL",      "mode": "REQUIRED"},
    {"name": "is_critically_low_docks",     "type": "BOOL",      "mode": "REQUIRED"},
    {"name": "is_installed",                "type": "BOOL",      "mode": "REQUIRED"},
    {"name": "is_renting",                  "type": "BOOL",      "mode": "REQUIRED"},
    {"name": "is_returning",                "type": "BOOL",      "mode": "REQUIRED"},
    {"name": "station_last_reported_utc",   "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "feed_last_updated_utc",       "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "ingested_at_utc",             "type": "TIMESTAMP", "mode": "REQUIRED"},
]

# Partition column — BigQuery table will be partitioned on this field.
# Keeps query costs low when analysts filter by day/hour.
BQ_PARTITION_FIELD = "ingested_at_utc"

# Cluster columns — BQ will co-locate rows with the same station together.
# Drastically reduces bytes scanned for per-station time series queries.
BQ_CLUSTER_FIELDS = ["station_id"]
