"""
gbfs_client.py
--------------
Responsible for fetching and merging the two ECOBICI GBFS feeds:
  - station_status   : live availability (refreshes ~every 5 min)
  - station_information : static metadata (name, coords, capacity)

Design principle: each function has a single responsibility.
The merge happens in get_enriched_snapshot(), which is the only
function that other modules should call.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GBFS_ROOT_URL = "https://gbfs.mex.lyftbikes.com/gbfs/gbfs.json"
LANGUAGE = "en"

# Columns we keep from each feed — explicit allowlist, not "everything"
STATUS_COLUMNS = [
    "station_id",
    "num_bikes_available",
    "num_bikes_disabled",
    "num_docks_available",
    "num_docks_disabled",
    "is_installed",
    "is_renting",
    "is_returning",
    "last_reported",
]

INFO_COLUMNS = [
    "station_id",
    "name",
    "short_name",
    "lat",
    "lon",
    "capacity",
]

# ---------------------------------------------------------------------------
# HTTP session with retry logic
# ---------------------------------------------------------------------------

def _build_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    timeout: int = 10,
) -> requests.Session:
    """
    Returns a requests.Session with automatic retries on transient failures.
    The GBFS feed is public but occasionally returns 5xx under load.
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,               # waits: 0s, 0.5s, 1s
        status_forcelist=[429, 500, 502, 503, 504],  # retry on these HTTP codes
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.request_timeout = timeout  # stored for use in .get() calls
    return session


# ---------------------------------------------------------------------------
# Feed discovery — parses gbfs.json to get canonical feed URLs
# ---------------------------------------------------------------------------

def _discover_feed_urls(session: requests.Session) -> dict[str, str]:
    """
    Fetches gbfs.json (the root manifest) and returns a dict mapping
    feed name → URL for the configured language.

    Example return value:
        {
          "station_status":      "https://gbfs.mex.lyftbikes.com/gbfs/en/station_status.json",
          "station_information": "https://gbfs.mex.lyftbikes.com/gbfs/en/station_information.json",
          ...
        }
    """
    response = session.get(GBFS_ROOT_URL, timeout=10)
    response.raise_for_status()

    root = response.json()
    feeds = root["data"][LANGUAGE]["feeds"]
    return {feed["name"]: feed["url"] for feed in feeds}


# ---------------------------------------------------------------------------
# Individual feed fetchers
# ---------------------------------------------------------------------------

def _fetch_station_status(session: requests.Session, url: str) -> tuple[pd.DataFrame, int]:
    """
    Fetches station_status feed and returns:
      - A DataFrame with columns defined in STATUS_COLUMNS
      - The feed-level last_updated Unix timestamp (for freshness checks)
    """
    response = session.get(url, timeout=10)
    response.raise_for_status()
    payload = response.json()

    df = pd.DataFrame(payload["data"]["stations"])

    # Validate all expected columns are present
    missing = set(STATUS_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"station_status feed missing expected columns: {missing}")

    return df[STATUS_COLUMNS], payload["last_updated"]


def _fetch_station_information(session: requests.Session, url: str) -> pd.DataFrame:
    """
    Fetches station_information feed. This endpoint is mostly static
    (stations rarely open/close), so we cache it in the caller to avoid
    a redundant HTTP round-trip on every 5-min poll.
    """
    response = session.get(url, timeout=10)
    response.raise_for_status()
    payload = response.json()

    df = pd.DataFrame(payload["data"]["stations"])

    missing = set(INFO_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"station_information feed missing expected columns: {missing}")

    return df[INFO_COLUMNS]


# ---------------------------------------------------------------------------
# Enrichment & derived metrics
# ---------------------------------------------------------------------------

def _enrich_snapshot(
    status_df: pd.DataFrame,
    info_df: pd.DataFrame,
    feed_timestamp: int,
) -> pd.DataFrame:
    """
    Merges status + info, computes derived columns, and adds pipeline metadata.

    Derived columns added here:
      - total_operational_capacity : bikes_available + docks_available
            (excludes disabled slots; this is what users actually see)
      - availability_pct           : how full the bike supply is (0.0 → 1.0)
      - dock_pct                   : how full the dock supply is (0.0 → 1.0)
      - is_critically_low_bikes    : True when availability_pct < 0.10
      - is_critically_low_docks    : True when dock_pct < 0.10
      - feed_last_updated_utc      : UTC datetime from the feed's last_updated
      - ingested_at_utc            : Wall-clock time our pipeline ran
    """
    df = pd.merge(status_df, info_df, on="station_id", how="left")

    # --- Operational capacity (excludes disabled/broken slots) ---
    df["total_operational_capacity"] = (
        df["num_bikes_available"] + df["num_docks_available"]
    )

    # Guard against stations with 0 operational slots (maintenance mode)
    safe_capacity = df["total_operational_capacity"].replace(0, pd.NA)

    # --- Availability ratios ---
    df["availability_pct"] = (df["num_bikes_available"] / safe_capacity).round(4)
    df["dock_pct"] = (df["num_docks_available"] / safe_capacity).round(4)

    # --- Alert flags (the core of our rebalancing logic) ---
    df["is_critically_low_bikes"] = df["availability_pct"] < 0.10
    df["is_critically_low_docks"] = df["dock_pct"] < 0.10

    # --- Boolean normalization (GBFS sends 0/1 integers) ---
    for col in ["is_installed", "is_renting", "is_returning"]:
        df[col] = df[col].astype(bool)

    # --- Timestamp enrichment ---
    df["feed_last_updated_utc"] = datetime.fromtimestamp(
        feed_timestamp, tz=timezone.utc
    )
    df["station_last_reported_utc"] = pd.to_datetime(
        df["last_reported"], unit="s", utc=True
    )
    df["ingested_at_utc"] = datetime.now(tz=timezone.utc)

    # Drop the raw Unix timestamp — we have the clean UTC version now
    df.drop(columns=["last_reported"], inplace=True)

    # --- Enforce column order for BigQuery schema stability ---
    ordered_columns = [
        # Identity
        "station_id", "short_name", "name",
        # Geography
        "lat", "lon",
        # Capacity (raw)
        "capacity", "num_bikes_available", "num_bikes_disabled",
        "num_docks_available", "num_docks_disabled",
        # Derived metrics
        "total_operational_capacity", "availability_pct", "dock_pct",
        # Alert flags
        "is_critically_low_bikes", "is_critically_low_docks",
        # Operational status
        "is_installed", "is_renting", "is_returning",
        # Timestamps
        "station_last_reported_utc", "feed_last_updated_utc", "ingested_at_utc",
    ]
    return df[ordered_columns]


# ---------------------------------------------------------------------------
# Public interface — this is what extract.py calls
# ---------------------------------------------------------------------------

def get_enriched_snapshot(
    cached_info_df: Optional[pd.DataFrame] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Main entry point for the ingestion layer.

    Args:
        cached_info_df: Pass a previously fetched station_information DataFrame
                        to skip the redundant HTTP call. Pass None (default)
                        on the first run or when you want a fresh fetch.

    Returns:
        (snapshot_df, info_df)
          - snapshot_df : fully enriched DataFrame, one row per station
          - info_df     : station_information DataFrame (for caching by caller)

    Usage:
        # First call (cold start)
        snapshot, info_cache = get_enriched_snapshot()

        # Subsequent calls (reuse cached station info)
        snapshot, _ = get_enriched_snapshot(cached_info_df=info_cache)
    """
    session = _build_session()

    feed_urls = _discover_feed_urls(session)
    logging.info("Feed URLs discovered: %s", list(feed_urls.keys()))

    status_df, feed_timestamp = _fetch_station_status(
        session, feed_urls["station_status"]
    )
    logging.info("Fetched status for %d stations.", len(status_df))

    if cached_info_df is None:
        info_df = _fetch_station_information(session, feed_urls["station_information"])
        logging.info("Fetched station info for %d stations.", len(info_df))
    else:
        info_df = cached_info_df
        logging.info("Using cached station information (%d stations).", len(info_df))

    snapshot_df = _enrich_snapshot(status_df, info_df, feed_timestamp)

    return snapshot_df, info_df
