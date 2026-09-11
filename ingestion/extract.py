"""
extract.py
----------
Local entrypoint for Phase 1 testing.
Run this script directly to:
  1. Fetch a live snapshot from the ECOBICI GBFS feed.
  2. Validate every row against the Pydantic schema.
  3. Print a summary report to stdout.
  4. Save the snapshot as Parquet (for incremental local testing).

In Phase 3, this logic migrates into functions/main.py (Cloud Run Function),
where BigQuery replaces the local Parquet write.

Usage:
    python -m ingestion.extract
    python -m ingestion.extract --output-dir data/processed --no-save
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.gbfs_client import get_enriched_snapshot
from ingestion.schema import StationSnapshot

# ---------------------------------------------------------------------------
# Logging setup — structured format for easy grep in Cloud Logs later
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_snapshot(df: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """
    Runs each row through StationSnapshot Pydantic model.

    Returns:
        - valid_df   : rows that passed validation
        - error_rows : list of dicts with row data + validation error message
    """
    valid_rows = []
    error_rows = []

    for _, row in df.iterrows():
        try:
            StationSnapshot(**row.to_dict())
            valid_rows.append(row)
        except Exception as e:
            error_rows.append({"station_id": row.get("station_id"), "error": str(e)})

    valid_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
    return valid_df, error_rows


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    """
    Prints a human-readable rebalancing intelligence report.
    This is the output you'll demo in interviews — shows business value
    from the very first script.
    """
    total = len(df)
    critical_bikes = df["is_critically_low_bikes"].sum()
    critical_docks = df["is_critically_low_docks"].sum()
    offline = (~df["is_installed"] | ~df["is_renting"]).sum()

    print("\n" + "="*60)
    print("  ECOBICI SNAPSHOT SUMMARY")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("="*60)
    print(f"  Total stations polled   : {total}")
    print(f"  Stations offline        : {offline}")
    print(f"  Critical bike shortage  : {critical_bikes}  (< 10% bikes left)")
    print(f"  Critical dock shortage  : {critical_docks}  (< 10% docks left)")
    print("-"*60)

    # --- Top 5 emptiest stations (residential drain candidates) ---
    emptiest = (
        df[df["is_installed"] & df["is_renting"]]
        .nsmallest(5, "availability_pct")[
            ["station_id", "short_name", "name", "num_bikes_available",
             "total_operational_capacity", "availability_pct"]
        ]
    )
    print("\n  EMPTIEST STATIONS (rebalancing priority — need BIKES):")
    print(emptiest.to_string(index=False))

    # --- Top 5 fullest stations (corporate overflow candidates) ---
    fullest = (
        df[df["is_installed"] & df["is_renting"]]
        .nlargest(5, "availability_pct")[
            ["station_id", "short_name", "name", "num_bikes_available",
             "total_operational_capacity", "availability_pct"]
        ]
    )
    print("\n  FULLEST STATIONS (rebalancing priority — need DOCKS):")
    print(fullest.to_string(index=False))
    print("="*60 + "\n")


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def save_snapshot(df: pd.DataFrame, output_dir: Path) -> Path:
    """
    Saves snapshot as Parquet with a timestamp-partitioned filename.
    Parquet is the right format here:
      - Columnar: efficient for the time-series queries dbt will run
      - Schema-aware: preserves datetime types unlike CSV
      - Compressed by default
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = output_dir / f"snapshot_{ts}.parquet"
    df.to_parquet(filepath, index=False, engine="pyarrow")
    logger.info("Snapshot saved → %s (%d rows)", filepath, len(df))
    return filepath


def save_raw_json(raw_data: dict, output_dir: Path) -> None:
    """
    Saves the raw API response alongside the processed Parquet.
    Essential for debugging — if a schema change breaks the pipeline,
    you can inspect exactly what the API sent.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filepath = output_dir / f"raw_{ts}.json"
    with open(filepath, "w") as f:
        json.dump(raw_data, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def run(output_dir: Path = Path("data/processed"), save: bool = True) -> pd.DataFrame:
    """
    Orchestrates the full Phase 1 extraction.
    Returns the validated DataFrame (useful when called from other scripts).
    """
    logger.info("Starting ECOBICI extraction pipeline...")

    # 1. Fetch
    snapshot_df, _ = get_enriched_snapshot()
    logger.info("Raw snapshot: %d stations fetched.", len(snapshot_df))

    # 2. Validate
    valid_df, errors = validate_snapshot(snapshot_df)
    if errors:
        logger.warning("%d rows failed validation:", len(errors))
        for err in errors:
            logger.warning("  station_id=%s | %s", err["station_id"], err["error"])
    else:
        logger.info("All %d rows passed schema validation. ✓", len(valid_df))

    # 3. Report
    print_summary(valid_df)

    # 4. Persist
    if save and not valid_df.empty:
        save_snapshot(valid_df, output_dir)

    return valid_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ECOBICI GBFS extractor — Phase 1")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory to write Parquet snapshots (default: data/processed)",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Run extraction and print report without writing any files",
    )
    args = parser.parse_args()

    try:
        run(output_dir=args.output_dir, save=not args.no_save)
        sys.exit(0)
    except Exception as exc:
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)
