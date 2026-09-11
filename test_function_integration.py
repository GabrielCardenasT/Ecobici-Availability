"""
test_function_integration.py
------------------------------
Validates the Cloud Run Function's HTTP handling, logging, and orchestration
logic without a real GCP project or live GBFS connection.

Tests cover:
  1. Health check GET / returns 200
  2. Missing Content-Type returns 400
  3. Empty body returns 400
  4. Valid POST triggers ingestion cycle (dry run)
  5. Structured log output is valid JSON
  6. Type normalization produces BQ-safe values
  7. bq_writer handles empty DataFrames gracefully
"""

import sys, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "functions"))
sys.path.insert(0, os.path.dirname(__file__))

# ── Patch environment before importing main ────────────────────────────────────
os.environ["DRY_RUN"]        = "true"
os.environ["GCP_PROJECT_ID"] = "test-project"
os.environ["BQ_DATASET_ID"]  = "ecobici_raw"
os.environ["BQ_TABLE_ID"]    = "station_snapshots"

import io
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

# ── Import function modules ────────────────────────────────────────────────────
import importlib

# Import bq_writer directly (no GCP call at import time)
import functions.bq_writer as bq_writer_module
from functions.bq_writer import BigQueryWriter


# =============================================================================
# Test helpers
# =============================================================================

def make_flask_request(
    method="POST",
    path="/",
    json_body=None,
    content_type="application/json",
):
    """Creates a minimal Flask request mock."""
    req = MagicMock()
    req.method  = method
    req.path    = path
    req.content_type = content_type
    req.is_json = content_type == "application/json"
    if json_body is not None:
        req.get_json = MagicMock(return_value=json_body)
    else:
        req.get_json = MagicMock(return_value=None)
    return req


VALID_SCHEDULER_BODY = {
    "source": "cloud_scheduler",
    "feed":   "station_status",
    "version": "1.0",
}

# Real snapshot data from Phase 1 test
MOCK_SNAPSHOT_DATA = {
    "station_id": ["15", "16", "19"],
    "short_name": ["094", "405", "337"],
    "name": [
        "CE-094 Lic. Verdad-Moneda",
        "CE-405 División Del Norte",
        "CE-337 San Borja",
    ],
    "lat":  [19.433766, 19.370566, 19.384849],
    "lon":  [-99.130918, -99.156622, -99.167040],
    "capacity":                    [23, 27, 19],
    "num_bikes_available":         [20,  0, 12],
    "num_bikes_disabled":          [ 1,  1,  6],
    "num_docks_available":         [ 2, 26,  1],
    "num_docks_disabled":          [ 0,  0,  0],
    "total_operational_capacity":  [22, 26, 13],
    "availability_pct":            [0.9091, 0.0, 0.9231],
    "dock_pct":                    [0.0909, 1.0, 0.0769],
    "is_critically_low_bikes":     [False, True, False],
    "is_critically_low_docks":     [True, False, True],
    "is_installed":                [True, True, True],
    "is_renting":                  [True, True, True],
    "is_returning":                [True, True, True],
    "station_last_reported_utc":   [
        pd.Timestamp("2026-05-18 22:46:46+00:00"),
        pd.Timestamp("2026-05-18 22:44:47+00:00"),
        pd.Timestamp("2026-05-18 22:45:15+00:00"),
    ],
    "feed_last_updated_utc":       [
        pd.Timestamp("2026-05-18 22:46:19+00:00"),
    ] * 3,
    "ingested_at_utc": [datetime.now(timezone.utc)] * 3,
}


# =============================================================================
# Tests
# =============================================================================

def test_health_check():
    """GET / should return 200 healthy without triggering ingestion."""
    from functions.main import ingest
    req = make_flask_request(method="GET", path="/health")
    resp = ingest(req)
    assert resp.status_code == 200
    body = json.loads(resp.get_data())
    assert body["status"] == "healthy"
    print("  ✓ Health check GET /health → 200")


def test_non_json_content_type_returns_400():
    """Requests without application/json should be rejected cleanly."""
    from functions.main import ingest
    req = make_flask_request(method="POST", content_type="text/plain")
    req.is_json = False
    resp = ingest(req)
    assert resp.status_code == 400
    body = json.loads(resp.get_data())
    assert "error" in body
    print("  ✓ Non-JSON Content-Type → 400")


def test_empty_body_returns_400():
    """Empty POST body should return 400."""
    from functions.main import ingest
    req = make_flask_request(method="POST", json_body=None)
    resp = ingest(req)
    assert resp.status_code == 400
    print("  ✓ Empty body → 400")


def test_valid_scheduler_request_dry_run():
    """Valid POST in DRY_RUN mode should return 200 with ingestion stats."""
    from functions.main import ingest
    df = pd.DataFrame(MOCK_SNAPSHOT_DATA)

    with patch("functions.main.get_enriched_snapshot", return_value=(df, df)):
        req = make_flask_request(method="POST", json_body=VALID_SCHEDULER_BODY)
        resp = ingest(req)

    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.get_data()}"
    stats = json.loads(resp.get_data())

    assert stats["status"]      == "ok"
    assert stats["dry_run"]     == True
    assert stats["station_count"] == 3
    assert stats["valid_rows"]    == 3
    assert stats["rows_written_to_bq"] == 3
    assert stats["critical_bike_alerts"] == 1   # station 16
    assert stats["critical_dock_alerts"] == 2   # stations 15 and 19
    print(f"  ✓ Valid DRY_RUN POST → 200, stats: {json.dumps({k: v for k, v in stats.items() if k != 'ingested_at_utc'})}")


def test_gbfs_upstream_failure_returns_503():
    """GBFS API failure should return 503 so Scheduler retries."""
    from functions.main import ingest
    with patch("functions.main.get_enriched_snapshot", side_effect=ConnectionError("timeout")):
        req = make_flask_request(method="POST", json_body=VALID_SCHEDULER_BODY)
        resp = ingest(req)
    assert resp.status_code == 503
    print("  ✓ GBFS upstream failure → 503 (Scheduler will retry)")


def test_bq_writer_type_normalization():
    """Every pandas/numpy type should normalize to a JSON-safe Python native."""
    writer = BigQueryWriter.__new__(BigQueryWriter)  # skip __init__ (no BQ client)

    test_cases = [
        (pd.Timestamp("2026-05-18 22:46:19+00:00"), str),   # → ISO string
        (datetime(2026, 5, 18, 22, 0, tzinfo=timezone.utc), str),
        (np.int64(42),    int),
        (np.float64(3.14), float),
        (np.bool_(True),   bool),
        (np.bool_(False),  bool),
        (float("nan"),     type(None)),  # NaN → None
        (pd.NA,            type(None)),  # pandas NA → None
        (None,             type(None)),
        ("plain string",   str),
        (True,             bool),
        (42,               int),
    ]

    for val, expected_type in test_cases:
        result = writer._normalize_value(val)
        assert isinstance(result, expected_type) or result is None, (
            f"normalize({val!r}) → {result!r} (type {type(result).__name__}), "
            f"expected {expected_type.__name__}"
        )

    print(f"  ✓ Type normalization: {len(test_cases)} cases all correct")


def test_bq_writer_empty_dataframe():
    """write_snapshot on empty DataFrame should return 0 without calling BQ."""
    writer = BigQueryWriter.__new__(BigQueryWriter)
    writer._client = MagicMock()
    result = writer.write_snapshot(pd.DataFrame())
    writer._client.insert_rows_json.assert_not_called()
    assert result == 0
    print("  ✓ Empty DataFrame write → 0 rows, no BQ API call")


def test_structured_log_output_is_valid_json(capsys=None):
    """Log entries must be parseable JSON (Cloud Logging requirement)."""
    import logging
    from functions.main import _CloudRunJsonFormatter

    formatter = _CloudRunJsonFormatter()
    record = logging.LogRecord(
        name="ecobici.test", level=logging.INFO,
        pathname="", lineno=0, msg="test message",
        args=(), exc_info=None,
    )
    output = formatter.format(record)
    parsed = json.loads(output)  # raises if not valid JSON
    assert parsed["severity"] == "INFO"
    assert parsed["message"]  == "test message"
    assert "timestamp" in parsed
    print("  ✓ Structured log formatter outputs valid JSON with severity + timestamp")


# =============================================================================
# Runner
# =============================================================================

if __name__ == "__main__":
    print("\nRunning Phase 3 function integration tests...\n")

    test_health_check()
    test_non_json_content_type_returns_400()
    test_empty_body_returns_400()
    test_valid_scheduler_request_dry_run()
    test_gbfs_upstream_failure_returns_503()
    test_bq_writer_type_normalization()
    test_bq_writer_empty_dataframe()
    test_structured_log_output_is_valid_json()

    print("\nAll Phase 3 tests passed ✓\n")
