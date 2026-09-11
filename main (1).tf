# =============================================================================
# modules/functions/main.tf
#
# Provisions the Cloud Run Function (Gen 2) that runs the ECOBICI ingestion.
#
# Gen 2 vs Gen 1:
#   Gen 2 is built on Cloud Run, giving us:
#   - Longer timeouts (up to 60 minutes vs 9 minutes for Gen 1)
#   - Better cold start performance with minimum instances
#   - Concurrency support (not relevant here — we use max_instance_count=1)
#   - Direct integration with Cloud Run IAM (no separate Functions IAM)
#
# Always Free tier budget:
#   Cloud Run: 2M requests/month, 360,000 GB-seconds, 180,000 vCPU-seconds
#   Our usage : 288 requests/day × 30 = 8,640/month  → well within 2M
#   Compute   : 288 × 10s × 0.5GB = ~432 GB-seconds/day × 30 = ~13K/month
#               → well within 360K/month free allowance
#
# Source ZIP contract:
#   The function source ZIP must be uploaded to GCS BEFORE running
#   `terraform apply` on this module. Use scripts/build_function.sh first.
#   Terraform references the GCS object by path; if the object doesn't
#   exist, the apply will fail with a 404 — the correct failure mode.
# =============================================================================

locals {
  # GCS path where build_function.sh uploads the ZIP.
  # This must match the path in scripts/build_function.sh.
  source_zip_path = "functions/function_source.zip"
}

# ── Source archive — reads the ZIP uploaded by build_function.sh ──────────────

data "google_storage_bucket_object" "function_source" {
  bucket = var.staging_bucket_name
  name   = local.source_zip_path
}

# ── Cloud Run Function (Gen 2) ────────────────────────────────────────────────

resource "google_cloudfunctions2_function" "ingestion" {
  project  = var.project_id
  location = var.region
  name     = "${var.name_prefix}-ingestion"

  description = "ECOBICI GBFS station availability ingestion — runs every 5 minutes."

  # ── Build configuration ─────────────────────────────────────────────────────
  build_config {
    runtime     = "python312"
    entry_point = "ingest"   # matches @functions_framework.http def ingest(...)

    source {
      storage_source {
        bucket = var.staging_bucket_name
        object = data.google_storage_bucket_object.function_source.name
        # generation pins to the exact ZIP version that was uploaded.
        # When you run build_function.sh + upload + terraform apply, this
        # updates to the new object generation, triggering a redeployment.
        generation = data.google_storage_bucket_object.function_source.generation
      }
    }
  }

  # ── Service (runtime) configuration ─────────────────────────────────────────
  service_config {
    # Memory: 512MB — minimum that runs pandas comfortably.
    # The ~500-station DataFrame is ~1MB in memory; the rest is Python overhead.
    # Increase to 1024MB if you observe OOM kills in Cloud Logging.
    available_memory = "512M"

    # CPU: 1 — default, sufficient for our serial workload.
    # pandas operations on 500 rows are CPU-trivial.
    available_cpu = "1"

    # Timeout: 60 seconds.
    # Typical successful invocation: ~3-5 seconds.
    # Gives ample headroom for GBFS retries (3 × 2s backoff) + BQ write.
    timeout_seconds = 60

    # Max instances: 1 — prevents concurrent runs.
    # If two Scheduler invocations overlap, the second waits.
    # At our 5-min cadence this should never happen, but it's defensive.
    max_instance_count = 1

    # Min instances: 0 — allows scale-to-zero (free tier).
    # Cold starts add ~2-3s overhead; acceptable for a 5-min poll cycle.
    # Set to 1 if you want zero cold starts (incurs Always-On billing).
    min_instance_count = 0

    # Service account — the identity this function runs as.
    # Inherits the BigQuery, GCS, and Logging IAM roles from Phase 2.
    service_account_email = var.pipeline_sa_email

    # Ingress: internal-only means the function only accepts requests from
    # within the same GCP project (Cloud Scheduler lives here).
    # Blocks all public internet traffic without needing a VPC.
    ingress_settings = "ALLOW_INTERNAL_ONLY"

    # Environment variables injected at runtime.
    # These tell main.py and bq_writer.py which BQ table to write to.
    # Sensitive values (if any) should use secret_environment_variables
    # pointing to Secret Manager — not plain environment_variables.
    environment_variables = {
      GCP_PROJECT_ID  = var.project_id
      BQ_DATASET_ID   = var.bq_dataset_id
      BQ_TABLE_ID     = var.bq_table_id
      DRY_RUN         = tostring(var.dry_run)
      LOG_LEVEL       = var.log_level
    }
  }

  labels = var.common_labels
}

# ── IAM: only the pipeline SA can invoke the function ─────────────────────────
# Without this, Cloud Scheduler's OIDC tokens would be rejected with 403.
# The binding says: "our pipeline SA is allowed to invoke this Cloud Run service."

resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloudfunctions2_function.ingestion.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${var.pipeline_sa_email}"
}
