# =============================================================================
# modules/cloud_scheduler/main.tf
#
# Provisions the Cloud Scheduler job that triggers the ingestion function
# every 5 minutes via an authenticated HTTP POST.
#
# Architecture note:
#   Scheduler → (OIDC token) → Cloud Run Function HTTP trigger
#
# The OIDC token is automatically attached by GCP to the HTTP request.
# The function verifies it, rejecting unauthenticated calls.
# This means our ingestion endpoint is NOT publicly callable — only the
# Scheduler (running as our pipeline SA) can invoke it.
#
# Free tier: 3 Scheduler jobs/month. We use 1. Budget: unlimited.
# =============================================================================

# Cloud Scheduler requires App Engine to exist in the project.
# This resource is a no-op if App Engine is already initialized.
resource "google_app_engine_application" "app" {
  project     = var.project_id
  location_id = var.region

  # App Engine location IDs differ from standard region names.
  # us-central1 → us-central (no trailing digit)
  # If your region differs, check: gcloud app regions list
}

resource "google_cloud_scheduler_job" "ingestion" {
  project  = var.project_id
  region   = var.region
  name     = "${var.name_prefix}-ingest-job"
  schedule = var.schedule_cron

  # GBFS TTL is 10 seconds; we poll every 5 minutes.
  # Timezone here is informational only — the cron expression is always UTC.
  # Using CDMX local time so the schedule is readable in operations dashboards.
  time_zone = "America/Mexico_City"

  description = "Triggers the ECOBICI GBFS ingestion function every 5 minutes."

  http_target {
    uri         = var.function_url
    http_method = "POST"

    # Payload tells the function which feed to ingest.
    # In Phase 3 we'll expand this to support selective station groups.
    body = base64encode(jsonencode({
      source  = "cloud_scheduler"
      feed    = "station_status"
      version = "1.0"
    }))

    headers = {
      "Content-Type" = "application/json"
    }

    # OIDC authentication — Scheduler attaches a signed token to every request.
    # The Cloud Run function validates this token automatically when
    # `--no-allow-unauthenticated` is set (which we enforce in Phase 3).
    oidc_token {
      service_account_email = var.pipeline_sa_email
      # audience must match the function URL exactly (no trailing slash)
      audience = var.function_url
    }
  }

  retry_config {
    retry_count          = 3
    min_backoff_duration = "5s"
    max_backoff_duration = "60s"
    max_doublings        = 2
  }

  depends_on = [google_app_engine_application.app]
}
