# =============================================================================
# modules/gcs/main.tf
#
# Provisions a GCS bucket used as the staging area for:
#   1. Cloud Function source ZIPs (Phase 3)
#   2. Optional: raw JSON snapshots for backup / replay
#
# Free tier: 5 GB storage, 5,000 Class A ops (writes), 50,000 Class B ops (reads)
# At our scale (one ZIP upload per deploy, ~1 KB/payload) we will never come
# close to these limits.
#
# Security decisions:
#   - uniform_bucket_level_access = true  → disables legacy per-object ACLs
#   - public_access_prevention = "enforced" → blocks any accidental public exposure
#   - versioning enabled → allows rollback if a bad function ZIP is deployed
# =============================================================================

resource "google_storage_bucket" "staging" {
  project  = var.project_id
  name     = "${var.name_prefix}-staging-${var.project_id}"
  location = var.region

  # Uniform access: all permissions via IAM, not legacy object ACLs
  uniform_bucket_level_access = true

  # Hard block — this pipeline should never be publicly readable
  public_access_prevention = "enforced"

  # Versioning: lets us roll back to a previous function ZIP if a deploy breaks
  versioning {
    enabled = true
  }

  # Lifecycle: delete non-current (old) function ZIPs after 30 days.
  # Keeps the bucket tidy without manual cleanup.
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 3          # keep last 3 versions of any object
      with_state         = "ARCHIVED" # only applies to non-current versions
    }
  }

  # Delete the bucket even if it contains objects (safe for dev environments).
  # Set to false in prod to prevent accidental data loss.
  force_destroy = var.environment == "dev" ? true : false

  labels = var.common_labels
}

# ── Folder structure (empty placeholder objects) ──────────────────────────────
# GCS has no real "folders", but creating zero-byte prefix objects makes the
# bucket structure visible in the GCP Console — good for discoverability.

resource "google_storage_bucket_object" "functions_prefix" {
  name    = "functions/.keep"
  bucket  = google_storage_bucket.staging.name
  content = "# Cloud Function source ZIPs are uploaded here during Phase 3 deployment."
}

resource "google_storage_bucket_object" "raw_prefix" {
  name    = "raw/.keep"
  bucket  = google_storage_bucket.staging.name
  content = "# Optional: raw JSON backups from the ingestion function."
}
