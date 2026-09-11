# =============================================================================
# variables.tf — Root module inputs
#
# All variables have defaults suitable for a solo dev / free-tier deployment.
# Override them in terraform.tfvars (git-ignored) or via environment:
#   export TF_VAR_project_id="my-gcp-project"
# =============================================================================

# ── GCP Identity ──────────────────────────────────────────────────────────────

variable "project_id" {
  description = "GCP project ID. Find it in the GCP Console → Project selector."
  type        = string
  # No default — must be set explicitly. Prevents accidental deployment
  # to the wrong project.
}

variable "project_short" {
  description = <<-EOT
    Short alphanumeric slug used as a prefix in resource names.
    Keep it under 10 chars to avoid BQ dataset/bucket name length limits.
    Example: "ecobici"
  EOT
  type    = string
  default = "ecobici"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,9}$", var.project_short))
    error_message = "project_short must be 2–10 lowercase alphanumeric chars or hyphens, starting with a letter."
  }
}

variable "region" {
  description = <<-EOT
    GCP region for all regional resources (GCS, Cloud Functions, Scheduler).
    BigQuery datasets use this as the location.
    us-central1 is chosen because it has the most Always Free products.
  EOT
  type    = string
  default = "us-central1"
}

variable "environment" {
  description = "Deployment environment tag. Controls resource naming and labels."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

# ── BigQuery ───────────────────────────────────────────────────────────────────

variable "bq_data_retention_days" {
  description = <<-EOT
    Default table expiration in days for the raw snapshots table.
    Set to 0 to disable automatic expiry (keep data forever).
    For free-tier: keep this at 90 to stay well within the 10 GB free quota.
    At ~500 stations × 21 columns × 5-min polls = ~5 MB/day → 90 days ≈ 450 MB.
  EOT
  type    = number
  default = 90

  validation {
    condition     = var.bq_data_retention_days >= 0
    error_message = "bq_data_retention_days must be 0 (no expiry) or a positive integer."
  }
}

# ── Cloud Scheduler ───────────────────────────────────────────────────────────

variable "ingestion_schedule_cron" {
  description = <<-EOT
    Cron expression for the ingestion job.
    Default: every 5 minutes, aligned with the GBFS feed TTL.
    Note: GCP Scheduler uses unix-cron format (supports */5 syntax).
    Free tier: 3 jobs/month. We're using 1.
  EOT
  type    = string
  default = "*/5 * * * *"
}

variable "function_dry_run" {
  description = <<-EOT
    When true, the function fetches and validates data but skips the BigQuery write.
    Useful for testing the deployed function without touching the warehouse.
    Set to false in production.
  EOT
  type    = bool
  default = false
}

variable "function_log_level" {
  description = "Python logging level for the Cloud Run Function."
  type        = string
  default     = "INFO"
}
  description = <<-EOT
    HTTP trigger URL for the Cloud Run Function.
    Set to a placeholder now; replaced with the real URL in Phase 3 after
    `terraform apply` of the functions module produces the output.
    Using a placeholder avoids a chicken-and-egg dependency between
    the scheduler module and the function module across phases.
  EOT
  type    = string
  default = "https://placeholder.run.app/ingest"
}
