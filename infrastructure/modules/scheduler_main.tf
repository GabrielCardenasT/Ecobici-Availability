# =============================================================================
# ecobici-rebalancing-pipeline / infrastructure / main.tf
#
# Root module — composes all child modules into a deployable stack.
#
# Always Free tier constraints enforced throughout:
#   BigQuery  : 10 GB storage / month, 1 TB queries / month
#   GCS       : 5 GB storage, 5,000 Class A ops, 50,000 Class B ops
#   Cloud Run : 2M invocations / month, 400K GB-seconds compute
#   Scheduler : 3 jobs free / month
#
# Target environment: single GCP project, single region (us-central1).
# Multi-environment support added in Phase 3 via workspace or tfvars.
# =============================================================================

terraform {
  required_version = ">= 1.9.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # ── Remote state (uncomment after creating the GCS bucket manually once) ──
  # backend "gcs" {
  #   bucket = "ecobici-tfstate-<your-project-id>"
  #   prefix = "terraform/state"
  # }
  #
  # WHY: Terraform state contains resource IDs and metadata. Local state is
  # fine for solo development but breaks as soon as a second person (or CI/CD)
  # tries to apply. The GCS backend makes state atomic and auditable.
  # The bucket must exist BEFORE running `terraform init` with the backend.
}

provider "google" {
  project = var.project_id
  region  = var.region

  # Impersonate the pipeline service account instead of using a key file.
  # This is the recommended pattern for local dev — no JSON keys on disk.
  # Run once: gcloud auth application-default login
}

# =============================================================================
# Data sources — read existing GCP project metadata
# =============================================================================

data "google_project" "current" {}

# =============================================================================
# Locals — computed values shared across modules
# =============================================================================

locals {
  # Resource naming convention: {project_short}-{component}-{env}
  # Keeps names consistent and easy to filter in the GCP console.
  name_prefix = "${var.project_short}-${var.environment}"

  # Standard labels applied to every resource.
  # Critical for cost attribution and lifecycle management in real orgs.
  common_labels = {
    project     = var.project_short
    environment = var.environment
    managed_by  = "terraform"
    team        = "data-engineering"
  }
}

# =============================================================================
# Module: IAM — service account + role bindings
# Must be provisioned first; other modules depend on the SA email.
# =============================================================================

module "iam" {
  source = "./modules/iam"

  project_id   = var.project_id
  name_prefix  = local.name_prefix
  common_labels = local.common_labels
}

# =============================================================================
# Module: GCS — staging bucket for Cloud Function source archives
# =============================================================================

module "gcs" {
  source = "./modules/gcs"

  project_id           = var.project_id
  region               = var.region
  name_prefix          = local.name_prefix
  common_labels        = local.common_labels
  pipeline_sa_email    = module.iam.pipeline_sa_email

  depends_on = [module.iam]
}

# =============================================================================
# Module: BigQuery — datasets + tables
# =============================================================================

module "bigquery" {
  source = "./modules/bigquery"

  project_id           = var.project_id
  region               = var.region
  name_prefix          = local.name_prefix
  common_labels        = local.common_labels
  pipeline_sa_email    = module.iam.pipeline_sa_email
  data_retention_days  = var.bq_data_retention_days

  depends_on = [module.iam]
}

# =============================================================================
# Module: Cloud Run Function — the ingestion serverless worker (Phase 3)
#
# DEPLOY ORDER:
#   Phase 2 apply: skip this module (function ZIP doesn't exist yet)
#     terraform apply -target=module.iam -target=module.gcs -target=module.bigquery
#
#   Phase 3 apply: build the ZIP, then apply everything
#     ./scripts/build_function.sh --upload --project <id> --bucket <name>
#     terraform apply
# =============================================================================

module "functions" {
  source = "./modules/functions"

  project_id           = var.project_id
  region               = var.region
  name_prefix          = local.name_prefix
  common_labels        = local.common_labels
  pipeline_sa_email    = module.iam.pipeline_sa_email
  staging_bucket_name  = module.gcs.staging_bucket_name
  bq_dataset_id        = module.bigquery.raw_dataset_id
  bq_table_id          = "station_snapshots"
  dry_run              = var.function_dry_run
  log_level            = var.function_log_level

  depends_on = [module.iam, module.gcs, module.bigquery]
}

# =============================================================================
# Module: Cloud Scheduler — triggers the ingestion function every 5 minutes
#
# PHASE 3 WIRE-UP:
# After `terraform apply` produces module.functions.function_url, update
# terraform.tfvars:
#   function_url_placeholder = "<url from output>"
# Then run: terraform apply -target=module.cloud_scheduler
# =============================================================================

module "cloud_scheduler" {
  source = "./modules/cloud_scheduler"

  project_id         = var.project_id
  region             = var.region
  name_prefix        = local.name_prefix
  common_labels      = local.common_labels
  pipeline_sa_email  = module.iam.pipeline_sa_email
  schedule_cron      = var.ingestion_schedule_cron
  function_url       = coalesce(module.functions.function_url, var.function_url_placeholder)

  depends_on = [module.iam, module.functions]
}
