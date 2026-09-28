# =============================================================================
# outputs.tf — Root module outputs
#
# These values are printed after `terraform apply` and can be referenced
# by other Terraform configurations (e.g., the Phase 3 functions module)
# via `terraform_remote_state` data source.
# =============================================================================

output "pipeline_service_account_email" {
  description = "Email of the pipeline service account. Used in Phase 3 to configure Cloud Run identity."
  value       = module.iam.pipeline_sa_email
}

output "raw_snapshots_table_id" {
  description = "Fully-qualified BigQuery table ID: project.dataset.table"
  value       = module.bigquery.raw_snapshots_table_id
}

output "staging_bucket_name" {
  description = "GCS bucket name for Cloud Function source archives. Used in Phase 3."
  value       = module.gcs.staging_bucket_name
}

output "function_url" {
  description = <<-EOT
    Cloud Run Function HTTPS trigger URL.
    Copy this value into terraform.tfvars as function_url_placeholder,
    then run: terraform apply -target=module.cloud_scheduler
  EOT
  value = module.functions.function_url
}

output "scheduler_job_name" {
  description = "Full resource name of the Cloud Scheduler job."
  value       = module.cloud_scheduler.job_name
}

output "bq_dataset_id" {
  description = "BigQuery dataset ID for reference in dbt profiles.yml (Phase 4)."
  value       = module.bigquery.raw_dataset_id
}

output "project_number" {
  description = "GCP project number. Required for some IAM bindings."
  value       = data.google_project.current.number
}
