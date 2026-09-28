#!/usr/bin/env bash
# =============================================================================
# scripts/build_function.sh
#
# Packages the Cloud Run Function for deployment.
#
# PROBLEM: Our project has ingestion/ at the root level (good for local dev
# and Phase 1 testing), but Cloud Run Functions need a SELF-CONTAINED source
# directory where all imports resolve from the root of the ZIP.
#
# SOLUTION: This script assembles a temporary staging directory that:
#   1. Contains all files from functions/ (main.py, bq_writer.py, etc.)
#   2. Contains ingestion/ as a sub-package (copied, not symlinked)
#   3. Zips the result to functions/dist/function_source.zip
#   4. Uploads the ZIP to GCS (where Terraform's function module reads it)
#
# The original source files are NEVER duplicated in git — only in the
# ephemeral staging dir and GCS.
#
# Usage:
#   ./scripts/build_function.sh [--upload] [--project PROJECT_ID] [--bucket BUCKET_NAME]
#
# Options:
#   --upload             Upload the ZIP to GCS after building (requires gcloud)
#   --project <id>       GCP project ID (overrides GCP_PROJECT_ID env var)
#   --bucket <name>      GCS bucket name (overrides STAGING_BUCKET env var)
#
# Examples:
#   ./scripts/build_function.sh                            # local build only
#   ./scripts/build_function.sh --upload \
#     --project ecobici-pipeline-dev \
#     --bucket ecobici-dev-staging-ecobici-pipeline-dev    # build + upload
# =============================================================================

set -euo pipefail

# ── Resolve project root regardless of where script is called from ───────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

FUNCTIONS_DIR="${PROJECT_ROOT}/functions"
INGESTION_DIR="${PROJECT_ROOT}/ingestion"
DIST_DIR="${FUNCTIONS_DIR}/dist"
STAGING_DIR="${DIST_DIR}/staging"
OUTPUT_ZIP="${DIST_DIR}/function_source.zip"

# ── Parse arguments ───────────────────────────────────────────────────────────
UPLOAD=false
GCP_PROJECT="${GCP_PROJECT_ID:-}"
STAGING_BUCKET="${STAGING_BUCKET:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upload)    UPLOAD=true; shift ;;
    --project)   GCP_PROJECT="$2"; shift 2 ;;
    --bucket)    STAGING_BUCKET="$2"; shift 2 ;;
    *)           echo "Unknown argument: $1"; exit 1 ;;
  esac
done

# ── Validation ────────────────────────────────────────────────────────────────
if [[ ! -d "${FUNCTIONS_DIR}" ]]; then
  echo "ERROR: functions/ directory not found at ${FUNCTIONS_DIR}"
  exit 1
fi

if [[ ! -d "${INGESTION_DIR}" ]]; then
  echo "ERROR: ingestion/ directory not found at ${INGESTION_DIR}"
  exit 1
fi

if [[ "${UPLOAD}" == "true" ]]; then
  if [[ -z "${GCP_PROJECT}" ]]; then
    echo "ERROR: --project or GCP_PROJECT_ID env var required for --upload"
    exit 1
  fi
  if [[ -z "${STAGING_BUCKET}" ]]; then
    echo "ERROR: --bucket or STAGING_BUCKET env var required for --upload"
    exit 1
  fi
fi

# ── Build ─────────────────────────────────────────────────────────────────────
echo ""
echo "Building Cloud Run Function package..."
echo "  Source : ${FUNCTIONS_DIR}"
echo "  Output : ${OUTPUT_ZIP}"
echo ""

# Clean and recreate staging directory
rm -rf "${STAGING_DIR}"
mkdir -p "${STAGING_DIR}"

# Copy function source files
# Note: we deliberately copy only .py files and requirements.txt
# (Dockerfile and .gcloudignore stay out of the ZIP)
cp "${FUNCTIONS_DIR}/main.py"          "${STAGING_DIR}/main.py"
cp "${FUNCTIONS_DIR}/bq_writer.py"     "${STAGING_DIR}/bq_writer.py"
cp "${FUNCTIONS_DIR}/requirements.txt" "${STAGING_DIR}/requirements.txt"

# Copy the ingestion sub-package into the staging directory.
# After this, the ZIP root will contain:
#   main.py
#   bq_writer.py
#   requirements.txt
#   ingestion/__init__.py
#   ingestion/gbfs_client.py
#   ingestion/schema.py
#
# This matches how Python resolves `from ingestion.gbfs_client import ...`
# when running inside the Cloud Run container.
mkdir -p "${STAGING_DIR}/ingestion"
cp "${INGESTION_DIR}/__init__.py"    "${STAGING_DIR}/ingestion/__init__.py"
cp "${INGESTION_DIR}/gbfs_client.py" "${STAGING_DIR}/ingestion/gbfs_client.py"
cp "${INGESTION_DIR}/schema.py"      "${STAGING_DIR}/ingestion/schema.py"
# Deliberately NOT copying extract.py — that's the local CLI, not needed in cloud

# Create the ZIP from inside the staging directory so paths in the ZIP
# are relative (no leading staging/ prefix)
mkdir -p "${DIST_DIR}"
(cd "${STAGING_DIR}" && zip -r "${OUTPUT_ZIP}" . -x "*.pyc" -x "__pycache__/*" > /dev/null)

ZIP_SIZE=$(du -sh "${OUTPUT_ZIP}" | cut -f1)
echo "  ✓ Package built: ${OUTPUT_ZIP} (${ZIP_SIZE})"
echo ""

# ── Contents summary ──────────────────────────────────────────────────────────
echo "Package contents:"
(cd "${STAGING_DIR}" && find . -type f | sort | sed 's/^/    /')
echo ""

# ── Upload to GCS ─────────────────────────────────────────────────────────────
if [[ "${UPLOAD}" == "true" ]]; then
  GCS_PATH="gs://${STAGING_BUCKET}/functions/function_source.zip"
  echo "Uploading to GCS..."
  echo "  Target: ${GCS_PATH}"

  gcloud storage cp "${OUTPUT_ZIP}" "${GCS_PATH}" \
    --project="${GCP_PROJECT}"

  echo "  ✓ Upload complete"
  echo ""
  echo "Next step: terraform apply to deploy/update the Cloud Run Function."
  echo "  cd infrastructure && terraform apply -var=\"project_id=${GCP_PROJECT}\""
fi

echo "Build complete."
echo ""
