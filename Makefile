# =============================================================================
# Makefile — Developer workflow shortcuts
# Usage: make <target>
# =============================================================================

INFRA_DIR := infrastructure
TF        := terraform -chdir=$(INFRA_DIR)

.PHONY: help init validate plan apply destroy fmt check-env

help:
	@echo ""
	@echo "  ECOBICI Pipeline — Makefile targets"
	@echo "  ─────────────────────────────────────────────────────────"
	@echo "  PHASE 1 / 2 — Local & IaC"
	@echo "    make install        Install Python dependencies"
	@echo "    make test           Run Phase 1 test suite (real data)"
	@echo "    make tf-init        Initialize Terraform providers"
	@echo "    make tf-validate    Validate HCL syntax"
	@echo "    make tf-plan        Show infrastructure diff (dry run)"
	@echo "    make tf-apply       Apply all infrastructure"
	@echo "    make tf-destroy     Tear down all resources"
	@echo "    make fmt            Auto-format all .tf files"
	@echo ""
	@echo "  PHASE 3 — Cloud Run Deployment"
	@echo "    make build          Package function + ingestion/ into ZIP"
	@echo "    make build-upload   Package and upload ZIP to GCS"
	@echo "    make deploy         Full deploy: build + upload + terraform apply"
	@echo "    make wire-scheduler Update Scheduler with real function URL"
	@echo "    make smoke-test     Hit the deployed function endpoint once"
	@echo "    make logs           Tail Cloud Run logs in real time"
	@echo ""

check-env:
	@test -f $(INFRA_DIR)/terraform.tfvars || \
		(echo "ERROR: $(INFRA_DIR)/terraform.tfvars not found." && \
		 echo "       cp $(INFRA_DIR)/terraform.tfvars.example $(INFRA_DIR)/terraform.tfvars" && \
		 echo "       Then fill in your project_id." && exit 1)

init: check-env
	$(TF) init

validate: init
	$(TF) validate

fmt:
	$(TF) fmt -recursive

plan: validate
	$(TF) plan -out=tfplan

apply: plan
	$(TF) apply tfplan

destroy: check-env
	$(TF) destroy

# ── Phase 3 — Build & Deploy ──────────────────────────────────────────────────
# Reads project_id and staging bucket from terraform.tfvars automatically.
PROJECT_ID    := $(shell grep 'project_id' $(INFRA_DIR)/terraform.tfvars 2>/dev/null | grep -v '#' | head -1 | cut -d'"' -f2)
BUCKET_NAME   := $(shell grep 'staging_bucket_name' $(INFRA_DIR)/terraform.tfvars 2>/dev/null | grep -v '#' | head -1 | cut -d'"' -f2)
REGION        := $(shell grep 'region' $(INFRA_DIR)/terraform.tfvars 2>/dev/null | grep -v '#' | head -1 | cut -d'"' -f2 || echo "us-central1")
FUNCTION_NAME := ecobici-dev-ingestion

build:
	@echo "Building function package (local only)..."
	@bash scripts/build_function.sh

build-upload: check-env
	@echo "Building and uploading function package..."
	@bash scripts/build_function.sh --upload \
		--project "$(PROJECT_ID)" \
		--bucket "$(BUCKET_NAME)"

# Full deploy: build → upload → terraform apply
# Run this after any change to functions/ or ingestion/
deploy: build-upload apply
	@echo ""
	@echo "Deployment complete. Function URL:"
	@$(TF) output -raw function_url 2>/dev/null || echo "(run terraform apply to see URL)"

# After initial deploy, wire the Scheduler to the real function URL
wire-scheduler:
	@FUNC_URL=$$($(TF) output -raw function_url 2>/dev/null); \
	if [ -z "$$FUNC_URL" ]; then \
		echo "ERROR: function_url output not available. Run 'make deploy' first."; \
		exit 1; \
	fi; \
	echo "Wiring Scheduler to: $$FUNC_URL"; \
	$(TF) apply -target=module.cloud_scheduler \
		-var="function_url_placeholder=$$FUNC_URL"

# Send a single test invocation to the deployed function (with DRY_RUN)
smoke-test: check-env
	@FUNC_URL=$$($(TF) output -raw function_url 2>/dev/null); \
	if [ -z "$$FUNC_URL" ]; then \
		echo "ERROR: function_url output not available."; exit 1; \
	fi; \
	echo "Sending smoke test to: $$FUNC_URL"; \
	gcloud functions call "$(FUNCTION_NAME)" \
		--region="$(REGION)" \
		--gen2 \
		--data='{"source":"smoke_test","feed":"station_status","version":"1.0"}' \
		--project="$(PROJECT_ID)"

# Stream Cloud Run logs in real time
logs: check-env
	gcloud beta run services logs tail "$(FUNCTION_NAME)" \
		--region="$(REGION)" \
		--project="$(PROJECT_ID)"

# ── Python ────────────────────────────────────────────────────────────────────

test:
	PYTHONPATH=. python test_pipeline_local.py

install:
	pip install -r requirements.txt --break-system-packages
