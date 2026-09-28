# =============================================================================
# Dockerfile — local Cloud Run Function emulator
#
# Replicates the exact runtime environment that GCP uses for Python 3.12
# Cloud Run Functions (Gen 2). Run this locally to catch environment-specific
# issues (missing packages, wrong paths) before deploying to GCP.
#
# Usage:
#   docker build -t ecobici-function .
#   docker run --rm -p 8080:8080 \
#     -e DRY_RUN=true \
#     -e GCP_PROJECT_ID=your-project \
#     -e BQ_DATASET_ID=ecobici_raw \
#     -e BQ_TABLE_ID=station_snapshots \
#     ecobici-function
#
#   # Trigger a test invocation:
#   curl -X POST http://localhost:8080 \
#     -H "Content-Type: application/json" \
#     -d '{"source":"local_docker","feed":"station_status","version":"1.0"}'
#
#   # Health check:
#   curl http://localhost:8080/health
# =============================================================================

FROM python:3.12-slim

# Security: run as non-root user — matches Cloud Run's runtime user
RUN groupadd -r gcf && useradd -r -g gcf gcf

WORKDIR /app

# Install dependencies first (layer cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy function source (built by scripts/build_function.sh into functions/dist/)
# This includes main.py, bq_writer.py, and the ingestion/ sub-package
COPY . .

# Cloud Run Functions framework listens on PORT (default 8080)
ENV PORT=8080

USER gcf

# functions-framework --target matches the @functions_framework.http decorated
# function name in main.py
CMD ["functions-framework", "--target", "ingest", "--port", "8080"]
