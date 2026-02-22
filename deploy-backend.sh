#!/usr/bin/env bash
set -euo pipefail

# ─────────────────────────────────────────────────────────────
# Deploy F1 OmniSense backend to GCP Cloud Run
# ─────────────────────────────────────────────────────────────
# Prerequisites:
#   1. gcloud CLI installed and authenticated: gcloud auth login
#   2. A GCP project selected: gcloud config set project <PROJECT_ID>
#   3. Artifact Registry API enabled
#   4. Cloud Run API enabled
#
# Usage:
#   chmod +x deploy-backend.sh
#   ./deploy-backend.sh
# ─────────────────────────────────────────────────────────────

REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="f1-backend"
PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

if [ -z "$PROJECT_ID" ]; then
  echo "ERROR: No GCP project set. Run: gcloud config set project <PROJECT_ID>"
  exit 1
fi

REPO="${REGION}-docker.pkg.dev/${PROJECT_ID}/f1-omnisense"
IMAGE="${REPO}/backend:latest"

echo "═══════════════════════════════════════════"
echo "  F1 OmniSense — Backend Deploy to Cloud Run"
echo "  Project:  ${PROJECT_ID}"
echo "  Region:   ${REGION}"
echo "  Image:    ${IMAGE}"
echo "═══════════════════════════════════════════"

# ── Step 1: Create Artifact Registry repo (if not exists) ──
echo ""
echo "▸ Ensuring Artifact Registry repo exists..."
gcloud artifacts repositories describe f1-omnisense \
  --location="${REGION}" --format="value(name)" 2>/dev/null || \
gcloud artifacts repositories create f1-omnisense \
  --repository-format=docker \
  --location="${REGION}" \
  --description="F1 OmniSense Docker images"

# ── Step 2: Configure Docker auth ─────────────────────────
echo "▸ Configuring Docker authentication..."
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

# ── Step 3: Build the image ───────────────────────────────
echo "▸ Building backend image..."
docker build -t "${IMAGE}" -f pipeline/Dockerfile .

# ── Step 4: Push to Artifact Registry ─────────────────────
echo "▸ Pushing image to Artifact Registry..."
docker push "${IMAGE}"

# ── Step 5: Deploy to Cloud Run ───────────────────────────
echo "▸ Deploying to Cloud Run..."

# Read env vars from .env (skip comments and empty lines)
ENV_VARS=""
if [ -f .env ]; then
  while IFS= read -r line; do
    # Skip comments and empty lines
    [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
    # Skip lines with spaces in key (malformed)
    key="${line%%=*}"
    [[ "$key" =~ [[:space:]] ]] && continue
    if [ -z "$ENV_VARS" ]; then
      ENV_VARS="${line}"
    else
      ENV_VARS="${ENV_VARS},${line}"
    fi
  done < .env
fi

gcloud run deploy "${SERVICE_NAME}" \
  --image="${IMAGE}" \
  --region="${REGION}" \
  --platform=managed \
  --port=8100 \
  --memory=2Gi \
  --cpu=2 \
  --min-instances=0 \
  --max-instances=3 \
  --timeout=300s \
  --allow-unauthenticated \
  --set-env-vars="API_PORT=8100,${ENV_VARS}"

# ── Step 6: Get the URL ───────────────────────────────────
BACKEND_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --region="${REGION}" \
  --format="value(status.url)")

echo ""
echo "═══════════════════════════════════════════"
echo "  Backend deployed!"
echo "  URL: ${BACKEND_URL}"
echo ""
echo "  Next: Set this as BACKEND_URL in Vercel:"
echo "    vercel env add BACKEND_URL"
echo "    → paste: ${BACKEND_URL}"
echo "═══════════════════════════════════════════"
