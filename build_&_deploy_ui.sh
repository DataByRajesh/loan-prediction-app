#!/bin/bash

# ===== CONFIGURATION =====
PROJECT_ID="default-prediction-472915"          # <-- replace with your GCP project ID
SERVICE_NAME="loan-officer-ui" # Streamlit frontend service name
REGION="us-central1"
IMAGE="gcr.io/$PROJECT_ID/$SERVICE_NAME"

# ===== AUTHENTICATION =====
echo "🔑 Configuring gcloud authentication..."
gcloud config set project $PROJECT_ID
gcloud auth configure-docker

# ===== BUILD & PUSH IMAGE =====
echo "📦 Building Docker image for Streamlit..."
gcloud builds submit --tag $IMAGE

# ===== DEPLOY TO CLOUD RUN =====
echo "🚀 Deploying Streamlit frontend to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars "API_URL=https://loan-prediction-api-xxxx.a.run.app/predict"

# ===== OUTPUT SERVICE URL =====
SERVICE_URL=$(gcloud run services describe $SERVICE_NAME --region $REGION --format 'value(status.url)')
echo "✅ Deployment complete!"
echo "🌍 Streamlit frontend is live at: $SERVICE_URL"
