#!/bin/bash

# ==========================
# CONFIGURATION
# ==========================
PROJECT_ID="default-prediction-472915"                  # <-- replace with your GCP project ID
REGION="us-central1"
REPO_NAME="loan-prediction-repo"

# Backend
BACKEND_NAME="loan-prediction-api"
BACKEND_IMAGE="us-central1-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$BACKEND_NAME:latest"

# Frontend
FRONTEND_NAME="loan-officer-ui"
FRONTEND_IMAGE="us-central1-docker.pkg.dev/$PROJECT_ID/$REPO_NAME/$FRONTEND_NAME:latest"

# ==========================
# AUTHENTICATION
# ==========================
echo "🔑 Configuring gcloud authentication..."
gcloud config set project $PROJECT_ID
gcloud auth configure-docker

# ==========================
# CREATE ARTIFACT REGISTRY REPO
# ==========================
echo "📦 Creating Artifact Registry repository..."
gcloud artifacts repositories create $REPO_NAME \
    --repository-format=docker \
    --location=$REGION \
    --description="Docker repo for loan prediction project" \
    --async || echo "Repo may already exist, continuing..."

# ==========================
# BUILD & PUSH BACKEND IMAGE
# ==========================
echo "🚀 Building backend Docker image..."
docker build -t $BACKEND_IMAGE -f Dockerfile.backend .

echo "📤 Pushing backend image to Artifact Registry..."
docker push $BACKEND_IMAGE

# ==========================
# DEPLOY BACKEND TO CLOUD RUN
# ==========================
echo "⚡ Deploying backend to Cloud Run..."
gcloud run deploy $BACKEND_NAME \
  --image $BACKEND_IMAGE \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated

BACKEND_URL=$(gcloud run services describe $BACKEND_NAME --region $REGION --format 'value(status.url)')
echo "✅ Backend deployed at: $BACKEND_URL"

# ==========================
# BUILD & PUSH FRONTEND IMAGE
# ==========================
echo "🚀 Building frontend Docker image..."
docker build -t $FRONTEND_IMAGE -f Dockerfile.frontend .

echo "📤 Pushing frontend image to Artifact Registry..."
docker push $FRONTEND_IMAGE

# ==========================
# DEPLOY FRONTEND TO CLOUD RUN
# ==========================
echo "⚡ Deploying frontend to Cloud Run..."
gcloud run deploy $FRONTEND_NAME \
  --image $FRONTEND_IMAGE \
  --platform managed \
  --region $REGION \
  --allow-unauthenticated \
  --set-env-vars "API_URL=$BACKEND_URL"

FRONTEND_URL=$(gcloud run services describe $FRONTEND_NAME --region $REGION --format 'value(status.url)')
echo "✅ Frontend deployed at: $FRONTEND_URL"

echo "🎉 Full deployment complete!"
echo "Backend URL: $BACKEND_URL"
echo "Frontend URL: $FRONTEND_URL"
