#!/bin/bash

# Variables
PROJECT_ID="default-prediction-472915"
USER_EMAIL="raj.analystdata@gmail.com"


# 3️⃣ Enable required services
SERVICES=(
    "compute.googleapis.com"
    "storage.googleapis.com"
    "dataproc.googleapis.com"
    "aiplatform.googleapis.com"
)

for service in "${SERVICES[@]}"; do
    gcloud services enable $service --project=$PROJECT_ID
done

echo "✅ Services enabled: ${SERVICES[*]}"
