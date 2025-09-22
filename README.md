# Loan Default — Spark MLlib Streamlit App

## High-Accuracy Loan Risk Assessment
Leveraging Big Data and Machine Learning for enhanced loan default prediction. Led the development of a machine learning pipeline, achieving 94% model accuracy and ensuring robust evaluation metrics.

- Tech stack: Python, Spark MLlib, Scikit-learn, Streamlit, PySpark

## Deploy (Streamlit Community Cloud)
- Main file: `streamlit_app.py`
- Python packages: `requirements_streamlit_cloud.txt`
- System packages: `packages.txt`
- Secrets / Env:
  - MODEL_DIR = model
  - MODEL_URL = <Google Drive share link to model.zip>

The app downloads your zipped Spark `PipelineModel` from MODEL_URL on first run, extracts it to MODEL_DIR, and serves predictions via:
- Loan Officer form
- CSV upload
- JSON row

## Run with Docker (local)

Build image:
```bash
docker build -t loan-default-app .
```

Run (provide MODEL_URL if no `model/` directory is present):
```bash
docker run --rm -p 8501:8501 \
  -e MODEL_URL="https://<your-model-archive>" \
  -e MODEL_DIR="model" \
  --name loan-default-app \
  loan-default-app
```

Then open http://localhost:8501

## Deploy to Google Cloud Run

Prereqs:
- Install gcloud and authenticate (`gcloud auth login` and `gcloud config set project <PROJECT_ID>`)
- Push container to Artifact Registry or use Cloud Build

Build and deploy with Cloud Build + Cloud Run:
```bash
gcloud builds submit --tag "${REGION}-docker.pkg.dev/${PROJECT_ID}/loan-default/loan-default-app:latest"
gcloud run deploy loan-default-app \
  --image "${REGION}-docker.pkg.dev/${PROJECT_ID}/loan-default/loan-default-app:latest" \
  --region ${REGION} \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars MODEL_DIR=model,APP_NAME=loan-default-app \
  --set-env-vars MODEL_URL="gs://<bucket>/<path>/model.zip"
```

Notes:
- `MODEL_URL` supports `gs://...` (GCS), Google Drive links, or generic HTTP(S).
- Service account running the Cloud Run service must have `storage.objects.get` on the model object if using GCS.
- The Docker image respects `$PORT` for Cloud Run.
