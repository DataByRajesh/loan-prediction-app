# Loan Default — Spark MLlib Streamlit App

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
