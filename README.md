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
