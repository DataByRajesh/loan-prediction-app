from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import json
from google.cloud import bigquery
import uuid, datetime, logging

# ----------------------------
# Logging
# ----------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "time": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "exception": self.formatException(record.exc_info) if record.exc_info else None
        })

logger = logging.getLogger("loan_officer_api")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()    
ch.setFormatter(JsonFormatter())
logger.addHandler(ch)

# ----------------------------
# Spark & Model
# ----------------------------

spark = SparkSession.builder \
    .appName("LoanOfficerAPI") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.20") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .getOrCreate()


model_path = "gs://default-prediction-bucket/loan_data/pipeline/loan_rf_pipeline-20250922T123026Z-1-001/loan_rf_pipeline"
model = PipelineModel.load(model_path)  # Path to your PySpark model

# ----------------------------
# BigQuery client
# ----------------------------

bq_client = bigquery.Client()
BQ_TABLE = "default-prediction-472915.loan_ds.loan_predictions"


# ----------------------------
# Pydantic Models
# ----------------------------
class Applicant(BaseModel):
    person_age: int
    person_income: float
    person_home_ownership: str
    person_emp_length: float
    loan_intent: str
    loan_grade: str
    loan_amnt: float
    loan_int_rate: float
    loan_status: str
    loan_percent_income: float
    cb_person_default_on_file: str
    cb_person_cred_hist_length: float

app = FastAPI(title="Loan Officer Prediction API")

# ----------------------------
# Single Prediction
# ----------------------------
@app.post("/predict")
def predict(applicant: Applicant):
    try:
        df = spark.createDataFrame([applicant.dict()])
        pred_df = model.transform(df)
        row = pred_df.select("prediction","probability","risk_score").collect()[0]
        logger.info(f"Prediction successful for applicant")
        return {
            "prediction": float(row["prediction"]),
            "probability": row["probability"].tolist(),
            "risk_score": float(row.get("risk_score",0))
        }
    except Exception as e:
        logger.error(f"Prediction failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Prediction failed")

# ----------------------------
# Batch Prediction
# ----------------------------
@app.post("/predict_batch")
def predict_batch(applicants: List[Applicant]):
    try:
        df = spark.createDataFrame([a.dict() for a in applicants])
        pred_df = model.transform(df)
        results = []
        for row in pred_df.select("prediction","probability","risk_score").collect():
            results.append({
                "prediction": float(row["prediction"]),
                "probability": row["probability"].tolist(),
                "risk_score": float(row.get("risk_score",0))
            })
        logger.info(f"Batch prediction successful, count={len(results)}")
        return {"results": results}
    except Exception as e:
        logger.error(f"Batch prediction failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Batch prediction failed")
