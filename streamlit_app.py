import os, io, json, zipfile, tarfile, tempfile, shutil
import pandas as pd
import requests
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

st.set_page_config(page_title="Loan Default Risk — Spark MLlib", layout="wide")
st.title("🏦 Loan Default Risk — Spark MLlib Pipeline")
st.caption("Enter applicant details via the form, or upload CSV/JSON. The app loads a saved Spark ML PipelineModel and runs `.transform()`.")

MODEL_DIR = os.getenv("MODEL_DIR", "model")
APP_NAME = os.getenv("APP_NAME", "loan-default-app")
MASTER = os.getenv("SPARK_MASTER", "local[*]")

@st.cache_resource(show_spinner=False)
def get_spark():
    return (
        SparkSession.builder
        .appName(APP_NAME).master(MASTER)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

@st.cache_resource(show_spinner=False)
def load_model(model_path: str):
    return PipelineModel.load(model_path)

def _gdown_download(url: str, out_path: str):
    import gdown
    gdown.download(url, out_path, quiet=False, fuzzy=True)

def ensure_model_present(model_dir: str) -> None:
    # If already there, return
    if os.path.isdir(model_dir) and os.path.isdir(os.path.join(model_dir, "metadata")) and os.path.isdir(os.path.join(model_dir, "stages")):
        return
    model_url = os.getenv("MODEL_URL", "").strip()
    if not model_url:
        st.error("MODEL_DIR not found and MODEL_URL not provided. Set MODEL_URL to a zip/tar of your Spark PipelineModel.")
        st.stop()
    st.info("Downloading model archive…")
    tmp = tempfile.mkdtemp()
    archive_path = os.path.join(tmp, "model_archive")

    # Download (Drive or generic HTTP)
    try:
        if "drive.google.com" in model_url or "id=" in model_url:
            _gdown_download(model_url, archive_path)
        else:
            r = requests.get(model_url, stream=True, timeout=300)
            r.raise_for_status()
            with open(archive_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk: f.write(chunk)
    except Exception as e:
        st.error(f"Model download failed: {e}"); st.stop()

    # Extract .zip or .tar.*
    extracted_root = tmp
    try:
        if zipfile.is_zipfile(archive_path):
            with zipfile.ZipFile(archive_path) as z: z.extractall(tmp)
        else:
            try:
                with tarfile.open(archive_path) as t: t.extractall(tmp)
            except Exception:
                pass
    except Exception as e:
        st.error(f"Extraction failed: {e}"); st.stop()

    # Find a PipelineModel folder (metadata/ + stages/)
    cand = None
    for root, dirs, files in os.walk(extracted_root):
        for d in dirs:
            p = os.path.join(root, d)
            if os.path.isdir(os.path.join(p, "metadata")) and os.path.isdir(os.path.join(p, "stages")):
                cand = p; break
        if cand: break
    if cand is None:
        st.error("Could not locate a valid Spark PipelineModel in the downloaded archive."); st.stop()

    try:
        if os.path.exists(model_dir): shutil.rmtree(model_dir)
        shutil.move(cand, model_dir)
        st.success(f"Model ready at: {model_dir}")
    except Exception as e:
        st.error(f"Failed to place model: {e}"); st.stop()

spark = get_spark()
st.success(f"SparkSession started (master={MASTER}).")
ensure_model_present(MODEL_DIR)

try:
    model = load_model(MODEL_DIR)
    st.success(f"Loaded PipelineModel from: {MODEL_DIR}")
except Exception as e:
    st.error(f"Failed to load model from '{MODEL_DIR}': {e}")
    st.stop()

with st.sidebar:
    st.header("Settings")
    MODEL_DIR = st.text_input("Model directory", MODEL_DIR)
    show_prob = st.checkbox("Show probabilities (if available)", value=True)
    if st.button("Reload model"):
        try:
            model = load_model(MODEL_DIR); st.success(f"Reloaded: {MODEL_DIR}")
        except Exception as e:
            st.error(f"Reload failed: {e}")

tab1, tab2, tab3 = st.tabs(["📝 Loan Officer Form", "📦 CSV Upload", "📄 JSON Row"])

def predict_from_pdf(pdf: pd.DataFrame):
    sdf = spark.createDataFrame(pdf)
    preds = model.transform(sdf)
    cols = [c for c in ["prediction","probability","rawPrediction"] if c in preds.columns]
    out_pdf = preds.select(list(pdf.columns) + cols).toPandas()
    return out_pdf

with tab1:
    st.subheader("Loan Officer — Enter Applicant Details")
    c1, c2, c3 = st.columns(3)
    with c1:
        loan_amnt = st.number_input("Loan Amount", min_value=0.0, value=15000.0, step=500.0)
        int_rate = st.number_input("Interest Rate (%)", min_value=0.0, max_value=100.0, value=13.56, step=0.01)
        installment = st.number_input("Installment", min_value=0.0, value=512.5, step=1.0)
        annual_inc = st.number_input("Annual Income", min_value=0.0, value=72000.0, step=1000.0)
    with c2:
        term = st.selectbox("Term", ["36 months","60 months"], index=0)
        grade = st.selectbox("Grade", ["A","B","C","D","E","F","G"], index=2)
        emp_length = st.selectbox("Employment Length", ["< 1 year","1 year","2 years","3 years","4 years","5 years","6 years","7 years","8 years","9 years","10+ years"], index=10)
        home_ownership = st.selectbox("Home Ownership", ["MORTGAGE","RENT","OWN","OTHER"], index=0)
    with c3:
        purpose = st.selectbox("Purpose", ["debt_consolidation","credit_card","home_improvement","major_purchase","small_business","car","medical","vacation","moving","house","other"], index=0)
        addr_state = st.selectbox("State", ["AL","AK","AZ","AR","CA","CO","CT","DC","DE","FL","GA","HI","IA","ID","IL","IN","KS","KY","LA","MA","MD","ME","MI","MN","MO","MS","MT","NC","ND","NE","NH","NJ","NM","NV","NY","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VA","VT","WA","WI","WV","WY"], index=4)
        dti = st.number_input("Debt-to-Income (%)", min_value=0.0, max_value=100.0, value=18.3, step=0.1)
        revol_util = st.number_input("Revolving Utilization (%)", min_value=0.0, max_value=100.0, value=52.3, step=0.1)
    c4, c5, c6, c7 = st.columns(4)
    with c4: delinq_2yrs = st.number_input("Delinquencies (2 yrs)", min_value=0, value=0, step=1)
    with c5: inq_last_6mths = st.number_input("Inquiries (6m)", min_value=0, value=1, step=1)
    with c6: open_acc = st.number_input("Open Accounts", min_value=0, value=9, step=1)
    with c7: pub_rec = st.number_input("Public Records", min_value=0, value=0, step=1)
    c8, c9 = st.columns(2)
    with c8: revol_bal = st.number_input("Revolving Balance", min_value=0.0, value=8000.0, step=100.0)
    with c9: total_acc = st.number_input("Total Accounts", min_value=0, value=23, step=1)

    if st.button("Predict Default Risk"):
        row = {
            "loan_amnt": loan_amnt, "term": term, "int_rate": int_rate, "installment": installment,
            "grade": grade, "emp_length": emp_length, "home_ownership": home_ownership, "annual_inc": annual_inc,
            "purpose": purpose, "addr_state": addr_state, "dti": dti, "delinq_2yrs": int(delinq_2yrs),
            "inq_last_6mths": int(inq_last_6mths), "open_acc": int(open_acc), "pub_rec": int(pub_rec),
            "revol_bal": revol_bal, "revol_util": revol_util, "total_acc": int(total_acc)
        }
        out_pdf = predict_from_pdf(pd.DataFrame([row]))
        st.success("Prediction complete.")
        st.dataframe(out_pdf)

with tab2:
    st.subheader("Batch CSV Upload")
    up = st.file_uploader("Upload CSV", type=["csv"])
    if up is not None:
        try:
            pdf = pd.read_csv(up)
            out_pdf = predict_from_pdf(pdf)
            st.success("Predictions generated.")
            st.dataframe(out_pdf.head(50))
            buf = io.BytesIO(); out_pdf.to_csv(buf, index=False); buf.seek(0)
            st.download_button("Download predictions CSV", data=buf, file_name="predictions.csv", mime="text/csv")
        except Exception as e:
            st.error(f"Batch prediction failed: {e}")

with tab3:
    st.subheader("Single JSON Row")
    json_text = st.text_area("JSON row", height=180, placeholder='{"loan_amnt": 15000, "term": "36 months", ...}')
    if st.button("Predict (JSON)"):
        try:
            obj = json.loads(json_text); out_pdf = predict_from_pdf(pd.DataFrame([obj]))
            st.success("Prediction complete."); st.dataframe(out_pdf)
        except Exception as e:
            st.error(f"Prediction failed: {e}")

st.divider()
st.caption("Pass raw input features (not pre-encoded). The PipelineModel handles indexing/encoding/assembling/scaling + classification.")
