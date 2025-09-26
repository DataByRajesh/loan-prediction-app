import streamlit as st
import pandas as pd
import requests
import os

# FastAPI backend URL (Cloud Run or local)
API_BASE = os.environ.get("API_URL", "http://localhost:8080")

st.set_page_config(page_title="Loan Officer Dashboard", layout="wide")
st.title("Loan Default Prediction - Officer Dashboard")

# ---------------------
# Single Applicant Input
# ---------------------
st.header("Single Applicant Prediction")
col1, col2 = st.columns(2)
with col1:
    person_age = st.slider("Age", 18, 100, 30)
    person_income = st.number_input("Annual Income (£)", 0, 1_000_000, 50000)
    person_emp_length = st.slider("Years of Employment", 0, 50, 5)
    loan_amnt = st.number_input("Loan Amount (£)", 0, 1_000_000, 10000)
    loan_int_rate = st.slider("Loan Interest Rate (%)", 0.0, 100.0, 12.5)
    cb_person_cred_hist_length = st.slider("Credit History Length (Years)", 0, 50, 5)

with col2:
    person_home_ownership = st.selectbox("Home Ownership", ["Own", "Rent", "Mortgage"])
    loan_intent = st.selectbox("Loan Purpose", ["Personal", "Education", "Medical", "Venture", "Home Improvement"])
    loan_grade = st.selectbox("Loan Grade", ["A","B","C","D","E","F","G"])
    cb_person_default_on_file = st.radio("Has Prior Default?", ["Yes","No"])
    loan_status = st.selectbox("Loan Status", ["Current","Paid","Default"])

loan_percent_income = loan_amnt / person_income if person_income>0 else 0

if st.button("Predict Single Applicant"):
    payload = {
        "person_age": person_age,
        "person_income": person_income,
        "person_home_ownership": person_home_ownership.upper(),
        "person_emp_length": person_emp_length,
        "loan_intent": loan_intent.upper().replace(" ", ""),
        "loan_grade": loan_grade,
        "loan_amnt": loan_amnt,
        "loan_int_rate": loan_int_rate,
        "loan_status": loan_status.upper(),
        "loan_percent_income": loan_percent_income,
        "cb_person_default_on_file": "Y" if cb_person_default_on_file=="Yes" else "N",
        "cb_person_cred_hist_length": cb_person_cred_hist_length
    }
    try:
        response = requests.post(API_BASE + "/predict", json=payload)
        if response.status_code==200:
            result = response.json()
            colors = {"A":"#00FF00","B":"#66FF66","C":"#FFFF66","D":"#FFCC66","E":"#FF9966","F":"#FF6666","G":"#FF0000"}
            st.markdown(f"**Prediction:** {result['prediction']}")
            st.markdown(f"**Probability:** {result['probability']}")
            st.markdown(f"**Risk Score:** {result['risk_score']}")
            st.markdown(f"<span style='color:{colors[loan_grade]}; font-weight:bold'>Loan Grade: {loan_grade}</span>", unsafe_allow_html=True)
        else:
            st.error(f"Prediction failed: {response.json().get('detail')}")
    except Exception as e:
        st.error(f"Error connecting to API: {e}")

# ---------------------
# Batch Prediction
# ---------------------
st.header("Batch Prediction")
uploaded_file = st.file_uploader("Upload CSV for batch prediction", type=["csv"])
if uploaded_file is not None:
    batch_df = pd.read_csv(uploaded_file)
    if st.button("Predict Batch"):
        try:
            response = requests.post(API_BASE + "/predict_batch", json=batch_df.to_dict(orient="records"))
            if response.status_code==200:
                results = response.json()["results"]
                batch_df["prediction"] = [r["prediction"] for r in results]
                batch_df["prob_class_0"] = [r["probability"][0] for r in results]
                batch_df["prob_class_1"] = [r["probability"][1] for r in results]
                batch_df["risk_score"] = [r["risk_score"] for r in results]
                st.dataframe(batch_df)
            else:
                st.error(f"Batch prediction failed: {response.json().get('detail')}")
        except Exception as e:
            st.error(f"Error connecting to API: {e}")
