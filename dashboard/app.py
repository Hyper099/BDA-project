"""Streamlit dashboard for credit risk analytics."""
from __future__ import annotations

import json

import pandas as pd
import requests
import streamlit as st

from utils.config import METRICS_PATH, PROCESSED_DATA_PATH

API_URL = "http://127.0.0.1:8000/predict"

st.set_page_config(page_title="Financial Inclusion Analytics", layout="wide")
st.title("Smart Financial Inclusion Analytics System")
st.caption("Alternative Credit Scoring Dashboard")

nav1, nav2 = st.columns(2)
with nav1:
    st.page_link("pages/1_DAG_and_HDFS.py", label="Open Spark DAG Page")
with nav2:
    st.link_button("Open HDFS Dashboard", "http://127.0.0.1:9870")

left, right = st.columns([1, 1])

with left:
    st.subheader("Single User Scoring")
    monthly_upi_transactions = st.number_input("Monthly UPI Transactions", min_value=0, value=12)
    avg_transaction_amount = st.number_input("Average Transaction Amount", min_value=0.0, value=900.0)
    bill_payment_timeliness = st.slider("Bill Payment Timeliness", 0.0, 1.0, 0.8)
    mobile_recharge_frequency = st.number_input("Mobile Recharge Frequency", min_value=0, value=7)
    age = st.number_input("Age", min_value=18, max_value=100, value=29)
    location_type = st.selectbox("Location Type", ["urban", "rural"])
    income = st.number_input("Income", min_value=0.0, value=35000.0)
    occupation = st.selectbox(
        "Occupation",
        ["farmer", "driver", "small_business", "teacher", "nurse", "gig_worker", "artisan", "student"],
    )
    savings_ratio = st.slider("Savings Ratio", 0.0, 1.0, 0.25)
    spending_ratio = st.slider("Spending Ratio", 0.0, 1.5, 0.65)
    payment_delay_days = st.number_input("Payment Delay Days", min_value=0, value=5)

    if st.button("Predict Risk"):
        payload = {
            "monthly_upi_transactions": int(monthly_upi_transactions),
            "avg_transaction_amount": float(avg_transaction_amount),
            "bill_payment_timeliness": float(bill_payment_timeliness),
            "mobile_recharge_frequency": int(mobile_recharge_frequency),
            "age": int(age),
            "location_type": location_type,
            "income": float(income),
            "occupation": occupation,
            "savings_ratio": float(savings_ratio),
            "spending_ratio": float(spending_ratio),
            "payment_delay_days": int(payment_delay_days),
        }

        try:
            response = requests.post(API_URL, json=payload, timeout=5)
            response.raise_for_status()
            result = response.json()
            st.success("Prediction completed")
            st.metric("Risk Score", result["risk_score"])
            st.write(f"Risk Category: **{result['risk_category']}**")
            st.write(f"Repayment Probability: {result['probability_of_repayment']}")
        except Exception as ex:
            st.error(f"API call failed: {ex}")

with right:
    st.subheader("Batch Upload Scoring")
    uploaded_file = st.file_uploader("Upload CSV for scoring", type=["csv"])
    if uploaded_file:
        batch_df = pd.read_csv(uploaded_file)
        st.write("Preview", batch_df.head())

        scored = []
        for _, row in batch_df.iterrows():
            payload = row.to_dict()
            try:
                response = requests.post(API_URL, json=payload, timeout=5)
                if response.ok:
                    scored.append({**payload, **response.json()})
            except Exception:
                continue

        if scored:
            scored_df = pd.DataFrame(scored)
            st.dataframe(scored_df.head(20), use_container_width=True)
            st.download_button(
                "Download Scored CSV",
                data=scored_df.to_csv(index=False).encode("utf-8"),
                file_name="scored_customers.csv",
                mime="text/csv",
            )

st.subheader("Portfolio Insights")

if PROCESSED_DATA_PATH.exists():
    df = pd.read_csv(PROCESSED_DATA_PATH)
    if METRICS_PATH.exists():
        with open(METRICS_PATH, "r", encoding="utf-8") as f:
            metrics = json.load(f)
        st.write("Model Summary", metrics)

    # Approximate risk score for visualization from heuristic repayment signal.
    score = (
        (0.035 * df["monthly_upi_transactions"] + 0.00001 * df["income"])
        + (1.35 * df["bill_payment_timeliness"] + 1.1 * df["savings_ratio"])
        - (0.85 * df["spending_ratio"] + 0.04 * df["payment_delay_days"])
    )
    df["risk_score"] = (1 / (1 + pow(2.718281828, -(score - 1.8)))) * 100

    c1, c2, c3 = st.columns(3)

    with c1:
        st.write("Risk Distribution")
        st.bar_chart(df["risk_score"].round(0).value_counts().sort_index())

    with c2:
        st.write("Income vs Risk")
        st.scatter_chart(df[["income", "risk_score"]], x="income", y="risk_score")

    with c3:
        st.write("Urban vs Rural Comparison")
        comp = df.groupby("location_type")["risk_score"].mean().reset_index()
        st.bar_chart(comp, x="location_type", y="risk_score")
else:
    st.info("Processed dataset not found. Run ETL and training first.")
