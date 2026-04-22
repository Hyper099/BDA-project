"""Reusable Streamlit section for Spark runtime controls and status."""
from __future__ import annotations

import requests
import streamlit as st

API_SPARK_RUN = "http://127.0.0.1:8000/spark/run"
API_SPARK_STATUS = "http://127.0.0.1:8000/spark/status"
SPARK_UI_FALLBACK_URL = "http://localhost:4040/jobs"


def _check_spark_ui_accessible(url: str, timeout: int = 2) -> bool:
    try:
        response = requests.get(url, timeout=timeout)
        return response.ok
    except requests.RequestException:
        return False


def render_spark_panel() -> None:
    st.subheader("Data Processing and Spark Observability")
    st.caption("Trigger runtime Spark jobs and inspect DAG and stages in Spark UI.")

    persist_intermediate = st.checkbox("Cache intermediate dataframe", value=True)
    keep_ui_alive_seconds = st.slider("Keep Spark UI alive (seconds)", min_value=10, max_value=300, value=90)

    spark_ui_url = SPARK_UI_FALLBACK_URL
    status = None

    try:
        status_response = requests.get(API_SPARK_STATUS, timeout=5)
        status_response.raise_for_status()
        status = status_response.json()
        spark_ui_url = status.get("spark_ui_url") or SPARK_UI_FALLBACK_URL
    except requests.RequestException:
        status = None

    col1, col2, col3 = st.columns([1.2, 1, 1])

    with col1:
        if st.button("Run Spark Pipeline", use_container_width=True):
            try:
                response = requests.post(
                    API_SPARK_RUN,
                    json={
                        "persist_intermediate": persist_intermediate,
                        "keep_ui_alive_seconds": keep_ui_alive_seconds,
                    },
                    timeout=10,
                )
                response.raise_for_status()
                payload = response.json()
                st.success(f"Spark pipeline started. Job ID: {payload.get('job_id')}")
                spark_ui_url = payload.get("spark_ui_url") or spark_ui_url
            except requests.RequestException as ex:
                st.error(f"Failed to start Spark pipeline: {ex}")

    with col2:
        st.link_button("Open Spark UI", spark_ui_url, use_container_width=True)

    with col3:
        if st.button("Refresh Spark Status", use_container_width=True):
            st.rerun()

    try:
        if status is None:
            status_response = requests.get(API_SPARK_STATUS, timeout=5)
            status_response.raise_for_status()
            status = status_response.json()

        status_label = status.get("status", "unknown").capitalize()
        st.write(f"Status: **{status_label}**")
        st.write(f"Message: {status.get('message', '')}")
        st.write(f"Spark UI: {spark_ui_url}")
        if status.get("job_id"):
            st.write(f"Job ID: {status['job_id']}")
        if status.get("started_at"):
            st.write(f"Started: {status['started_at']}")
        if status.get("finished_at"):
            st.write(f"Finished: {status['finished_at']}")
        if status.get("error"):
            st.error(f"Error: {status['error']}")
    except requests.RequestException as ex:
        st.warning(f"Could not read Spark status from backend: {ex}")

    if _check_spark_ui_accessible(spark_ui_url):
        st.success(f"Spark UI is reachable at {spark_ui_url}")
    else:
        st.warning(
            f"Spark UI is not reachable right now at {spark_ui_url}. Start a Spark job first or check local networking/port availability."
        )
