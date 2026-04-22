"""Streamlit page to inspect Spark DAG output and HDFS references."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.config import LOG_DIR

DAG_LOG_PATH = LOG_DIR / "dag_hdfs_output.txt"

st.set_page_config(page_title="Spark DAG and HDFS", layout="wide")
st.title("Spark DAG and HDFS Inspection")

st.link_button("Open HDFS Dashboard", "http://127.0.0.1:9870")

st.subheader("HDFS-backed DAG Output")


def _sanitize_output(raw: str) -> str:
    """Remove common PowerShell wrapper lines from captured native command output."""
    cleaned = []
    skip_prefixes = (
        "at line:",
        "+ ...",
        "+ categoryinfo",
        "+ fullyqualifiederrorid",
    )
    for line in raw.splitlines():
        l = line.strip().lower()
        if any(l.startswith(prefix) for prefix in skip_prefixes):
            continue
        if line.startswith("docker : ") and "failed to connect to the docker api" not in l:
            # Keep Spark log lines that can be prefixed with 'docker : ' by PowerShell.
            cleaned.append(line.replace("docker : ", "", 1))
            continue
        cleaned.append(line)
    return "\n".join(cleaned)

if DAG_LOG_PATH.exists():
    raw_content = DAG_LOG_PATH.read_text(encoding="utf-8", errors="ignore")
    content = _sanitize_output(raw_content)

    st.success(f"Loaded DAG output from {DAG_LOG_PATH}")

    if "failed to connect to the docker API" in content.lower():
        st.error("Docker is not running. Start Docker Desktop and regenerate the DAG output.")

    with st.expander("Show full Spark output", expanded=False):
        st.code(content, language="text")

    st.subheader("Extracted DAG/Plan Lines")
    extracted = []
    for line in content.splitlines():
        lowered = line.lower()
        if (
            "physical plan" in lowered
            or "rdd lineage" in lowered
            or "filescan" in lowered
            or "hdfs://" in lowered
            or line.strip().startswith("(1) MapPartitionsRDD")
        ):
            extracted.append(line)

    if extracted:
        st.code("\n".join(extracted[:250]), language="text")
    else:
        st.info("No DAG-specific lines found yet in the output file.")
else:
    st.warning("DAG output file not found yet.")
    st.markdown(
        "Run this command and refresh the page:\n"
        "`docker exec fi-spark /opt/spark/bin/spark-submit --master local[*] "
        "--conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 "
        "/app/scripts/dag_inspect_hdfs.py > logs/dag_hdfs_output.txt 2>&1`"
    )

st.markdown("[Back to Main Dashboard](../)")
