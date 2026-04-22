"""Background runtime manager for Spark jobs triggered by the web API."""
from __future__ import annotations

import threading
import time
import uuid
from pathlib import Path
from typing import Any

from spark_jobs.observable_pipeline import run_observable_pipeline
from utils.config import DATA_DIR, RAW_DATA_PATH
from utils.logger import get_logger

logger = get_logger(__name__)

_state_lock = threading.Lock()
_job_state: dict[str, Any] = {
    "job_id": None,
    "status": "idle",
    "started_at": None,
    "finished_at": None,
    "message": "No Spark jobs have been started yet.",
    "spark_ui_url": "http://localhost:4040/jobs",
    "error": None,
    "result": None,
}


def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def get_spark_job_status() -> dict[str, Any]:
    with _state_lock:
        return dict(_job_state)


def _run_pipeline_worker(job_id: str, input_csv: Path, output_dir: Path, persist_intermediate: bool, keep_ui_alive_seconds: int) -> None:
    with _state_lock:
        _job_state.update(
            {
                "job_id": job_id,
                "status": "running",
                "started_at": _utc_now(),
                "finished_at": None,
                "message": "Spark pipeline is running.",
                "spark_ui_url": "http://localhost:4040/jobs",
                "error": None,
                "result": None,
            }
        )

    try:
        logger.info("Starting Spark pipeline job %s", job_id)
        result = run_observable_pipeline(
            input_csv=input_csv,
            output_dir=output_dir,
            persist_intermediate=persist_intermediate,
            keep_ui_alive_seconds=keep_ui_alive_seconds,
        )

        with _state_lock:
            _job_state.update(
                {
                    "status": "completed",
                    "finished_at": _utc_now(),
                    "message": "Spark pipeline completed successfully.",
                    "spark_ui_url": result.get("spark_ui_url", "http://localhost:4040/jobs"),
                    "result": result,
                }
            )
        logger.info("Spark pipeline job %s completed", job_id)
    except Exception as ex:  # noqa: BLE001
        logger.exception("Spark pipeline job %s failed", job_id)
        with _state_lock:
            _job_state.update(
                {
                    "status": "failed",
                    "finished_at": _utc_now(),
                    "message": "Spark pipeline execution failed.",
                    "spark_ui_url": "http://localhost:4040/jobs",
                    "error": str(ex),
                }
            )


def start_spark_job(
    input_csv: Path | None = None,
    output_dir: Path | None = None,
    persist_intermediate: bool = True,
    keep_ui_alive_seconds: int = 30,
) -> dict[str, Any]:
    input_path = input_csv or RAW_DATA_PATH
    output_path = output_dir or (DATA_DIR / "processed" / "spark_ui_observable")

    with _state_lock:
        if _job_state["status"] == "running":
            raise RuntimeError("A Spark pipeline job is already running.")

    job_id = str(uuid.uuid4())
    worker = threading.Thread(
        target=_run_pipeline_worker,
        args=(job_id, input_path, output_path, persist_intermediate, keep_ui_alive_seconds),
        daemon=True,
        name=f"spark-job-{job_id[:8]}",
    )
    worker.start()

    return {
        "job_id": job_id,
        "status": "running",
        "message": "Spark pipeline started.",
        "spark_ui_url": "http://localhost:4040/jobs",
    }
