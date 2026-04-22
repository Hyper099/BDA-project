"""FastAPI service for risk scoring."""
from __future__ import annotations

from fastapi import FastAPI, HTTPException

from api.schemas import (PredictionRequest, PredictionResponse,
                         SparkRunRequest, SparkRunResponse,
                         SparkStatusResponse)
from models.predict import CreditScoringService
from spark_jobs.runtime import get_spark_job_status, start_spark_job
from utils.db import init_db, log_prediction
from utils.logger import get_logger

app = FastAPI(title="Smart Financial Inclusion Analytics API", version="1.0.0")
logger = get_logger(__name__)
service = CreditScoringService()


@app.on_event("startup")
def startup_event() -> None:
    init_db()
    logger.info("SQLite database initialized")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.post("/predict", response_model=PredictionResponse)
def predict(request: PredictionRequest) -> PredictionResponse:
    payload = request.model_dump()

    payload["digital_activity_score"] = (
        payload["monthly_upi_transactions"] * payload["avg_transaction_amount"] / 1000.0
    )
    payload["financial_discipline_score"] = (
        payload["bill_payment_timeliness"] + payload["savings_ratio"]
    ) / 2.0
    payload["is_underbanked"] = 1 if payload["monthly_upi_transactions"] < 8 else 0

    result = service.predict(payload)
    log_prediction(payload, result)
    logger.info("Prediction completed with risk category: %s", result["risk_category"])
    return PredictionResponse(**result)


@app.post("/spark/run", response_model=SparkRunResponse)
def run_spark_pipeline(request: SparkRunRequest | None = None) -> SparkRunResponse:
    request = request or SparkRunRequest()
    try:
        payload = start_spark_job(
            persist_intermediate=request.persist_intermediate,
            keep_ui_alive_seconds=request.keep_ui_alive_seconds,
        )
        logger.info("Spark pipeline job started: %s", payload["job_id"])
        return SparkRunResponse(**payload)
    except RuntimeError as ex:
        raise HTTPException(status_code=409, detail=str(ex)) from ex
    except Exception as ex:  # noqa: BLE001
        logger.exception("Failed to start Spark pipeline")
        raise HTTPException(status_code=500, detail=f"Unable to start Spark pipeline: {ex}") from ex


@app.get("/spark/status", response_model=SparkStatusResponse)
def spark_status() -> SparkStatusResponse:
    return SparkStatusResponse(**get_spark_job_status())
