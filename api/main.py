"""FastAPI service for risk scoring."""
from __future__ import annotations

from fastapi import FastAPI

from api.schemas import PredictionRequest, PredictionResponse
from models.predict import CreditScoringService
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
