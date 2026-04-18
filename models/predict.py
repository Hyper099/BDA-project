"""Prediction utility used by API and dashboard."""
from __future__ import annotations

import joblib
import pandas as pd

from utils.config import MODEL_PATH
from utils.risk import probability_to_risk_score, risk_category_from_score


class CreditScoringService:
    """Wrap trained model to produce probability, risk score, and category."""

    def __init__(self, model_path: str = str(MODEL_PATH)) -> None:
        self.model = joblib.load(model_path)

    def predict(self, payload: dict) -> dict:
        frame = pd.DataFrame([payload])
        probability = float(self.model.predict_proba(frame)[:, 1][0])
        risk_score = probability_to_risk_score(probability)
        return {
            "probability_of_repayment": round(probability, 4),
            "risk_score": risk_score,
            "risk_category": risk_category_from_score(risk_score),
        }
