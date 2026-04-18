"""Model training module for alternative credit scoring."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple, cast

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBClassifier

from utils.config import (METRICS_PATH, MODEL_PATH, MODELS_DIR,
                          PREPROCESSOR_PATH, PROCESSED_DATA_PATH)
from utils.logger import get_logger

logger = get_logger(__name__)


FEATURE_COLUMNS = [
    "monthly_upi_transactions",
    "avg_transaction_amount",
    "bill_payment_timeliness",
    "mobile_recharge_frequency",
    "age",
    "location_type",
    "income",
    "occupation",
    "savings_ratio",
    "spending_ratio",
    "payment_delay_days",
    "digital_activity_score",
    "financial_discipline_score",
    "is_underbanked",
]

TARGET_COLUMN = "loan_repaid"


def load_data(path: Path = PROCESSED_DATA_PATH) -> Tuple[pd.DataFrame, pd.Series]:
    df = pd.read_csv(path)
    X = cast(pd.DataFrame, df.loc[:, FEATURE_COLUMNS])
    y = cast(pd.Series, df.loc[:, TARGET_COLUMN])
    return X, y


def build_preprocessor() -> ColumnTransformer:
    categorical = ["location_type", "occupation"]
    numeric = [c for c in FEATURE_COLUMNS if c not in categorical]

    return ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), numeric),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
        ]
    )


def get_models() -> Dict[str, object]:
    return {
        "logistic_regression": LogisticRegression(max_iter=500, class_weight="balanced"),
        "random_forest": RandomForestClassifier(
            n_estimators=220,
            max_depth=12,
            random_state=42,
            class_weight="balanced",
        ),
        "xgboost": XGBClassifier(
            n_estimators=250,
            max_depth=5,
            learning_rate=0.06,
            subsample=0.9,
            colsample_bytree=0.9,
            eval_metric="logloss",
            random_state=42,
        ),
    }


def train_and_select_best() -> dict:
    X, y = load_data()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    preprocessor = build_preprocessor()
    models = get_models()

    metrics = {}
    best_model_name = None
    best_model_pipeline = None
    best_f1 = -1.0

    for name, model in models.items():
        pipeline = Pipeline(steps=[("preprocessor", preprocessor), ("model", model)])
        pipeline.fit(X_train, y_train)
        preds = pipeline.predict(X_test)

        acc = accuracy_score(y_test, preds)
        f1 = f1_score(y_test, preds)
        metrics[name] = {"accuracy": round(float(acc), 4), "f1": round(float(f1), 4)}
        logger.info("%s -> accuracy: %.4f, f1: %.4f", name, acc, f1)

        if f1 > best_f1:
            best_f1 = f1
            best_model_name = name
            best_model_pipeline = pipeline

    if best_model_pipeline is None or best_model_name is None:
        raise RuntimeError("Training did not produce a valid model")

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(best_model_pipeline, MODEL_PATH)

    # Saving preprocessor separately can help if feature transforms are needed independently.
    joblib.dump(best_model_pipeline.named_steps["preprocessor"], PREPROCESSOR_PATH)

    output = {
        "best_model": best_model_name,
        "metrics": metrics,
    }
    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    logger.info("Best model: %s (f1=%.4f)", best_model_name, best_f1)
    return output


if __name__ == "__main__":
    result = train_and_select_best()
    print(json.dumps(result, indent=2))
