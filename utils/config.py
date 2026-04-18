"""Centralized configuration for paths and app settings."""
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"
RAW_DATA_PATH = DATA_DIR / "raw" / "financial_inclusion_data.csv"
PROCESSED_DATA_PATH = DATA_DIR / "processed" / "financial_inclusion_features.csv"

S3_SIM_DIR = DATA_DIR / "warehouse" / "s3_simulated"
S3_PROCESSED_PATH = S3_SIM_DIR / "processed" / "financial_inclusion_features.csv"

MODELS_DIR = BASE_DIR / "models" / "artifacts"
MODEL_PATH = MODELS_DIR / "best_model.joblib"
PREPROCESSOR_PATH = MODELS_DIR / "preprocessor.joblib"
METRICS_PATH = MODELS_DIR / "metrics.json"

DB_PATH = BASE_DIR / "storage" / "financial_inclusion.db"
LOG_DIR = BASE_DIR / "logs"

RISK_BANDS = {
    "high": (0, 40),
    "medium": (40, 70),
    "low": (70, 100),
}
