"""SQLite helpers for storing prediction history."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Dict

from utils.config import DB_PATH


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                monthly_upi_transactions INTEGER,
                avg_transaction_amount REAL,
                bill_payment_timeliness REAL,
                mobile_recharge_frequency INTEGER,
                age INTEGER,
                location_type TEXT,
                income REAL,
                occupation TEXT,
                savings_ratio REAL,
                spending_ratio REAL,
                payment_delay_days INTEGER,
                probability_of_repayment REAL,
                risk_score REAL,
                risk_category TEXT
            )
            """
        )
        conn.commit()


def log_prediction(payload: Dict, result: Dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO predictions (
                created_at,
                monthly_upi_transactions,
                avg_transaction_amount,
                bill_payment_timeliness,
                mobile_recharge_frequency,
                age,
                location_type,
                income,
                occupation,
                savings_ratio,
                spending_ratio,
                payment_delay_days,
                probability_of_repayment,
                risk_score,
                risk_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.utcnow().isoformat(),
                payload["monthly_upi_transactions"],
                payload["avg_transaction_amount"],
                payload["bill_payment_timeliness"],
                payload["mobile_recharge_frequency"],
                payload["age"],
                payload["location_type"],
                payload["income"],
                payload["occupation"],
                payload["savings_ratio"],
                payload["spending_ratio"],
                payload["payment_delay_days"],
                result["probability_of_repayment"],
                result["risk_score"],
                result["risk_category"],
            ),
        )
        conn.commit()
