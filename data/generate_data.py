"""Generate synthetic alternative credit-scoring dataset."""
from __future__ import annotations

import argparse
from typing import Tuple

import numpy as np
import pandas as pd

from utils.config import RAW_DATA_PATH
from utils.logger import get_logger

logger = get_logger(__name__)


OCCUPATIONS = [
    "farmer",
    "driver",
    "small_business",
    "teacher",
    "nurse",
    "gig_worker",
    "artisan",
    "student",
]


def _bounded_normal(size: int, mean: float, std: float, low: float, high: float) -> np.ndarray:
    data = np.random.normal(mean, std, size)
    return np.clip(data, low, high)


def generate_dataset(num_rows: int = 5000, seed: int = 42) -> pd.DataFrame:
    """Create realistic synthetic data with a probabilistic loan repayment target."""
    np.random.seed(seed)

    age = np.random.randint(18, 61, size=num_rows)
    location_type = np.random.choice(["urban", "rural"], size=num_rows, p=[0.55, 0.45])
    income = _bounded_normal(num_rows, mean=42000, std=18000, low=6000, high=150000)
    occupation = np.random.choice(OCCUPATIONS, size=num_rows)

    monthly_upi_transactions = np.random.poisson(lam=18, size=num_rows) + np.random.binomial(1, 0.25, num_rows) * 8
    avg_transaction_amount = _bounded_normal(num_rows, mean=850, std=500, low=50, high=8000)
    bill_payment_timeliness = _bounded_normal(num_rows, mean=0.82, std=0.18, low=0.05, high=1.0)
    mobile_recharge_frequency = np.random.randint(1, 16, size=num_rows)

    savings_ratio = _bounded_normal(num_rows, mean=0.22, std=0.15, low=0.01, high=0.85)
    spending_ratio = _bounded_normal(num_rows, mean=0.68, std=0.18, low=0.1, high=1.25)
    payment_delay_days = np.maximum(0, np.random.normal(loc=8, scale=7, size=num_rows)).astype(int)

    urban_boost = (location_type == "urban").astype(float) * 0.15
    stable_jobs = np.isin(occupation, ["teacher", "nurse", "small_business"]).astype(float) * 0.2

    # Alternative data-driven repayment probability.
    raw_score = (
        0.035 * monthly_upi_transactions
        + 0.00001 * income
        + 1.35 * bill_payment_timeliness
        + 1.1 * savings_ratio
        - 0.85 * spending_ratio
        - 0.04 * payment_delay_days
        + urban_boost
        + stable_jobs
        - 0.00009 * avg_transaction_amount
    )
    probability = 1 / (1 + np.exp(-(raw_score - 1.8)))
    loan_repaid = np.random.binomial(1, probability, size=num_rows)

    df = pd.DataFrame(
        {
            "monthly_upi_transactions": monthly_upi_transactions,
            "avg_transaction_amount": np.round(avg_transaction_amount, 2),
            "bill_payment_timeliness": np.round(bill_payment_timeliness, 3),
            "mobile_recharge_frequency": mobile_recharge_frequency,
            "age": age,
            "location_type": location_type,
            "income": np.round(income, 2),
            "occupation": occupation,
            "savings_ratio": np.round(savings_ratio, 3),
            "spending_ratio": np.round(spending_ratio, 3),
            "payment_delay_days": payment_delay_days,
            "loan_repaid": loan_repaid,
        }
    )

    # Inject small missingness for realistic cleaning stage.
    missing_idx = np.random.choice(df.index, size=max(1, int(0.01 * num_rows)), replace=False)
    df.loc[missing_idx, "income"] = np.nan

    return df


def save_dataset(df: pd.DataFrame, output_path: str | None = None) -> Tuple[str, int]:
    path = RAW_DATA_PATH if output_path is None else output_path
    RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return str(path), len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic financial inclusion dataset")
    parser.add_argument("--rows", type=int, default=5000, help="Number of rows")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    df = generate_dataset(num_rows=args.rows, seed=args.seed)
    path, count = save_dataset(df)
    logger.info("Synthetic dataset generated at %s with %d rows", path, count)


if __name__ == "__main__":
    main()
