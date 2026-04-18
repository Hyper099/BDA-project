"""Pydantic schemas for API payloads."""
from pydantic import BaseModel, Field


class PredictionRequest(BaseModel):
    monthly_upi_transactions: int = Field(..., ge=0)
    avg_transaction_amount: float = Field(..., ge=0)
    bill_payment_timeliness: float = Field(..., ge=0, le=1)
    mobile_recharge_frequency: int = Field(..., ge=0)
    age: int = Field(..., ge=18, le=100)
    location_type: str = Field(..., pattern="^(urban|rural)$")
    income: float = Field(..., ge=0)
    occupation: str
    savings_ratio: float = Field(..., ge=0, le=1)
    spending_ratio: float = Field(..., ge=0)
    payment_delay_days: int = Field(..., ge=0)


class PredictionResponse(BaseModel):
    probability_of_repayment: float
    risk_score: float
    risk_category: str
