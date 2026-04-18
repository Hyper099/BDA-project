"""Risk score helper functions."""


def probability_to_risk_score(probability_of_repayment: float) -> float:
    """Convert repayment probability (0-1) to risk score (0-100)."""
    clipped = min(max(probability_of_repayment, 0.0), 1.0)
    return round(clipped * 100, 2)


def risk_category_from_score(score: float) -> str:
    """Map score to risk category."""
    if score < 40:
        return "High Risk"
    if score < 70:
        return "Medium Risk"
    return "Low Risk"
