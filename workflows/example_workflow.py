"""Tier 2: data-driven workflow. Turns raw numbers into a proactive insight."""
from engine.base import BaseModule, Tier
from engine.context import ExecutionContext


class MetricsAnalysisWorkflow(BaseModule):
    name = "analyze_metrics"
    tier = Tier.WORKFLOW
    description = "Derives a churn-risk signal from the raw metrics produced upstream."

    def run(self, context: ExecutionContext) -> dict:
        metrics = context.get("raw_metrics", {})
        signups = metrics.get("signups", 0)
        churn = metrics.get("churn", 0)
        churn_rate = churn / signups if signups else 0.0
        risk_level = "high" if churn_rate > 0.1 else "low"
        return {"insight": {"churn_rate": round(churn_rate, 4), "risk_level": risk_level}}
