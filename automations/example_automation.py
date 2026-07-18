"""Tier 1: deterministic, rule-based data pull. No ML, no NLP — a fixed procedure."""
from engine.base import BaseModule, Tier
from engine.context import ExecutionContext


class DataFetchAutomation(BaseModule):
    name = "fetch_raw_metrics"
    tier = Tier.AUTOMATION
    description = "Simulates pulling raw metrics from an external system on a fixed schedule."

    def run(self, context: ExecutionContext) -> dict:
        raw_metrics = {"signups": 128, "churn": 14, "revenue": 4210.50}
        return {"raw_metrics": raw_metrics}
