"""Tier 3: autonomous agent. Consumes the workflow's insight and decides + narrates an action.

Wire a real LLM call here (e.g. the Anthropic API) in place of the stubbed reasoning below —
the context object is already carrying everything upstream tiers produced.
"""
from engine.base import BaseModule, Tier
from engine.context import ExecutionContext


class ChurnResponseAgent(BaseModule):
    name = "churn_response_agent"
    tier = Tier.AGENT
    description = "Decides and narrates a follow-up action based on the churn-risk insight."

    def run(self, context: ExecutionContext) -> dict:
        insight = context.get("insight", {})
        risk_level = insight.get("risk_level", "unknown")
        churn_rate = insight.get("churn_rate", 0)

        if risk_level == "high":
            action = "Escalate to the retention team and draft a personalized outreach email."
        else:
            action = "No action needed — churn is within the acceptable range."

        message = f"Churn rate is {churn_rate:.2%} ({risk_level} risk). {action}"
        return {"agent_decision": {"action": action, "message": message}}
