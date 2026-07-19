"""Tier 3: a specialized agent that only acts when handed a case by another
agent (`churn_response_agent`, in this same package) — genuine agent-to-agent
delegation rather than the usual upstream-tier handoff. It reads the
`handoff` key another agent's output wrote into the shared context; if no
handoff was requested, it declines cleanly instead of inventing work.
"""
import time

from engine.base import BaseModule, Tier
from engine.context import ExecutionContext

_SPECIALIST_BY_PRIORITY = {
    "high": "senior_retention_specialist",
    "normal": "retention_specialist",
}


class EscalationAgent(BaseModule):
    name = "escalation_agent"
    tier = Tier.AGENT
    description = "Picks up a case another agent hands off, and drafts a retention plan for it."

    def run(self, context: ExecutionContext) -> dict:
        handoff = context.get("handoff", {}) or {}

        if not handoff.get("requested"):
            context.emit("thought", "No handoff waiting — nothing for escalation_agent to do.")
            return {"escalation_result": {"status": "skipped", "reason": "no handoff requested"}}

        reason = handoff.get("reason", "")
        priority = handoff.get("priority", "normal")
        context.emit("thought", f"Received a handoff: {reason}")
        time.sleep(0.2)

        specialist = _SPECIALIST_BY_PRIORITY.get(priority, "retention_specialist")
        context.emit("tool_call", f'assign_specialist(role="{specialist}", priority="{priority}")')
        time.sleep(0.2)

        risk_level = handoff.get("risk_level", "unknown")
        churn_rate = handoff.get("churn_rate", 0)
        plan = (
            f"Assigned to {specialist}: reach out within 24h, review account history, "
            f"and offer a retention incentive (risk level: {risk_level}, churn rate {churn_rate:.2%})."
        )
        context.emit("thought", "Retention plan drafted and assigned.")

        return {
            "escalation_result": {
                "status": "escalated",
                "assigned_to": specialist,
                "plan": plan,
            }
        }
