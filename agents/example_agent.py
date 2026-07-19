"""Tier 3: autonomous agent. Consumes the workflow's insight and decides + narrates an action.

Wire a real LLM call here (e.g. the Anthropic API) in place of the stubbed reasoning below —
the context object is already carrying everything upstream tiers produced. The context.emit()
calls below stand in for what a real agent would stream: its intermediate reasoning and the
tools it decides to call, so a watching dashboard has something to show mid-run.

On a high-risk case this agent also writes a `handoff` output — a structured
request for a second, specialized agent (`escalation_agent`, in this same
package) to pick up. This is genuine agent-to-agent delegation, not just the
usual tier-1 -> tier-2 -> tier-3 handoff: one agent deciding *another agent*
should take over, and handing it just enough context to act without redoing
the upstream analysis. See `escalation_agent.py` for the receiving side.
"""
import time

from engine.base import BaseModule, Tier
from engine.context import ExecutionContext

_TONE_PREFIX = {
    "professional": "Summary:",
    "urgent": "ACTION REQUIRED:",
    "casual": "Heads up —",
}


class ChurnResponseAgent(BaseModule):
    name = "churn_response_agent"
    tier = Tier.AGENT
    description = "Decides and narrates a follow-up action based on the churn-risk insight."

    def run(self, context: ExecutionContext) -> dict:
        insight = context.get("insight", {})
        risk_level = insight.get("risk_level", "unknown")
        churn_rate = insight.get("churn_rate", 0)
        tone = context.get("tone", "professional")
        notify_slack = context.get("notify_slack", False)
        enable_handoff = context.get("enable_handoff", True)
        # Already interpolated (e.g. "{signups} signups, {churn} churn events" ->
        # "128 signups, 14 churn events") before this module ever sees it — the
        # orchestrator resolves `template`-type fields against the shared context
        # right before this step runs, so this is just plain text by the time we
        # read it here.
        custom_note = context.get("custom_note", "")

        context.emit("thought", "Reviewing the churn-risk insight from the upstream workflow...")
        time.sleep(0.3)

        # context.memory persists across separate runs (unlike context.variables,
        # which starts fresh every time) — remembering the last risk level here
        # lets a completely independent later run notice a trend, not just react
        # to this run's numbers in isolation.
        previous_risk_level = context.memory.get("last_risk_level")
        context.memory.set("last_risk_level", risk_level)

        context.emit("tool_call", f'risk_policy_lookup(risk_level="{risk_level}")')
        time.sleep(0.3)

        if risk_level == "high":
            action = "Escalate to the retention team and draft a personalized outreach email."
        else:
            action = "No action needed — churn is within the acceptable range."
        context.emit("thought", f"Decided on action: {action}")
        time.sleep(0.2)

        if notify_slack:
            context.emit("tool_call", 'slack_notify(channel="#retention")')
            time.sleep(0.2)

        prefix = _TONE_PREFIX.get(tone, _TONE_PREFIX["professional"])
        message = f"{prefix} churn rate is {churn_rate:.2%} ({risk_level} risk). {action}"
        if notify_slack:
            message += " (Slack notification queued.)"
        if previous_risk_level is not None and previous_risk_level != risk_level:
            message += f" (Risk moved from {previous_risk_level} to {risk_level} since the last run.)"
        if custom_note:
            message += f" Note: {custom_note}"

        context.emit("thought", "Drafting final response.")

        handoff_requested = enable_handoff and risk_level == "high"
        if handoff_requested:
            context.emit("thought", "Risk is high — handing this off to escalation_agent for retention follow-through.")
        handoff = {
            "requested": handoff_requested,
            "reason": f"churn rate {churn_rate:.2%} is in the {risk_level}-risk band" if handoff_requested else "",
            "priority": "high" if handoff_requested else "normal",
            "risk_level": risk_level,
            "churn_rate": churn_rate,
        }

        return {"agent_decision": {"action": action, "message": message}, "handoff": handoff}
