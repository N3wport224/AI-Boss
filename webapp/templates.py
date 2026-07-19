"""Built-in pipeline starter templates.

A curated, hand-written list -- not user data, not persisted, not editable
through the API -- so a new user has a few genuinely useful starting points
to clone into their own saved pipelines instead of always building from a
single blank step in the visual builder. Each entry uses only the modules
bundled with this repo, so every template works offline, out of the box,
on a fresh checkout.
"""

PIPELINE_TEMPLATES = [
    {
        "id": "basic-churn-response",
        "name": "Basic Churn Response",
        "description": (
            "Fetch metrics, analyze churn risk, and have an agent decide and narrate "
            "a response. The same 3-step chain the built-in demo runs, saved as an "
            "editable starting point you can add conditions or retries to."
        ),
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {}},
            {"tier": "agent", "name": "churn_response_agent", "inputs": {}},
        ],
    },
    {
        "id": "churn-response-with-escalation",
        "name": "Churn Response with Escalation",
        "description": (
            "Adds a second, specialized agent that picks up whatever high-risk case "
            "churn_response_agent hands off -- a ready-made example of genuine "
            "agent-to-agent delegation, not just the usual tier-to-tier handoff."
        ),
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {}},
            {"tier": "agent", "name": "churn_response_agent", "inputs": {}},
            {
                "tier": "agent",
                "name": "escalation_agent",
                "inputs": {},
                "mappings": {"handoff": {"step": 2, "output": "handoff"}},
            },
        ],
    },
    {
        "id": "metrics-webhook-notifier",
        "name": "Metrics Webhook Notifier",
        "description": (
            "Fetches metrics, then POSTs them to any HTTP endpoint you point it at -- "
            "fill in the real URL after cloning to notify an external service, a log "
            "collector, or a local script."
        ),
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {}},
            {
                "tier": "automation",
                "name": "http_request",
                "inputs": {"url": "https://example.com/webhook", "method": "POST"},
            },
        ],
    },
]
