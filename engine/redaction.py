"""Best-effort redaction of secret-shaped values before they're persisted to
the state store or streamed to the dashboard.

Scoped narrowly: this only masks a dict value whose *key name* looks like a
secret (password, token, api_key, ...). It can't know a value is sensitive
just by looking at it, so a module that names its field something else (or
embeds a secret inside a larger string) isn't protected — catching every
possible secret shape would mean guessing at arbitrary string content, a much
bigger and much less reliable problem than this is trying to solve.

Deliberately applied only to what gets *recorded or emitted* (StepRecord.output,
SSE events, the final run context) — not to the live `ExecutionContext.variables`
a later step actually reads, so a module that legitimately needs a real secret
value to do its job still gets one; only the audit trail is masked.
"""
import re
from typing import Any

_SECRET_KEY_PATTERN = re.compile(
    r"(password|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential)",
    re.IGNORECASE,
)

REDACTED = "***REDACTED***"


def _is_secret_key(key: str) -> bool:
    return bool(_SECRET_KEY_PATTERN.search(key))


def redact_secrets(value: Any) -> Any:
    """Return a copy of `value` with every dict value whose key looks like a
    secret replaced by a fixed placeholder, recursing into nested dicts and
    lists. Non-dict/list values are returned unchanged."""
    if isinstance(value, dict):
        return {
            key: (REDACTED if _is_secret_key(key) else redact_secrets(val))
            for key, val in value.items()
        }
    if isinstance(value, list):
        return [redact_secrets(item) for item in value]
    return value
