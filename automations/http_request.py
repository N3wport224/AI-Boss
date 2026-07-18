"""Tier 1: deterministic HTTP call to an arbitrary, user-supplied URL.

No credentials are baked in here — the URL, headers, and body are all
per-run inputs, so this stays within the "no third-party SaaS integration"
constraint while still letting a pipeline talk to any HTTP endpoint the
user points it at (including other local services).
"""
import json

import httpx

from engine.base import BaseModule, Tier
from engine.context import ExecutionContext


class HttpRequestAutomation(BaseModule):
    name = "http_request"
    tier = Tier.AUTOMATION
    description = "Calls an arbitrary URL with a configurable method, headers, and body."

    def run(self, context: ExecutionContext) -> dict:
        url = context.get("url", "")
        if not url:
            raise ValueError("http_request requires a non-empty 'url'")

        method = str(context.get("method", "GET") or "GET").upper()
        timeout = float(context.get("timeout", 10) or 10)

        headers = _parse_json_object(context.get("headers", ""), "headers")
        json_body, data_body = _parse_body(context.get("body", ""))

        kwargs = {"headers": headers}
        if json_body is not None:
            kwargs["json"] = json_body
        elif data_body is not None:
            kwargs["content"] = data_body

        try:
            with httpx.Client(timeout=timeout, follow_redirects=True) as client:
                response = client.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            # httpx's own message often omits the URL ("[Errno 111] Connection
            # refused") — include it so run history and its search say *what*
            # couldn't be reached, not just why.
            raise RuntimeError(f"{method} {url} failed: {exc}") from exc

        try:
            parsed_body = response.json()
        except ValueError:
            parsed_body = response.text

        return {
            "http_response": {
                "status_code": response.status_code,
                "ok": response.is_success,
                "headers": dict(response.headers),
                "body": parsed_body,
                "elapsed_ms": round(response.elapsed.total_seconds() * 1000, 2),
                "url": str(response.url),
            }
        }


def _parse_json_object(raw, field_name: str) -> dict:
    """Coerce a manifest 'headers'-style field (JSON object text) into a dict."""
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"'{field_name}' must be valid JSON object text") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"'{field_name}' must be a JSON object")
    return parsed


def _parse_body(raw) -> tuple:
    """Return (json_body, data_body); at most one is non-None.

    Valid JSON is sent with a JSON content-type via httpx's `json=`; anything
    else is sent verbatim as raw text/bytes via `content=`.
    """
    if not raw:
        return None, None
    if isinstance(raw, (dict, list)):
        return raw, None
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        return None, raw
    return parsed, None
