"""Result caching for single-module runs.

Scoped deliberately to single-module card runs, not pipelines — a pipeline's
step inputs can depend on live upstream context, so "same inputs" isn't a
well-defined question the same way it is for one module run in isolation.

This assumes a module is a pure function of its inputs (true for every
bundled example here). A module with real side effects or genuine
non-determinism (a real LLM call, a live API poll) shouldn't be cached
blindly — that's a per-module decision left for later, not automated here.
"""
import hashlib
import json


def make_cache_key(tier: str, name: str, inputs: dict) -> str:
    payload = json.dumps({"tier": tier, "name": name, "inputs": inputs}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()
