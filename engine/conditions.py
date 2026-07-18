"""Declarative condition evaluation for skip-unless-condition pipeline steps.

A condition compares one value pulled out of the shared context (`actual`)
against a literal (`expected`) with a named operator. Kept deliberately
tiny and total: an unknown operator raises at build/validation time, but
evaluation itself never raises — a missing key or type mismatch just makes
the condition false, so a bad condition skips a step rather than failing
the run.
"""
from typing import Any

OPERATORS = ("equals", "not_equals", "contains", "gt", "lt", "truthy", "falsy")


def _as_number(value: Any):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def evaluate_condition(actual: Any, operator: str, expected: Any = None) -> bool:
    """Compare `actual` (from context) against `expected` (a literal).

    Numeric-looking values are compared as numbers for equals/gt/lt, so a
    context value of `0.62` matches an expected string `"0.62"` and
    `gt "0.5"` — everything the dashboard sends arrives as text.
    """
    if operator not in OPERATORS:
        raise ValueError(f"Unknown condition operator '{operator}' (expected one of {', '.join(OPERATORS)})")

    if operator == "truthy":
        return bool(actual)
    if operator == "falsy":
        return not bool(actual)

    if operator == "contains":
        if isinstance(actual, (list, tuple, set, dict)):
            return expected in actual
        if actual is None:
            return False
        return str(expected) in str(actual)

    actual_num, expected_num = _as_number(actual), _as_number(expected)
    both_numeric = actual_num is not None and expected_num is not None

    if operator == "gt":
        return both_numeric and actual_num > expected_num
    if operator == "lt":
        return both_numeric and actual_num < expected_num

    if both_numeric:
        matched = actual_num == expected_num
    else:
        matched = str(actual) == str(expected) if actual is not None else expected in (None, "")
    return matched if operator == "equals" else not matched
