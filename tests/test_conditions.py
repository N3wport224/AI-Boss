import pytest

from engine.conditions import OPERATORS, evaluate_condition


def test_equals_compares_numeric_looking_strings_as_numbers():
    assert evaluate_condition(0.62, "equals", "0.62")
    assert evaluate_condition("14", "equals", 14)
    assert not evaluate_condition(0.62, "equals", "0.63")


def test_equals_falls_back_to_string_comparison():
    assert evaluate_condition("high", "equals", "high")
    assert not evaluate_condition("high", "equals", "low")


def test_not_equals_is_the_negation_of_equals():
    assert evaluate_condition("high", "not_equals", "low")
    assert not evaluate_condition("14", "not_equals", 14)


def test_missing_value_is_not_equal_to_a_real_expected_value():
    assert not evaluate_condition(None, "equals", "high")
    assert evaluate_condition(None, "not_equals", "high")
    assert evaluate_condition(None, "equals", "")


def test_gt_and_lt_require_numbers_and_never_raise():
    assert evaluate_condition(0.7, "gt", "0.5")
    assert evaluate_condition("3", "lt", 10)
    assert not evaluate_condition("not a number", "gt", 5)
    assert not evaluate_condition(None, "lt", 5)


def test_contains_covers_strings_and_collections():
    assert evaluate_condition("high risk detected", "contains", "risk")
    assert evaluate_condition(["a", "b"], "contains", "a")
    assert evaluate_condition({"risk_level": "high"}, "contains", "risk_level")
    assert not evaluate_condition(None, "contains", "risk")


def test_truthy_and_falsy_ignore_expected():
    assert evaluate_condition("anything", "truthy")
    assert not evaluate_condition("", "truthy")
    assert evaluate_condition(0, "falsy")
    assert not evaluate_condition([1], "falsy")


def test_unknown_operator_raises_at_evaluation():
    with pytest.raises(ValueError):
        evaluate_condition("x", "matches_regex", "y")


def test_operator_list_is_the_public_contract():
    assert set(OPERATORS) == {"equals", "not_equals", "contains", "gt", "lt", "truthy", "falsy"}
