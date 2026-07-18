from engine.templating import interpolate_template_fields, render_template, resolve_path


def test_render_template_substitutes_known_names():
    assert render_template("{a} and {b}", {"a": 1, "b": "two"}.get) == "1 and two"


def test_render_template_leaves_unknown_names_literal():
    assert render_template("Hello {missing}!", {}.get) == "Hello {missing}!"


def test_render_template_resolves_a_dotted_nested_path():
    lookup = {"insight": {"risk_level": "high", "churn_rate": 0.9}}.get
    assert render_template("Risk: {insight.risk_level}", lookup) == "Risk: high"


def test_render_template_leaves_a_bad_nested_path_literal():
    lookup = {"insight": {"risk_level": "high"}}.get
    assert render_template("{insight.no_such_key}", lookup) == "{insight.no_such_key}"
    assert render_template("{no_such_base.anything}", lookup) == "{no_such_base.anything}"


def test_resolve_path_walks_nested_dicts():
    lookup = {"a": {"b": {"c": 42}}}.get
    assert resolve_path("a", lookup) == {"b": {"c": 42}}
    assert resolve_path("a.b", lookup) == {"c": 42}
    assert resolve_path("a.b.c", lookup) == 42


def test_resolve_path_returns_none_past_a_non_dict_value():
    lookup = {"a": "just a string"}.get
    assert resolve_path("a.b", lookup) is None


def test_interpolate_template_fields_only_touches_template_type_fields():
    manifest = {
        "inputs": [
            {"name": "note", "type": "template"},
            {"name": "count", "type": "number"},
        ]
    }
    values = {"note": "Signups: {signups}", "count": "{signups}"}  # count is NOT a template field
    result = interpolate_template_fields(manifest, values, {"signups": 42}.get)

    assert result["note"] == "Signups: 42"
    assert result["count"] == "{signups}"  # left untouched — not a template-type field
