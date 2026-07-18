from engine.templating import interpolate_template_fields, render_template


def test_render_template_substitutes_known_names():
    assert render_template("{a} and {b}", {"a": 1, "b": "two"}.get) == "1 and two"


def test_render_template_leaves_unknown_names_literal():
    assert render_template("Hello {missing}!", {}.get) == "Hello {missing}!"


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
