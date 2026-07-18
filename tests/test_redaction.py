from engine.redaction import redact_secrets


def test_redacts_top_level_secret_shaped_keys():
    result = redact_secrets({"api_key": "sk-abcd1234", "username": "alice"})
    assert result == {"api_key": "***REDACTED***", "username": "alice"}


def test_redacts_case_insensitively_and_common_variants():
    result = redact_secrets(
        {
            "Password": "hunter2",
            "AUTH_TOKEN": "xyz",
            "access-key": "abc",
            "private_key": "-----BEGIN-----",
            "credential": "shh",
        }
    )
    assert all(value == "***REDACTED***" for value in result.values())


def test_recurses_into_nested_dicts_and_lists():
    result = redact_secrets(
        {
            "insight": {"api_key": "secret-value", "risk_level": "high"},
            "records": [{"token": "t1"}, {"token": "t2"}, {"name": "ok"}],
        }
    )
    assert result["insight"] == {"api_key": "***REDACTED***", "risk_level": "high"}
    assert result["records"] == [{"token": "***REDACTED***"}, {"token": "***REDACTED***"}, {"name": "ok"}]


def test_leaves_non_secret_keys_and_non_dict_values_untouched():
    assert redact_secrets({"revenue": 4210.5, "signups": 128}) == {"revenue": 4210.5, "signups": 128}
    assert redact_secrets("just a string") == "just a string"
    assert redact_secrets(42) == 42
    assert redact_secrets(None) is None
