import pandas as pd
import pytest

from src.service.util import validations as v


@pytest.mark.parametrize(
    "provided,derived,should_exit",
    [
        (None, None, False),  # both missing → OK
        (pd.NA, pd.NA, False),  # both missing (pandas NA) → OK
        (7.5, 7.5, False),  # both present, match → OK
        (7.5, 7.51, True),  # both present, mismatch → FAIL
        (None, 7.5, True),  # only one missing → FAIL
        (7.5, None, True),  # only one missing → FAIL
    ],
)
def test_validate_coupon_rate_match(monkeypatch, provided, derived, should_exit):
    # Patch sys.exit to prevent actual exit
    exit_called = {}

    def fake_exit(code):
        exit_called["code"] = code
        raise SystemExit(code)

    monkeypatch.setattr("sys.exit", fake_exit)

    if should_exit:
        with pytest.raises(SystemExit):
            v.validate_coupon_rate_match("TESTSYM", provided, derived, "ISIN123")
        assert exit_called["code"] == 1
    else:
        v.validate_coupon_rate_match("TESTSYM", provided, derived, "ISIN123")


@pytest.mark.parametrize(
    "provided,derived,should_exit",
    [
        (None, None, False),  # both missing → OK
        (pd.NaT, pd.NaT, False),  # both missing → OK
        ("2026-03-20", "2026-12-31", False),  # both present, same year → OK
        ("2026-03-20", "2027-03-20", True),  # year mismatch → FAIL
        (None, "2026-03-20", True),  # only one missing → FAIL
        ("2026-03-20", None, True),  # only one missing → FAIL
    ],
)
def test_validate_maturity_year_consistency(
    monkeypatch, provided, derived, should_exit
):
    # Patch sys.exit
    exit_called = {}

    def fake_exit(code):
        exit_called["code"] = code
        raise SystemExit(code)

    monkeypatch.setattr("sys.exit", fake_exit)

    if should_exit:
        with pytest.raises(SystemExit):
            v.validate_maturity_year_consistency(
                "TESTSYM", provided, derived, "ISIN123"
            )
        assert exit_called["code"] == 1
    else:
        v.validate_maturity_year_consistency("TESTSYM", provided, derived, "ISIN123")
