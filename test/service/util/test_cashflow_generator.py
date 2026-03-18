from datetime import date
from unittest.mock import patch

from src.service.util import cashflow_generator as cg


def test_market_shifted():
    dt = date(2026, 3, 16)  # Monday
    shifted = cg.market_shifted(dt)
    assert shifted == dt

    dt = date(2026, 3, 14)  # Saturday
    shifted = cg.market_shifted(dt)
    assert shifted == date(2026, 3, 16)

    dt = date(2026, 3, 15)  # Sunday
    shifted = cg.market_shifted(dt)
    assert shifted == date(2026, 3, 16)


def test_generate_coupon_dates():
    start_date = date(2026, 3, 17)
    maturity_date = date(2027, 3, 20)
    coupon_frequency = 2
    coupons = cg.generate_coupon_dates(start_date, maturity_date, coupon_frequency)
    expected_coupons = [date(2026, 3, 20), date(2026, 9, 21)]
    assert coupons == expected_coupons


def test_empty_txn_slot():
    slot = cg.empty_txn_slot()
    assert slot == {"quantity": 0, "transaction_amount": 0}


def test_empty_coupon_slot():
    slot = cg.empty_coupon_slot()
    assert slot == {"quantity": 0, "coupon_date": True}


def test_apply_coupon_and_principal_simple():
    # input
    cf = {
        date(2026, 3, 17): {
            "quantity": 10,
            "coupon_date": False,
            "transaction_amount": -1000,
        },
        date(2026, 3, 20): {"quantity": 0, "coupon_date": True},
        date(2026, 3, 20): {"quantity": 0, "coupon_date": True},
        date(2026, 4, 17): {
            "quantity": -5,
            "coupon_date": False,
            "transaction_amount": 600,
        },
        date(2026, 9, 21): {"quantity": 0, "coupon_date": True},
        date(2027, 3, 22): {"quantity": 0, "coupon_date": True},
        date(2027, 9, 20): {"quantity": 0, "coupon_date": True},
        date(2028, 3, 20): {"quantity": 0, "coupon_date": True},
    }
    # function call
    cg.apply_coupon_and_principal(
        cf, coupon_rate=10, coupon_frequency=2, face_value=100
    )
    # expected_cf
    expected_cf = {
        date(2026, 3, 17): {
            "quantity": 10,
            "coupon_date": False,
            "transaction_amount": -1000,
            "total_cashflow": -1000,
        },
        date(2026, 3, 20): {
            "quantity": 10,
            "coupon_date": True,
            "coupon_payment": 50,
            "total_cashflow": 50,
        },
        date(2026, 3, 20): {
            "quantity": 10,
            "coupon_date": True,
            "coupon_payment": 50,
            "total_cashflow": 50,
        },
        date(2026, 4, 17): {
            "quantity": 5,
            "coupon_date": False,
            "transaction_amount": 600,
            "total_cashflow": 600,
        },
        date(2026, 9, 21): {
            "quantity": 5,
            "coupon_date": True,
            "coupon_payment": 25,
            "total_cashflow": 25,
        },
        date(2027, 3, 22): {
            "quantity": 5,
            "coupon_date": True,
            "coupon_payment": 25,
            "total_cashflow": 25,
        },
        date(2027, 9, 20): {
            "quantity": 5,
            "coupon_date": True,
            "coupon_payment": 25,
            "total_cashflow": 25,
        },
        date(2028, 3, 20): {
            "quantity": 0,
            "coupon_date": True,
            "coupon_payment": 25,
            "principal_repayment": 500,
            "total_cashflow": 525,
        },
    }
    # assert
    for dt, expected_val in expected_cf.items():
        for key, val in expected_val.items():
            assert (
                cf[dt][key] == val
            ), f"Mismatch on {dt} key {key}: expected {val}, got {cf[dt].get(key)}"


def test_build_gsec_cashflows():
    fixed_today = date(2026, 3, 17)

    maturity_str = "Mar 20 2028"
    coupon_rate = 10
    coupon_frequency = 2
    face_value = 100

    with patch("src.service.util.cashflow_generator.date") as mock_date:
        mock_date.today.return_value = fixed_today
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)

        dates, cashflows = cg.build_gsec_cashflows(
            maturity_date=maturity_str,
            coupon_rate=coupon_rate,
            coupon_frequency=coupon_frequency,
            face_value=face_value,
        )

    expected_dates = [
        date(2026, 3, 19),
        date(2026, 3, 20),
        date(2026, 9, 21),
        date(2027, 3, 22),
        date(2027, 9, 20),
        date(2028, 3, 20),
    ]

    expected_cashflows = [0.0, 5.0, 5.0, 5.0, 5.0, 105.0]

    assert dates == expected_dates
    assert cashflows == expected_cashflows
