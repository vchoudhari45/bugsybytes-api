from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

from src.data.config import QUANTITY_LAG_DAYS
from src.service.util.date_util import to_date
from src.service.util.holiday_calculator import next_market_day


def market_shifted(dt: date):
    """Shift date to next market day"""
    return next_market_day(dt, lag_days=0)


def generate_coupon_dates(start_date, maturity_date, coupon_frequency):
    """
    Generate market-shifted coupon dates after start_date
    and before maturity_date
    """
    months_per_coupon = 12 // coupon_frequency

    coupon_month = maturity_date.month
    coupon_day = maturity_date.day
    year = start_date.year

    first_coupon_date = date(year, coupon_month, coupon_day)
    first_coupon_date -= relativedelta(months=months_per_coupon)
    while first_coupon_date <= start_date:
        first_coupon_date += relativedelta(months=months_per_coupon)

    coupon_dates = []
    coupon_date = first_coupon_date
    while coupon_date < maturity_date:
        coupon_dates.append(market_shifted(coupon_date))
        coupon_date += relativedelta(months=months_per_coupon)

    return coupon_dates


def empty_txn_slot():
    """Empty transaction slot"""
    return {"quantity": 0, "transaction_amount": 0}


def empty_coupon_slot():
    """Empty coupon slot"""
    return {"quantity": 0, "coupon_date": True}


def apply_coupon_and_principal(cf, coupon_rate, coupon_frequency, face_value):
    """
    Apply coupon + principal using running quantity
    """
    running_qty = 0
    last_date = next(reversed(cf))

    for dt, val in cf.items():
        running_qty += val.get("quantity", 0)
        val["quantity"] = running_qty

        if running_qty > 0 and val.get("coupon_date"):
            val["coupon_payment"] = (
                face_value * running_qty * (coupon_rate / 100) / coupon_frequency
            )

        if dt == last_date and running_qty > 0:
            val["principal_repayment"] = face_value * running_qty
            val["quantity"] = 0

        val["total_cashflow"] = (
            val.get("coupon_payment", 0)
            + val.get("principal_repayment", 0)
            + val.get("transaction_amount", 0)
        )


def build_gsec_cashflows(row):
    """
    Builds date-sorted cashflows for a G-Sec.
    Initial cashflow amount is 0 and must be replaced by price.
    """

    maturity_date = to_date(row["MATURITY DATE"])
    coupon_rate = row["COUPON RATE"] / 100
    coupon_frequency = 2
    face_value = 100

    trade_date = date.today()
    settlement_date = market_shifted(trade_date + timedelta(days=QUANTITY_LAG_DAYS))

    cf = {settlement_date: 0.0}

    coupon_dates = generate_coupon_dates(
        settlement_date, maturity_date, coupon_frequency
    )

    coupon_amount = face_value * coupon_rate / coupon_frequency

    for d in coupon_dates:
        cf[d] = cf.get(d, 0) + coupon_amount

    shifted_maturity = market_shifted(maturity_date)
    cf[shifted_maturity] = cf.get(shifted_maturity, 0) + coupon_amount + face_value

    dates = sorted(cf.keys())
    cashflows = [cf[d] for d in dates]

    return dates, cashflows
