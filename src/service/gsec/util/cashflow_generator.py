import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from sortedcontainers import SortedDict

from src.data.config import QUANTITY_LAG_DAYS
from src.service.util.holiday_calculator import next_market_day
from src.service.util.xirr_calculator import forward_xirr, xirr


def to_date(val):
    """Normalize any date-like input to datetime.date"""
    if isinstance(val, pd.Timestamp):
        return val.date()
    if isinstance(val, date):
        return val
    return parse(str(val)).date()


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


def generate_cashflows(joined_df):
    xirr_per_instrument = {}
    forward_xirr_per_instrument = {}
    portfolio_investment = 0
    investment_by_symbol = {}

    symbol_to_cashflow_map = {}
    symbol_to_coupon_meta_map = {}
    for _, row in joined_df.iterrows():
        event_type = row["EVENT TYPE"].replace(" ", "").upper()
        symbol = row["SYMBOL"]

        event_date = to_date(row["EVENT DATE"])
        maturity_date = to_date(row["MATURITY DATE"])

        if event_date > maturity_date:
            print(f"Invalid event_date > maturity_date for {symbol}")
            sys.exit(1)

        if symbol not in symbol_to_coupon_meta_map:
            symbol_to_coupon_meta_map[symbol] = {
                "isin": row["ISIN"],
                "coupon_rate": float(row["COUPON RATE"]),
                "coupon_frequency": int(row["COUPON FREQUENCY"]),
                "face_value": float(row["FACE VALUE"]),
                "maturity_date": maturity_date,
            }

        cf = symbol_to_cashflow_map.setdefault(symbol, SortedDict())

        if event_date not in cf:
            cf[event_date] = empty_txn_slot()

        units = float(row["UNITS"])
        price = float(row["PRICE PER UNIT"])

        if event_type == "BUY":
            cf[event_date]["transaction_amount"] -= units * price

            qty_date = next_market_day(event_date, QUANTITY_LAG_DAYS)
            cf.setdefault(qty_date, empty_txn_slot())["quantity"] += units

            portfolio_investment += units * price
            investment_by_symbol[symbol] = investment_by_symbol.get(symbol, 0) + (
                units * price
            )

        elif event_type == "SELL":
            cf[event_date]["quantity"] -= units
            cf[event_date]["transaction_amount"] += units * price

            portfolio_investment -= units * price
            investment_by_symbol[symbol] = investment_by_symbol.get(symbol, 0) - (
                units * price
            )

        else:
            print(f"Invalid EVENT TYPE for {symbol}")
            sys.exit(1)

    # --------------------------------------------------------
    # ADD COUPONS & MATURITY
    # --------------------------------------------------------

    for symbol, cf in symbol_to_cashflow_map.items():
        first_date = min(cf.keys())

        if cf[first_date]["quantity"] < 0:
            print(f"Negative initial quantity for {symbol}")
            sys.exit(1)

        meta = symbol_to_coupon_meta_map[symbol]

        for d in generate_coupon_dates(
            first_date, meta["maturity_date"], meta["coupon_frequency"]
        ):
            cf.setdefault(d, empty_coupon_slot())

        cf.setdefault(
            market_shifted(meta["maturity_date"]),
            {"quantity": 0, "coupon_date": True, "maturity": True},
        )

        apply_coupon_and_principal(
            cf,
            meta["coupon_rate"],
            meta["coupon_frequency"],
            meta["face_value"],
        )

    # --------------------------------------------------------
    # BUILD CASHFLOW DATAFRAME
    # --------------------------------------------------------

    all_dates = sorted(
        {pd.to_datetime(dt) for cf in symbol_to_cashflow_map.values() for dt in cf}
    )

    cashflow_df = pd.DataFrame(index=all_dates)

    for symbol, cf in symbol_to_cashflow_map.items():
        series = pd.Series(
            {pd.to_datetime(dt): cf[dt].get("total_cashflow", 0) for dt in cf}
        )
        cashflow_df[symbol] = series.reindex(all_dates, fill_value=0)

    cashflow_df = cashflow_df.reset_index().rename(columns={"index": "DATE"})
    cashflow_df["TOTAL CASHFLOW"] = cashflow_df.drop(columns=["DATE"]).sum(axis=1)

    # --------------------------------------------------------
    # XIRR CALCULATIONS
    # --------------------------------------------------------

    for symbol in symbol_to_cashflow_map:
        try:
            xirr_per_instrument[symbol] = xirr(cashflow_df["DATE"], cashflow_df[symbol])
            forward_xirr_per_instrument[symbol] = forward_xirr(
                cashflow_df["DATE"],
                cashflow_df[symbol],
                investment_by_symbol[symbol],
            )
        except Exception:
            xirr_per_instrument[symbol] = np.nan
            forward_xirr_per_instrument[symbol] = np.nan

    cashflow_df.attrs["PORTFOLIO INVESTMENT"] = portfolio_investment
    cashflow_df.attrs["PORTFOLIO XIRR"] = xirr(
        cashflow_df["DATE"], cashflow_df["TOTAL CASHFLOW"]
    )
    cashflow_df.attrs["PORTFOLIO FORWARD XIRR"] = forward_xirr(
        cashflow_df["DATE"],
        cashflow_df["TOTAL CASHFLOW"],
        portfolio_investment,
    )

    cashflow_metadata_df = pd.DataFrame(
        {
            "SYMBOL": list(xirr_per_instrument.keys()),
            "ISIN": [symbol_to_coupon_meta_map[s]["isin"] for s in xirr_per_instrument],
            "INDIVIDUAL XIRR": [
                xirr_per_instrument[s] * 100 for s in xirr_per_instrument
            ],
            "INDIVIDUAL FORWARD XIRR": [
                forward_xirr_per_instrument[s] * 100 for s in xirr_per_instrument
            ],
            "INVESTMENT": [investment_by_symbol[s] for s in xirr_per_instrument],
        }
    )
    cashflow_metadata_df = cashflow_metadata_df.sort_values(
        by="INDIVIDUAL FORWARD XIRR", ascending=False
    )

    return cashflow_df, cashflow_metadata_df
