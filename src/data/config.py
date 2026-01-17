import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

# Gsec Configs
NSE_GSEC_LIVE_DATA_DIR = BASE_DIR / "gsec/nse_live_data/"
GSEC_MATURITY_DATE_OVERRIDE_FILE = (
    BASE_DIR / "gsec/lookup/gsec_maturity_date_override_file.csv"
)
GSEC_PORTFOLIO_FILE = BASE_DIR / "gsec/lookup/gsec_portfolio.csv"
GSEC_PORTFOLIO_CASHFLOW_FILE = BASE_DIR / "gsec/lookup/gsec_portfolio_cashflow.csv"
GSEC_PORTFOLIO_CASHFLOW_BY_YEAR_FILE = (
    BASE_DIR / "gsec/lookup/gsec_portfolio_cashflow_by_year.csv"
)

# Nifty50 Configs
NSE_NIFTY50_PRE_MARKET_DATA_DIR = BASE_DIR / "nifty_50/nse_pre_market_data/"
NSE_NIFTY50_UPSTOX_HOLDINGS_FILE = BASE_DIR / "nifty_50/ub/holdings.csv"
NIFTY50_NSE_PORTFOLIO_FILE = BASE_DIR / "nifty_50/portfolio.csv"

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")

QUANTITY_LAG_DAYS = 2
DEFAULT_TARGET_XIRR = 0.0801

# Portfolio
LEDGER_PRICE_DB_DIR = BASE_DIR / "portfolio/common/prices"
LEDGER_ME_DIR = BASE_DIR / "portfolio/ledger-me/"
LEDGER_MOM_DIR = BASE_DIR / "portfolio/ledger-mom/"
LEDGER_PAPA_DIR = BASE_DIR / "portfolio/ledger-papa/"
TRANSACTION_ME_DIR = BASE_DIR / "portfolio/transactions-me/"
TRANSACTION_MOM_DIR = BASE_DIR / "portfolio/transactions-mom/"
TRANSACTION_PAPA_DIR = BASE_DIR / "portfolio/transactions-papa/"

LEDGER_ME_MAIN = BASE_DIR / "portfolio/ledger-me/main.ledger"
LEDGER_MOM_MAIN = BASE_DIR / "portfolio/ledger-mom/main.ledger"
LEDGER_PAPA_MAIN = BASE_DIR / "portfolio/ledger-papa/main.ledger"

# Account
LEDGER_ACCOUNT_LIST = BASE_DIR / "portfolio/common/accounts.db"

# Portfolio Report
PORTFOLIO_RECON_REPORT = BASE_DIR / "portfolio/portfolio_recon_report.xlsx"
PORTFOLIO_EXPECTED_BALANCES = BASE_DIR / "portfolio/expected_balances.json"

# Terminal Codes
RED_BOLD = "\033[1;91m"
RESET = "\033[0m"
