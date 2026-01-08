# BugsyBytes API

BugsyBytes is a minimal blogging platform that uses MDX files for posts, along with finance calculators and small utility tools. The BugsyBytes API is a FastAPI-based backend that powers the platform’s core services.

## Copy Files From
[NSE Bonds](https://www.nseindia.com/market-data/bonds-traded-in-capital-market)<br />
[Nifty 50 Index Weightage](https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market)

## Important Links
[NSE Index Reconstitution Calendar](https://www.niftyindices.com/resources/index-rebalancing-schedule)<br />
[Nifty 50 Index Factsheet](https://niftyindices.com/reports/index-factsheet)<br />
[Nifty 50 Index Weightage from TickerTape](https://www.tickertape.in/indices/nifty-50-index-.NSEI)<br />
[Nifty 50 PE Scanner](https://www.screener.in/company/NIFTY/)

## Running FastAPI Backend

Create a virtual environment

```bash
rm -rf .venv/
python3 -m venv .venv
```

Activate the virtual environment

```bash
source .venv/bin/activate
```

Install dependencies

```bash
pip install -r requirements.txt
```

Install dev dependencies

```bash
pip install -r requirements-dev.txt
```

Start the FastAPI development server

```bash
uvicorn src.main:app --reload

# GSEC Trackers
# export UPSTOX_ACCESS_TOKEN
python -m src.service.gsec.market_feed_tracker --target_xirr=0.0801
python -m src.service.gsec.portfolio_tracker

# Nifty 50 Trackers
python -m src.service.nifty50.portfolio_tracker
```

Run tox to lint code, execute pytest tests, and generate/validate tests with schemathesis

```bash
tox
```

## Setting up git-crypt

Install git-crypt

```bash
brew install git-crypt
```

Init git repo

```bash
cd bugsybytes-api
git init
```

Initialize git-crypt with Symmetric Key

```bash
git-crypt init
```

Export and Securely Store the Key

```bash
git-crypt export-key ~/my-repo-key
```

Convert to base64 for password manager storage (safe text format):

```bash
base64 ~/my-repo-key
# Copy the output (e.g., a long string like abc123def...==).
# Paste it into a new entry in your password manager
# Delete the local ~/my-repo-key
```

For future unlocks, retrieve it like below

```bash
echo 'replace_this_base64_output_stored_in_your_password_manager' | base64 -d > ~/temp-key
git-crypt unlock ~/temp-key
rm ~/temp-key
```

Define Encryption Patterns in .gitattributes

```bash
touch .gitattributes

# add following content to it

# Encrypt all .env files
.env filter=git-crypt diff=git-crypt
.csv filter=git-crypt diff=git-crypt

# Encrypt entire folders (recursive)
src/data/** filter=git-crypt diff=git-crypt

# Encrypt all .csv files from folder (recursive)
src/data/**/*.csv filter=git-crypt diff=git-crypt

# Exclude .git files (safety)
.gitattributes !filter !diff
```
