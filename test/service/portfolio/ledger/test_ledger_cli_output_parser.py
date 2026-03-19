from unittest.mock import MagicMock, patch

import pytest

from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
    parse_ledger_cli_balance_output,
    parse_ledger_cli_commodities_output,
    parse_ledger_cli_gsec_register_output,
    parse_ledger_cli_register_output,
    run_ledger_cli_command,
)


@pytest.mark.parametrize(
    "command_type, cli_output, expected",
    [
        # Commodities
        ("commodities", "Gold\nSilver\nINR\nUSD", ["Gold", "Silver"]),
        # Register
        (
            "register",
            "2026-03-18 | Assets | 100 units | 1000 INR",
            [
                {
                    "date": "2026-03-18",
                    "account": "Assets",
                    "quantity": 100.0,
                    "amount": 1000.0,
                }
            ],
        ),
        # GSEC register
        (
            "gsec_register",
            "2026-03-18 | GSEC XYZ | 100 units | 1000 INR",
            [{"date": "2026-03-18", "quantity": 100.0, "amount": 1000.0}],
        ),
        # Balance
        (
            "balance",
            "1000 INR Assets",
            [{"currency": "INR", "amount": 1000.0, "account": "Assets", "level": 0}],
        ),
    ],
)
def test_get_ledger_cli_output_by_config(command_type, cli_output, expected):
    config = {"command": "dummy", "filter_not": [], "us_account": []}
    ledger_files = ["dummy.ledger"]

    with patch(
        "src.service.portfolio.ledger.ledger_cli_output_parser.run_ledger_cli_command"
    ) as mock_run:
        mock_run.return_value = cli_output
        result = get_ledger_cli_output_by_config(
            config, ledger_files, command_type=command_type
        )
        assert result == expected
        mock_run.assert_called_once()


@pytest.mark.parametrize(
    "mock_returncode, mock_stdout, mock_stderr, expected",
    [
        (0, "ok", "", "ok"),
        (1, "", "error", RuntimeError),
    ],
)
@patch("subprocess.run")
def test_run_ledger_cli_command(
    mock_run, mock_returncode, mock_stdout, mock_stderr, expected
):
    mock_run.return_value = MagicMock(
        returncode=mock_returncode,
        stdout=mock_stdout,
        stderr=mock_stderr,
    )
    if expected == RuntimeError:
        with pytest.raises(RuntimeError):
            run_ledger_cli_command(["ledger"])
    else:
        assert run_ledger_cli_command(["ledger"]) == expected


@pytest.mark.parametrize(
    "input_data, expected",
    [
        ('"ABC"\nINR\nUSD\nGOLD\n', ["ABC", "GOLD"]),
        ('"X"\n"Y"\n', ["X", "Y"]),
    ],
)
def test_parse_commodities(input_data, expected):
    output = parse_ledger_cli_commodities_output(input_data)
    assert output == expected


@pytest.mark.parametrize(
    "input_data, expected",
    [
        (
            "2024-01-01|Assets:Equity|10 ABC|1000 INR",
            [
                {
                    "date": "2024-01-01",
                    "account": "Assets:Equity",
                    "quantity": 10.0,
                    "amount": 1000.0,
                }
            ],
        ),
        (
            "2024-01-01|<Adjustment>|5 ABC|500 INR",
            [
                {
                    "date": "2024-01-01",
                    "account": "<Adjustment>",
                    "quantity": 0.0,
                    "amount": 500.0,
                }
            ],
        ),
    ],
)
def test_parse_register(input_data, expected):
    output = parse_ledger_cli_register_output(input_data)
    assert output == expected


@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        # Empty or separator lines
        ("---", []),
        # Single top-level account
        (
            "2000 USD Income",
            [
                {"currency": "USD", "amount": 2000.0, "account": "Income", "level": 0},
            ],
        ),
        # Top-level account and one sub-account
        (
            """
              1000 INR  Assets
                500 INR    Bank
            """,
            [
                {"currency": "INR", "amount": 1000.0, "account": "Assets", "level": 0},
                {
                    "currency": "INR",
                    "amount": 500.0,
                    "account": "Assets:Bank",
                    "level": 1,
                },
            ],
        ),
    ],
)
def test_parse_balance(input_data, expected_output):
    output = parse_ledger_cli_balance_output(input_data)
    print(output)
    assert output == expected_output


@pytest.mark.parametrize(
    "input_data, expected_output",
    [
        # Empty input
        ("", []),
        # Single line
        (
            "2026-03-18|Assets:GSec|1,000 XYZ| 10,500.50 INR",
            [{"date": "2026-03-18", "quantity": 1000.0, "amount": 10500.50}],
        ),
        # Multiple lines
        (
            """2026-03-18 | Assets:GSec | 1,000 XYZ | 10,500.50 INR
               2026-03-19 | Assets:GSec | 500 ABC | 5,250.00 INR""",
            [
                {"date": "2026-03-18", "quantity": 1000.0, "amount": 10500.50},
                {"date": "2026-03-19", "quantity": 500.0, "amount": 5250.0},
            ],
        ),
        # Line with extra spaces
        (
            " 2026-03-18 | Assets:GSec | 1,000 ABC | 10,500.50 INR ",
            [{"date": "2026-03-18", "quantity": 1000.0, "amount": 10500.50}],
        ),
    ],
)
def test_parse_gsec_register(input_data, expected_output):
    output = parse_ledger_cli_gsec_register_output(input_data)
    assert output == expected_output
