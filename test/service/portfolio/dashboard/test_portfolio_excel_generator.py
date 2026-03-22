from datetime import date
from unittest.mock import MagicMock

import pytest

from src.service.portfolio.dashboard.portfolio_excel_generator import (
    print_kpi_cards,
    print_table,
)


def make_layout():
    keys = [
        "kpi_label_fmt",
        "kpi_value_fmt_percent",
        "kpi_value_fmt_amount",
        "kpi_value_fmt_text",
        "header_fmt",
        "account_fmt",
        "percent_fmt",
        "amount_fmt",
        "date_fmt",
        "link_fmt",
        "title_fmt",
    ]
    return {k: MagicMock(name=k) for k in keys}


# print_kpi_cards
def test_print_kpi_cards_returns_integer_end_row():
    kpi_list = [{"KPI": "PORTFOLIO NAME", "VALUE": "My Portfolio"}]
    expected_type = int

    result = print_kpi_cards(MagicMock(), make_layout(), kpi_list, start_row=0)

    assert isinstance(result, expected_type)


def test_print_kpi_cards_empty_list_returns_start_row_plus_two():
    kpi_list = []
    start_row = 0
    expected = 2

    result = print_kpi_cards(MagicMock(), make_layout(), kpi_list, start_row=start_row)

    assert result == expected


def test_print_kpi_cards_single_card_calls_merge_range_twice():
    kpi_list = [{"KPI": "PORTFOLIO NAME", "VALUE": "My Portfolio"}]
    expected_call_count = 2

    worksheet = MagicMock()
    print_kpi_cards(worksheet, make_layout(), kpi_list, start_row=0)

    assert worksheet.merge_range.call_count == expected_call_count


def test_print_kpi_cards_amount_kpi_uses_amount_format():
    kpi_list = [{"KPI": "INVESTED AMOUNT", "VALUE": 500_000}]
    layout = make_layout()
    expected_fmt = layout["kpi_value_fmt_amount"]

    worksheet = MagicMock()
    print_kpi_cards(worksheet, layout, kpi_list, start_row=0)

    value_call = worksheet.merge_range.call_args_list[1]
    assert value_call.args[-1] == expected_fmt


def test_print_kpi_cards_percent_kpi_divides_value_by_100():
    kpi_list = [{"KPI": "XIRR", "VALUE": 15.5}]
    expected_display_value = 15.5 / 100

    worksheet = MagicMock()
    print_kpi_cards(worksheet, make_layout(), kpi_list, start_row=0)

    value_call = worksheet.merge_range.call_args_list[1]
    assert value_call.args[4] == pytest.approx(expected_display_value)


def test_print_kpi_cards_percent_kpi_uses_percent_format():
    kpi_list = [{"KPI": "XIRR", "VALUE": 15.5}]
    layout = make_layout()
    expected_fmt = layout["kpi_value_fmt_percent"]

    worksheet = MagicMock()
    print_kpi_cards(worksheet, layout, kpi_list, start_row=0)

    value_call = worksheet.merge_range.call_args_list[1]
    assert value_call.args[-1] == expected_fmt


def test_print_kpi_cards_text_kpi_uses_text_format():
    kpi_list = [{"KPI": "PORTFOLIO NAME", "VALUE": "Growth"}]
    layout = make_layout()
    expected_fmt = layout["kpi_value_fmt_text"]

    worksheet = MagicMock()
    print_kpi_cards(worksheet, layout, kpi_list, start_row=0)

    value_call = worksheet.merge_range.call_args_list[1]
    assert value_call.args[-1] == expected_fmt


def test_print_kpi_cards_numeric_non_named_field_uses_amount_format():
    kpi_list = [{"KPI": "SOME NUMERIC", "VALUE": 42}]
    layout = make_layout()
    expected_fmt = layout["kpi_value_fmt_amount"]

    worksheet = MagicMock()
    print_kpi_cards(worksheet, layout, kpi_list, start_row=0)

    value_call = worksheet.merge_range.call_args_list[1]
    assert value_call.args[-1] == expected_fmt


def test_print_kpi_cards_start_row_offset_is_respected():
    kpi_list = [{"KPI": "INVESTED AMOUNT", "VALUE": 100_000}]
    start_row = 5
    expected_row = 5

    worksheet = MagicMock()
    print_kpi_cards(worksheet, make_layout(), kpi_list, start_row=start_row)

    first_label_call = worksheet.merge_range.call_args_list[0]
    assert first_label_call.args[0] == expected_row


def test_print_kpi_cards_start_col_offset_is_respected():
    kpi_list = [{"KPI": "INVESTED AMOUNT", "VALUE": 100_000}]
    start_col = 3
    expected_col = 3

    worksheet = MagicMock()
    print_kpi_cards(
        worksheet, make_layout(), kpi_list, start_row=0, start_col=start_col
    )

    first_label_call = worksheet.merge_range.call_args_list[0]
    assert first_label_call.args[1] == expected_col


def test_print_kpi_cards_four_cards_all_on_same_row():
    kpi_list = [
        {"KPI": "INVESTED AMOUNT", "VALUE": 100_000},
        {"KPI": "XIRR", "VALUE": 12.0},
        {"KPI": "PORTFOLIO NAME", "VALUE": "Growth"},
        {"KPI": "INCOME", "VALUE": 50_000},
    ]
    cards_per_row = 4
    expected_label_row = 0

    worksheet = MagicMock()
    print_kpi_cards(
        worksheet, make_layout(), kpi_list, start_row=0, cards_per_row=cards_per_row
    )

    label_rows = [c.args[0] for c in worksheet.merge_range.call_args_list[::2]]
    assert all(r == expected_label_row for r in label_rows)


def test_print_kpi_cards_fifth_card_wraps_to_new_row():
    kpi_list = [
        {"KPI": "INVESTED AMOUNT", "VALUE": 100_000},
        {"KPI": "XIRR", "VALUE": 12.0},
        {"KPI": "PORTFOLIO NAME", "VALUE": "Growth"},
        {"KPI": "INCOME", "VALUE": 50_000},
        {"KPI": "DIVIDEND", "VALUE": 2_000},
    ]
    cards_per_row = 4
    first_card_row = 0

    worksheet = MagicMock()
    print_kpi_cards(
        worksheet, make_layout(), kpi_list, start_row=0, cards_per_row=cards_per_row
    )

    label_rows = [c.args[0] for c in worksheet.merge_range.call_args_list[::2]]
    assert label_rows[4] > first_card_row


def test_print_kpi_cards_label_text_is_written_correctly():
    kpi_list = [{"KPI": "PORTFOLIO NAME", "VALUE": "My Portfolio"}]
    expected_label = "PORTFOLIO NAME"

    worksheet = MagicMock()
    print_kpi_cards(worksheet, make_layout(), kpi_list, start_row=0)

    label_call = worksheet.merge_range.call_args_list[0]
    assert label_call.args[4] == expected_label


# print_table
def test_print_table_empty_data_returns_start_row_and_zero_width():
    data = []
    start_row, start_col = 0, 0
    expected = (0, 0)

    result = print_table(
        MagicMock(), MagicMock(), make_layout(), "Title", data, start_row, start_col
    )

    assert result == expected


def test_print_table_empty_data_does_not_write_to_worksheet():
    data = []

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), make_layout(), "Title", data, 0, 0)

    worksheet.write.assert_not_called()


def test_print_table_header_uses_header_fmt():
    data = [{"Name": "Alice", "Type": "Equity"}]
    layout = make_layout()
    expected_fmt = layout["header_fmt"]

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), layout, "Title", data, 0, 0)

    header_calls = worksheet.write.call_args_list[:2]
    assert all(c.args[-1] == expected_fmt for c in header_calls)


def test_print_table_amount_field_uses_amount_fmt():
    data = [{"INVESTED": 250_000}]
    layout = make_layout()
    expected_fmt = layout["amount_fmt"]

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), layout, "Title", data, 0, 0)

    amount_calls = [
        c for c in worksheet.write.call_args_list if c.args[-1] == expected_fmt
    ]
    assert len(amount_calls) == 1


def test_print_table_percent_field_multiplies_value_by_100():
    data = [{"XIRR": 0.12}]
    layout = make_layout()
    expected_written_value = pytest.approx(12.0)
    expected_fmt = layout["percent_fmt"]

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), layout, "Title", data, 0, 0)

    percent_writes = [
        c for c in worksheet.write.call_args_list if c.args[-1] == expected_fmt
    ]
    assert percent_writes[0].args[2] == expected_written_value


def test_print_table_percent_field_uses_percent_fmt():
    data = [{"XIRR": 0.12}]
    layout = make_layout()
    expected_fmt = layout["percent_fmt"]

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), layout, "Title", data, 0, 0)

    percent_calls = [
        c for c in worksheet.write.call_args_list if c.args[-1] == expected_fmt
    ]
    assert len(percent_calls) == 1


def test_print_table_link_field_calls_write_url():
    data = [{"NEWS LINK": "https://example.com/news"}]
    expected_url = "https://example.com/news"
    expected_string = "View"

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), make_layout(), "Title", data, 0, 0)

    worksheet.write_url.assert_called_once()
    url_call = worksheet.write_url.call_args
    assert url_call.args[2] == expected_url
    assert url_call.kwargs.get("string") == expected_string


def test_print_table_date_field_calls_write_datetime():
    data = [{"Purchase Date": date(2024, 1, 15)}]

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), make_layout(), "Title", data, 0, 0)

    worksheet.write_datetime.assert_called_once()


def test_print_table_none_value_writes_empty_string():
    data = [{"Name": None}]
    layout = make_layout()
    expected_value = ""
    expected_fmt = layout["account_fmt"]

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), layout, "Title", data, 0, 0)

    none_writes = [
        c
        for c in worksheet.write.call_args_list
        if c.args[2] == expected_value and c.args[-1] == expected_fmt
    ]
    assert len(none_writes) == 1


def test_print_table_width_equals_number_of_columns():
    data = [{"Col1": "A", "Col2": "B", "Col3": "C"}]
    expected_width = 3

    _, width = print_table(MagicMock(), MagicMock(), make_layout(), "Title", data, 0, 0)

    assert width == expected_width


def test_print_table_end_row_advances_correctly():
    data = [
        {"Name": "Stock A", "INVESTED": 100_000},
        {"Name": "Stock B", "INVESTED": 200_000},
    ]
    start_row = 0
    expected_end_row = start_row + 1 + len(data) + 1  # header + data rows + spacing

    end_row, _ = print_table(
        MagicMock(), MagicMock(), make_layout(), "Title", data, start_row, 0
    )

    assert end_row == expected_end_row


def test_print_table_start_row_offset_is_respected():
    data = [{"Name": "Alice"}]
    start_row = 5
    expected_header_row = 5

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), make_layout(), "Title", data, start_row, 0)

    first_write = worksheet.write.call_args_list[0]
    assert first_write.args[0] == expected_header_row


def test_print_table_start_col_offset_is_respected():
    data = [{"Name": "Alice"}]
    start_col = 4
    expected_header_col = 4

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), make_layout(), "Title", data, 0, start_col)

    first_write = worksheet.write.call_args_list[0]
    assert first_write.args[1] == expected_header_col


def test_print_table_none_start_col_defaults_to_zero():
    data = [{"Name": "Alice"}]
    start_col = None
    expected_col = 0

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), make_layout(), "Title", data, 0, start_col)

    first_write = worksheet.write.call_args_list[0]
    assert first_write.args[1] == expected_col


def test_print_table_none_start_row_defaults_to_zero():
    data = [{"Name": "Alice"}]
    start_row = None
    expected_row = 0

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), make_layout(), "Title", data, start_row, 0)

    first_write = worksheet.write.call_args_list[0]
    assert first_write.args[0] == expected_row


def test_print_table_text_value_uses_account_fmt():
    data = [{"Name": "Alice"}]
    layout = make_layout()
    expected_fmt = layout["account_fmt"]

    worksheet = MagicMock()
    print_table(worksheet, MagicMock(), layout, "Title", data, 0, 0)

    data_writes = worksheet.write.call_args_list[1:]  # skip header row
    account_fmt_calls = [c for c in data_writes if c.args[-1] == expected_fmt]
    assert len(account_fmt_calls) > 0
