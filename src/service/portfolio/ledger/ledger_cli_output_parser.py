import subprocess

from src.service.util.number_util import is_not_number


def get_ledger_cli_output_by_config(
    config, ledger_files, commodity=None, command_type="balance"
):
    cmd = config["command"].split()

    begin_date = config.get("begin_date")
    if begin_date:
        cmd.extend(["--begin", begin_date])

    end_date = config.get("end_date")
    if end_date:
        cmd.extend(["--end", end_date])

    for path in ledger_files:
        cmd.extend(["-f", str(path)])

    if commodity:
        cmd.extend(["--limit", f'commodity=="{commodity}"'])

    output = run_ledger_cli_command(cmd)

    if command_type.lower() == "commodities":
        return parse_ledger_cli_commodities_output(output)
    if command_type.lower() == "register":
        return parse_ledger_cli_register_output(output)
    if command_type.lower() == "gsec_register":
        return parse_ledger_cli_gsec_register_output(output)
    else:
        all_accounts = parse_ledger_cli_balance_output(output)

        accounts = [p["account"] for p in all_accounts]
        account_set = set(accounts)

        filter_not_account = config.get("filter_not", [])

        leaf_accounts = []
        for p in all_accounts:
            prefix = p["account"] + ":"
            if (
                not any(a.startswith(prefix) for a in account_set)
                and p["account"] not in filter_not_account
            ):
                leaf_accounts.append(p)

        return leaf_accounts


def run_ledger_cli_command(cmd):
    print()
    print(" ".join(cmd))
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=False,
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr)

    return result.stdout.strip()


def parse_ledger_cli_commodities_output(output):
    parsed_lines = []
    lines = output.splitlines()
    for line in lines:
        line = line.strip().replace('"', "")
        if not line:
            continue
        parsed_lines.append(line)
    return parsed_lines


def parse_ledger_cli_gsec_register_output(output):
    parsed_lines = []
    lines = output.splitlines()
    current_date = None
    for line in lines:
        line = line.strip()
        if not line:
            continue

        arr = line.split("|")
        arr_filtered = [x for x in arr if x]

        current_date = arr_filtered[0]
        quantity = float(arr_filtered[-2].split(" ")[0].replace(",", ""))
        amount = float(arr_filtered[-1].split(" ")[0].replace(",", ""))

        if current_date:
            parsed_lines.append(
                {"date": current_date, "quantity": quantity, "amount": amount}
            )
    return parsed_lines


def parse_ledger_cli_register_output(output):
    parsed_lines = []
    lines = output.splitlines()
    current_date = None
    for line in lines:
        line = line.strip()
        if not line:
            continue
        arr = line.split(" ")
        arr_filtered = [x for x in arr if x]

        if len(arr_filtered) > 0 and "-" in arr_filtered[0]:
            current_date = arr_filtered[0]

        if is_not_number(arr_filtered[-4]):
            amount_index = -3
        else:
            amount_index = -4

        if current_date:
            parsed_lines.append(
                {
                    "date": current_date,
                    "amount": float(arr_filtered[amount_index].replace(",", "")),
                }
            )
    return parsed_lines


def parse_ledger_cli_balance_output(output):
    parsed_lines = []
    stack = []
    lines = output.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("-") or line.startswith("0"):
            continue

        arr = line.split(" ")
        arr_filtered = [x for x in arr if x]
        if len(arr_filtered) < 3:
            continue

        amount = float(arr_filtered[0].replace(",", ""))
        currency = arr_filtered[1]
        account_name = " ".join(arr_filtered[2:])

        level = max(0, len(arr) - 2 - (len(arr_filtered) - 1)) // 2

        if level < len(stack):
            stack = stack[:level]

        if level == len(stack):
            stack.append(account_name)
        else:
            stack[level] = account_name

        full_account = ":".join(stack)

        parsed_lines.append(
            {
                "currency": currency,
                "amount": amount,
                "account": full_account,
                "level": level,
            }
        )

    return parsed_lines
