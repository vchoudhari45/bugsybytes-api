import subprocess


def get_ledger_cli_output(cmd, ledger_files):
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


def parse_ledger_cli_output(output):
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

        currency = arr_filtered[0]
        amount = float(arr_filtered[1].replace(",", ""))
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
