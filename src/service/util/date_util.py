from datetime import datetime


def parse_indian_date_format(date_str: str) -> datetime:
    """
    Strict Indian-first date parser.

    Supported formats:
        18-02-2025
        18/02/2025
        18/02/25
        18 Feb 2025
        18 February 2025
        Sep 11 2026
        September 11 2026
        18-02-2025 09:19 AM
        18/02/2025 09:19 AM
        2025-01-23
        2025-01-23 09:19:00
        2025/01/23
        23.01.2025

    Raises:
        ValueError if format is not explicitly supported.
    """

    if not isinstance(date_str, str):
        raise TypeError("date_str must be a string")

    date_str = date_str.strip()

    if not date_str:
        raise ValueError("Empty date string")

    SUPPORTED_FORMATS = (
        "%d-%m-%Y",  # 18-02-2025
        "%d/%m/%Y",  # 18/02/2025
        "%d/%m/%y",  # 18/02/25
        "%d %b %Y",  # 18 Feb 2025
        "%d-%b-%Y",  # 18-Feb-2025
        "%d %B %Y",  # 18 February 2025
        "%b %d %Y",  # Sep 11 2026
        "%B %d %Y",  # September 11 2026
        "%d-%m-%Y %I:%M %p",  # 18-02-2025 09:19 AM
        "%d/%m/%Y %I:%M %p",  # 18/02/2025 09:19 AM
        "%Y-%m-%d",  # 2025-01-23
        "%Y-%m-%d %H:%M:%S",  # 2025-01-23 09:19:00
        "%Y/%m/%d",  # 2025/01/23
        "%d.%m.%Y",  # 23.01.2025
    )

    for fmt in SUPPORTED_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    raise ValueError(
        f"Unsupported date format: '{date_str}'. "
        "Add the explicit format if this is expected input."
    )
