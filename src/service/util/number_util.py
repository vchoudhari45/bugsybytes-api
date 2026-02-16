def is_not_number(value):
    try:
        value = value.replace(",", "")
        float(value)
        return False
    except (TypeError, ValueError):
        return True
