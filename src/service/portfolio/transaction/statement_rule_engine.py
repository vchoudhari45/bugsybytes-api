import sys

import yaml

from src.data.config import STATEMENTS_INGESTOR_RULES_LOOKUP


def debug_rule_match(context, rule):
    print(
        "\nRule matched\n"
        "------------\n"
        f"Context:\n"
        f"  statement_type : {context.get('statement_type')}\n"
        f"  who            : {context.get('who')}\n"
        f"  remark         : {context.get('remark')}\n"
        f"  net_amount     : {context.get('net_amount')}\n"
        "\n"
        f"When conditions:\n"
        f"  {rule.get('when', {})}\n"
        "\n"
        f"Set values:\n"
        f"  {rule.get('set', {})}\n"
    )


def conditions_match(conditions, context):
    for key, value in conditions.items():
        if key == "statement_type":
            if context["statement_type"] != value:
                return False

        elif key == "who":
            if context["who"] != value:
                return False

        elif key == "remark_contains":
            if isinstance(value, list):
                for v in value:
                    if v not in context["remark"]:
                        return False
            else:
                if value not in context["remark"]:
                    return False

        elif key == "net_amount_gt":
            if context["net_amount"] <= value:
                return False

        elif key == "net_amount_lt":
            if context["net_amount"] >= value:
                return False

        else:
            raise ValueError(f"Unknown condition: {key}")

    return True


def freeze(obj):
    if isinstance(obj, dict):
        return tuple(sorted((k, freeze(v)) for k, v in obj.items()))
    if isinstance(obj, list):
        return tuple(freeze(v) for v in obj)
    return obj


def validate_rules(rules):
    seen = {}

    for idx, rule in enumerate(rules):
        key = (freeze(rule.get("when", {})),)

        if key in seen:
            print("❌ Duplicate rule detected!\n")
            print("First occurrence:")
            print(seen[key])
            print("\nDuplicate occurrence:")
            print(rule)
            sys.exit(1)

        seen[key] = rule


# load & validate rules
with open(STATEMENTS_INGESTOR_RULES_LOOKUP, "r", encoding="utf-8") as f:
    RULES = yaml.safe_load(f)["rules"]

validate_rules(RULES)


def apply_rules(context):
    result = {}
    for rule in RULES:
        if conditions_match(rule.get("when", {}), context):
            # debug_rule_match()
            result.update(rule.get("set", {}))
            break
    return result


def create_transaction(
    statement_type, who, date, remark, from_value, to_value, net_amount
):
    context = {
        "statement_type": statement_type.lower(),
        "who": who.lower(),
        "remark": remark.lower(),
        "net_amount": net_amount,
    }
    rule_output = apply_rules(context)

    from_account = rule_output.get("from_account", "")
    to_account = rule_output.get("to_account", "")
    adjustment_account = rule_output.get("adjustment_account", "")
    adjustment_value = rule_output.get("adjustment_value", "")
    lot_selection_method = rule_output.get("lot_selection_method", "")

    return {
        "DATE(YYYY-MM-DD)": date,
        "TRANSACTION_REMARK": remark,
        "FROM_ACCOUNT": from_account,
        "FROM_VALUE": from_value,
        "TO_ACCOUNT": to_account,
        "TO_VALUE": to_value,
        "ADJUSTMENT_ACCOUNT": adjustment_account,
        "ADJUSTMENT_VALUE": adjustment_value,
        "LOT_SELECTION_METHOD": lot_selection_method,
    }
