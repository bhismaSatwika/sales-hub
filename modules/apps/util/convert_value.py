import datetime as dt
from decimal import Decimal


def convert_value(value):
    if value is None:
        return ""
    elif isinstance(value, dt.date):
        return value.strftime("%d-%m-%Y")
    elif isinstance(value, int):
        return "{:,}".format(value).replace(",", ".")
    elif isinstance(value, float):
        formatted = (
            "{:,.2f}".format(value)
            .replace(",", " ")
            .replace(".", ",")
            .replace(" ", ".")
        )
        if formatted.endswith(",00"):
            formatted = formatted[:-3]

        return formatted
    elif isinstance(value, Decimal):
        value = float(value)

        formatted = (
            "{:,.2f}".format(value)
            .replace(",", " ")
            .replace(".", ",")
            .replace(" ", ".")
        )
        if formatted.endswith(",00"):
            formatted = formatted[:-3]

        return formatted

    return value
