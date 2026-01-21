def format_money(x) -> str:
    """
    Format a numeric value as $1,234.56.
    If not convertible to float, return as string.
    """
    try:
        val = float(x)
    except (TypeError, ValueError):
        return str(x)
    return f"${val:,.2f}"
