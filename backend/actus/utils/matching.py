def normalize(value) -> str:
    """
    Normalize string for matching:
      - convert to string
      - strip spaces
      - uppercase
    """
    if value is None:
        return ""
    return str(value).strip().upper()
