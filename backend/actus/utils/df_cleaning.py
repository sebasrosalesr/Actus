import pandas as pd

def coerce_date(series: pd.Series) -> pd.Series:
    """
    Safely convert a Series to timezone-naive pandas Timestamps.
    Any unparsable values become NaT.
    """
    s = pd.to_datetime(series, errors="coerce")
    try:
        s = s.dt.tz_localize(None)
    except Exception:
        # already tz-naive or not datetime-like
        pass
    return s
