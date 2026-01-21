#take this script into considerations when adding the NLP 
#model with open router

from typing import Optional, Tuple, Dict
import pandas as pd

def normalize_result(
    intent_name: str,
    result,
    *,
    mode: str = "rules",
    confidence: float = 1.0,
) -> Tuple[str, Optional[pd.DataFrame], Dict[str, object]]:
    """
    Normalize any intent output into:
    (text, df_or_none, meta)

    meta is always present and consistent.
    """

    meta: Dict[str, object] = {
        "intent": intent_name,
        "mode": mode,
        "confidence": confidence,
    }

    # Intent returned only text
    if isinstance(result, str):
        return result, None, meta

    # Intent returned (text, df)
    if isinstance(result, tuple) and len(result) == 2:
        text, df = result
        return text, df, meta

    # Intent returned (text, df, meta_override)
    if isinstance(result, tuple) and len(result) == 3:
        text, df, meta_override = result
        if isinstance(meta_override, dict):
            meta.update(meta_override)
        return text, df, meta

    raise ValueError(f"Invalid return format from intent '{intent_name}'")