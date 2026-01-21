from __future__ import annotations
import numpy as np
from sentence_transformers import SentenceTransformer

_model: SentenceTransformer | None = None

def embed_texts(texts: list[str]) -> np.ndarray:
    global _model
    if _model is None:
        _model = SentenceTransformer("all-MiniLM-L6-v2")
    vecs = _model.encode(texts, show_progress_bar=False)
    return np.asarray(vecs, dtype=np.float32)
