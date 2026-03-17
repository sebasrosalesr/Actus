from __future__ import annotations
import warnings

import numpy as np

_model = None

def embed_texts(texts: list[str]) -> np.ndarray:
    global _model
    if _model is None:
        warnings.filterwarnings(
            "ignore",
            message="`resume_download` is deprecated.*",
            category=FutureWarning,
            module="huggingface_hub.file_download",
        )

        # Compatibility shim:
        # Some sentence-transformers releases import
        # `is_torch_npu_available` from transformers top-level.
        # If the installed transformers build does not expose it,
        # provide a no-op fallback so import can proceed.
        try:
            import transformers  # type: ignore

            if not hasattr(transformers, "is_torch_npu_available"):
                setattr(transformers, "is_torch_npu_available", lambda: False)
        except Exception:
            pass

        from sentence_transformers import SentenceTransformer

        _model = SentenceTransformer("all-MiniLM-L6-v2")
    vecs = _model.encode(texts, show_progress_bar=False)
    return np.asarray(vecs, dtype=np.float32)
