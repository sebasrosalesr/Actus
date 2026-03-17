from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
import warnings

import numpy as np

_MODEL_NAME = "all-MiniLM-L6-v2"
_model = None
_model_lock = Lock()
_warm_lock = Lock()
_model_warmed = False


def _bool_env(name: str) -> bool | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return None


def _sentence_transformer_kwargs() -> dict[str, object]:
    kwargs: dict[str, object] = {}
    cache_dir = os.environ.get("HF_HOME", "").strip()
    if cache_dir:
        kwargs["cache_folder"] = cache_dir

    local_only = _bool_env("ACTUS_HF_LOCAL_ONLY")
    if local_only is None and cache_dir and Path(cache_dir).exists():
        local_only = True
    if local_only is not None:
        kwargs["local_files_only"] = local_only

    return kwargs


def _load_model():
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

    kwargs = _sentence_transformer_kwargs()
    try:
        return SentenceTransformer(_MODEL_NAME, **kwargs)
    except Exception:
        if kwargs.get("local_files_only") is True:
            retry_kwargs = dict(kwargs)
            retry_kwargs["local_files_only"] = False
            return SentenceTransformer(_MODEL_NAME, **retry_kwargs)
        raise


def get_embedding_model():
    global _model
    if _model is None:
        with _model_lock:
            if _model is None:
                _model = _load_model()
    return _model


def preload_embedding_model() -> None:
    warm_embedding_model()


def warm_embedding_model() -> None:
    global _model_warmed
    if _model_warmed:
        return
    with _warm_lock:
        if _model_warmed:
            return
        model = get_embedding_model()
        model.encode(["warmup"], show_progress_bar=False)
        _model_warmed = True


def embed_texts(texts: list[str]) -> np.ndarray:
    global _model_warmed
    model = get_embedding_model()
    vecs = model.encode(texts, show_progress_bar=False)
    _model_warmed = True
    return np.asarray(vecs, dtype=np.float32)
