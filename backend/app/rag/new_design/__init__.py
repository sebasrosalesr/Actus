from __future__ import annotations

from typing import Any


def build_pipeline_artifacts(*args: Any, **kwargs: Any):
    from .index_build import build_pipeline_artifacts as _impl

    return _impl(*args, **kwargs)


def build_retrieval_chunks(*args: Any, **kwargs: Any):
    from .index_build import build_retrieval_chunks as _impl

    return _impl(*args, **kwargs)


def index_pipeline_artifacts(*args: Any, **kwargs: Any):
    from .index_build import index_pipeline_artifacts as _impl

    return _impl(*args, **kwargs)


def routed_hybrid_search_real(*args: Any, **kwargs: Any):
    from .retrieve import routed_hybrid_search_real as _impl

    return _impl(*args, **kwargs)


def search(*args: Any, **kwargs: Any):
    from .retrieve import search as _impl

    return _impl(*args, **kwargs)


def get_runtime_service(*args: Any, **kwargs: Any):
    from .service import get_runtime_service as _impl

    return _impl(*args, **kwargs)


__all__ = [
    "build_pipeline_artifacts",
    "build_retrieval_chunks",
    "index_pipeline_artifacts",
    "routed_hybrid_search_real",
    "search",
    "get_runtime_service",
]
