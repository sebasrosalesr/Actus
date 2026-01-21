from __future__ import annotations

import os


def ensure_openmp_env() -> None:
    """Set OpenMP env vars to avoid runtime conflicts in ML deps."""
    os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
