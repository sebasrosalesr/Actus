from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import APIRouter, Depends, Query

from app.api.security import require_api_key
from app.quality.store import quality_summary, quality_trends


router = APIRouter(dependencies=[Depends(require_api_key)])


def _release_tag_default() -> str | None:
    raw = os.environ.get("ACTUS_RELEASE_TAG", "").strip()
    return raw or None


@router.get("/api/quality/summary")
def api_quality_summary(
    window: str = Query(default="28d"),
    release_tag: str | None = Query(default=None),
) -> Dict[str, Any]:
    return quality_summary(
        window=window,
        release_tag=release_tag or _release_tag_default(),
    )


@router.get("/api/quality/trends")
def api_quality_trends(
    window: str = Query(default="12w"),
    group_by: str = Query(default="week"),
    release_tag: str | None = Query(default=None),
) -> Dict[str, Any]:
    return quality_trends(
        window=window,
        group_by=group_by,
        release_tag=release_tag or _release_tag_default(),
    )
