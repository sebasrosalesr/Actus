from typing import Dict

from fastapi import APIRouter, Depends

from actus.help_text import HELP_TEXT
from app.api.security import require_internal_request

router = APIRouter(dependencies=[Depends(require_internal_request)])


@router.get("/api/help")
def help_text() -> Dict[str, str]:
    return {"text": HELP_TEXT}
