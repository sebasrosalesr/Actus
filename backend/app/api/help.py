from typing import Dict

from fastapi import APIRouter, Depends

from actus.help_text import HELP_TEXT
from app.api.security import require_api_key

router = APIRouter(dependencies=[Depends(require_api_key)])


@router.get("/api/help")
def help_text() -> Dict[str, str]:
    return {"text": HELP_TEXT}
