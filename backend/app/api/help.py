from typing import Dict

from fastapi import APIRouter

from actus.help_text import HELP_TEXT

router = APIRouter()


@router.get("/api/help")
def help_text() -> Dict[str, str]:
    return {"text": HELP_TEXT}
