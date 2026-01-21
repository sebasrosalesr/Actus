from __future__ import annotations

import json
from pathlib import Path

from dotenv import load_dotenv

from app.rag.store import get_rag_store


def _load_env() -> None:
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)


def main() -> None:
    _load_env()
    store = get_rag_store()
    try:
        payload = {
            "provider": store.provider_name(),
            "has_data": store.has_data(),
            "stats": store.stats(),
        }
        print(json.dumps(payload, indent=2))
    finally:
        try:
            store.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
