from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import firebase_admin
from dotenv import load_dotenv
from firebase_admin import credentials, db


def load_env() -> None:
    env_path = Path(__file__).resolve().parents[3] / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)


def _firebase_config_from_env() -> dict[str, Any]:
    firebase_json = os.environ.get("ACTUS_FIREBASE_JSON")
    firebase_path = os.environ.get("ACTUS_FIREBASE_PATH")

    if firebase_json:
        cfg = json.loads(firebase_json)
    elif firebase_path:
        with open(firebase_path, "r", encoding="utf-8") as handle:
            cfg = json.load(handle)
    else:
        raise RuntimeError("Missing Firebase credentials. Set ACTUS_FIREBASE_JSON or ACTUS_FIREBASE_PATH.")

    if "private_key" in cfg and "\\n" in str(cfg["private_key"]):
        cfg["private_key"] = str(cfg["private_key"]).replace("\\n", "\n")

    return cfg


def ensure_firebase_initialized() -> None:
    if firebase_admin._apps:
        return

    cfg = _firebase_config_from_env()
    database_url = os.environ.get("ACTUS_FIREBASE_URL", "https://creditapp-tm-default-rtdb.firebaseio.com/")
    cred = credentials.Certificate(cfg)
    firebase_admin.initialize_app(cred, {"databaseURL": database_url})


def fetch_node(node_name: str) -> dict[str, Any]:
    ensure_firebase_initialized()
    ref = db.reference(node_name)
    raw = ref.get() or {}
    return raw if isinstance(raw, dict) else {}


def load_credit_requests() -> list[dict[str, Any]]:
    raw = fetch_node("credit_requests")
    rows: list[dict[str, Any]] = []
    for value in raw.values():
        if isinstance(value, dict):
            rows.append(value)
    return rows


def load_investigation_notes() -> list[dict[str, Any]]:
    raw = fetch_node("investigation_notes")
    rows: list[dict[str, Any]] = []
    for note_id, value in raw.items():
        if not isinstance(value, dict):
            continue
        row = dict(value)
        row.setdefault("note_id", str(note_id))
        rows.append(row)
    return rows
