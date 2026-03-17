from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
BACKEND_DIR = SCRIPT_DIR.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.rag.runtime_env import ensure_openmp_env

ensure_openmp_env()


def main_new_design() -> None:
    from app.rag.new_design.index_build import build_pipeline_artifacts, index_pipeline_artifacts
    from app.rag.new_design.ingest import load_credit_requests, load_env, load_investigation_notes

    load_env()
    credit_rows = load_credit_requests()
    investigation_rows = load_investigation_notes()

    artifacts = build_pipeline_artifacts(
        credit_rows=credit_rows,
        investigation_rows=investigation_rows,
    )

    target_data_dir_raw = os.environ.get("ACTUS_NEW_RAG_DATA_DIR", "").strip()
    target_data_dir = (
        Path(target_data_dir_raw)
        if target_data_dir_raw
        else (Path(__file__).resolve().parents[1] / "rag_data" / "new_design")
    )
    info = index_pipeline_artifacts(artifacts, data_dir=target_data_dir)
    print(
        f"Indexed {info['chunk_count']} chunks using new_design pipeline "
        f"(dim={info['vector_dim']})."
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build RAG index (new_design pipeline only).",
    )
    parser.add_argument(
        "--pipeline",
        default="new_design",
        help="Deprecated. Only `new_design` is supported.",
    )
    args = parser.parse_args()

    if str(args.pipeline).strip().lower() != "new_design":
        raise SystemExit(
            "Legacy RAG pipeline was removed. Use: python scripts/build_rag_index.py --pipeline new_design"
        )

    main_new_design()


if __name__ == "__main__":
    main()
