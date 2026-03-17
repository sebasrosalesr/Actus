from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from scripts import build_rag_index


class BuildRagIndexTests(unittest.TestCase):
    def test_main_accepts_explicit_argv_without_reading_process_args(self) -> None:
        with patch.object(build_rag_index, "main_new_design") as main_new_design:
            build_rag_index.main([])

        main_new_design.assert_called_once_with()

    def test_main_new_design_writes_canonical_snapshot(self) -> None:
        class _Artifacts:
            canonical_tickets = {"R-1": {"ticket_id": "R-1"}}

        artifacts = _Artifacts()
        with (
            patch("app.rag.new_design.ingest.load_env"),
            patch("app.rag.new_design.ingest.load_credit_requests", return_value=[]),
            patch("app.rag.new_design.ingest.load_investigation_notes", return_value=[]),
            patch("app.rag.new_design.index_build.build_pipeline_artifacts", return_value=artifacts),
            patch("app.rag.new_design.snapshot.save_canonical_tickets", return_value=Path("/tmp/canonical.json.gz")) as save_snapshot,
            patch("app.rag.new_design.index_build.index_pipeline_artifacts", return_value={"chunk_count": 1, "vector_dim": 384}),
            patch("builtins.print"),
        ):
            build_rag_index.main_new_design()

        save_snapshot.assert_called_once_with({"R-1": {"ticket_id": "R-1"}})


if __name__ == "__main__":
    unittest.main()
