from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import main


class _FakeThread:
    def __init__(self, *, target, name, daemon) -> None:
        self.target = target
        self.name = name
        self.daemon = daemon
        self.started = False

    def start(self) -> None:
        self.started = True


class StartupPreloadTests(unittest.TestCase):
    def setUp(self) -> None:
        main._RAG_PRELOAD_STARTED = False

    def tearDown(self) -> None:
        main._RAG_PRELOAD_STARTED = False

    def test_preload_runs_in_background_thread(self) -> None:
        created: list[_FakeThread] = []

        def _thread_factory(*, target, name, daemon):
            thread = _FakeThread(target=target, name=name, daemon=daemon)
            created.append(thread)
            return thread

        with patch.object(main, "_should_preload_new_rag", return_value=True):
            with patch.object(main.threading, "Thread", side_effect=_thread_factory):
                main._preload_new_rag_service()

        self.assertEqual(len(created), 1)
        self.assertTrue(created[0].started)
        self.assertEqual(created[0].name, "rag-new-design-preload")
        self.assertTrue(created[0].daemon)

    def test_preload_starts_only_once(self) -> None:
        created: list[_FakeThread] = []

        def _thread_factory(*, target, name, daemon):
            thread = _FakeThread(target=target, name=name, daemon=daemon)
            created.append(thread)
            return thread

        with patch.object(main, "_should_preload_new_rag", return_value=True):
            with patch.object(main.threading, "Thread", side_effect=_thread_factory):
                main._preload_new_rag_service()
                main._preload_new_rag_service()

        self.assertEqual(len(created), 1)


if __name__ == "__main__":
    unittest.main()
