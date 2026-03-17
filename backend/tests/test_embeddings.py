from __future__ import annotations

from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parents[1]
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.rag import embeddings


class EmbeddingsTests(unittest.TestCase):
    def tearDown(self) -> None:
        embeddings._model = None
        embeddings._model_warmed = False

    def test_get_embedding_model_loads_once_under_concurrency(self) -> None:
        embeddings._model = None
        created: list[object] = []
        start = threading.Barrier(4)
        done = threading.Barrier(4)

        def _fake_load():
            model = object()
            created.append(model)
            return model

        def _worker(results: list[object]) -> None:
            start.wait()
            results.append(embeddings.get_embedding_model())
            done.wait()

        results: list[object] = []
        threads = [threading.Thread(target=_worker, args=(results,)) for _ in range(3)]

        with patch.object(embeddings, "_load_model", side_effect=_fake_load):
            for thread in threads:
                thread.start()
            start.wait()
            done.wait()
            for thread in threads:
                thread.join()

        self.assertEqual(len(created), 1)
        self.assertEqual(len(results), 3)
        self.assertTrue(all(result is created[0] for result in results))

    def test_warm_embedding_model_encodes_once(self) -> None:
        class _FakeModel:
            def __init__(self) -> None:
                self.calls = 0

            def encode(self, texts, show_progress_bar=False):
                _ = texts
                _ = show_progress_bar
                self.calls += 1
                return [[1.0, 0.0, 0.0]]

        model = _FakeModel()
        embeddings._model = model
        embeddings._model_warmed = False

        embeddings.warm_embedding_model()
        embeddings.warm_embedding_model()

        self.assertEqual(model.calls, 1)

    def test_load_model_prefers_local_cache_when_available(self) -> None:
        recorded: dict[str, object] = {}

        def _fake_sentence_transformer(name, **kwargs):
            recorded["name"] = name
            recorded["kwargs"] = kwargs
            return object()

        with (
            patch.dict(embeddings.os.environ, {"HF_HOME": str(BACKEND_DIR), "ACTUS_HF_LOCAL_ONLY": "1"}, clear=False),
            patch("sentence_transformers.SentenceTransformer", side_effect=_fake_sentence_transformer),
        ):
            embeddings._model = None
            model = embeddings._load_model()

        self.assertIsNotNone(model)
        self.assertEqual(recorded["name"], "all-MiniLM-L6-v2")
        self.assertEqual(
            recorded["kwargs"],
            {"cache_folder": str(BACKEND_DIR), "local_files_only": True},
        )

    def test_load_model_falls_back_when_local_only_cache_misses(self) -> None:
        calls: list[dict[str, object]] = []

        def _fake_sentence_transformer(name, **kwargs):
            calls.append({"name": name, "kwargs": kwargs})
            if kwargs.get("local_files_only") is True:
                raise OSError("cache miss")
            return object()

        with (
            patch.dict(embeddings.os.environ, {"HF_HOME": str(BACKEND_DIR), "ACTUS_HF_LOCAL_ONLY": "1"}, clear=False),
            patch("sentence_transformers.SentenceTransformer", side_effect=_fake_sentence_transformer),
        ):
            embeddings._model = None
            model = embeddings._load_model()

        self.assertIsNotNone(model)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["kwargs"], {"cache_folder": str(BACKEND_DIR), "local_files_only": True})
        self.assertEqual(calls[1]["kwargs"], {"cache_folder": str(BACKEND_DIR), "local_files_only": False})


if __name__ == "__main__":
    unittest.main()
