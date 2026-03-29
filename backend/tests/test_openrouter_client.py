from __future__ import annotations

from io import BytesIO
import urllib.error
import unittest
from unittest.mock import patch

from actus.openrouter_client import openrouter_chat


class TestOpenRouterClient(unittest.TestCase):
    def test_production_hides_raw_http_error_detail(self) -> None:
        error = urllib.error.HTTPError(
            url="https://openrouter.ai/api/v1/chat/completions",
            code=500,
            msg="Internal Server Error",
            hdrs=None,
            fp=BytesIO(b'{"error":"raw upstream body"}'),
        )
        with patch.dict(
            "os.environ",
            {
                "ACTUS_ENV": "production",
                "ACTUS_OPENROUTER_API_KEY": "secret",
                "ACTUS_OPENROUTER_MODEL": "openai/gpt-4o-mini",
            },
            clear=False,
        ):
            with patch("urllib.request.urlopen", side_effect=error):
                with self.assertRaises(RuntimeError) as ctx:
                    openrouter_chat([{"role": "user", "content": "hello"}])

        self.assertEqual("OpenRouter request failed (status 500).", str(ctx.exception))

    def test_development_keeps_raw_http_error_detail(self) -> None:
        error = urllib.error.HTTPError(
            url="https://openrouter.ai/api/v1/chat/completions",
            code=500,
            msg="Internal Server Error",
            hdrs=None,
            fp=BytesIO(b'{"error":"raw upstream body"}'),
        )
        with patch.dict(
            "os.environ",
            {
                "ACTUS_ENV": "development",
                "ACTUS_OPENROUTER_API_KEY": "secret",
                "ACTUS_OPENROUTER_MODEL": "openai/gpt-4o-mini",
            },
            clear=False,
        ):
            with patch("urllib.request.urlopen", side_effect=error):
                with self.assertRaises(RuntimeError) as ctx:
                    openrouter_chat([{"role": "user", "content": "hello"}])

        self.assertIn("raw upstream body", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
