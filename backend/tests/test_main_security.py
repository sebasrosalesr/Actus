import asyncio
from contextlib import contextmanager
import importlib
import os
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.requests import Request
from starlette.responses import Response

import app.api.help as help_api
import app.api.quality as quality_api
import app.api.rag as rag_api
import app.api.security as security_api
import main as main_module


@contextmanager
def _reloaded_main(env_updates: dict[str, str]):
    try:
        with patch.dict(os.environ, env_updates, clear=False):
            importlib.reload(security_api)
            importlib.reload(help_api)
            importlib.reload(quality_api)
            importlib.reload(rag_api)
            module = importlib.reload(main_module)
            yield module
    finally:
        importlib.reload(security_api)
        importlib.reload(help_api)
        importlib.reload(quality_api)
        importlib.reload(rag_api)
        importlib.reload(main_module)


def _request_for(
    path: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    client: tuple[str, int] = ("127.0.0.1", 1234),
) -> Request:
    raw_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "headers": raw_headers,
            "scheme": "http",
            "server": ("testserver", 80),
            "client": client,
            "query_string": b"",
        }
    )


@contextmanager
def _test_client(module):
    with patch.object(module, "init_quality_db"):
        with patch.object(module, "resolve_db_path", return_value="test-quality.db"):
            with patch.object(module, "_preload_new_rag_service"):
                with patch.object(module, "_start_rag_rebuild_loop"):
                    with TestClient(module.APP) as client:
                        yield client


class TestMainSecurity(unittest.TestCase):
    def test_production_boot_fails_without_api_key(self) -> None:
        with self.assertRaises(RuntimeError):
            with _reloaded_main(
                {
                    "ACTUS_ENV": "production",
                    "ACTUS_AUTH_MODE": "api_key",
                    "ACTUS_API_KEY": "",
                    "ACTUS_CORS_ORIGINS": "https://app.example.com",
                    "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                    "ACTUS_DOCS_ENABLED": "false",
                    "ACTUS_CORS_ORIGIN_REGEX": "",
                }
            ):
                pass

    def test_production_boot_fails_when_docs_enabled(self) -> None:
        with self.assertRaises(RuntimeError):
            with _reloaded_main(
                {
                    "ACTUS_ENV": "production",
                    "ACTUS_AUTH_MODE": "api_key",
                    "ACTUS_API_KEY": "secret",
                    "ACTUS_CORS_ORIGINS": "https://app.example.com",
                    "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                    "ACTUS_DOCS_ENABLED": "true",
                    "ACTUS_CORS_ORIGIN_REGEX": "",
                }
            ):
                pass

    def test_production_boot_fails_in_public_mode_without_api_key(self) -> None:
        with self.assertRaises(RuntimeError):
            with _reloaded_main(
                {
                    "ACTUS_ENV": "production",
                    "ACTUS_AUTH_MODE": "public",
                    "ACTUS_API_KEY": "",
                    "ACTUS_CORS_ORIGINS": "https://app.example.com",
                    "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                    "ACTUS_DOCS_ENABLED": "false",
                    "ACTUS_CORS_ORIGIN_REGEX": "",
                }
            ):
                pass

    def test_production_boot_fails_when_cloudflare_access_config_missing(self) -> None:
        with self.assertRaises(RuntimeError):
            with _reloaded_main(
                {
                    "ACTUS_ENV": "production",
                    "ACTUS_AUTH_MODE": "cloudflare_access",
                    "ACTUS_CORS_ORIGINS": "https://app.example.com",
                    "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                    "ACTUS_DOCS_ENABLED": "false",
                    "ACTUS_CORS_ORIGIN_REGEX": "",
                    "ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN": "",
                    "ACTUS_CLOUDFLARE_ACCESS_AUD": "",
                }
            ):
                pass

    def test_production_boot_accepts_cloudflare_access_mode(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "cloudflare_access",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
                "ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN": "example.cloudflareaccess.com",
                "ACTUS_CLOUDFLARE_ACCESS_AUD": "aud-123",
            }
        ) as module:
            self.assertIsNone(module.APP.docs_url)

    def test_production_boot_accepts_public_mode(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "public",
                "ACTUS_API_KEY": "internal-secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            self.assertIsNone(module.APP.docs_url)

    def test_production_app_disables_docs_and_uses_strict_middleware(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            self.assertIsNone(module.APP.docs_url)
            self.assertIsNone(module.APP.redoc_url)
            self.assertIsNone(module.APP.openapi_url)

            trusted = next(item for item in module.APP.user_middleware if item.cls.__name__ == "TrustedHostMiddleware")
            cors = next(item for item in module.APP.user_middleware if item.cls.__name__ == "CORSMiddleware")

            self.assertEqual(["actus-app.fly.dev"], trusted.kwargs["allowed_hosts"])
            self.assertEqual(["GET", "POST", "OPTIONS"], cors.kwargs["allow_methods"])
            self.assertEqual(["Content-Type", "Authorization", "X-API-Key"], cors.kwargs["allow_headers"])

    def test_health_openrouter_requires_auth_when_api_key_is_set(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "development",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_DOCS_ENABLED": "true",
            }
        ) as module:
            async def call_next(_request: Request):
                return Response("ok")

            request = Request(
                {
                    "type": "http",
                    "method": "GET",
                    "path": "/api/health/openrouter",
                    "headers": [],
                    "scheme": "http",
                    "server": ("testserver", 80),
                    "client": ("127.0.0.1", 1234),
                    "query_string": b"",
                }
            )
            response = asyncio.run(module._api_key_guard(request, call_next))
            self.assertEqual(401, response.status_code)

    def test_require_authenticated_request_allows_public_mode(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "public",
                "ACTUS_API_KEY": "internal-secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ):
            request = _request_for("/api/ask", method="POST")
            security_api.require_authenticated_request(request)

    def test_require_internal_request_requires_api_key_in_public_mode(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "public",
                "ACTUS_API_KEY": "internal-secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ):
            request = _request_for("/api/health/openrouter")
            with self.assertRaises(main_module.HTTPException) as ctx:
                security_api.require_internal_request(request)

            self.assertEqual(401, ctx.exception.status_code)

    def test_require_authenticated_request_accepts_cloudflare_access(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "cloudflare_access",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
                "ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN": "example.cloudflareaccess.com",
                "ACTUS_CLOUDFLARE_ACCESS_AUD": "aud-123",
            }
        ) as module:
            request = _request_for(
                "/api/ask",
                method="POST",
                headers={"cf-access-jwt-assertion": "good-token"},
            )
            with patch.object(security_api, "_verify_cloudflare_access_jwt", return_value={"sub": "user@example.com"}):
                security_api.require_authenticated_request(request)

    def test_require_authenticated_request_rejects_missing_cloudflare_token(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "cloudflare_access",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
                "ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN": "example.cloudflareaccess.com",
                "ACTUS_CLOUDFLARE_ACCESS_AUD": "aud-123",
            }
        ):
            request = _request_for("/api/ask", method="POST")
            with self.assertRaises(main_module.HTTPException) as ctx:
                security_api.require_authenticated_request(request)

            self.assertEqual(401, ctx.exception.status_code)

    def test_health_endpoint_remains_public(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "development",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_DOCS_ENABLED": "true",
            }
        ) as module:
            async def call_next(_request: Request):
                return Response("ok", status_code=200)

            request = Request(
                {
                    "type": "http",
                    "method": "GET",
                    "path": "/api/health",
                    "headers": [],
                    "scheme": "http",
                    "server": ("testserver", 80),
                    "client": ("127.0.0.1", 1234),
                    "query_string": b"",
                }
            )
            response = asyncio.run(module._api_key_guard(request, call_next))
            self.assertEqual(200, response.status_code)

    def test_trusted_host_rejects_unexpected_host_in_production(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            with _test_client(module) as client:
                response = client.get("/api/health", headers={"host": "bad.example.com"})

            self.assertEqual(400, response.status_code)

    def test_rag_routes_require_auth_when_api_key_is_set(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            with _test_client(module) as client:
                response = client.get("/rag/health", headers={"host": "actus-app.fly.dev"})

            self.assertEqual(401, response.status_code)

    def test_openrouter_health_hides_upstream_detail_in_production(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            with patch.object(module, "_openrouter_call", side_effect=RuntimeError("raw upstream body")):
                with self.assertRaises(module.HTTPException) as ctx:
                    module.health_openrouter()

            self.assertEqual(500, ctx.exception.status_code)
            self.assertEqual("Provider health check failed.", ctx.exception.detail)

    def test_quality_logging_fingerprints_query_in_production(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
                "ACTUS_LOG_RAW_QUERIES": "false",
            }
        ) as module:
            with patch.object(module, "record_quality_event") as record_mock:
                module._safe_log_quality_event(
                    query="analyze ticket R-123456",
                    rows=[],
                    meta={"intent": "ticket_analysis"},
                    ok=False,
                    elapsed_ms=12.5,
                    provider="actus",
                    error="raw upstream body",
                )

            event = record_mock.call_args[0][0]
            self.assertTrue(str(event["query"]).startswith("sha256:"))
            self.assertNotEqual("analyze ticket R-123456", event["query"])
            self.assertEqual("fingerprint", event["meta"]["query_logging"])
            self.assertEqual(23, event["meta"]["query_length"])
            self.assertEqual("Request failed.", event["error"])

    def test_quality_logging_keeps_raw_query_in_development(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "development",
                "ACTUS_DOCS_ENABLED": "true",
                "ACTUS_LOG_RAW_QUERIES": "true",
            }
        ) as module:
            with patch.object(module, "record_quality_event") as record_mock:
                module._safe_log_quality_event(
                    query="show anomalies this month",
                    rows=[],
                    meta={"intent": "credit_anomalies"},
                    ok=True,
                    elapsed_ms=8.0,
                    provider="actus",
                )

            event = record_mock.call_args[0][0]
            self.assertEqual("show anomalies this month", event["query"])
            self.assertEqual("raw", event["meta"]["query_logging"])
            self.assertEqual(25, event["meta"]["query_length"])

    def test_rate_limit_blocks_ask_bursts(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "development",
                "ACTUS_DOCS_ENABLED": "true",
                "ACTUS_RATE_LIMIT_ENABLED": "true",
                "ACTUS_RATE_LIMIT_ASK_PER_MIN": "2",
            }
        ) as module:
            async def call_next(_request: Request):
                return Response("ok", status_code=200)

            first = asyncio.run(module._rate_limit_guard(_request_for("/api/ask", method="POST"), call_next))
            second = asyncio.run(module._rate_limit_guard(_request_for("/api/ask", method="POST"), call_next))
            third = asyncio.run(module._rate_limit_guard(_request_for("/api/ask", method="POST"), call_next))

            self.assertEqual(200, first.status_code)
            self.assertEqual(200, second.status_code)
            self.assertEqual(429, third.status_code)
            self.assertIn("Retry-After", third.headers)

    def test_rate_limit_blocks_openrouter_health_bursts(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "development",
                "ACTUS_DOCS_ENABLED": "true",
                "ACTUS_RATE_LIMIT_ENABLED": "true",
                "ACTUS_RATE_LIMIT_OPENROUTER_HEALTH_PER_MIN": "1",
            }
        ) as module:
            async def call_next(_request: Request):
                return Response("ok", status_code=200)

            first = asyncio.run(
                module._rate_limit_guard(
                    _request_for("/api/health/openrouter", headers={"x-forwarded-for": "10.0.0.5"}),
                    call_next,
                )
            )
            second = asyncio.run(
                module._rate_limit_guard(
                    _request_for("/api/health/openrouter", headers={"x-forwarded-for": "10.0.0.5"}),
                    call_next,
                )
            )

            self.assertEqual(200, first.status_code)
            self.assertEqual(429, second.status_code)

    def test_rate_limit_blocks_rag_bursts(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "development",
                "ACTUS_DOCS_ENABLED": "true",
                "ACTUS_RATE_LIMIT_ENABLED": "true",
                "ACTUS_RATE_LIMIT_RAG_PER_MIN": "1",
            }
        ) as module:
            async def call_next(_request: Request):
                return Response("ok", status_code=200)

            first = asyncio.run(module._rate_limit_guard(_request_for("/rag/new/search", method="POST"), call_next))
            second = asyncio.run(module._rate_limit_guard(_request_for("/rag/new/search", method="POST"), call_next))

            self.assertEqual(200, first.status_code)
            self.assertEqual(429, second.status_code)

    def test_user_context_hides_backend_detail_in_production(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            with patch.object(module, "_ensure_firebase_app", side_effect=RuntimeError("firebase file path missing")):
                with self.assertRaises(module.HTTPException) as ctx:
                    module.user_context(email="ops@example.com")

            self.assertEqual(500, ctx.exception.status_code)
            self.assertEqual("User context backend is unavailable.", ctx.exception.detail)

    def test_internal_rag_refresh_requires_api_key_in_public_mode(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "public",
                "ACTUS_API_KEY": "internal-secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            with _test_client(module) as client:
                response = client.post(
                    "/rag/new/refresh",
                    headers={"host": "actus-app.fly.dev"},
                    json={"index": False},
                )

            self.assertEqual(401, response.status_code)

    def test_rag_search_hides_upstream_detail_in_production(self) -> None:
        with _reloaded_main(
            {
                "ACTUS_ENV": "production",
                "ACTUS_AUTH_MODE": "api_key",
                "ACTUS_API_KEY": "secret",
                "ACTUS_CORS_ORIGINS": "https://app.example.com",
                "ACTUS_ALLOWED_HOSTS": "actus-app.fly.dev",
                "ACTUS_DOCS_ENABLED": "false",
                "ACTUS_CORS_ORIGIN_REGEX": "",
            }
        ) as module:
            with _test_client(module) as client:
                with patch.object(rag_api, "get_runtime_service", side_effect=RuntimeError("raw pinecone detail")):
                    with patch.object(rag_api.traceback, "print_exc") as trace_mock:
                        response = client.post(
                            "/rag/new/search",
                            headers={"host": "actus-app.fly.dev", "x-api-key": "secret"},
                            json={"query": "hello"},
                        )
                trace_mock.assert_not_called()

            self.assertEqual(500, response.status_code)
            self.assertEqual({"detail": "RAG search failed."}, response.json())


if __name__ == "__main__":
    unittest.main()
