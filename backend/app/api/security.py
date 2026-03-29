from __future__ import annotations

from functools import lru_cache
import os

from fastapi import HTTPException, Request
import jwt


def _env_name() -> str:
    value = os.environ.get("ACTUS_ENV", "").strip().lower()
    return value or "development"


def is_production() -> bool:
    return _env_name() in {"production", "prod"}


def client_safe_detail(detail: str, *, generic: str) -> str:
    return generic if is_production() else detail


def should_log_tracebacks() -> bool:
    raw = os.environ.get("ACTUS_LOG_TRACEBACKS", "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return not is_production()


def auth_mode() -> str:
    raw = os.environ.get("ACTUS_AUTH_MODE", "").strip().lower()
    if raw:
        return raw
    return "api_key" if current_api_key() else "none"


def current_api_key() -> str:
    return os.environ.get("ACTUS_API_KEY", "").strip()


def extract_api_key(request: Request) -> str | None:
    provided = request.headers.get("x-api-key")
    if provided:
        provided = provided.strip()
    if provided:
        return provided

    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth.split(" ", 1)[1].strip()
        return token or None
    return None


def _cloudflare_access_team_domain() -> str:
    return os.environ.get("ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN", "").strip()


def _cloudflare_access_audiences() -> list[str]:
    raw = os.environ.get("ACTUS_CLOUDFLARE_ACCESS_AUD", "").strip()
    return [value.strip() for value in raw.split(",") if value.strip()]


def _cloudflare_access_issuer() -> str:
    team = _cloudflare_access_team_domain()
    return f"https://{team}/cdn-cgi/access" if team else ""


def _cloudflare_access_jwks_url() -> str:
    raw = os.environ.get("ACTUS_CLOUDFLARE_ACCESS_JWKS_URL", "").strip()
    if raw:
        return raw
    issuer = _cloudflare_access_issuer()
    return f"{issuer}/certs" if issuer else ""


def cloudflare_access_config_errors() -> list[str]:
    errors: list[str] = []
    if not _cloudflare_access_team_domain():
        errors.append("ACTUS_CLOUDFLARE_ACCESS_TEAM_DOMAIN is required when ACTUS_AUTH_MODE=cloudflare_access.")
    if not _cloudflare_access_audiences():
        errors.append("ACTUS_CLOUDFLARE_ACCESS_AUD is required when ACTUS_AUTH_MODE=cloudflare_access.")
    return errors


def _extract_cloudflare_access_jwt(request: Request) -> str | None:
    header = request.headers.get("cf-access-jwt-assertion", "").strip()
    return header or None


@lru_cache(maxsize=4)
def _cf_access_jwk_client(jwks_url: str) -> jwt.PyJWKClient:
    return jwt.PyJWKClient(jwks_url)


def _verify_cloudflare_access_jwt(token: str) -> dict:
    jwks_url = _cloudflare_access_jwks_url()
    signing_key = _cf_access_jwk_client(jwks_url).get_signing_key_from_jwt(token)
    return jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256"],
        audience=_cloudflare_access_audiences(),
        issuer=_cloudflare_access_issuer(),
    )


def require_authenticated_request(request: Request) -> None:
    mode = auth_mode()
    if mode in {"", "none", "public"}:
        return

    if mode == "api_key":
        expected = current_api_key()
        if not expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        provided = extract_api_key(request)
        if not provided or provided != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return

    if mode == "cloudflare_access":
        token = _extract_cloudflare_access_jwt(request)
        if not token:
            raise HTTPException(status_code=401, detail="Unauthorized")
        try:
            _verify_cloudflare_access_jwt(token)
        except Exception:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return

    raise HTTPException(status_code=500, detail="Unsupported auth mode.")


def require_internal_request(request: Request) -> None:
    expected = current_api_key()
    if expected:
        provided = extract_api_key(request)
        if not provided or provided != expected:
            raise HTTPException(status_code=401, detail="Unauthorized")
        return

    mode = auth_mode()
    if mode == "cloudflare_access":
        require_authenticated_request(request)
        return

    if is_production():
        raise HTTPException(status_code=401, detail="Unauthorized")


def require_api_key(request: Request) -> None:
    expected = current_api_key()
    if not expected:
        return

    provided = extract_api_key(request)
    if not provided or provided != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
