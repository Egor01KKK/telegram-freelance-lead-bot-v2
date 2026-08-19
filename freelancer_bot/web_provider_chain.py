"""Provider-neutral Web Discovery backend construction.

The primary endpoint is intentionally generic and opt-in.  Its response
contract is the same bounded ``{"results": [{"url", "title", "content"}]}``
shape used by SearXNG, so campaign logic remains independent of a vendor.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from .web_discovery import SearxngSearchBackend, WebSearchBackend, WebSearchBackendError, WebSearchResult


PRIMARY_WEB_SEARCH_ENV = "WEB_PRIMARY_SEARCH_URL"
PRIMARY_WEB_SEARCH_KEY_ENV = "WEB_PRIMARY_SEARCH_API_KEY"
BRAVE_SEARCH_ENV = "BRAVE_SEARCH_API_KEY"
BRAVE_SEARCH_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"


class JsonWebSearchBackend:
    health_identity = "primary_json_search"

    def __init__(self, endpoint: str, *, api_key: str | None = None, timeout_seconds: float = 15.0, max_response_bytes: int = 1_000_000) -> None:
        parsed = urllib.parse.urlsplit(endpoint.strip())
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("primary Web search endpoint must be an absolute HTTP(S) URL")
        self.endpoint = endpoint
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes

    async def search(self, query: str, *, language: str, limit: int) -> Sequence[WebSearchResult]:
        return await asyncio.to_thread(self._search_sync, query, language, limit)

    def _search_sync(self, query: str, language: str, limit: int) -> tuple[WebSearchResult, ...]:
        separator = "&" if "?" in self.endpoint else "?"
        url = f"{self.endpoint}{separator}{urllib.parse.urlencode({'q': query, 'language': language, 'limit': limit})}"
        headers = {"Accept": "application/json", "User-Agent": "telegram-freelance-lead-bot/1"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=self.timeout_seconds) as response:
                body = response.read(self.max_response_bytes + 1)
        except urllib.error.HTTPError as exc:
            failure_class = "http_429" if exc.code == 429 else "http_403" if exc.code == 403 else "http_error"
            raise WebSearchBackendError("primary Web search request failed", failure_class=failure_class, status_code=exc.code) from exc
        except (urllib.error.URLError, TimeoutError) as exc:
            raise WebSearchBackendError("primary Web search request failed", failure_class="network_error") from exc
        if len(body) > self.max_response_bytes:
            raise WebSearchBackendError("primary Web search response is too large", failure_class="invalid_response")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise WebSearchBackendError("primary Web search returned invalid JSON", failure_class="invalid_response") from exc
        rows = payload.get("results") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list):
            raise WebSearchBackendError("primary Web search response has no results", failure_class="invalid_response")
        results: list[WebSearchResult] = []
        for row in rows[:limit]:
            if not isinstance(row, Mapping) or not isinstance(row.get("url"), str):
                continue
            results.append(WebSearchResult(url=row["url"], title=str(row.get("title") or ""), snippet=str(row.get("content") or row.get("snippet") or "")))
        return tuple(results)


class BraveSearchBackend:
    """Provider-neutral WebSearchBackend adapter for Brave Web Search API.

    The adapter owns only HTTP and response normalization.  Provider health,
    pacing, fallback and durable campaign accounting remain in the existing
    WebDiscoveryGovernor and campaign services.
    """

    health_identity = "brave"

    def __init__(
        self,
        api_key: str,
        *,
        timeout_seconds: float = 15.0,
        max_response_bytes: int = 1_000_000,
        endpoint: str = BRAVE_SEARCH_ENDPOINT,
    ) -> None:
        if not api_key.strip():
            raise ValueError("Brave Search API key must not be blank")
        parsed = urllib.parse.urlsplit(endpoint.strip())
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("Brave Search endpoint must be an absolute HTTPS URL")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if max_response_bytes < 1024:
            raise ValueError("max_response_bytes must be at least 1024")
        self._api_key = api_key.strip()
        self._endpoint = endpoint
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._last_result_count = 0
        self._empty_results = 0
        self._last_failure_class: str | None = None

    @property
    def health_observability(self) -> dict[str, object]:
        return {
            "last_result_count": self._last_result_count,
            "empty_results": self._empty_results,
            "last_failure_class": self._last_failure_class or "",
        }

    async def search(
        self,
        query: str,
        *,
        language: str,
        limit: int,
    ) -> Sequence[WebSearchResult]:
        if not 1 <= limit <= 20:
            raise ValueError("Brave Search result limit must be between 1 and 20")
        if not isinstance(language, str) or not language.strip():
            raise ValueError("Brave Search language must not be blank")
        return await asyncio.to_thread(
            self._search_sync,
            query.strip(),
            language.strip().casefold(),
            limit,
        )

    def _search_sync(
        self,
        query: str,
        language: str,
        limit: int,
    ) -> tuple[WebSearchResult, ...]:
        params = urllib.parse.urlencode(
            {
                "q": query,
                "count": limit,
                "search_lang": language,
            }
        )
        request = urllib.request.Request(
            f"{self._endpoint}?{params}",
            headers={
                "Accept": "application/json",
                "User-Agent": "telegram-freelance-lead-bot/1",
                "X-Subscription-Token": self._api_key,
            },
        )
        self._last_failure_class = None
        try:
            with urllib.request.urlopen(request, timeout=self._timeout_seconds) as response:
                body = response.read(self._max_response_bytes + 1)
        except urllib.error.HTTPError as exc:
            failure_class = _brave_http_failure_class(exc.code)
            self._last_failure_class = failure_class
            raise WebSearchBackendError(
                "Brave Search request failed",
                failure_class=failure_class,
                status_code=exc.code,
                retry_after_seconds=_retry_after_seconds(exc.headers),
            ) from exc
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", None)
            failure_class = "timeout" if isinstance(reason, TimeoutError) else "network_error"
            self._last_failure_class = failure_class
            raise WebSearchBackendError(
                "Brave Search request failed",
                failure_class=failure_class,
            ) from exc
        except TimeoutError as exc:
            self._last_failure_class = "timeout"
            raise WebSearchBackendError(
                "Brave Search request timed out",
                failure_class="timeout",
            ) from exc
        if len(body) > self._max_response_bytes:
            self._last_failure_class = "invalid_response"
            raise WebSearchBackendError(
                "Brave Search response exceeds the configured limit",
                failure_class="invalid_response",
            )
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            self._last_failure_class = "malformed_json"
            raise WebSearchBackendError(
                "Brave Search returned malformed JSON",
                failure_class="malformed_json",
            ) from exc
        if not isinstance(payload, Mapping):
            self._last_failure_class = "malformed_json"
            raise WebSearchBackendError(
                "Brave Search response is not an object",
                failure_class="malformed_json",
            )
        web = payload.get("web")
        rows = web.get("results") if isinstance(web, Mapping) else None
        if not isinstance(rows, list):
            self._last_failure_class = "malformed_json"
            raise WebSearchBackendError(
                "Brave Search response has no web.results list",
                failure_class="malformed_json",
            )
        if not rows:
            self._last_result_count = 0
            self._empty_results += 1
            self._last_failure_class = "empty_result"
            return ()
        results: list[WebSearchResult] = []
        for row in rows[:limit]:
            if not isinstance(row, Mapping) or not isinstance(row.get("url"), str):
                continue
            results.append(
                WebSearchResult(
                    url=row["url"],
                    title=str(row.get("title") or ""),
                    snippet=str(row.get("description") or row.get("snippet") or ""),
                )
            )
        self._last_result_count = len(results)
        if not results:
            self._empty_results += 1
            self._last_failure_class = "empty_result"
        return tuple(results)


def build_web_search_backends(config: Any) -> tuple[WebSearchBackend, ...]:
    backends: list[WebSearchBackend] = []
    brave_key = getattr(config, "brave_search_api_key", None)
    if brave_key:
        value = brave_key.get_secret_value() if hasattr(brave_key, "get_secret_value") else str(brave_key)
        if value.strip():
            backends.append(
                BraveSearchBackend(
                    value,
                    timeout_seconds=float(getattr(config, "brave_search_timeout_seconds", 15.0)),
                )
            )
    primary_url = getattr(config, "primary_web_search_url", None)
    if primary_url:
        key = getattr(config, "primary_web_search_api_key", None)
        backends.append(JsonWebSearchBackend(primary_url, api_key=None if key is None else key.get_secret_value()))
    searxng_url = getattr(config, "searxng_url", None)
    if searxng_url:
        backends.append(SearxngSearchBackend(searxng_url))
    return tuple(backends)


def web_discovery_readiness(config: Any) -> dict[str, object]:
    brave_value = getattr(config, "brave_search_api_key", None)
    if hasattr(brave_value, "get_secret_value"):
        brave = bool(brave_value.get_secret_value().strip())
    else:
        brave = bool(str(brave_value).strip()) if brave_value is not None else False
    primary = brave or bool(getattr(config, "primary_web_search_url", None))
    fallback = bool(getattr(config, "searxng_url", None))
    return {
        "primary_configured": primary,
        "brave_configured": brave,
        "searxng_configured": fallback,
        "state": "READY" if primary else "WEB_DISCOVERY_DEGRADED" if fallback else "UNAVAILABLE",
        "missing_primary_environment_variable": None if primary else BRAVE_SEARCH_ENV,
    }


def _brave_http_failure_class(status_code: int) -> str:
    if status_code in {401, 403, 422, 429}:
        return f"http_{status_code}"
    if 500 <= status_code <= 599:
        return "http_5xx"
    return "http_error"


def _retry_after_seconds(headers: Any) -> float | None:
    if headers is None:
        return None
    raw = headers.get("Retry-After") if hasattr(headers, "get") else None
    if raw is None:
        return None
    try:
        value = float(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return value if value >= 0 else None
