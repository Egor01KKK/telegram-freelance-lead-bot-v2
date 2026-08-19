"""Bounded SSRF-safe extraction of Telegram community links from public pages."""

from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
import ipaddress
import socket
import time
from typing import Callable, Iterable
from urllib.parse import urljoin, urlsplit
import urllib.error
import urllib.request

from .telegram_references import InvalidTelegramReference, TelegramReference, normalize_telegram_reference


_METADATA_HOSTS = {"metadata.google.internal", "metadata.google", "instance-data"}


class UnsafePageURL(ValueError):
    pass


class PageFetchError(RuntimeError):
    def __init__(self, message: str, *, failure_class: str, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.failure_class = failure_class
        self.retry_after_seconds = retry_after_seconds


@dataclass(frozen=True)
class ExtractedTelegramLink:
    reference: TelegramReference
    page_url: str
    result_url: str
    domain: str


class _LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() not in {"a", "link"}:
            return
        for key, value in attrs:
            if key.casefold() == "href" and value:
                self.hrefs.append(value)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Expose each redirect so its destination is validated before use."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def validate_public_page_url(url: str) -> str:
    parsed = urlsplit(url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise UnsafePageURL("page URL must be absolute HTTP(S)")
    host = parsed.hostname.casefold().rstrip(".")
    if host in {"localhost", *_METADATA_HOSTS} or host.endswith(".localhost"):
        raise UnsafePageURL("local or metadata host is blocked")
    if parsed.username or parsed.password:
        raise UnsafePageURL("page URL credentials are blocked")
    _assert_public_host(host)
    return parsed.geturl()


def _assert_public_host(host: str) -> None:
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        try:
            addresses = {item[4][0] for item in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)}
        except OSError as exc:
            raise UnsafePageURL("page host cannot be resolved safely") from exc
        if not addresses:
            raise UnsafePageURL("page host has no address")
        for address in addresses:
            _assert_public_ip(address)
        return
    _assert_public_ip(str(ip))


def _assert_public_ip(value: str) -> None:
    ip = ipaddress.ip_address(value)
    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    ):
        raise UnsafePageURL("private, loopback, link-local or reserved destination is blocked")


class SafeWebPageFetcher:
    """Fetch at most one public page with strict bounds and redirect checks."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 10.0,
        max_response_bytes: int = 1_000_000,
        max_redirects: int = 2,
        min_domain_delay_seconds: float = 1.0,
        user_agent: str = "telegram-freelance-lead-bot-source-bootstrap/1",
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        if timeout_seconds <= 0 or max_response_bytes < 1024 or max_redirects < 0:
            raise ValueError("invalid page fetch bounds")
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        self.max_redirects = max_redirects
        self.min_domain_delay_seconds = max(0.0, min_domain_delay_seconds)
        self.user_agent = user_agent[:255]
        self._last_by_domain: dict[str, float] = {}
        self._sleeper = sleeper or time.sleep
        self._opener = urllib.request.build_opener(_NoRedirectHandler())

    def fetch(self, url: str) -> tuple[str, str]:
        current = validate_public_page_url(url)
        for redirect_count in range(self.max_redirects + 1):
            parsed = urlsplit(current)
            domain = parsed.hostname or ""
            last = self._last_by_domain.get(domain)
            if last is not None:
                wait = self.min_domain_delay_seconds - (time.monotonic() - last)
                if wait > 0:
                    self._sleeper(wait)
            self._last_by_domain[domain] = time.monotonic()
            request = urllib.request.Request(
                current,
                headers={"Accept": "text/html,text/plain", "User-Agent": self.user_agent},
            )
            try:
                with self._opener.open(request, timeout=self.timeout_seconds) as response:
                    content_type = str(response.headers.get("Content-Type", "")).casefold()
                    if content_type and not ("text/html" in content_type or "text/plain" in content_type):
                        raise PageFetchError("page is not HTML/text", failure_class="content_type")
                    body = response.read(self.max_response_bytes + 1)
                    final_url = validate_public_page_url(response.geturl())
            except urllib.error.HTTPError as exc:
                if exc.code in {301, 302, 303, 307, 308}:
                    location = exc.headers.get("Location")
                    if not location:
                        raise PageFetchError(
                            "redirect response has no destination",
                            failure_class="redirect_invalid",
                        ) from exc
                    if redirect_count >= self.max_redirects:
                        raise PageFetchError(
                            "page redirect limit exceeded",
                            failure_class="redirect_limit",
                        ) from exc
                    current = validate_public_page_url(urljoin(current, location))
                    continue
                retry = _retry_after_seconds(exc.headers)
                failure = "http_429" if exc.code == 429 else "http_error"
                raise PageFetchError("page fetch returned HTTP error", failure_class=failure, retry_after_seconds=retry) from exc
            except urllib.error.URLError as exc:
                raise PageFetchError("page fetch failed", failure_class="network_error") from exc
            if len(body) > self.max_response_bytes:
                raise PageFetchError("page exceeds response bound", failure_class="response_too_large")
            return body.decode("utf-8", errors="replace"), final_url
        raise PageFetchError("page redirect limit exceeded", failure_class="redirect_limit")

    def extract_telegram_links(self, *, result_url: str, page_url: str, max_links: int = 100) -> tuple[ExtractedTelegramLink, ...]:
        html, final_url = self.fetch(page_url)
        parser = _LinkParser()
        parser.feed(html)
        result: list[ExtractedTelegramLink] = []
        seen: set[str] = set()
        result_domain = (urlsplit(result_url).hostname or "").casefold()
        for href in parser.hrefs:
            absolute = urljoin(final_url, href)
            try:
                reference = normalize_telegram_reference(absolute)
            except InvalidTelegramReference:
                continue
            if reference.source_key in seen:
                continue
            seen.add(reference.source_key)
            result.append(
                ExtractedTelegramLink(
                    reference=reference,
                    page_url=final_url,
                    result_url=result_url,
                    domain=result_domain,
                )
            )
            if len(result) >= max_links:
                break
        return tuple(result)


def _retry_after_seconds(headers: object) -> float | None:
    value = headers.get("Retry-After") if hasattr(headers, "get") else None
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None
