"""Provider selection shared by source discovery AI stages.

This module deliberately owns only provider-neutral configuration plumbing for
Telegram Chat Screening and Source Audit.  Opportunity Analysis and
onboarding keep their existing provider configuration paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
DEEPSEEK_CHAT_COMPLETIONS_URL = "https://api.deepseek.com/chat/completions"
SUPPORTED_SOURCE_AI_PROVIDERS = frozenset({"openai", "deepseek", "tokenrouter"})


class SourceAIProviderConfigurationError(RuntimeError):
    """A selected source-stage provider cannot be used safely."""


class SourceAIProviderUnavailable(SourceAIProviderConfigurationError):
    """The selected provider is supported but its selected key is absent."""


class UnsupportedSourceAIProvider(SourceAIProviderConfigurationError):
    """The source-stage provider is outside the closed supported set."""


@dataclass(frozen=True)
class SourceAIProviderSettings:
    name: str
    api_key: str
    api_key_name: str
    base_url: str


def normalize_chat_completions_url(base_url: str) -> str:
    normalized = base_url.strip().rstrip("/")
    if normalized.endswith("/chat/completions"):
        return normalized
    return f"{normalized}/chat/completions"


def resolve_source_ai_provider(
    config: Any,
    provider: str | None = None,
) -> SourceAIProviderSettings:
    selected_provider = (
        provider
        if provider is not None
        else getattr(config, "source_audit_provider", "")
    )
    provider = str(selected_provider).strip().lower()
    if provider not in SUPPORTED_SOURCE_AI_PROVIDERS:
        raise UnsupportedSourceAIProvider(
            f"Unsupported source AI provider: {provider or '<empty>'}"
        )

    if provider == "openai":
        api_key_name = "OPENAI_API_KEY"
        base_url = OPENAI_CHAT_COMPLETIONS_URL
        secret = getattr(config, "openai_api_key", None)
    elif provider == "deepseek":
        api_key_name = "DEEPSEEK_API_KEY"
        base_url = DEEPSEEK_CHAT_COMPLETIONS_URL
        secret = getattr(config, "deepseek_api_key", None)
    else:
        api_key_name = "TOKENROUTER_API_KEY"
        base_url = normalize_chat_completions_url(
            str(getattr(config, "tokenrouter_base_url", ""))
        )
        secret = getattr(config, "tokenrouter_api_key", None)

    api_key = ""
    if secret is not None:
        getter = getattr(secret, "get_secret_value", None)
        api_key = str(getter() if getter is not None else secret)
    if not api_key.strip():
        raise SourceAIProviderUnavailable(
            f"{api_key_name} is not configured for source AI provider {provider}"
        )

    return SourceAIProviderSettings(
        name=provider,
        api_key=api_key,
        api_key_name=api_key_name,
        base_url=base_url,
    )


def source_ai_provider_available(config: Any, provider: str | None = None) -> bool:
    try:
        resolve_source_ai_provider(config, provider=provider)
    except SourceAIProviderConfigurationError:
        return False
    return True
