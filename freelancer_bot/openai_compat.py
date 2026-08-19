"""Small, explicit model-capability rules shared by OpenAI adapters."""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any


def model_supports_temperature(model: str) -> bool:
    """Return whether the adapter should send a configured temperature.

    GPT-5 models currently accept only their provider default. Unknown models
    retain the historical behavior until their capability is explicitly added;
    no replacement temperature is invented for unsupported models.
    """

    return not model.strip().lower().startswith("gpt-5")


def add_sampling_parameter(
    payload: MutableMapping[str, Any],
    *,
    model: str,
    temperature: float,
) -> MutableMapping[str, Any]:
    if model_supports_temperature(model):
        payload["temperature"] = temperature
    return payload
