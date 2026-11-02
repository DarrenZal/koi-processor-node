"""
Chat provider abstraction — per-surface LLM decoupling.

One ABC, two implementations (OpenAI, Anthropic), three independent
factory functions for classifier, chat answer, and query expansion.

Follows the same pattern as api/embedding_provider.py.
"""

import asyncio
import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ChatProvider(ABC):
    """Abstract base class for chat completion providers."""

    provider_name: str
    default_model: str

    @abstractmethod
    async def complete(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 1024,
        json_mode: bool = False,
    ) -> str:
        """Return the assistant's text response."""


class OpenAIChatProvider(ChatProvider):
    """OpenAI chat completions via the openai package."""

    provider_name = "openai"

    def __init__(self, api_key: str, default_model: str = "gpt-4o-mini"):
        from openai import OpenAI
        self._client = OpenAI(api_key=api_key)
        self.default_model = default_model

    async def complete(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 1024,
        json_mode: bool = False,
    ) -> str:
        kwargs: dict = dict(
            model=model or self.default_model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        if json_mode:
            kwargs["response_format"] = {"type": "json_object"}

        response = await asyncio.to_thread(
            self._client.chat.completions.create, **kwargs
        )
        return response.choices[0].message.content or ""


class AnthropicChatProvider(ChatProvider):
    """Anthropic chat completions via the anthropic package."""

    provider_name = "anthropic"

    def __init__(self, api_key: str, default_model: str = "claude-sonnet-4-6"):
        import anthropic
        self._client = anthropic.Anthropic(api_key=api_key)
        self.default_model = default_model

    async def complete(
        self,
        messages: list[dict[str, str]],
        *,
        model: str | None = None,
        temperature: float = 0.3,
        max_tokens: int = 1024,
        json_mode: bool = False,
    ) -> str:
        # Anthropic API: system message is a separate kwarg
        system_text = None
        user_messages = []
        for m in messages:
            if m["role"] == "system":
                system_text = m["content"]
            else:
                user_messages.append({"role": m["role"], "content": m["content"]})

        # json_mode: append instruction (Anthropic has no native json_mode)
        if json_mode and system_text:
            system_text += "\n\nRespond ONLY with valid JSON."
        elif json_mode:
            system_text = "Respond ONLY with valid JSON."

        kwargs: dict = dict(
            model=model or self.default_model,
            messages=user_messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        if system_text:
            kwargs["system"] = system_text

        response = await asyncio.to_thread(
            self._client.messages.create, **kwargs
        )
        return response.content[0].text


# ---------------------------------------------------------------------------
# Factory functions — one per surface
# ---------------------------------------------------------------------------

_OPENAI_KEY_VAR = "OPENAI_API_KEY"
_ANTHROPIC_KEY_VAR = "ANTHROPIC_API_KEY"


def _require_key(env_var: str) -> str:
    key = os.getenv(env_var, "")
    if not key:
        raise ValueError(f"{env_var} is required but not set")
    return key


def create_classifier_provider() -> ChatProvider:
    """Classifier provider. OpenAI only (structured output sensitive)."""
    provider = os.getenv("CLASSIFIER_PROVIDER", "openai").strip().lower()
    model = os.getenv("CLASSIFIER_MODEL", "gpt-4o")

    if provider == "openai":
        return OpenAIChatProvider(api_key=_require_key(_OPENAI_KEY_VAR), default_model=model)

    raise ValueError(f"Unsupported CLASSIFIER_PROVIDER: {provider!r} (only 'openai' supported)")


def create_chat_provider() -> ChatProvider:
    """Chat answer provider. Supports OpenAI and Anthropic."""
    provider = os.getenv("CHAT_PROVIDER", "openai").strip().lower()
    # CHAT_MODEL with CHAT_LLM_MODEL as backward-compat alias
    model = os.getenv("CHAT_MODEL") or os.getenv("CHAT_LLM_MODEL", "gpt-4o-mini")

    if provider == "openai":
        return OpenAIChatProvider(api_key=_require_key(_OPENAI_KEY_VAR), default_model=model)

    if provider == "anthropic":
        model = os.getenv("CHAT_MODEL", "claude-sonnet-4-6")
        return AnthropicChatProvider(api_key=_require_key(_ANTHROPIC_KEY_VAR), default_model=model)

    raise ValueError(f"Unsupported CHAT_PROVIDER: {provider!r} (supported: 'openai', 'anthropic')")


def create_expansion_provider() -> ChatProvider:
    """Query expansion provider. OpenAI only."""
    provider = os.getenv("EXPANSION_PROVIDER", "openai").strip().lower()
    model = os.getenv("EXPANSION_MODEL", "gpt-4o-mini")

    if provider == "openai":
        return OpenAIChatProvider(api_key=_require_key(_OPENAI_KEY_VAR), default_model=model)

    raise ValueError(f"Unsupported EXPANSION_PROVIDER: {provider!r} (only 'openai' supported)")
