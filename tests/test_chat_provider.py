"""Unit tests for chat_provider.py — all mocked, no API key needed."""

import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from api.chat_provider import (
    ChatProvider,
    OpenAIChatProvider,
    AnthropicChatProvider,
    create_classifier_provider,
    create_chat_provider,
    create_expansion_provider,
)


# ---------------------------------------------------------------------------
# Helper: build a provider with a mocked internal client
# ---------------------------------------------------------------------------

def _make_openai_provider(default_model="gpt-4o-mini"):
    with patch("openai.OpenAI"):
        provider = OpenAIChatProvider(api_key="fake-key", default_model=default_model)
    provider._client = MagicMock()
    return provider


def _make_anthropic_provider(default_model="claude-sonnet-4-20250514"):
    with patch("anthropic.Anthropic"):
        provider = AnthropicChatProvider(api_key="fake-key", default_model=default_model)
    provider._client = MagicMock()
    return provider


# ---------------------------------------------------------------------------
# OpenAIChatProvider
# ---------------------------------------------------------------------------

class TestOpenAIChatProvider:

    @pytest.mark.asyncio
    async def test_complete_plain_text(self):
        """Basic completion returns content string."""
        provider = _make_openai_provider()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="Hello world"))]
        provider._client.chat.completions.create.return_value = mock_response

        async def _fake_to_thread(fn, **kw):
            return fn(**kw)

        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            result = await provider.complete(
                [{"role": "user", "content": "Hi"}],
                temperature=0.5,
            )

        assert result == "Hello world"
        call_kwargs = provider._client.chat.completions.create.call_args[1]
        assert call_kwargs["model"] == "gpt-4o-mini"
        assert call_kwargs["temperature"] == 0.5
        assert "response_format" not in call_kwargs

    @pytest.mark.asyncio
    async def test_complete_json_mode(self):
        """json_mode=True passes response_format to OpenAI."""
        provider = _make_openai_provider()
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content='{"key": "val"}'))]
        provider._client.chat.completions.create.return_value = mock_response

        async def _fake_to_thread(fn, **kw):
            return fn(**kw)

        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            result = await provider.complete(
                [{"role": "system", "content": "Return JSON"}, {"role": "user", "content": "Go"}],
                json_mode=True,
            )

        assert json.loads(result) == {"key": "val"}
        call_kwargs = provider._client.chat.completions.create.call_args[1]
        assert call_kwargs["response_format"] == {"type": "json_object"}

    @pytest.mark.asyncio
    async def test_model_override(self):
        """Explicit model kwarg overrides default_model."""
        provider = _make_openai_provider(default_model="gpt-4o-mini")
        mock_response = MagicMock()
        mock_response.choices = [MagicMock(message=MagicMock(content="ok"))]
        provider._client.chat.completions.create.return_value = mock_response

        async def _fake_to_thread(fn, **kw):
            return fn(**kw)

        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            await provider.complete(
                [{"role": "user", "content": "test"}],
                model="gpt-4o",
            )

        call_kwargs = provider._client.chat.completions.create.call_args[1]
        assert call_kwargs["model"] == "gpt-4o"


# ---------------------------------------------------------------------------
# AnthropicChatProvider
# ---------------------------------------------------------------------------

class TestAnthropicChatProvider:

    @pytest.mark.asyncio
    async def test_complete_extracts_system_message(self):
        """System message is passed as system= kwarg, not in messages list."""
        provider = _make_anthropic_provider()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="Response")]
        provider._client.messages.create.return_value = mock_response

        async def _fake_to_thread(fn, **kw):
            return fn(**kw)

        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            result = await provider.complete([
                {"role": "system", "content": "You are helpful"},
                {"role": "user", "content": "Hi"},
            ])

        assert result == "Response"
        call_kwargs = provider._client.messages.create.call_args[1]
        assert call_kwargs["system"] == "You are helpful"
        assert len(call_kwargs["messages"]) == 1
        assert call_kwargs["messages"][0]["role"] == "user"

    @pytest.mark.asyncio
    async def test_json_mode_appends_instruction(self):
        """json_mode=True appends JSON instruction to system prompt."""
        provider = _make_anthropic_provider()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='{"x": 1}')]
        provider._client.messages.create.return_value = mock_response

        async def _fake_to_thread(fn, **kw):
            return fn(**kw)

        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            await provider.complete(
                [{"role": "system", "content": "Be brief"}, {"role": "user", "content": "Go"}],
                json_mode=True,
            )

        call_kwargs = provider._client.messages.create.call_args[1]
        assert "Respond ONLY with valid JSON" in call_kwargs["system"]
        assert call_kwargs["system"].startswith("Be brief")

    @pytest.mark.asyncio
    async def test_json_mode_no_system_message(self):
        """json_mode=True with no system message creates one."""
        provider = _make_anthropic_provider()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text='{}')]
        provider._client.messages.create.return_value = mock_response

        async def _fake_to_thread(fn, **kw):
            return fn(**kw)

        with patch("asyncio.to_thread", side_effect=_fake_to_thread):
            await provider.complete(
                [{"role": "user", "content": "Go"}],
                json_mode=True,
            )

        call_kwargs = provider._client.messages.create.call_args[1]
        assert call_kwargs["system"] == "Respond ONLY with valid JSON."


# ---------------------------------------------------------------------------
# Factory tests
# ---------------------------------------------------------------------------

class TestFactories:

    def test_classifier_defaults_openai(self):
        """create_classifier_provider() defaults to OpenAI gpt-4o."""
        with patch.dict("os.environ", {"OPENAI_API_KEY": "fake"}, clear=False), \
             patch("openai.OpenAI"):
            p = create_classifier_provider()
        assert isinstance(p, OpenAIChatProvider)
        assert p.default_model == "gpt-4o"

    def test_classifier_rejects_anthropic(self):
        """Classifier does not support Anthropic."""
        with patch.dict("os.environ", {"CLASSIFIER_PROVIDER": "anthropic", "ANTHROPIC_API_KEY": "fake"}, clear=False):
            with pytest.raises(ValueError, match="Unsupported CLASSIFIER_PROVIDER"):
                create_classifier_provider()

    def test_classifier_missing_key_raises(self):
        """Missing OPENAI_API_KEY raises ValueError."""
        with patch.dict("os.environ", {"OPENAI_API_KEY": ""}, clear=False):
            with pytest.raises(ValueError, match="OPENAI_API_KEY"):
                create_classifier_provider()

    def test_chat_provider_openai_default(self):
        """create_chat_provider() defaults to OpenAI."""
        with patch.dict("os.environ", {"OPENAI_API_KEY": "fake", "CHAT_LLM_MODEL": "gpt-4o-mini"}, clear=False), \
             patch("openai.OpenAI"):
            p = create_chat_provider()
        assert isinstance(p, OpenAIChatProvider)
        assert p.default_model == "gpt-4o-mini"

    def test_chat_provider_anthropic(self):
        """CHAT_PROVIDER=anthropic creates AnthropicChatProvider."""
        with patch.dict("os.environ", {
            "CHAT_PROVIDER": "anthropic",
            "ANTHROPIC_API_KEY": "fake",
            "CHAT_MODEL": "claude-sonnet-4-20250514",
        }, clear=False), \
             patch("anthropic.Anthropic"):
            p = create_chat_provider()
        assert isinstance(p, AnthropicChatProvider)

    def test_chat_provider_unsupported_raises(self):
        """Unknown CHAT_PROVIDER raises ValueError."""
        with patch.dict("os.environ", {"CHAT_PROVIDER": "gemini"}, clear=False):
            with pytest.raises(ValueError, match="Unsupported CHAT_PROVIDER"):
                create_chat_provider()

    def test_expansion_defaults_openai(self):
        """create_expansion_provider() defaults to OpenAI gpt-4o-mini."""
        with patch.dict("os.environ", {"OPENAI_API_KEY": "fake"}, clear=False), \
             patch("openai.OpenAI"):
            p = create_expansion_provider()
        assert isinstance(p, OpenAIChatProvider)
        assert p.default_model == "gpt-4o-mini"

    def test_expansion_rejects_anthropic(self):
        """Expansion does not support Anthropic."""
        with patch.dict("os.environ", {"EXPANSION_PROVIDER": "anthropic", "ANTHROPIC_API_KEY": "fake"}, clear=False):
            with pytest.raises(ValueError, match="Unsupported EXPANSION_PROVIDER"):
                create_expansion_provider()

    def test_chat_model_env_precedence(self):
        """CHAT_MODEL takes precedence over CHAT_LLM_MODEL."""
        with patch.dict("os.environ", {
            "OPENAI_API_KEY": "fake",
            "CHAT_MODEL": "gpt-4o",
            "CHAT_LLM_MODEL": "gpt-4o-mini",
        }, clear=False), \
             patch("openai.OpenAI"):
            p = create_chat_provider()
        assert p.default_model == "gpt-4o"

    def test_classifier_custom_model(self):
        """CLASSIFIER_MODEL overrides default gpt-4o."""
        with patch.dict("os.environ", {
            "OPENAI_API_KEY": "fake",
            "CLASSIFIER_MODEL": "gpt-4.1",
        }, clear=False), \
             patch("openai.OpenAI"):
            p = create_classifier_provider()
        assert p.default_model == "gpt-4.1"


# ---------------------------------------------------------------------------
# Endpoint-level: Anthropic provider model respected end-to-end
# ---------------------------------------------------------------------------

class TestEndToEndProviderModel:

    @pytest.mark.asyncio
    async def test_anthropic_chat_provider_uses_chat_model(self):
        """CHAT_PROVIDER=anthropic + CHAT_MODEL=claude-sonnet-4-20250514
        -> provider.complete() called with the Anthropic model, not gpt-4o-mini."""
        from api.schemas.query_plan import EvidenceBundle, RetrievalOp, SourceType

        entity_bundle = EvidenceBundle(
            source_uri="urn:e:1", source_type=SourceType.LOCAL_AUTHORITATIVE,
            retrieval_op=RetrievalOp.ENTITY_LOOKUP, confidence=0.9,
            text="Mock entity",
            metadata={"entity_type": "Concept", "label": "Test", "fuseki_uri": "urn:e:1"},
        )

        mock_classifier = AsyncMock()
        mock_expansion = AsyncMock()

        # Chat answer provider: track the model it receives
        mock_chat = AsyncMock()
        mock_chat.complete = AsyncMock(return_value="Anthropic answer")
        mock_chat.default_model = "claude-sonnet-4-20250514"

        mock_cm = AsyncMock()
        mock_conn = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_pool = MagicMock()
        mock_pool.acquire.return_value = mock_cm

        with patch("api.personal_ingest_api.db_pool", mock_pool), \
             patch("api.personal_ingest_api.classifier_provider", mock_classifier), \
             patch("api.personal_ingest_api.chat_answer_provider", mock_chat), \
             patch("api.personal_ingest_api.expansion_provider", mock_expansion), \
             patch("api.personal_ingest_api.generate_embedding", AsyncMock(return_value=[0.1]*1536)), \
             patch("api.personal_ingest_api._try_structured_graph_query", AsyncMock(return_value="")), \
             patch("api.retrieval_executors.entity_lookup", AsyncMock(return_value=[entity_bundle])), \
             patch("api.retrieval_executors.relationship_traverse", AsyncMock(return_value=[])), \
             patch("api.retrieval_executors.text_search", AsyncMock(return_value=[])), \
             patch("api.retrieval_executors.web_source_lookup", AsyncMock(return_value=[])):

            from api.personal_ingest_api import chat_endpoint, ChatRequest
            response = await chat_endpoint(ChatRequest(query="What is eelgrass?"))

        assert response["answer"] == "Anthropic answer"
        # The call should NOT pass model= (uses provider default_model)
        call_kwargs = mock_chat.complete.call_args
        assert "model" not in call_kwargs.kwargs or call_kwargs.kwargs.get("model") is None


# ---------------------------------------------------------------------------
# Live smoke tests: they send paid requests, so they run only with KOI_TEST_LLM=1 and
# the provider's key (tests/koi_test_targets.py requires_live_llm).
# ---------------------------------------------------------------------------

from tests import koi_test_targets as _targets  # noqa: E402


@pytest.mark.live
@_targets.requires_live_llm("OPENAI_API_KEY")
@pytest.mark.asyncio
async def test_openai_live_smoke():
    """Quick OpenAI round-trip."""
    provider = create_chat_provider()
    result = await provider.complete(
        [{"role": "user", "content": "Say 'hello' and nothing else."}],
        max_tokens=10,
    )
    assert "hello" in result.lower()


@pytest.mark.live
@_targets.requires_live_llm("ANTHROPIC_API_KEY")
@pytest.mark.asyncio
async def test_anthropic_live_smoke(monkeypatch):
    """Quick Anthropic round-trip (needs ANTHROPIC_API_KEY)."""
    monkeypatch.setenv("CHAT_PROVIDER", "anthropic")
    provider = create_chat_provider()
    result = await provider.complete(
        [{"role": "user", "content": "Say 'hello' and nothing else."}],
        max_tokens=10,
    )
    assert "hello" in result.lower()
