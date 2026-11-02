"""
Embedding provider abstraction.

Supports OpenAI, Ollama, sentence-transformers, and remote (poly) backends
with a factory that reads env vars.

Query/document split:
  - embed() / embed_batch() = DOCUMENT mode (no instruction prefix)
  - embed_query() = QUERY mode (with instruction prefix for retrieval)

Phase 8 B2 (2026-04-29): per-prompt-type token tracking. The abstract
`EmbeddingProvider.embed_or_none` accepts a `prompt_type` parameter and emits
one JSONL record per call to `~/.koi/logs/embedding-tokens.jsonl`.
"""

import asyncio
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


# ─── B2: per-prompt-type token tracking ──────────────────────────────────────
_METRICS_DIR = Path(os.path.expanduser("~/.koi/logs"))
_METRICS_PATH = _METRICS_DIR / "embedding-tokens.jsonl"
_VALID_PROMPT_TYPES = {"extraction", "dedup", "query", "rerank", "unknown"}


# ─── Pack 2.2 (2026-04-28): per-request fallback-fired observability ────────
# ContextVar tracks whether the FallbackChainEmbeddingProvider had to fall
# through to the secondary provider on a query path. Async-safe across
# await boundaries (each request gets its own context). Read by the
# unified-search handler to inject `degraded_embedding: true` into responses
# when the primary embedding provider failed but the fallback recovered the
# read.
_fallback_fired: ContextVar[bool] = ContextVar("koi_embedding_fallback_fired", default=False)


def reset_fallback_fired() -> None:
    """Reset the fallback-fired marker for the current async context.

    Callers (request handlers) should invoke this at request entry so a
    prior request's flag does not bleed in. Note: ContextVar values are
    already scoped per-task, so explicit reset is belt-and-suspenders;
    cheap and unambiguous.
    """
    _fallback_fired.set(False)


def was_fallback_fired() -> bool:
    """Return True if FallbackChainEmbeddingProvider fell through during
    the current async context's most recent embed_query call(s)."""
    return _fallback_fired.get()


def _emit_embedding_metric(
    provider: str,
    model: str,
    prompt_type: str,
    text_len: int,
    prompt_tokens: Optional[int],
    is_query: bool,
    duration_ms: float,
    ok: bool,
    is_batch: bool = False,
    batch_size: Optional[int] = None,
) -> None:
    """Best-effort metric emission. Never raises (would block embedding calls).

    Wave 3 C4 (2026-04-30): supports batch records via `is_batch=true` +
    `batch_size`. For batches, `text_len` is the sum of per-text char lengths
    and `prompt_tokens` is the aggregate (when known). One metric line per
    batch call (not per text within the batch).
    """
    try:
        if prompt_type not in _VALID_PROMPT_TYPES:
            prompt_type = "unknown"
        _METRICS_DIR.mkdir(parents=True, exist_ok=True)
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "provider": provider,
            "model": model,
            "prompt_type": prompt_type,
            "is_query": is_query,
            "text_chars": text_len,
            "prompt_tokens": prompt_tokens,  # exact for OpenAI; None elsewhere
            "duration_ms": round(duration_ms, 1),
            "ok": ok,
            "is_batch": is_batch,
        }
        if is_batch:
            rec["batch_size"] = batch_size
        with _METRICS_PATH.open("a") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception:
        # metrics emission MUST NEVER block the caller
        pass


class EmbeddingProvider(ABC):
    """Abstract base class for embedding providers."""

    dimension: int
    model_name: str

    @abstractmethod
    async def embed(self, text: str) -> List[float]:
        """Generate embedding for a single text (DOCUMENT mode). Raises on failure."""

    async def embed_query(self, text: str) -> List[float]:
        """Generate embedding for a query (QUERY mode — with instruction prefix).

        Default: same as embed(). Subclasses override for instruction-aware models.
        """
        return await self.embed(text)

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple texts (DOCUMENT mode). Default: sequential."""
        return [await self.embed(t) for t in texts]

    async def embed_batch_or_none(
        self,
        texts: List[str],
        prompt_type: str = "unknown",
    ) -> Optional[List[List[float]]]:
        """Generate batch embeddings with B2/C4 token-tracking metric emission.

        Wave 3 C4 (2026-04-30): one aggregate JSONL record per batch call
        (not per text). Token count is the OpenAI-reported aggregate when
        the provider supplies it via `_last_prompt_tokens`. Returns None
        on failure (matches embed_or_none semantics).
        """
        t0 = time.monotonic()
        provider_name = type(self).__name__.replace("EmbeddingProvider", "").lower()
        text_chars = sum(len(t or "") for t in texts)
        batch_size = len(texts)
        try:
            embs = await self.embed_batch(texts)
            duration_ms = (time.monotonic() - t0) * 1000
            tokens = getattr(self, "_last_prompt_tokens", None)
            _emit_embedding_metric(
                provider=provider_name,
                model=self.model_name,
                prompt_type=prompt_type,
                text_len=text_chars,
                prompt_tokens=tokens,
                is_query=False,
                duration_ms=duration_ms,
                ok=True,
                is_batch=True,
                batch_size=batch_size,
            )
            return embs
        except Exception as e:
            duration_ms = (time.monotonic() - t0) * 1000
            logger.warning(f"Batch embedding failed ({self.model_name}): {e}")
            _emit_embedding_metric(
                provider=provider_name,
                model=self.model_name,
                prompt_type=prompt_type,
                text_len=text_chars,
                prompt_tokens=None,
                is_query=False,
                duration_ms=duration_ms,
                ok=False,
                is_batch=True,
                batch_size=batch_size,
            )
            return None

    async def embed_or_none(
        self,
        text: str,
        is_query: bool = False,
        prompt_type: str = "unknown",
    ) -> Optional[List[float]]:
        """Generate embedding, returning None on failure instead of raising.

        Phase 8 B2: emits a JSONL metric to `~/.koi/logs/embedding-tokens.jsonl`
        with `prompt_type` tag. Valid prompt_type values:
          extraction | dedup | query | rerank | unknown
        Unrecognized values are coerced to 'unknown' in the metric line.
        Token count is exact for OpenAI; None for Ollama/SentenceTransformer/Remote.
        """
        t0 = time.monotonic()
        provider_name = type(self).__name__.replace("EmbeddingProvider", "").lower()
        try:
            if is_query:
                emb = await self.embed_query(text)
            else:
                emb = await self.embed(text)
            duration_ms = (time.monotonic() - t0) * 1000
            tokens = getattr(self, "_last_prompt_tokens", None)
            _emit_embedding_metric(
                provider=provider_name,
                model=self.model_name,
                prompt_type=prompt_type,
                text_len=len(text or ""),
                prompt_tokens=tokens,
                is_query=is_query,
                duration_ms=duration_ms,
                ok=True,
            )
            return emb
        except Exception as e:
            duration_ms = (time.monotonic() - t0) * 1000
            logger.warning(f"Embedding failed ({self.model_name}): {e}")
            _emit_embedding_metric(
                provider=provider_name,
                model=self.model_name,
                prompt_type=prompt_type,
                text_len=len(text or ""),
                prompt_tokens=None,
                is_query=is_query,
                duration_ms=duration_ms,
                ok=False,
            )
            return None


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """OpenAI embeddings via the openai package (existing dep)."""

    def __init__(self, api_key: str, model: str = "text-embedding-ada-002",
                 dimension: Optional[int] = None):
        from openai import OpenAI
        from api.provider_http import (
            provider_sync_client, provider_timeout, PROVIDER_MAX_RETRIES)
        # #36: SDK defaults are Timeout(read=600, write=600, pool=600) with
        # max_retries=2 — i.e. up to 600s x 3 = 30 MINUTES hanging on one embed call.
        # That is what wedged the shared :8351 service fleet-wide on 2026-07-15: these
        # calls run under asyncio.to_thread, so a stuck provider socket parks a
        # ThreadPoolExecutor worker, and once the pool is exhausted every to_thread
        # caller stalls and the service is effectively down. Bound per phase, bound the
        # retries, and hand the SDK a client whose pool evicts idle (possibly
        # CLOSE_WAIT) sockets instead of reusing them.
        self._client = OpenAI(
            api_key=api_key,
            timeout=provider_timeout(),
            max_retries=PROVIDER_MAX_RETRIES,
            http_client=provider_sync_client(),
        )
        self.model_name = model
        self.dimension = dimension or self._default_dimension(model)

    @staticmethod
    def _default_dimension(model: str) -> int:
        defaults = {
            "text-embedding-ada-002": 1536,
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
        }
        return defaults.get(model, 1536)

    def _supports_dimensions(self) -> bool:
        """`dimensions` param is supported on text-embedding-3-* models, not ada-002."""
        return self.model_name.startswith("text-embedding-3-")

    def _create_kwargs(self, input_):
        kwargs = {"model": self.model_name, "input": input_}
        if self._supports_dimensions():
            kwargs["dimensions"] = self.dimension
        return kwargs

    async def embed(self, text: str) -> List[float]:
        response = await asyncio.to_thread(
            self._client.embeddings.create,
            **self._create_kwargs(text),
        )
        # B2: capture prompt_tokens for token-tracking metric.
        try:
            self._last_prompt_tokens = int(response.usage.prompt_tokens)
        except Exception:
            self._last_prompt_tokens = None
        return response.data[0].embedding

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        response = await asyncio.to_thread(
            self._client.embeddings.create,
            **self._create_kwargs(texts),
        )
        try:
            self._last_prompt_tokens = int(response.usage.prompt_tokens)
        except Exception:
            self._last_prompt_tokens = None
        return [d.embedding for d in response.data]


class OllamaEmbeddingProvider(EmbeddingProvider):
    """Ollama embeddings via HTTP (uses httpx, existing dep)."""

    def __init__(self, model: str = "nomic-embed-text",
                 base_url: str = "http://localhost:11434",
                 dimension: Optional[int] = None):
        import httpx
        self._client = httpx.AsyncClient(base_url=base_url, timeout=120.0)
        self.model_name = model
        self.dimension = dimension or self._default_dimension(model)

    @staticmethod
    def _default_dimension(model: str) -> int:
        defaults = {
            "nomic-embed-text": 768,
            "mxbai-embed-large": 1024,
            "bge-large": 1024,
            "all-minilm": 384,
        }
        return defaults.get(model, 768)

    async def embed(self, text: str) -> List[float]:
        resp = await self._client.post("/api/embeddings", json={
            "model": self.model_name,
            "prompt": text,
        })
        resp.raise_for_status()
        return resp.json()["embedding"]

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Batch embed using /api/embed endpoint (Ollama 0.4+)."""
        resp = await self._client.post("/api/embed", json={
            "model": self.model_name,
            "input": texts,
        })
        resp.raise_for_status()
        return resp.json()["embeddings"]


class SentenceTransformerProvider(EmbeddingProvider):
    """Local sentence-transformers embeddings (no network calls)."""

    def __init__(self, model: str = "BAAI/bge-large-en-v1.5",
                 dimension: Optional[int] = None):
        from sentence_transformers import SentenceTransformer
        self._model = SentenceTransformer(model)
        self.model_name = model
        self.dimension = dimension or self._model.get_sentence_embedding_dimension()
        self._has_query_prompt = "query" in (self._model.prompts or {})
        logger.info(f"SentenceTransformer loaded: {model} (dim={self.dimension}, query_prompt={self._has_query_prompt})")

    async def embed(self, text: str) -> List[float]:
        """Embed a single text (DOCUMENT mode). No instruction prefix."""
        embedding = await asyncio.to_thread(
            self._model.encode, text, normalize_embeddings=True
        )
        return embedding.tolist()

    async def embed_query(self, text: str) -> List[float]:
        """Embed a query (QUERY mode). Uses prompt_name='query' for instruction-aware models."""
        kwargs = {"normalize_embeddings": True}
        if self._has_query_prompt:
            kwargs["prompt_name"] = "query"
        embedding = await asyncio.to_thread(
            self._model.encode, text, **kwargs
        )
        return embedding.tolist()

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed multiple texts (DOCUMENT mode). No instruction prefix."""
        embeddings = await asyncio.to_thread(
            self._model.encode, texts, normalize_embeddings=True
        )
        return [e.tolist() for e in embeddings]


class RemoteEmbeddingProvider(EmbeddingProvider):
    """Remote embedding service (e.g. poly FastAPI). Query/document split handled server-side."""

    def __init__(self, base_url: str, dimension: int = 1024,
                 model: str = "Qwen/Qwen3-Embedding-0.6B"):
        import httpx
        self._client = httpx.AsyncClient(
            base_url=base_url,
            # read=25s stays under MCP's 30s client budget so callers hit the
            # degrade-to-ILIKE fallback in unified-search rather than a raw
            # timeout. Healthy /embed latency is <1s; 25s is ~25× headroom.
            timeout=httpx.Timeout(connect=5.0, read=25.0, write=5.0, pool=5.0),
        )
        self.model_name = model
        self.dimension = dimension
        self._base_url = base_url

    async def _embed_texts(self, texts: List[str], is_query: bool) -> List[List[float]]:
        """Call remote /embed endpoint with retry."""
        payload = {"texts": texts, "is_query": is_query}
        last_err = None
        for attempt in range(2):
            try:
                resp = await self._client.post("/embed", json=payload)
                resp.raise_for_status()
                return resp.json()["embeddings"]
            except Exception as e:
                last_err = e
                if attempt == 0:
                    logger.warning(f"Remote embed attempt 1 failed ({e}), retrying...")
                    await asyncio.sleep(0.5)
        raise RuntimeError(f"Remote embedding failed after 2 attempts: {last_err}")

    async def embed(self, text: str) -> List[float]:
        """DOCUMENT mode — no instruction prefix."""
        results = await self._embed_texts([text], is_query=False)
        return results[0]

    async def embed_query(self, text: str) -> List[float]:
        """QUERY mode — with instruction prefix."""
        results = await self._embed_texts([text], is_query=True)
        return results[0]

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """DOCUMENT mode batch."""
        return await self._embed_texts(texts, is_query=False)


class FallbackChainEmbeddingProvider(EmbeddingProvider):
    """Wave A A3 (2026-05-01): composing provider with primary + read-side fallback.

    Wraps a `primary` provider (e.g. OpenAI 3072-dim) and a `fallback` provider
    (e.g. Ollama nomic-embed-text 768-dim). On QUERY-path failure of the primary,
    falls through to the fallback, zero-pads the result to primary's dimension,
    and tags the metric line with `prompt_type` unchanged plus `provider` reflecting
    "openaiemb-fallback" so observers see the degraded read.

    WRITE-path (embed/embed_batch) NEVER falls back. Writes that fail surface
    via Wave A A2's null-embed observability; reads stay functional but degraded
    quality during outage. (A2 + A3 are paired: A3 keeps reads alive when
    primary is down; A2 makes silent-fail writes visible.)
    """

    def __init__(
        self,
        primary: "EmbeddingProvider",
        fallback: "EmbeddingProvider",
    ):
        self._primary = primary
        self._fallback = fallback
        # Report primary dimension externally so cosine queries against existing
        # 3072-dim documents don't break. Fallback embeddings are zero-padded.
        self.model_name = primary.model_name
        self.dimension = primary.dimension

    async def embed(self, text: str) -> List[float]:
        # WRITE path: never falls back. Raises on primary failure so callers
        # (embed_or_none) see None and the A2 path emits warning + counter.
        return await self._primary.embed(text)

    async def embed_query(self, text: str) -> List[float]:
        # QUERY path: try primary; on any exception, fall back to secondary
        # and zero-pad the result up to primary dimension.
        # Pack 2.2: clear the per-context fallback marker on entry; set it
        # if the fallback path actually fires. Handlers read the marker
        # after the embedding call to surface `degraded_embedding: true`
        # at response scope.
        _fallback_fired.set(False)
        try:
            emb = await self._primary.embed_query(text)
            return emb
        except Exception as e:
            logger.warning(
                "Primary embedding provider failed on query path (%s); "
                "falling back to %s. Read quality will be degraded.",
                type(self._primary).__name__,
                type(self._fallback).__name__,
            )
            _fallback_fired.set(True)
            fb_emb = await self._fallback.embed_query(text)
            return _pad_to_dim(fb_emb, self.dimension)

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        # WRITE path batch: never falls back.
        return await self._primary.embed_batch(texts)


def _pad_to_dim(vec: List[float], target_dim: int) -> List[float]:
    """Zero-pad or truncate `vec` to `target_dim`. Used by FallbackChain to
    align fallback-provider output (e.g. 768-dim) with primary dim (3072) so
    downstream halfvec(3072) cosine queries don't error.
    Note: pad-to-zero is a degraded-quality signal (cosine valid but
    information-poor across the padded dims). Documented in class docstring.
    """
    if len(vec) == target_dim:
        return vec
    if len(vec) > target_dim:
        return list(vec[:target_dim])
    return list(vec) + [0.0] * (target_dim - len(vec))


def create_embedding_provider() -> Optional[EmbeddingProvider]:
    """
    Factory: read env vars and return a provider (or None if disabled).

    Precedence:
      EMBEDDING_PROVIDER="openai"  → requires OPENAI_API_KEY
      EMBEDDING_PROVIDER="ollama"  → uses OLLAMA_BASE_URL
      EMBEDDING_PROVIDER=""        → disabled (explicit)
      EMBEDDING_PROVIDER unset + OPENAI_API_KEY set → auto-select openai
      EMBEDDING_PROVIDER unset + no key             → disabled
    """
    provider_env = os.environ.get("EMBEDDING_PROVIDER")  # None if unset
    model = os.getenv("EMBEDDING_MODEL", "")
    dim_str = os.getenv("EMBEDDING_DIMENSION", "")
    dim = int(dim_str) if dim_str else None

    # Explicit empty string = disabled
    if provider_env is not None and provider_env.strip() == "":
        logger.info("Embedding provider explicitly disabled (EMBEDDING_PROVIDER='')")
        return None

    # Determine effective provider
    if provider_env is not None:
        provider_name = provider_env.strip().lower()
    else:
        # Backward compat: unset EMBEDDING_PROVIDER + key present → openai
        if os.getenv("OPENAI_API_KEY", ""):
            provider_name = "openai"
            logger.info("Auto-selecting openai embedding provider (OPENAI_API_KEY present)")
        else:
            logger.info("No embedding provider configured (no EMBEDDING_PROVIDER, no OPENAI_API_KEY)")
            return None

    primary = _build_provider(provider_name, model, dim)
    if primary is None:
        return None

    # Wave A A3 (2026-05-01): optional read-side fallback chain.
    # When EMBEDDING_FALLBACK is set, compose primary + fallback into a
    # FallbackChainEmbeddingProvider. Currently supported fallback shapes:
    #   EMBEDDING_FALLBACK="ollama"                 → Ollama nomic-embed-text
    #   EMBEDDING_FALLBACK="ollama:<model_name>"    → Ollama with named model
    # Fallback fires only on QUERY path; writes never fall back (A2 surfaces).
    fallback_env = os.environ.get("EMBEDDING_FALLBACK", "").strip().lower()
    if fallback_env:
        fb_kind, _, fb_model = fallback_env.partition(":")
        try:
            if fb_kind == "ollama":
                base_url = os.getenv(
                    "EMBEDDING_FALLBACK_OLLAMA_URL",
                    os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
                )
                fb = OllamaEmbeddingProvider(
                    model=fb_model or "nomic-embed-text",
                    base_url=base_url,
                )
                logger.info(
                    "Embedding fallback enabled: ollama/%s @ %s (dim=%s); "
                    "primary=%s (dim=%s); query-path only, "
                    "writes always use primary.",
                    fb.model_name, base_url, fb.dimension,
                    primary.model_name, primary.dimension,
                )
                return FallbackChainEmbeddingProvider(primary=primary, fallback=fb)
            else:
                logger.warning(
                    "Unknown EMBEDDING_FALLBACK=%r; ignoring (no fallback chain).",
                    fallback_env,
                )
        except Exception as e:
            logger.warning(
                "Failed to construct embedding fallback (%s); continuing without. %s",
                fallback_env, e,
            )
    return primary


def _build_provider(
    provider_name: str, model: str, dim: Optional[int]
) -> Optional[EmbeddingProvider]:
    """Construct a single embedding provider (no fallback chain).
    Extracted from create_embedding_provider for A3 composition."""
    if provider_name == "openai":
        api_key = os.getenv("OPENAI_API_KEY", "")
        if not api_key:
            logger.error("EMBEDDING_PROVIDER=openai but OPENAI_API_KEY is not set")
            raise SystemExit(1)
        p = OpenAIEmbeddingProvider(
            api_key=api_key,
            model=model or "text-embedding-ada-002",
            dimension=dim,
        )
        logger.info(f"Embedding provider: {p.model_name} (dim={p.dimension})")
        return p

    if provider_name == "ollama":
        base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        p = OllamaEmbeddingProvider(
            model=model or "nomic-embed-text",
            base_url=base_url,
            dimension=dim,
        )
        logger.info(f"Embedding provider: ollama/{p.model_name} (dim={p.dimension})")
        return p

    if provider_name == "sentence_transformers":
        p = SentenceTransformerProvider(
            model=model or "BAAI/bge-large-en-v1.5",
            dimension=dim,
        )
        logger.info(f"Embedding provider: st/{p.model_name} (dim={p.dimension})")
        return p

    if provider_name == "remote":
        remote_url = os.getenv("EMBEDDING_REMOTE_URL", "")
        if not remote_url:
            logger.error("EMBEDDING_PROVIDER=remote but EMBEDDING_REMOTE_URL is not set")
            raise SystemExit(1)
        p = RemoteEmbeddingProvider(
            base_url=remote_url,
            dimension=dim or 1024,
            model=model or "Qwen/Qwen3-Embedding-0.6B",
        )
        logger.info(f"Embedding provider: remote/{p.model_name} @ {remote_url} (dim={p.dimension})")
        return p

    logger.error(f"Unknown EMBEDDING_PROVIDER: {provider_name!r}")
    raise SystemExit(1)
