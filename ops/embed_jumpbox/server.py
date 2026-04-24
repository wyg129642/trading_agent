"""Qwen3-Embedding-8B FastAPI service — jumpbox side.

OpenAI-compatible `/v1/embeddings` endpoint. Keeps a single model in GPU
memory and serializes batches through one asyncio worker so the GPU never
sees concurrent forward() calls (which OOM on a 40 GB A100 under adversarial
batch sizing).

Endpoints:
    POST /v1/embeddings    OpenAI-compatible embedding request
    GET  /health           liveness + model-load state
    GET  /v1/models        sanity endpoint (returns the served model name)

Pooling follows Qwen3-Embedding's official recipe: **last-token** pooling on
left-padded input + L2 normalization. Do NOT swap in mean pooling or change
the padding side — evaluation scores drop a lot.

Bound to 0.0.0.0 for LAN access from server-yaojinghe (192.168.31.97).
A shared-secret bearer token (EMBED_API_KEY) is enforced as defense in depth.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any, Optional, Union

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger("embed")

# ── Configuration (env-overridable) ────────────────────────────

MODEL_PATH = os.environ.get(
    "EMBED_MODEL_PATH", str(Path.home() / "models" / "Qwen3-Embedding-8B"),
)
SERVED_MODEL_NAME = os.environ.get("EMBED_SERVED_MODEL_NAME", "qwen3-embed")
HOST = os.environ.get("EMBED_HOST", "0.0.0.0")
PORT = int(os.environ.get("EMBED_PORT", "8080"))
API_KEY = os.environ.get("EMBED_API_KEY", "")
MAX_SEQ_LEN = int(os.environ.get("EMBED_MAX_SEQ_LEN", "2048"))
MAX_BATCH_SIZE = int(os.environ.get("EMBED_MAX_BATCH_SIZE", "32"))
# Hard cap on total padded tokens per sub-batch. Picks a smaller effective
# batch when items are long. Prevents OOM from mixed-length padding.
# 16384 = 16 items × 1024 tokens worst case, comfortably fits on 40GB A100
# with Qwen3-Embedding-8B FP16 (~16 GB weights + activations headroom).
MAX_BATCH_TOKENS = int(os.environ.get("EMBED_MAX_BATCH_TOKENS", "16384"))
DEVICE = os.environ.get("EMBED_DEVICE", "cuda:0")
DTYPE = os.environ.get("EMBED_DTYPE", "bfloat16")

# ── Model loader (lazy, single instance) ───────────────────────

_model = None
_tokenizer = None
_model_err: Optional[str] = None
_model_lock = asyncio.Lock()
_embed_sem = asyncio.Semaphore(1)  # one GPU call at a time


def _dtype_from_name(name: str):
    import torch
    return {
        "bfloat16": torch.bfloat16,
        "float16": torch.float16,
        "fp16": torch.float16,
        "bf16": torch.bfloat16,
        "float32": torch.float32,
    }.get(name, torch.bfloat16)


def _load_model_sync():
    """Heavy load — called from a worker thread by /health warmup or first embed."""
    import torch
    from transformers import AutoTokenizer, AutoModel

    logger.info("loading model from %s onto %s (%s)", MODEL_PATH, DEVICE, DTYPE)
    t0 = time.time()
    # Qwen3-Embedding REQUIRES padding_side='left' for correct last-token pooling.
    tok = AutoTokenizer.from_pretrained(MODEL_PATH, padding_side="left")
    mdl = AutoModel.from_pretrained(
        MODEL_PATH,
        torch_dtype=_dtype_from_name(DTYPE),
        attn_implementation=os.environ.get("EMBED_ATTN_IMPL", "eager"),
    ).to(DEVICE)
    mdl.eval()
    logger.info("model loaded in %.1fs", time.time() - t0)
    return tok, mdl


async def _ensure_model():
    global _model, _tokenizer, _model_err
    if _model is not None:
        return _tokenizer, _model
    async with _model_lock:
        if _model is not None:
            return _tokenizer, _model
        try:
            tok, mdl = await asyncio.to_thread(_load_model_sync)
            _tokenizer, _model = tok, mdl
            return _tokenizer, _model
        except Exception as e:
            _model_err = f"{type(e).__name__}: {e}"
            logger.exception("model load failed")
            raise


def _last_token_pool(last_hidden_states, attention_mask):
    """Qwen3-Embedding's recommended pooling. Assumes padding_side='left'."""
    import torch
    left_padding = (attention_mask[:, -1].sum() == attention_mask.shape[0])
    if left_padding:
        return last_hidden_states[:, -1]
    sequence_lengths = attention_mask.sum(dim=1) - 1
    batch_size = last_hidden_states.shape[0]
    return last_hidden_states[torch.arange(batch_size, device=last_hidden_states.device),
                              sequence_lengths]


def _token_budget_batches(
    texts: list[str], tok, max_seq: int, max_batch_size: int, max_batch_tokens: int,
) -> list[list[int]]:
    """Group text indices into sub-batches that stay under a token budget.

    Rationale: HuggingFace tokenizers pad to the longest sequence in the batch,
    so memory is O(batch_size * max_len_in_batch). A few long sequences mixed
    with short ones blows past GPU VRAM. We estimate length (CN-heavy: 1 char
    ≈ 1 token under Qwen3's BBPE; EN: 1 char ≈ 0.25 tokens; mixed: use len(t)
    itself as a conservative upper bound) and cap padded tokens.

    Conservative over-estimation is safer than under-estimation — a smaller
    effective batch is a minor speed hit; an OOM tanks the whole service.
    """
    # Use len(t) clamped to max_seq — this is an upper bound for CJK text.
    # For EN text we overshoot by ~4x which reduces effective batch size but
    # never risks OOM.
    lengths = [min(max(1, len(t)), max_seq) for t in texts]
    order = sorted(range(len(texts)), key=lambda i: lengths[i])
    batches: list[list[int]] = []
    cur: list[int] = []
    cur_max = 0
    for i in order:
        candidate_max = max(cur_max, lengths[i])
        candidate_tokens = (len(cur) + 1) * candidate_max
        if cur and (candidate_tokens > max_batch_tokens or len(cur) >= max_batch_size):
            batches.append(cur)
            cur = [i]
            cur_max = lengths[i]
        else:
            cur.append(i)
            cur_max = candidate_max
    if cur:
        batches.append(cur)
    return batches


async def _embed(texts: list[str]) -> list[list[float]]:
    """Run the model on `texts` with token-budget-aware sub-batching. Serialized on GPU."""
    import torch
    import torch.nn.functional as F

    tok, mdl = await _ensure_model()
    out: list[list[float] | None] = [None] * len(texts)

    async with _embed_sem:
        loop = asyncio.get_running_loop()
        batch_groups = _token_budget_batches(
            texts, tok, MAX_SEQ_LEN, MAX_BATCH_SIZE, MAX_BATCH_TOKENS,
        )
        for idxs in batch_groups:
            batch = [texts[i] for i in idxs]

            def _run_batch(batch=batch):
                with torch.inference_mode():
                    enc = tok(
                        batch,
                        padding=True,
                        truncation=True,
                        max_length=MAX_SEQ_LEN,
                        return_tensors="pt",
                    ).to(mdl.device)
                    hidden = mdl(**enc).last_hidden_state
                    pooled = _last_token_pool(hidden, enc["attention_mask"])
                    normed = F.normalize(pooled, p=2, dim=1)
                    return normed.float().cpu().tolist()

            vecs = await loop.run_in_executor(None, _run_batch)
            for idx, v in zip(idxs, vecs):
                out[idx] = v
    return out  # type: ignore[return-value]


# ── HTTP models ────────────────────────────────────────────────


class EmbedRequest(BaseModel):
    input: Union[str, list[str]]
    model: Optional[str] = None
    # OpenAI spec has these; we accept but mostly ignore.
    encoding_format: Optional[str] = Field(default="float")
    dimensions: Optional[int] = None
    user: Optional[str] = None


class EmbedDatum(BaseModel):
    object: str = "embedding"
    embedding: list[float]
    index: int


class EmbedUsage(BaseModel):
    prompt_tokens: int = 0
    total_tokens: int = 0


class EmbedResponse(BaseModel):
    object: str = "list"
    data: list[EmbedDatum]
    model: str
    usage: EmbedUsage


# ── Auth ──────────────────────────────────────────────────────


def _require_auth(authorization: str = Header(default="")) -> None:
    if not API_KEY:
        return  # auth disabled when no key configured (dev mode)
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing bearer token")
    tok = authorization[len("Bearer "):].strip()
    if tok != API_KEY:
        raise HTTPException(401, "invalid bearer token")


# ── FastAPI app ───────────────────────────────────────────────

app = FastAPI(title="Qwen3-Embedding-8B (jumpbox)", version="1.0.0")


@app.on_event("startup")
async def _warmup():
    """Pre-load model at startup so first request isn't a 60s latency spike."""
    try:
        await _ensure_model()
        # Dry-run one embedding to JIT CUDA kernels.
        await _embed(["warmup"])
        logger.info("warmup embedding done")
    except Exception as e:
        logger.error("startup warmup failed: %s", e)


@app.get("/health")
async def health():
    try:
        import torch
        cuda_ok = torch.cuda.is_available()
        devs = [torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())]
    except Exception as e:
        return {"status": "error", "model_loaded": False, "cuda": False, "error": str(e)}
    return {
        "status": "ok" if _model is not None else "loading",
        "model_loaded": _model is not None,
        "model_error": _model_err,
        "cuda": cuda_ok,
        "cuda_devices": devs,
        "served_model_name": SERVED_MODEL_NAME,
    }


@app.get("/v1/models")
async def list_models(_=Depends(_require_auth)):
    return {
        "object": "list",
        "data": [{
            "id": SERVED_MODEL_NAME,
            "object": "model",
            "owned_by": "jumpbox",
        }],
    }


@app.post("/v1/embeddings", response_model=EmbedResponse)
async def embeddings(body: EmbedRequest, _=Depends(_require_auth)):
    texts: list[str]
    if isinstance(body.input, str):
        texts = [body.input]
    else:
        texts = list(body.input)
    if not texts:
        raise HTTPException(400, "input is empty")
    if len(texts) > 512:
        raise HTTPException(400, "input exceeds 512 items per request")

    t0 = time.monotonic()
    try:
        vecs = await _embed(texts)
    except Exception as e:
        logger.exception("embedding failed")
        raise HTTPException(500, f"embedding error: {e}")
    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info("embed batch=%d dim=%d elapsed=%dms", len(texts), len(vecs[0]) if vecs else 0, elapsed_ms)

    return EmbedResponse(
        data=[EmbedDatum(embedding=v, index=i) for i, v in enumerate(vecs)],
        model=body.model or SERVED_MODEL_NAME,
        usage=EmbedUsage(),
    )


if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
    )
    uvicorn.run(app, host=HOST, port=PORT, log_level="info")
