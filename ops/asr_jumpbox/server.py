"""Qwen3-ASR-1.7B FastAPI service — jumpbox side.

Thin HTTP wrapper around the `qwen-asr` Python package. Keeps a single
Qwen3ASRModel in-process, serializes transcription through one asyncio
worker so the GPU never sees concurrent transcribe() calls (which OOM
unpredictably on a 40 GB A100 with realistic meeting-length inputs).

Exposes a job-oriented API:

    POST   /transcribe   multipart upload -> {job_id, status}
    GET    /jobs/{id}    poll -> {status, percent, phase, text, language, error, ...}
    DELETE /jobs/{id}    cancel a queued or running job
    GET    /health       liveness + GPU + model-load state

Long audio is split into fixed-size segments with ffmpeg (via
imageio-ffmpeg's bundled binary) and transcribed one segment at a time
so per-segment progress can be reported back to the caller. Short clips
go through the model in a single pass.

Intended to be bound to 127.0.0.1 on the jumpbox; external access is
via a supervised SSH tunnel from the web server. A shared-secret bearer
token (ASR_API_KEY env) is still enforced as defense in depth.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import shutil
import subprocess
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal, Optional

from fastapi import Depends, FastAPI, File, Header, HTTPException, UploadFile
from fastapi.responses import JSONResponse

logger = logging.getLogger("asr")


# ── Configuration (env-overridable) ────────────────────────────

MODEL_PATH = os.environ.get(
    "ASR_MODEL_PATH", str(Path.home() / "models" / "Qwen3-ASR-1.7B"),
)
HOST = os.environ.get("ASR_HOST", "127.0.0.1")
PORT = int(os.environ.get("ASR_PORT", "8760"))
API_KEY = os.environ.get("ASR_API_KEY", "")
SEGMENT_SEC = int(os.environ.get("ASR_SEGMENT_SEC", "60"))
# Above this duration (seconds), split into chunks. Short audio goes through
# the model in one pass.
CHUNK_THRESHOLD_SEC = int(os.environ.get("ASR_CHUNK_THRESHOLD_SEC", "90"))
MAX_UPLOAD_BYTES = int(os.environ.get("ASR_MAX_UPLOAD_BYTES", str(1 * 1024 * 1024 * 1024)))  # 1 GB
JOB_TTL_SEC = int(os.environ.get("ASR_JOB_TTL_SEC", "3600"))  # 1 hour after terminal state
MAX_INFLIGHT_JOBS = int(os.environ.get("ASR_MAX_INFLIGHT_JOBS", "32"))
TMP_DIR = Path(os.environ.get("ASR_TMP_DIR", "/tmp/asr_jobs"))


# ── Model loader (lazy) ────────────────────────────────────────

_model = None
_model_err: Optional[str] = None
_model_lock = asyncio.Lock()


def _load_model_sync():
    """Heavy load — called from a worker thread by startup or first /transcribe."""
    import torch  # noqa: F401  (defer CUDA init until we actually need it)
    from qwen_asr import Qwen3ASRModel  # type: ignore

    logger.info("loading model from %s", MODEL_PATH)
    t0 = time.time()
    m = Qwen3ASRModel.from_pretrained(
        MODEL_PATH,
        dtype=__import__("torch").bfloat16,
        device_map=os.environ.get("ASR_DEVICE", "cuda:0"),
        max_inference_batch_size=int(os.environ.get("ASR_BATCH", "8")),
        max_new_tokens=int(os.environ.get("ASR_MAX_NEW_TOKENS", "1024")),
    )
    logger.info("model loaded in %.1fs", time.time() - t0)
    return m


async def ensure_model():
    """Return the loaded model, loading on first call. Raises on persistent load failure."""
    global _model, _model_err
    if _model is not None:
        return _model
    async with _model_lock:
        if _model is not None:
            return _model
        try:
            _model = await asyncio.to_thread(_load_model_sync)
            _model_err = None
            return _model
        except Exception as e:
            _model_err = f"{type(e).__name__}: {e}"
            logger.exception("model load failed")
            raise


# ── Job state ──────────────────────────────────────────────────


JobStatus = Literal["queued", "running", "done", "error", "cancelled"]


@dataclass
class Job:
    id: str
    status: JobStatus = "queued"
    phase: str = "queued"
    percent: int = 0
    segments_done: int = 0
    segments_total: int = 0
    text: str = ""
    # Per-chunk transcript with wall-clock offsets, derived from the
    # fixed-size segmentation done by ffmpeg. Short clips that skip the
    # chunking step emit a single segment spanning [0, duration_ms].
    segments: list[dict] = field(default_factory=list)
    language: Optional[str] = None
    error: Optional[str] = None
    duration_seconds: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    audio_path: Optional[str] = None
    original_filename: str = ""

    def touch(self) -> None:
        self.updated_at = time.time()

    def to_public(self) -> dict:
        d = asdict(self)
        d.pop("audio_path", None)  # internal only
        return d


_jobs: dict[str, Job] = {}
_queue: asyncio.Queue[str] = asyncio.Queue()
_worker_task: Optional[asyncio.Task] = None
_cleanup_task: Optional[asyncio.Task] = None


# ── ffmpeg helpers ─────────────────────────────────────────────


def _ffmpeg_bin() -> str:
    """Return a path to a usable ffmpeg binary.

    Prefers the system ffmpeg (faster, well-tested). Falls back to the static
    binary shipped with imageio-ffmpeg so the service still works on a host
    without ffmpeg installed system-wide (and we avoid needing sudo to apt
    install).
    """
    sys_ffmpeg = shutil.which("ffmpeg")
    if sys_ffmpeg:
        return sys_ffmpeg
    try:
        import imageio_ffmpeg  # type: ignore
        return imageio_ffmpeg.get_ffmpeg_exe()
    except Exception as e:
        raise RuntimeError(
            "ffmpeg not found on PATH and imageio_ffmpeg unavailable: " + str(e),
        )


_DURATION_RE = re.compile(r"Duration:\s+(\d+):(\d+):(\d+(?:\.\d+)?)")


def probe_duration(path: str) -> float:
    """Best-effort duration probe using ffmpeg's stderr output.

    Returns 0.0 when ffmpeg couldn't decode a duration (rare — malformed
    streams). Callers should treat 0.0 as "unknown" and fall through to
    single-pass transcription.
    """
    proc = subprocess.run(
        [_ffmpeg_bin(), "-hide_banner", "-i", path],
        capture_output=True,
        text=True,
        check=False,
    )
    match = _DURATION_RE.search(proc.stderr or "")
    if not match:
        return 0.0
    h, m, s = match.groups()
    return int(h) * 3600 + int(m) * 60 + float(s)


def split_audio(src: str, out_dir: str, segment_sec: int) -> list[str]:
    """Split audio into ~segment_sec chunks as 16 kHz mono WAV, return paths.

    We always re-encode to 16 kHz mono PCM because:
      1. Qwen3-ASR consumes 16 kHz internally.
      2. -f segment with -c copy is unreliable on some container / codec
         combinations (gives weirdly-long first chunks or drops audio).
      3. The overhead on a multi-minute file is trivial vs. transcription.
    """
    out_pattern = os.path.join(out_dir, "chunk_%04d.wav")
    cmd = [
        _ffmpeg_bin(), "-y", "-hide_banner", "-loglevel", "error",
        "-i", src,
        "-ac", "1", "-ar", "16000",
        "-f", "segment",
        "-segment_time", str(segment_sec),
        "-reset_timestamps", "1",
        "-c:a", "pcm_s16le",
        out_pattern,
    ]
    proc = subprocess.run(cmd, capture_output=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(
            "ffmpeg split failed: " + (proc.stderr.decode("utf-8", errors="replace")[:500]),
        )
    return sorted(str(p) for p in Path(out_dir).glob("chunk_*.wav"))


# ── Transcription worker ───────────────────────────────────────


def _transcribe_one(path: str) -> tuple[str, Optional[str]]:
    """Sync transcribe of a single file. Runs in a worker thread from the asyncio loop."""
    model = _model  # caller ensured loaded
    if model is None:
        raise RuntimeError("model not loaded")
    results = model.transcribe(audio=path, language=None)
    if not results:
        return "", None
    r = results[0]
    return (getattr(r, "text", "") or ""), (getattr(r, "language", None) or None)


async def _run_job(job: Job) -> None:
    """Execute a single queued job through to a terminal state.

    Never raises — stores the error on the job so the client can pick it up
    via polling. Caller (``_worker``) uses this to update state safely.
    """
    try:
        await ensure_model()
    except Exception as e:
        job.status = "error"
        job.error = f"model load failed: {e}"[:500]
        job.phase = "error"
        job.touch()
        return

    if job.status == "cancelled":
        return
    job.status = "running"
    job.phase = "probing"
    job.touch()

    try:
        duration = probe_duration(job.audio_path or "")
        if duration > 0:
            job.duration_seconds = duration
    except Exception:
        logger.exception("duration probe failed for %s", job.id)
        duration = 0.0

    try:
        # --- Short audio: one-shot ---
        if duration <= CHUNK_THRESHOLD_SEC:
            job.segments_total = 1
            job.phase = "transcribing"
            job.percent = 10
            job.touch()
            text, lang = await asyncio.to_thread(_transcribe_one, job.audio_path or "")
            cleaned = text.strip()
            job.text = cleaned
            end_ms = int((duration if duration > 0 else 0) * 1000)
            job.segments = [{
                "index": 0,
                "start_ms": 0,
                "end_ms": end_ms,
                "text": cleaned,
            }]
            job.language = lang
            job.segments_done = 1
            job.percent = 100
            job.phase = "done"
            job.status = "done"
            job.touch()
            return

        # --- Long audio: chunk + transcribe sequentially ---
        job.phase = "splitting"
        job.percent = 2
        job.touch()
        tmp = Path(TMP_DIR) / f"chunks_{job.id}"
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            chunks = await asyncio.to_thread(
                split_audio, job.audio_path or "", str(tmp), SEGMENT_SEC,
            )
            if not chunks:
                # ffmpeg produced nothing — fall back to single-pass
                job.segments_total = 1
                job.phase = "transcribing"
                job.percent = 10
                job.touch()
                text, lang = await asyncio.to_thread(_transcribe_one, job.audio_path or "")
                cleaned = text.strip()
                job.text = cleaned
                end_ms = int((duration if duration > 0 else 0) * 1000)
                job.segments = [{
                    "index": 0,
                    "start_ms": 0,
                    "end_ms": end_ms,
                    "text": cleaned,
                }]
                job.language = lang
                job.segments_done = 1
                job.percent = 100
                job.phase = "done"
                job.status = "done"
                job.touch()
                return

            job.segments_total = len(chunks)
            pieces: list[str] = []
            seg_records: list[dict] = []
            detected_lang: Optional[str] = None
            total_duration_ms = int((duration if duration > 0 else 0) * 1000)
            seg_ms = SEGMENT_SEC * 1000
            for i, chunk in enumerate(chunks):
                if job.status == "cancelled":
                    job.phase = "cancelled"
                    job.touch()
                    return
                job.phase = f"transcribing {i + 1}/{len(chunks)}"
                job.touch()
                text, lang = await asyncio.to_thread(_transcribe_one, chunk)
                cleaned = (text or "").strip()
                if cleaned:
                    pieces.append(cleaned)
                if detected_lang is None and lang:
                    detected_lang = lang
                start_ms = i * seg_ms
                end_ms = min(start_ms + seg_ms, total_duration_ms) if total_duration_ms else start_ms + seg_ms
                seg_records.append({
                    "index": i,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                    "text": cleaned,
                })
                job.segments = seg_records  # incremental — polling UI can stream
                job.segments_done = i + 1
                # Reserve 5% for splitting / bookkeeping so we never report 100%
                # before the final bookkeeping write.
                job.percent = int(5 + 90 * (i + 1) / len(chunks))
                job.touch()
            job.text = "\n".join(p for p in pieces if p)
            job.language = detected_lang
            job.percent = 100
            job.phase = "done"
            job.status = "done"
            job.touch()
        finally:
            try:
                shutil.rmtree(tmp, ignore_errors=True)
            except Exception:
                pass
    except Exception as e:
        logger.exception("job %s failed", job.id)
        job.status = "error"
        job.error = f"{type(e).__name__}: {e}"[:500]
        job.phase = "error"
        job.touch()


async def _worker() -> None:
    """Single-slot worker: pulls one job at a time off the queue."""
    while True:
        job_id = await _queue.get()
        job = _jobs.get(job_id)
        if job is None:
            continue
        if job.status == "cancelled":
            # Cleanup any uploaded file for a job cancelled while queued.
            if job.audio_path:
                try:
                    Path(job.audio_path).unlink(missing_ok=True)
                except Exception:
                    pass
            continue
        try:
            await _run_job(job)
        finally:
            # Uploaded audio is no longer needed after transcription — ditch it.
            if job.audio_path:
                try:
                    Path(job.audio_path).unlink(missing_ok=True)
                except Exception:
                    pass
            job.audio_path = None


async def _cleanup_loop() -> None:
    """Evict terminal jobs after JOB_TTL_SEC. Also caps the total jobs dict."""
    while True:
        await asyncio.sleep(300)
        now = time.time()
        terminal = {"done", "error", "cancelled"}
        ids = list(_jobs.keys())
        for jid in ids:
            j = _jobs.get(jid)
            if not j:
                continue
            if j.status in terminal and (now - j.updated_at) > JOB_TTL_SEC:
                _jobs.pop(jid, None)


# ── Auth ───────────────────────────────────────────────────────


def check_auth(authorization: str = Header(default="")) -> None:
    """Bearer token check. Disabled entirely when ASR_API_KEY is empty."""
    if not API_KEY:
        return
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "missing bearer token")
    token = authorization.split(None, 1)[1].strip() if " " in authorization else ""
    if token != API_KEY:
        raise HTTPException(403, "invalid token")


# ── FastAPI app ────────────────────────────────────────────────


app = FastAPI(title="Qwen3-ASR-1.7B service", version="1.0.0")


@app.on_event("startup")
async def _startup() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    global _worker_task, _cleanup_task
    _worker_task = asyncio.create_task(_worker(), name="asr_worker")
    _cleanup_task = asyncio.create_task(_cleanup_loop(), name="asr_cleanup")
    # Kick off the model load in the background so the first request isn't
    # penalized by a ~10 s load. If load fails, /health will report it.
    asyncio.create_task(_safe_preload())


async def _safe_preload() -> None:
    try:
        await ensure_model()
    except Exception:
        pass  # _model_err is set; /health exposes it


@app.get("/health")
async def health() -> dict:
    import torch  # lazy; avoids cuda init if we never serve traffic
    return {
        "ok": True,
        "model_loaded": _model is not None,
        "model_error": _model_err,
        "gpu": torch.cuda.is_available(),
        "gpu_count": torch.cuda.device_count() if torch.cuda.is_available() else 0,
        "queue_size": _queue.qsize(),
        "jobs_in_memory": len(_jobs),
        "model_path": MODEL_PATH,
    }


@app.post("/transcribe")
async def transcribe_endpoint(
    file: UploadFile = File(...),
    _auth: None = Depends(check_auth),
) -> JSONResponse:
    if len(_jobs) >= MAX_INFLIGHT_JOBS:
        raise HTTPException(503, "too many in-flight jobs, retry later")

    # Stream the body to disk; sha256 inline for light dedup / debug traceability.
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    job_id = uuid.uuid4().hex
    suffix = Path((file.filename or "audio")).suffix or ".bin"
    path = TMP_DIR / f"{job_id}{suffix}"
    total = 0
    with open(path, "wb") as out:
        while True:
            chunk = await file.read(1 << 20)
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_UPLOAD_BYTES:
                out.close()
                try:
                    path.unlink(missing_ok=True)
                except Exception:
                    pass
                raise HTTPException(413, f"upload exceeds {MAX_UPLOAD_BYTES} bytes")
            out.write(chunk)
    await file.close()

    if total == 0:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(400, "empty upload")

    job = Job(
        id=job_id,
        audio_path=str(path),
        original_filename=file.filename or "",
    )
    _jobs[job_id] = job
    await _queue.put(job_id)
    return JSONResponse(
        {
            "job_id": job_id,
            "status": job.status,
            "queue_position": _queue.qsize(),
            "bytes": total,
        },
    )


@app.get("/jobs/{job_id}")
async def get_job(job_id: str, _auth: None = Depends(check_auth)) -> dict:
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    return job.to_public()


@app.delete("/jobs/{job_id}")
async def cancel_job(job_id: str, _auth: None = Depends(check_auth)) -> dict:
    job = _jobs.get(job_id)
    if not job:
        raise HTTPException(404, "job not found")
    if job.status in ("done", "error", "cancelled"):
        return {"ok": False, "status": job.status}
    job.status = "cancelled"
    job.phase = "cancelled"
    job.touch()
    return {"ok": True, "status": job.status}


# ── Entrypoint ─────────────────────────────────────────────────


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    import uvicorn
    uvicorn.run(
        "server:app",
        host=HOST,
        port=PORT,
        workers=1,
        log_level="info",
        # Keep timeout_keep_alive modest — the /jobs polling pattern creates
        # lots of short-lived requests; we don't want idle connections piling
        # up. Actual transcription is backgrounded off the request lifecycle.
        timeout_keep_alive=10,
    )


if __name__ == "__main__":
    main()
