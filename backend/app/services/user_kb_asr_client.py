"""Async HTTP client for the Qwen3-ASR service on the jumpbox.

Architecture:

    backend  ──(HTTP over local SSH tunnel)──▶  jumpbox:127.0.0.1:8760
    (this)                                      Qwen3-ASR FastAPI service
                                                (see ops/asr_jumpbox/server.py)

The tunnel is supervised by ``ops/asr_tunnel/asr_tunnel.sh``. From this
client's perspective, the ASR service looks like ``http://127.0.0.1:8760``
— a local HTTP endpoint. If the tunnel is down we want to fail fast with
a clear error rather than stall uploads.

Job lifecycle (jumpbox side):

    queued  → running → done
                     ↘   error
                     ↘   cancelled

This client submits the audio bytes, then polls ``GET /jobs/{id}`` at a
fixed cadence until the job reaches a terminal state. Progress
(``percent``, ``phase``, ``segments_done``/``total``) is surfaced via an
optional ``on_progress`` callback so callers can update UI state (e.g.
the ``parse_progress_percent`` / ``parse_phase`` fields on the Mongo
documents row) in near-real-time.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

import httpx

from backend.app.config import get_settings

logger = logging.getLogger(__name__)


class AsrUnavailable(RuntimeError):
    """Raised when the ASR service is unreachable or returning 5xx.

    Almost always means the SSH tunnel is down or the jumpbox service
    crashed. Retry later; do not surface as a hard user-facing failure
    — let the stuck-parse recovery sweep pick it up.
    """


class AsrJobFailed(RuntimeError):
    """Raised when the job completed with status=error on the jumpbox.

    This is a per-file failure (corrupt audio, GPU OOM, etc.); the
    document should be marked ``parse_status=failed`` with this message.
    """


@dataclass
class AsrProgress:
    percent: int
    phase: str
    segments_done: int = 0
    segments_total: int = 0
    duration_seconds: Optional[float] = None


@dataclass
class AsrSegment:
    """One transcribed chunk with its offset in the source audio."""
    index: int
    start_ms: int
    end_ms: int
    text: str

    def to_dict(self) -> dict:
        return {
            "index": self.index,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
            "text": self.text,
        }


@dataclass
class AsrResult:
    text: str
    language: Optional[str]
    duration_seconds: Optional[float] = None
    # Segments are the per-chunk transcript with wall-clock offsets, used
    # by the audio player UI to drive click-to-seek and follow-along
    # highlighting. Older ASR servers that don't populate this field fall
    # back to a single synthetic segment spanning the whole file.
    segments: list[AsrSegment] = field(default_factory=list)


ProgressCallback = Callable[[AsrProgress], Awaitable[None]]


# ── Internal helpers ──────────────────────────────────────────


def _build_headers() -> dict[str, str]:
    settings = get_settings()
    headers: dict[str, str] = {}
    key = (settings.asr_service_api_key or "").strip()
    if key:
        headers["Authorization"] = f"Bearer {key}"
    return headers


def _base_url() -> str:
    return get_settings().asr_service_url.rstrip("/")


def _coerce_segments(
    raw: object,
    fallback_text: str,
    duration_seconds: Optional[float],
) -> list[AsrSegment]:
    """Normalize the ASR server's ``segments`` payload into AsrSegment objects.

    When the server is older and doesn't emit segments, synthesize a single
    segment covering [0, duration_ms] so the UI always has something to
    drive click-to-seek from.
    """
    out: list[AsrSegment] = []
    if isinstance(raw, list):
        for i, entry in enumerate(raw):
            if not isinstance(entry, dict):
                continue
            try:
                out.append(
                    AsrSegment(
                        index=int(entry.get("index", i)),
                        start_ms=int(entry.get("start_ms", 0)),
                        end_ms=int(entry.get("end_ms", 0)),
                        text=str(entry.get("text") or "").strip(),
                    ),
                )
            except (TypeError, ValueError):
                continue
    if out:
        return out
    end_ms = int((duration_seconds or 0.0) * 1000)
    return [AsrSegment(index=0, start_ms=0, end_ms=end_ms, text=fallback_text.strip())]


def _job_to_progress(job: dict) -> AsrProgress:
    return AsrProgress(
        percent=int(job.get("percent") or 0),
        phase=str(job.get("phase") or ""),
        segments_done=int(job.get("segments_done") or 0),
        segments_total=int(job.get("segments_total") or 0),
        duration_seconds=(
            float(job["duration_seconds"])
            if job.get("duration_seconds") is not None
            else None
        ),
    )


# ── Public API ────────────────────────────────────────────────


async def probe() -> tuple[bool, str]:
    """Health check. Returns (ok, reason).

    Used by a /ping-style endpoint so the UI can show a banner when the
    ASR service is down, so users understand why their upload hasn't
    made progress.
    """
    settings = get_settings()
    if not settings.asr_service_url:
        return False, "asr_service_url not configured"
    try:
        # trust_env=False: the ASR service is always on local-loopback via
        # an SSH tunnel. The shell's HTTP_PROXY/ALL_PROXY env vars (Clash at
        # 127.0.0.1:7890) otherwise hijack the request and manifest as
        # ReadTimeout / 502s even when the tunnel is healthy. NO_PROXY was
        # only set for uppercase on uvicorn; lowercase ``all_proxy`` still
        # leaked through. Force-disabling env-driven proxies removes the
        # whole class of bug.
        async with httpx.AsyncClient(
            timeout=5.0, headers=_build_headers(), trust_env=False,
        ) as client:
            r = await client.get(f"{_base_url()}/health")
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPError as e:
        return False, f"{type(e).__name__}: {e}"
    if not data.get("model_loaded"):
        err = data.get("model_error") or "(model still loading)"
        return False, f"model not ready: {err}"
    return True, "ok"


async def transcribe(
    audio_bytes: bytes,
    filename: str,
    *,
    on_progress: Optional[ProgressCallback] = None,
) -> AsrResult:
    """Submit ``audio_bytes`` to the ASR service and wait for completion.

    :param audio_bytes: Raw audio file contents (mp3/wav/m4a/flac/...).
    :param filename: Original filename; used for Content-Disposition so
        the server can infer the format from the suffix.
    :param on_progress: Optional async callback fired whenever percent or
        phase advances. Exceptions raised inside the callback are logged
        and swallowed — progress reporting is best-effort and must never
        break a transcription.

    :returns: :class:`AsrResult` on success.
    :raises AsrUnavailable: tunnel down, service 5xx, request timeout
        during submit.
    :raises AsrJobFailed: job reached terminal status=error or cancelled.
    """
    settings = get_settings()
    if not audio_bytes:
        raise AsrJobFailed("empty audio")
    base = _base_url()

    # Two timeouts worth distinguishing:
    # - submit_timeout: bounded wait for the upload itself. Big files on a
    #   degraded link can take a while so we're generous here.
    # - poll_timeout: per-poll HTTP call, tight. The polling loop handles
    #   transient errors by retrying up to asr_service_poll_retries times.
    submit_timeout = httpx.Timeout(
        connect=5.0,
        read=float(settings.asr_service_upload_timeout_seconds),
        write=float(settings.asr_service_upload_timeout_seconds),
        pool=10.0,
    )
    poll_timeout = httpx.Timeout(
        connect=3.0,
        read=10.0,
        write=5.0,
        pool=5.0,
    )

    # ── Submit ────────────────────────────────────────────
    try:
        async with httpx.AsyncClient(
            timeout=submit_timeout, headers=_build_headers(), trust_env=False,
        ) as client:
            files = {
                "file": (filename or "audio.bin", audio_bytes, "application/octet-stream"),
            }
            resp = await client.post(f"{base}/transcribe", files=files)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        # 4xx comes from the server: bad request, unauthenticated, too big.
        # Treat as a job failure, not an infra issue — retrying won't help.
        status = e.response.status_code
        body = (e.response.text or "")[:300]
        if 400 <= status < 500:
            raise AsrJobFailed(f"asr submit {status}: {body}") from e
        raise AsrUnavailable(f"asr submit {status}: {body}") from e
    except httpx.HTTPError as e:
        raise AsrUnavailable(f"asr submit transport error: {e}") from e

    job_id = str(data.get("job_id") or "")
    if not job_id:
        raise AsrJobFailed(f"asr submit returned no job_id: {data!r}")
    logger.info(
        "asr job submitted id=%s bytes=%s filename=%s",
        job_id, len(audio_bytes), filename,
    )

    # ── Poll ──────────────────────────────────────────────
    poll_interval = float(settings.asr_service_poll_interval_seconds)
    deadline = time.monotonic() + float(settings.asr_service_job_timeout_seconds)
    consecutive_errors = 0
    max_poll_errors = int(settings.asr_service_poll_retries)
    last_percent = -1
    last_phase = ""

    try:
        async with httpx.AsyncClient(
            timeout=poll_timeout, headers=_build_headers(), trust_env=False,
        ) as client:
            while True:
                if time.monotonic() > deadline:
                    # Attempt to cancel so the jumpbox frees its slot.
                    try:
                        await client.delete(f"{base}/jobs/{job_id}")
                    except Exception:
                        pass
                    raise AsrJobFailed(
                        f"asr job {job_id} exceeded "
                        f"{settings.asr_service_job_timeout_seconds}s",
                    )
                try:
                    r = await client.get(f"{base}/jobs/{job_id}")
                    r.raise_for_status()
                    job = r.json()
                    consecutive_errors = 0
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        # Unusual — the service restarted mid-job, wiping state.
                        raise AsrJobFailed(
                            f"asr job {job_id} disappeared (service restart?)",
                        ) from e
                    consecutive_errors += 1
                except httpx.HTTPError as e:
                    consecutive_errors += 1
                    logger.warning(
                        "asr poll %s error %d/%d: %s",
                        job_id, consecutive_errors, max_poll_errors, e,
                    )
                if consecutive_errors >= max_poll_errors:
                    raise AsrUnavailable(
                        f"asr poll failed {max_poll_errors} times "
                        f"(job {job_id} — tunnel down?)",
                    )

                if consecutive_errors == 0:
                    status = str(job.get("status") or "")
                    percent = int(job.get("percent") or 0)
                    phase = str(job.get("phase") or "")
                    if on_progress and (percent != last_percent or phase != last_phase):
                        try:
                            await on_progress(_job_to_progress(job))
                        except Exception:
                            logger.exception("on_progress callback raised")
                        last_percent = percent
                        last_phase = phase

                    if status == "done":
                        raw_text = str(job.get("text") or "")
                        duration = (
                            float(job["duration_seconds"])
                            if job.get("duration_seconds") is not None
                            else None
                        )
                        segments = _coerce_segments(
                            job.get("segments"), raw_text, duration,
                        )
                        return AsrResult(
                            text=raw_text,
                            language=job.get("language"),
                            duration_seconds=duration,
                            segments=segments,
                        )
                    if status == "error":
                        raise AsrJobFailed(
                            str(job.get("error") or "unknown ASR error"),
                        )
                    if status == "cancelled":
                        raise AsrJobFailed("asr job cancelled")
                await asyncio.sleep(poll_interval)
    except (AsrJobFailed, AsrUnavailable):
        raise
    except Exception as e:
        # Defensive: unknown error during polling → treat as infra issue so
        # the stuck-parse recovery sweep retries later.
        logger.exception("asr poll unexpected error for job %s", job_id)
        raise AsrUnavailable(f"asr poll crash: {e}") from e
