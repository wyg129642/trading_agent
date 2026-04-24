"""Base classes + context object shared by every step executor.

The recipe engine instantiates one ``StepContext`` per RecipeRun and passes
it to each executor in topological order. Executors stream ``StepEvent``
records which the run loop forwards to the SSE channel.
"""
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.recipe import RecipeRun
from backend.app.models.revenue_model import RevenueModel

logger = logging.getLogger(__name__)


# ── Step events (streamed) ───────────────────────────────────────

@dataclass
class StepEvent:
    type: str                # step_started / step_progress / step_completed / cell_update / verify_flag / log
    step_id: str
    payload: dict[str, Any] = field(default_factory=dict)
    ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def as_dict(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "step_id": self.step_id,
            "payload": self.payload,
            "ts": self.ts.isoformat(),
        }


@dataclass
class StepContext:
    """Execution context threaded through each step executor.

    Notes for maintainers:
      * The context is *not* a god object — step executors talk to the DB
        via the provided session.
      * The ``event_sink`` is a fire-and-forget async callback; a step
        should ``await`` it sparingly (once per substantial event) since
        the other end is an ``asyncio.Queue`` the SSE pump drains.
    """
    db: AsyncSession
    model: RevenueModel
    run: RecipeRun
    step_config: dict[str, Any]
    step_id: str
    event_sink: Callable[[StepEvent], Any]
    pack: Any   # IndustryPack (avoiding circular import)
    # A cache of { path -> ModelCell } so we don't re-query per lookup
    cell_cache: dict[str, Any] = field(default_factory=dict)
    # Accumulated per-run metrics
    total_tokens: int = 0
    total_latency_ms: int = 0
    # When True, skip LLM calls (dry run for smoke tests)
    dry_run: bool = False

    async def emit(self, event_type: str, payload: dict[str, Any] | None = None) -> None:
        evt = StepEvent(type=event_type, step_id=self.step_id, payload=payload or {})
        maybe = self.event_sink(evt)
        if hasattr(maybe, "__await__"):
            await maybe


# ── Base executor ───────────────────────────────────────────────

class BaseStepExecutor:
    """Contract for a step executor.

    Subclasses override ``run()``. Each step must:
      * emit ``step_started`` at entry and ``step_completed`` at exit
        (the engine wraps ``run()`` to guarantee this),
      * update ``ctx.total_tokens`` and ``ctx.total_latency_ms``,
      * write ModelCell rows via ``backend.app.services.model_cell_store``.
    """
    step_type: str = "BASE"

    async def run(self, ctx: StepContext) -> dict[str, Any]:
        """Override in subclass.

        Return value is written to ``RecipeRun.step_results[step_id]`` so
        subsequent steps can reach upstream output. Return a dict; include
        ``output_paths: [path, ...]`` listing cells written.
        """
        raise NotImplementedError


STEP_REGISTRY: dict[str, type[BaseStepExecutor]] = {}


def register_step(step_type: str, cls: type[BaseStepExecutor]) -> None:
    cls.step_type = step_type
    STEP_REGISTRY[step_type] = cls
