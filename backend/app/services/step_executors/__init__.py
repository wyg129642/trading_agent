"""Step executor registry.

Each step type is a small class implementing :class:`BaseStepExecutor`.
They do not directly hold business knowledge — that lives in the Industry
Pack's recipe config and playbook markdown. Executors are thin glue that:

  1. Read their required inputs from the ModelCell store.
  2. Call the LLM (with tool use) via ``chat_llm.call_model_stream_with_tools``.
  3. Parse structured output.
  4. Write ModelCell rows back to the DB with provenance + citations.
  5. Emit StepEvent objects for SSE streaming.
"""
from .base import BaseStepExecutor, StepContext, StepEvent, STEP_REGISTRY, register_step
from .gather_context import GatherContextStep
from .decompose_segments import DecomposeSegmentsStep
from .classify_growth_profile import ClassifyGrowthProfileStep
from .extract_historical import ExtractHistoricalStep
from .model_volume_price import ModelVolumePriceStep
from .apply_guidance import ApplyGuidanceStep
from .margin_cascade import MarginCascadeStep
from .verify_and_ask import VerifyAndAskStep
from .consensus_check import ConsensusCheckStep
from .classify_peers import ClassifyPeersStep
from .growth_decomposition import GrowthDecompositionStep
from .multi_path_check import MultiPathCheckStep


# Register in a stable order
register_step("GATHER_CONTEXT", GatherContextStep)
register_step("DECOMPOSE_SEGMENTS", DecomposeSegmentsStep)
register_step("CLASSIFY_GROWTH_PROFILE", ClassifyGrowthProfileStep)
register_step("EXTRACT_HISTORICAL", ExtractHistoricalStep)
register_step("CLASSIFY_PEERS", ClassifyPeersStep)
register_step("MODEL_VOLUME_PRICE", ModelVolumePriceStep)
register_step("APPLY_GUIDANCE", ApplyGuidanceStep)
register_step("GROWTH_DECOMPOSITION", GrowthDecompositionStep)
register_step("MARGIN_CASCADE", MarginCascadeStep)
register_step("MULTI_PATH_CHECK", MultiPathCheckStep)
register_step("CONSENSUS_CHECK", ConsensusCheckStep)
register_step("VERIFY_AND_ASK", VerifyAndAskStep)


__all__ = [
    "BaseStepExecutor",
    "StepContext",
    "StepEvent",
    "STEP_REGISTRY",
    "register_step",
]
