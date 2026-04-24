"""Pydantic schemas for the Revenue Modeling API."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


# ── Cells ───────────────────────────────────────────────────

class CitationItem(BaseModel):
    index: int | None = None
    source_id: str | None = None
    url: str | None = None
    title: str | None = None
    snippet: str | None = None
    date: str | None = None
    tool: str | None = None
    page: int | None = None


class AlternativeValue(BaseModel):
    value: float | None = None
    value_text: str | None = None
    source: str
    citation_idx: int | None = None
    label: str = ""
    notes: str = ""


class ModelCellRead(BaseModel):
    id: str
    model_id: str
    path: str
    label: str = ""
    period: str = ""
    unit: str = ""
    value: float | None = None
    value_text: str | None = None
    formula: str | None = None
    depends_on: list[str] = []
    value_type: Literal["number", "percent", "currency", "count", "text"] = "number"
    source_type: Literal[
        "historical", "guidance", "expert", "inferred", "assumption", "derived"
    ] = "assumption"
    confidence: Literal["HIGH", "MEDIUM", "LOW"] = "MEDIUM"
    confidence_reason: str = ""
    citations: list[CitationItem] = []
    notes: str = ""
    alternative_values: list[AlternativeValue] = []
    provenance_trace_id: str | None = None
    locked_by_human: bool = False
    human_override: bool = False
    review_status: Literal["pending", "approved", "flagged"] = "pending"
    extra: dict[str, Any] = {}
    created_at: datetime
    updated_at: datetime


class CellUpdate(BaseModel):
    """Patch a cell's user-editable fields."""
    value: float | None = None
    value_text: str | None = None
    formula: str | None = None
    source_type: Literal[
        "historical", "guidance", "expert", "inferred", "assumption", "derived",
    ] | None = None
    confidence: Literal["HIGH", "MEDIUM", "LOW"] | None = None
    notes: str | None = None
    alternative_values: list[AlternativeValue] | None = None
    locked_by_human: bool | None = None
    review_status: Literal["pending", "approved", "flagged"] | None = None
    edit_reason: str = ""
    # When set, also choose this alternative as the main value
    pick_alternative_idx: int | None = None


class CellCreate(BaseModel):
    path: str
    label: str = ""
    period: str = ""
    unit: str = ""
    value: float | None = None
    value_text: str | None = None
    formula: str | None = None
    value_type: Literal["number", "percent", "currency", "count", "text"] = "number"
    source_type: Literal[
        "historical", "guidance", "expert", "inferred", "assumption", "derived",
    ] = "assumption"
    confidence: Literal["HIGH", "MEDIUM", "LOW"] = "MEDIUM"
    confidence_reason: str = ""
    citations: list[CitationItem] = []
    notes: str = ""
    alternative_values: list[AlternativeValue] = []
    extra: dict[str, Any] = {}


# ── Revenue Model ──────────────────────────────────────────

class RevenueModelCreate(BaseModel):
    ticker: str
    company_name: str
    industry: str = "optical_modules"
    fiscal_periods: list[str] = ["FY25E", "FY26E", "FY27E"]
    title: str = ""
    notes: str = ""
    base_currency: str = "USD"
    recipe_id: str | None = None
    conversation_id: str | None = None


class RevenueModelUpdate(BaseModel):
    title: str | None = None
    notes: str | None = None
    fiscal_periods: list[str] | None = None
    status: Literal["draft", "running", "ready", "archived", "failed"] | None = None


class RevenueModelRead(BaseModel):
    id: str
    ticker: str
    company_name: str
    industry: str
    fiscal_periods: list[str]
    recipe_id: str | None
    recipe_version: int | None
    status: str
    title: str
    notes: str
    base_currency: str
    cell_count: int
    flagged_count: int
    owner_user_id: str
    last_run_id: str | None
    conversation_id: str | None
    # Hallucination-guard circuit-breaker state
    paused_by_guard: bool = False
    paused_reason: str | None = None
    created_at: datetime
    updated_at: datetime


class RevenueModelDetail(RevenueModelRead):
    cells: list[ModelCellRead] = []


# ── Recipe / Run ───────────────────────────────────────────

class RecipeNodeConfig(BaseModel):
    prompt_template: str | None = None
    tools: list[str] | None = None
    threshold: float | None = None
    # Other arbitrary node-type specific keys
    model_config = {"extra": "allow"}


class RecipeNode(BaseModel):
    id: str
    type: str
    label: str = ""
    config: dict[str, Any] = {}
    next_on_success: str | None = None
    next_on_fail: str | None = None


class RecipeEdge(BaseModel):
    # Note: field aliased as `from` since `from` is a python keyword
    from_id: str = Field(alias="from")
    to: str
    condition: str | None = None

    model_config = {"populate_by_name": True}


class RecipeGraph(BaseModel):
    nodes: list[RecipeNode] = []
    edges: list[RecipeEdge] = []


class RecipeCreate(BaseModel):
    name: str
    slug: str
    industry: str | None = None
    description: str = ""
    graph: RecipeGraph
    is_public: bool = False
    tags: list[str] = []
    pack_ref: str | None = None


class RecipeUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    graph: RecipeGraph | None = None
    is_public: bool | None = None
    tags: list[str] | None = None


class RecipeRead(BaseModel):
    id: str
    name: str
    slug: str
    industry: str | None
    description: str
    graph: dict[str, Any]
    version: int
    is_public: bool
    parent_recipe_id: str | None
    created_by: str | None
    pack_ref: str | None
    tags: list[str]
    canonical: bool = False
    created_at: datetime
    updated_at: datetime


class RecipeRunCreate(BaseModel):
    model_id: str
    recipe_id: str | None = None
    # Per-run settings (dry_run, skip_debate, llm_overrides)
    settings: dict[str, Any] = {}


class RecipeRunRead(BaseModel):
    id: str
    recipe_id: str
    recipe_version: int
    model_id: str
    ticker: str
    started_by: str | None
    status: str
    current_step_id: str | None
    step_results: dict[str, Any]
    total_tokens: int
    total_cost_usd: float
    estimated_cost_usd: float = 0.0
    cost_cap_usd: float | None = None
    paused_reason: str | None = None
    error: str | None
    settings: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None


# ── Sanity / Debate ────────────────────────────────────────

class SanityIssueRead(BaseModel):
    id: str
    model_id: str
    issue_type: str
    severity: Literal["info", "warn", "error"]
    cell_paths: list[str]
    message: str
    suggested_fix: str
    details: dict[str, Any]
    resolved: bool
    created_at: datetime


class DebateOpinionRead(BaseModel):
    id: str
    cell_id: str
    model_key: str
    role: Literal["drafter", "verifier", "tiebreaker"]
    value: float | None
    reasoning: str
    citations: list[CitationItem]
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    tokens_used: int
    latency_ms: int
    created_at: datetime


# ── Provenance ────────────────────────────────────────────

class ProvenanceStep(BaseModel):
    step_type: str = ""
    tool: str | None = None
    query: str | None = None
    result_preview: str | None = None
    llm_reasoning: str | None = None
    tokens: int = 0
    latency: int = 0


class ProvenanceTraceRead(BaseModel):
    id: str
    model_id: str
    cell_path: str | None
    step_id: str | None
    steps: list[dict[str, Any]]
    raw_evidence: list[dict[str, Any]]
    total_tokens: int
    total_latency_ms: int
    created_at: datetime


# ── Feedback ───────────────────────────────────────────────

class FeedbackEventCreate(BaseModel):
    event_type: str
    model_id: str | None = None
    cell_id: str | None = None
    recipe_id: str | None = None
    industry: str | None = None
    cell_path: str | None = None
    payload: dict[str, Any] = {}


class PendingLessonRead(BaseModel):
    id: str
    industry: str
    lesson_id: str
    title: str
    body: str
    scenario: str
    observation: str
    rule: str
    sources: list[dict[str, Any]]
    status: Literal["pending", "approved", "rejected", "archived"]
    reviewed_by: str | None
    review_note: str
    batch_week: str
    created_at: datetime
    reviewed_at: datetime | None


class PendingLessonReview(BaseModel):
    action: Literal["approve", "reject", "archive"]
    review_note: str = ""
    edited_body: str | None = None
