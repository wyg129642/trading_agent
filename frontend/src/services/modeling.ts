/**
 * Revenue Modeling API client.
 *
 * Keeps a single typed surface that the RevenueModel / RecipeEditor /
 * PlaybookReview pages consume. Matches the shapes in
 * backend/app/schemas/revenue_model.py.
 */
import api from './api'

// ── Shared types ────────────────────────────────────────────

export type SourceType =
  | 'historical'
  | 'guidance'
  | 'expert'
  | 'inferred'
  | 'assumption'
  | 'derived'

export type Confidence = 'HIGH' | 'MEDIUM' | 'LOW'

export type ValueType = 'number' | 'percent' | 'currency' | 'count' | 'text'

export interface Citation {
  index?: number
  source_id?: string
  url?: string
  title?: string
  snippet?: string
  date?: string
  tool?: string
  page?: number
}

export interface AlternativeValue {
  value?: number | null
  value_text?: string | null
  source: string
  label?: string
  notes?: string
  citation_idx?: number
}

export interface ModelCell {
  id: string
  model_id: string
  path: string
  label: string
  period: string
  unit: string
  value: number | null
  value_text: string | null
  formula: string | null
  depends_on: string[]
  value_type: ValueType
  source_type: SourceType
  confidence: Confidence
  confidence_reason: string
  citations: Citation[]
  notes: string
  alternative_values: AlternativeValue[]
  provenance_trace_id: string | null
  locked_by_human: boolean
  human_override: boolean
  review_status: 'pending' | 'approved' | 'flagged'
  extra: Record<string, any>
  created_at: string
  updated_at: string
}

export interface RevenueModel {
  id: string
  ticker: string
  company_name: string
  industry: string
  fiscal_periods: string[]
  recipe_id: string | null
  recipe_version: number | null
  status: string
  title: string
  notes: string
  base_currency: string
  cell_count: number
  flagged_count: number
  owner_user_id: string
  last_run_id: string | null
  conversation_id: string | null
  paused_by_guard?: boolean
  paused_reason?: string | null
  created_at: string
  updated_at: string
}

export interface RevenueModelDetail extends RevenueModel {
  cells: ModelCell[]
}

export interface Recipe {
  id: string
  name: string
  slug: string
  industry: string | null
  description: string
  graph: { nodes: any[]; edges: any[] }
  version: number
  is_public: boolean
  parent_recipe_id: string | null
  created_by: string | null
  pack_ref: string | null
  tags: string[]
  created_at: string
  updated_at: string
}

export interface RecipeRun {
  id: string
  recipe_id: string
  recipe_version: number
  model_id: string
  ticker: string
  started_by: string | null
  status: string
  current_step_id: string | null
  step_results: Record<string, any>
  total_tokens: number
  total_cost_usd: number
  error: string | null
  settings: Record<string, any>
  created_at: string
  updated_at: string
  completed_at: string | null
}

export interface SanityIssue {
  id: string
  model_id: string
  issue_type: string
  severity: 'info' | 'warn' | 'error'
  cell_paths: string[]
  message: string
  suggested_fix: string
  details: Record<string, any>
  resolved: boolean
  created_at: string
}

export interface DebateOpinion {
  id: string
  cell_id: string
  model_key: string
  role: 'drafter' | 'verifier' | 'tiebreaker'
  value: number | null
  reasoning: string
  citations: Citation[]
  confidence: Confidence
  tokens_used: number
  latency_ms: number
  created_at: string
}

export interface ProvenanceTrace {
  id: string
  model_id: string
  cell_path: string | null
  step_id: string | null
  steps: any[]
  raw_evidence: any[]
  total_tokens: number
  total_latency_ms: number
  created_at: string
}

export interface PackInfo {
  slug: string
  name: string
  description: string
  ticker_patterns: string[]
  default_periods: string[]
  recipe_count: number
  playbook_files: string[]
}

export interface Lesson {
  id: string
  title: string
  body: string
}

export interface PendingLesson {
  id: string
  industry: string
  lesson_id: string
  title: string
  body: string
  scenario: string
  observation: string
  rule: string
  sources: any[]
  status: 'pending' | 'approved' | 'rejected' | 'archived'
  reviewed_by: string | null
  review_note: string
  batch_week: string
  created_at: string
  reviewed_at: string | null
}

// ── Models ──────────────────────────────────────────────────

export const modelingApi = {
  listModels: (params?: { ticker?: string; industry?: string; status?: string }) =>
    api.get<RevenueModel[]>('/models', { params }).then(r => r.data),

  createModel: (body: {
    ticker: string
    company_name: string
    industry: string
    fiscal_periods?: string[]
    title?: string
    notes?: string
    base_currency?: string
    recipe_id?: string
    conversation_id?: string
  }) => api.post<RevenueModel>('/models', body).then(r => r.data),

  getModel: (id: string) =>
    api.get<RevenueModelDetail>(`/models/${id}`).then(r => r.data),

  updateModel: (id: string, body: Partial<RevenueModel>) =>
    api.patch<RevenueModel>(`/models/${id}`, body).then(r => r.data),

  deleteModel: (id: string) =>
    api.delete(`/models/${id}`).then(r => r.data),

  listCells: (model_id: string, prefix?: string) =>
    api.get<ModelCell[]>(`/models/${model_id}/cells`, {
      params: prefix ? { path_prefix: prefix } : undefined,
    }).then(r => r.data),

  updateCell: (model_id: string, cell_id: string, body: {
    value?: number | null
    value_text?: string | null
    formula?: string | null
    source_type?: SourceType
    confidence?: Confidence
    notes?: string
    alternative_values?: AlternativeValue[]
    locked_by_human?: boolean
    review_status?: 'pending' | 'approved' | 'flagged'
    edit_reason?: string
    pick_alternative_idx?: number
  }) =>
    api.patch<ModelCell>(`/models/${model_id}/cells/${cell_id}`, body).then(r => r.data),

  createCell: (model_id: string, body: Partial<ModelCell> & { path: string }) =>
    api.post<ModelCell>(`/models/${model_id}/cells`, body).then(r => r.data),

  deleteCell: (model_id: string, cell_id: string) =>
    api.delete(`/models/${model_id}/cells/${cell_id}`).then(r => r.data),

  evaluate: (model_id: string) =>
    api.post(`/models/${model_id}/evaluate`).then(r => r.data),

  cellHistory: (model_id: string, cell_id: string) =>
    api.get<any[]>(`/models/${model_id}/cells/${cell_id}/history`).then(r => r.data),

  listDebate: (model_id: string, cell_id: string) =>
    api.get<DebateOpinion[]>(`/models/${model_id}/cells/${cell_id}/debate`).then(r => r.data),

  getProvenance: (model_id: string, trace_id: string) =>
    api.get<ProvenanceTrace>(`/models/${model_id}/provenance/${trace_id}`).then(r => r.data),

  listSanity: (model_id: string) =>
    api.get<SanityIssue[]>(`/models/${model_id}/sanity`).then(r => r.data),

  startRun: (model_id: string, body: { recipe_id?: string; settings?: any }) =>
    api.post<RecipeRun>(`/models/${model_id}/runs`, { model_id, ...body }).then(r => r.data),

  listRuns: (model_id: string) =>
    api.get<RecipeRun[]>(`/models/${model_id}/runs`).then(r => r.data),

  getRun: (run_id: string) =>
    api.get<RecipeRun>(`/models/runs/${run_id}`).then(r => r.data),

  feedback: (model_id: string, body: {
    event_type: string
    cell_id?: string
    cell_path?: string
    payload?: any
  }) =>
    api.post(`/models/${model_id}/feedback`, body).then(r => r.data),

  // ── Cost governance ──────────────────────────────────
  estimateCost: (model_id: string, body: {
    recipe_id?: string
    model_id?: string
    debate_roles?: number
  }) =>
    api.post<{
      model_id: string
      total_usd: number
      total_input_tokens: number
      total_output_tokens: number
      step_count: number
      per_step_usd: Record<string, number>
      assumptions: string[]
      quota: {
        monthly_budget_usd: number
        spent_this_month_usd: number
        remaining_usd: number
        exceeded: boolean
        warn_threshold_usd: number
      }
      recommendation: 'ok' | 'warn' | 'blocked'
    }>(`/models/${model_id}/estimate-cost`, body).then(r => r.data),
}

// ── Governance APIs (cost, calibration, feedback dashboard) ────
export const governanceApi = {
  myQuota: () => api.get<{
    user_id: string
    username: string
    monthly_budget_usd: number
    spent_this_month_usd: number
    remaining_usd: number
    exceeded: boolean
    warn_threshold_usd: number
    default_budget_usd: number
    run_cap_usd: number | null
  }>('/cost/quota').then(r => r.data),

  costDashboard: (params?: { group_by?: 'industry' | 'user' | 'recipe' | 'day'; since_days?: number }) =>
    api.get<{
      group_by: string
      since_days: number
      scope: string
      rows: Array<{ key: string; runs: number; total_cost_usd: number; total_tokens: number }>
      total_usd: number
    }>('/cost/dashboard', { params }).then(r => r.data),

  patchUserBudget: (user_id: string, body: { monthly_budget_usd?: number; run_cap_usd?: number }) =>
    api.patch(`/admin/users/${user_id}/budget`, body).then(r => r.data),

  calibration: (industry?: string, since_days?: number) =>
    api.get<Array<{
      label: 'HIGH' | 'MEDIUM' | 'LOW'
      samples: number
      mae: number
      p50_err: number
      p90_err: number
      hit_rate: number
      expected_mae: number
      calibrated_label: 'HIGH' | 'MEDIUM' | 'LOW'
    }>>('/backtest/calibration', { params: { industry, since_days } }).then(r => r.data),

  feedbackDashboard: (params?: { since_days?: number; industry?: string }) =>
    api.get<{
      since_days: number
      industry: string | null
      events_by_type: Record<string, number>
      lessons_by_status: Record<string, number>
      total_events: number
      total_lessons: number
      hallucination_trend_weekly: Array<{
        week_start: string
        total_sampled: number
        mismatches: number
        hallucination_rate: number
        verdicts: Record<string, number>
      }>
      cells_by_review_status: Record<string, number>
      recent_lesson_impact: Array<{
        lesson_id: string
        title: string
        industry: string
        reviewed_at: string | null
        cells_touched_by_auto_apply: number
      }>
    }>('/playbook/feedback-dashboard', { params }).then(r => r.data),

  hallucinationRate: (since_days?: number) =>
    api.get<{
      since: string
      total_sampled: number
      verdicts: Record<string, number>
      hallucination_rate: number
    }>('/playbook/hallucination-rate', { params: { since_days } }).then(r => r.data),

  runReviewNow: (since_days = 7, auto_pause = true) =>
    api.post(`/citation-audit/review/run-now`, null, {
      params: { since_days, auto_pause },
    }).then(r => r.data),

  resumeGuardedModel: (model_id: string) =>
    api.post(`/citation-audit/review/resume-model/${model_id}`).then(r => r.data),
}

// ── Recipes ─────────────────────────────────────────────────

export const recipeApi = {
  list: (params?: { industry?: string; public_only?: boolean }) =>
    api.get<Recipe[]>('/recipes', { params }).then(r => r.data),

  get: (id: string) => api.get<Recipe>(`/recipes/${id}`).then(r => r.data),

  create: (body: Partial<Recipe>) =>
    api.post<Recipe>('/recipes', body).then(r => r.data),

  update: (id: string, body: Partial<Recipe>) =>
    api.patch<Recipe>(`/recipes/${id}`, body).then(r => r.data),

  fork: (id: string) =>
    api.post<Recipe>(`/recipes/${id}/fork`).then(r => r.data),

  delete: (id: string) => api.delete(`/recipes/${id}`).then(r => r.data),

  importPack: (slug: string) =>
    api.post(`/recipes/import-pack/${slug}`).then(r => r.data),
}

// ── Playbook ────────────────────────────────────────────────

export const playbookApi = {
  listPacks: () => api.get<PackInfo[]>('/playbook/packs').then(r => r.data),

  readPack: (slug: string) =>
    api.get<Record<string, string>>(`/playbook/packs/${slug}`).then(r => r.data),

  updatePack: (slug: string, filename: string, body: string) =>
    api.patch(`/playbook/packs/${slug}`, { filename, body }).then(r => r.data),

  listLessons: (slug: string) =>
    api.get<Lesson[]>(`/playbook/packs/${slug}/lessons`).then(r => r.data),

  searchLessons: (slug: string, cell_path: string) =>
    api.get<{ snippets: string }>(`/playbook/packs/${slug}/search`, {
      params: { cell_path },
    }).then(r => r.data),

  listPending: (status?: string, industry?: string) =>
    api.get<PendingLesson[]>('/playbook/pending', {
      params: { status, industry },
    }).then(r => r.data),

  review: (lesson_pk: string, body: {
    action: 'approve' | 'reject' | 'archive'
    review_note?: string
    edited_body?: string
  }) =>
    api.post(`/playbook/pending/${lesson_pk}/review`, body).then(r => r.data),

  consolidate: (dry_run = false) =>
    api.post('/playbook/consolidate', null, { params: { dry_run } }).then(r => r.data),
}

/**
 * Subscribe to a run's SSE stream.
 * Returns an EventSource; caller is responsible for `.close()`.
 * NOTE: EventSource does not support custom headers, so this requires
 * the SSE endpoint to accept session cookies OR a token query param.
 * Here we pass ?token=... manually so the nginx/auth layer can parse it.
 */
export function subscribeRun(
  run_id: string,
  onEvent: (type: string, data: any) => void,
): EventSource {
  const token = (localStorage.getItem('auth-storage') &&
    JSON.parse(localStorage.getItem('auth-storage') || '{}').state?.token) || ''
  const url = `/api/models/runs/${run_id}/stream?token=${encodeURIComponent(token)}`
  const es = new EventSource(url, { withCredentials: true })
  // Listen to both default "message" events and named events
  const handler = (ev: MessageEvent) => {
    let data: any = ev.data
    try { data = JSON.parse(ev.data) } catch { /* keep raw */ }
    onEvent(ev.type || 'message', data)
  }
  es.onmessage = handler
  ;[
    'subscribed', 'step_started', 'step_progress', 'step_completed',
    'cell_update', 'verify_flag', 'run_completed', 'step_failed', 'log',
  ].forEach(t => es.addEventListener(t, handler as any))
  return es
}
