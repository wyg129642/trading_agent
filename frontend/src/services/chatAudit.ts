import api from './api'

export interface AuditRunSummary {
  id: string
  trace_id: string
  user_id: string | null
  username: string
  conversation_id: string | null
  message_id: string | null
  user_content_preview: string
  models_requested: string[]
  mode: string
  web_search_mode: string
  feature_flags: Record<string, unknown>
  status: 'running' | 'done' | 'error' | 'cancelled' | string
  error_message: string | null
  rounds_used: number
  tool_calls_total: number
  tool_calls_by_name: Record<string, number>
  urls_searched: number
  urls_read: number
  citations_count: number
  total_tokens: number
  total_cost_usd: number | null
  total_latency_ms: number
  final_content_len: number
  started_at: string | null
  finished_at: string | null
}

export interface AuditEvent {
  id: string
  trace_id: string
  sequence: number
  event_type: string
  model_id: string | null
  round_num: number | null
  tool_name: string | null
  latency_ms: number | null
  payload: Record<string, any> | null
  payload_truncated: boolean
  created_at: string | null
}

export interface AuditMessage {
  id: string
  conversation_id: string
  role: string
  content: string
  attachments: any[]
  created_at: string | null
}

export interface AuditModelResponse {
  id: string
  model_id: string
  model_name: string
  content: string
  tokens_used: number | null
  latency_ms: number | null
  rating: number | null
  rating_comment: string | null
  error: string | null
  sources: any[]
  created_at: string | null
}

export interface AuditRunDetail {
  run: AuditRunSummary
  user_content_full: string
  message: AuditMessage | null
  model_responses: AuditModelResponse[]
}

export interface AuditStats {
  since: string
  days: number
  total_runs: number
  error_runs: number
  error_rate: number
  avg_latency_ms: number | null
  p95_latency_ms: number | null
  total_tokens: number
  top_tools: { name: string; count: number }[]
  top_users: { username: string; count: number }[]
  scope: 'all' | 'self'
}

export interface ListRunsResponse {
  runs: AuditRunSummary[]
  next_cursor: string | null
  scope: 'all' | 'self'
}

export interface ListEventsResponse {
  run_id: string
  events: AuditEvent[]
  count: number
}

export interface ListRunsParams {
  user_id?: string
  username?: string
  conversation_id?: string
  model?: string
  tool?: string
  status?: string
  has_error?: boolean
  q?: string
  started_from?: string
  started_to?: string
  cursor?: string
  limit?: number
}

export const chatAuditApi = {
  listRuns: async (params: ListRunsParams = {}): Promise<ListRunsResponse> => {
    const res = await api.get<ListRunsResponse>('/chat-audit/runs', { params })
    return res.data
  },
  getRun: async (runId: string): Promise<AuditRunDetail> => {
    const res = await api.get<AuditRunDetail>(`/chat-audit/runs/${runId}`)
    return res.data
  },
  listEvents: async (
    runId: string,
    params: { event_type?: string; model_id?: string; round_num?: number; tool_name?: string; after_seq?: number; limit?: number } = {},
  ): Promise<ListEventsResponse> => {
    const res = await api.get<ListEventsResponse>(`/chat-audit/runs/${runId}/events`, { params })
    return res.data
  },
  exportRun: (runId: string): string => `/api/chat-audit/runs/${runId}/export`,
  stats: async (days = 7): Promise<AuditStats> => {
    const res = await api.get<AuditStats>('/chat-audit/stats', { params: { days } })
    return res.data
  },
}
