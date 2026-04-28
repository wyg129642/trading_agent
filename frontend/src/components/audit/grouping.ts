import { AuditEvent } from '../../services/chatAudit'

export interface ToolPair {
  /** TOOL_EXEC_START event (always present in a well-formed pair). */
  start: AuditEvent
  /** TOOL_EXEC_DONE or TOOL_TIMEOUT. Null if execution is still in flight. */
  done: AuditEvent | null
  /** Inner events emitted while this tool was running (search keywords,
   * KB requests, webpage reads, etc.). */
  subEvents: AuditEvent[]
  /** Tool name (web_search, read_webpage, kb_search, ...). */
  name: string
  /** Parsed arguments from the start event. */
  args: Record<string, any>
  /** Latency in ms, sourced from the done event. */
  latencyMs: number | null
  /** Result text (truncated by backend if huge). */
  result: string
  /** Whether the tool call ended in error or timeout. */
  error: boolean
}

export interface RoundData {
  /** 1-based round number as the LLM saw it. */
  roundNum: number
  /** All events that logically belong to this round, in sequence order. */
  events: AuditEvent[]
  /** LLM_REQUEST event (the prompt sent to the model). */
  request: AuditEvent | null
  /** Model's chain-of-thought / reasoning text (Claude / Gemini thinking). */
  reasoning: AuditEvent | null
  /** Final assistant content for this round. */
  responseContent: AuditEvent | null
  /** LLM_DONE event with token / latency stats. */
  done: AuditEvent | null
  /** TOOL_CALLS_DETECTED — what the LLM decided to call. Null when the round
   * produced final text without tools. */
  toolCallsDecision: AuditEvent | null
  /** Paired tool executions, one per call the LLM made. */
  toolPairs: ToolPair[]
  /** Errors detected in this round (LLM_ERROR / TOOL_TIMEOUT / done with error). */
  errors: AuditEvent[]
}

export interface ModelGroup {
  modelId: string
  rounds: RoundData[]
  /** Events that didn't slot into a round (loose / pre-first-round noise). */
  loose: AuditEvent[]
  /** Final response content + LLM_FULL_RESPONSE for this model. */
  fullResponse: AuditEvent | null
}

export interface GroupedEvents {
  /** Truly request-level events: REQUEST_START / END / SUMMARY / MESSAGES_PAYLOAD. */
  requestLifecycle: AuditEvent[]
  models: ModelGroup[]
}

/** Sub-event types and which tool they typically belong to. */
const SUB_EVENT_TO_TOOL: Record<string, string[]> = {
  SEARCH_KEYWORDS: ['web_search'],
  SEARCH_ENGINE_CALL: ['web_search'],
  SEARCH_URLS_RETURNED: ['web_search'],
  SEARCH_TOP_RESULTS: ['web_search'],
  SEARCH_CACHE_HIT: ['web_search'],
  WEBPAGE_READ: ['read_webpage'],
  KB_REQUEST: ['kb_search', 'kb_fetch_document'],
  KB_RESULTS: ['kb_search', 'kb_fetch_document'],
  KB_FETCH: ['kb_fetch_document'],
  USER_KB_REQUEST: ['user_kb_search', 'user_kb_fetch_document'],
  USER_KB_RESULTS: ['user_kb_search', 'user_kb_fetch_document'],
  USER_KB_FETCH: ['user_kb_fetch_document'],
  GEMINI_GROUNDING: ['web_search'],
}

const ROUND_BOUNDARY_TYPES = new Set(['LLM_REQUEST'])

/** Round-level events we explicitly hoist out of the generic event pile. */
const ROUND_SLOT_TYPES = new Set([
  'LLM_REQUEST',
  'LLM_DONE',
  'LLM_RESPONSE_CONTENT',
  'LLM_FULL_RESPONSE',
  'MODEL_REASONING',
  'TOOL_CALLS_DETECTED',
  'GEMINI_FUNC_CALLS',
])

const ERROR_TYPES = new Set(['LLM_ERROR', 'TOOL_TIMEOUT'])

const TOOL_EXEC_TYPES = new Set([
  'TOOL_EXEC_START',
  'TOOL_EXEC_DONE',
  'TOOL_TIMEOUT',
])

/**
 * Slice a per-model event list (already sequence-sorted) into rounds.
 *
 * A round begins at LLM_REQUEST and ends just before the next LLM_REQUEST.
 * Within each round we recover paired tool executions and attribute sub-events
 * (SEARCH_*, KB_*, WEBPAGE_READ) to the matching tool by name; ties resolve to
 * the most recently STARTED tool of that name (LIFO) so that interleaved
 * parallel calls still attach to a sensible parent.
 */
function sliceIntoRounds(events: AuditEvent[]): RoundData[] {
  const rounds: RoundData[] = []
  let currentEvents: AuditEvent[] = []
  let currentRoundNum: number | null = null
  let pendingRoundNum: number | null = null

  const flush = () => {
    if (!currentEvents.length) return
    const rnum =
      currentRoundNum ?? pendingRoundNum ?? rounds.length + 1
    rounds.push(buildRoundData(rnum, currentEvents))
    currentEvents = []
  }

  for (const e of events) {
    if (ROUND_BOUNDARY_TYPES.has(e.event_type)) {
      flush()
      currentEvents = [e]
      const r = e.payload?.round_num
      currentRoundNum =
        typeof r === 'number'
          ? r
          : typeof e.round_num === 'number'
          ? e.round_num
          : rounds.length + 1
      pendingRoundNum = currentRoundNum
    } else {
      currentEvents.push(e)
    }
  }
  flush()
  return rounds
}

function buildRoundData(roundNum: number, events: AuditEvent[]): RoundData {
  // Pick out the named slots
  let request: AuditEvent | null = null
  let reasoning: AuditEvent | null = null
  let responseContent: AuditEvent | null = null
  let done: AuditEvent | null = null
  let toolCallsDecision: AuditEvent | null = null
  const errors: AuditEvent[] = []

  for (const e of events) {
    switch (e.event_type) {
      case 'LLM_REQUEST':
        request = e
        break
      case 'MODEL_REASONING':
        reasoning = e
        break
      case 'LLM_RESPONSE_CONTENT':
      case 'LLM_FULL_RESPONSE':
        responseContent = e
        break
      case 'LLM_DONE':
        done = e
        // LLM_DONE may carry an error flag in its payload
        if (e.payload?.error) errors.push(e)
        break
      case 'TOOL_CALLS_DETECTED':
      case 'GEMINI_FUNC_CALLS':
        toolCallsDecision = e
        break
      default:
        if (ERROR_TYPES.has(e.event_type)) errors.push(e)
    }
  }

  const toolPairs = recoverToolPairs(events)
  return {
    roundNum,
    events,
    request,
    reasoning,
    responseContent,
    done,
    toolCallsDecision,
    toolPairs,
    errors,
  }
}

function recoverToolPairs(events: AuditEvent[]): ToolPair[] {
  // Pair START with DONE/TIMEOUT by `tool_call_id` when available. The id
  // comes from the LLM's native tool_call.id (OpenAI/Claude) or a synthesized
  // `gemini-r{round}-{i}-{rand}` id we attach in the backend dispatch path.
  //
  // Legacy events (pre-2026-04-28) lack tool_call_id; fall back to FIFO-by-
  // tool-name. FIFO is still wrong for parallel async calls in general, but
  // any new event has the id so the fallback only matters for old runs.
  // Sub-events (KB_REQUEST, SEARCH_*, …) route to the most-recent open pair
  // of a candidate tool, same as before.
  const byId = new Map<string, ToolPair>()
  const fifoByName = new Map<string, ToolPair[]>()
  const recentOpenByName = new Map<string, ToolPair[]>()
  const all: ToolPair[] = []

  for (const e of events) {
    if (e.event_type === 'TOOL_EXEC_START') {
      const name = e.tool_name || e.payload?.tool_name || '?'
      const args = e.payload?.arguments || {}
      const tcid: string | null = e.payload?.tool_call_id ?? null
      const pair: ToolPair = {
        start: e,
        done: null,
        subEvents: [],
        name,
        args,
        latencyMs: null,
        result: '',
        error: false,
      }
      all.push(pair)
      if (tcid) byId.set(tcid, pair)
      else {
        const q = fifoByName.get(name) || []
        q.push(pair)
        fifoByName.set(name, q)
      }
      const stack = recentOpenByName.get(name) || []
      stack.push(pair)
      recentOpenByName.set(name, stack)
      continue
    }
    if (e.event_type === 'TOOL_EXEC_DONE' || e.event_type === 'TOOL_TIMEOUT') {
      const name = e.tool_name || e.payload?.tool_name || '?'
      const tcid: string | null = e.payload?.tool_call_id ?? null
      let pair: ToolPair | undefined
      if (tcid) {
        pair = byId.get(tcid)
        if (pair) byId.delete(tcid)
      }
      if (!pair) {
        // Fallback for legacy events without a tool_call_id: FIFO by name.
        const q = fifoByName.get(name) || []
        pair = q.shift()
        if (pair) fifoByName.set(name, q)
      }
      if (pair) {
        pair.done = e
        pair.latencyMs = e.latency_ms ?? null
        pair.result = e.payload?.result ?? ''
        pair.error =
          e.event_type === 'TOOL_TIMEOUT' || Boolean(e.payload?.error)
        // Remove from recentOpenByName so sub-event routing only sees still-open pairs.
        const stack = recentOpenByName.get(name)
        if (stack) {
          const idx = stack.indexOf(pair)
          if (idx >= 0) stack.splice(idx, 1)
        }
      }
      continue
    }
    // Sub-event candidate: route to the most recent still-open pair whose
    // name is in the candidate-tool list for this event type.
    const candidates = SUB_EVENT_TO_TOOL[e.event_type]
    if (!candidates) continue
    for (let i = candidates.length - 1; i >= 0; i--) {
      const stack = recentOpenByName.get(candidates[i])
      if (stack && stack.length) {
        stack[stack.length - 1].subEvents.push(e)
        break
      }
    }
  }
  return all
}

/**
 * Top-level grouper: separates request-level events from per-model events,
 * then per-model into rounds.
 */
export function groupEvents(events: AuditEvent[]): GroupedEvents {
  const requestLifecycle: AuditEvent[] = []
  const byModel = new Map<string, AuditEvent[]>()

  for (const e of events) {
    if (!e.model_id) {
      requestLifecycle.push(e)
    } else {
      const arr = byModel.get(e.model_id) || []
      arr.push(e)
      byModel.set(e.model_id, arr)
    }
  }

  requestLifecycle.sort((a, b) => a.sequence - b.sequence)

  const models: ModelGroup[] = []
  for (const [modelId, arr] of byModel.entries()) {
    arr.sort((a, b) => a.sequence - b.sequence)
    const rounds = sliceIntoRounds(arr)
    // Anything before the first LLM_REQUEST is "loose" (rare).
    const firstReqIdx = arr.findIndex((e) => e.event_type === 'LLM_REQUEST')
    const loose = firstReqIdx > 0 ? arr.slice(0, firstReqIdx) : []
    const fullResponse =
      [...arr].reverse().find((e) => e.event_type === 'LLM_FULL_RESPONSE') || null
    models.push({ modelId, rounds, loose, fullResponse })
  }

  return { requestLifecycle, models }
}

export const _internal_for_tests = {
  sliceIntoRounds,
  recoverToolPairs,
  TOOL_EXEC_TYPES,
  ROUND_SLOT_TYPES,
}
