/**
 * Multi-sheet spreadsheet editor for the workspace 估值表.
 *
 * Canonical data model (matches ``backend/app/services/user_kb_workbook.py``):
 *
 *     {
 *       active_sheet_id: "sheet-1",
 *       sheets: [
 *         {
 *           id: "sheet-1",
 *           name: "估值表",
 *           rows: 22,
 *           cols: 8,
 *           cells: { "A1": {v?: "...", f?: "=A2+1"}, ... },
 *           col_widths?: number[],
 *         }
 *       ]
 *     }
 *
 * The component ALSO accepts the legacy flat shape ``{rows, cols, cells,
 * col_widths}`` so existing documents don't need a schema migration up
 * front — it upgrades to the multi-sheet shape on the first edit.
 *
 * Feature set (vs the old editor):
 *   - Multiple sheets per workbook with a sheet tab bar at the bottom.
 *   - Drag to resize columns by grabbing the right edge of a column header.
 *   - Range selection (shift+click, shift+arrows), displayed in the status
 *     bar with sum / avg / count aggregates.
 *   - Copy/paste using the system clipboard: TSV on copy, paste splits on
 *     tab / newline so you can round-trip Excel, Google Sheets, and plain
 *     text.
 *   - Undo / redo with a 100-step ring buffer, Ctrl+Z / Ctrl+Y bindings.
 *   - Richer formula language: +, -, *, /, parens, range fns
 *     (SUM, AVG/AVERAGE, MIN, MAX, COUNT, PRODUCT, MEDIAN, STDEV) and
 *     scalar fns (IF, IFERROR, ROUND, ABS, SQRT, POWER, LOG, LN, EXP).
 *     Nested calls are supported (eg. ``=IF(A1>0, ROUND(A1*B1,2), 0)``).
 *
 * The component is controlled (``value`` + ``onChange``). The parent is
 * responsible for debounced persistence and error handling.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Button, Dropdown, Input, InputNumber, Space, Tooltip, Typography } from 'antd'
import {
  PlusOutlined, DeleteOutlined, UndoOutlined, RedoOutlined,
  SaveOutlined, MoreOutlined,
} from '@ant-design/icons'

const { Text } = Typography

// ── Shape types ─────────────────────────────────────────────────

export interface SpreadsheetCell {
  v?: string
  f?: string
}

export interface Sheet {
  id: string
  name: string
  rows: number
  cols: number
  cells: Record<string, SpreadsheetCell>
  col_widths?: number[] | null
}

export interface Workbook {
  active_sheet_id: string
  sheets: Sheet[]
}

/**
 * Legacy flat shape: ``{rows, cols, cells, col_widths}``. The component
 * accepts it transparently via ``normalizeWorkbook`` and the parent gets
 * the multi-sheet shape back on every onChange.
 */
export interface LegacyFlatSheet {
  rows: number
  cols: number
  cells: Record<string, SpreadsheetCell>
  col_widths?: number[] | null
}

export type SpreadsheetData = Workbook | LegacyFlatSheet

// Back-compat alias so any caller that used the old default export keeps working.
export type { Sheet as SheetData }


export interface SpreadsheetEditorProps {
  value: SpreadsheetData
  onChange: (next: Workbook) => void
  readonly?: boolean
  title?: React.ReactNode
  onSave?: () => Promise<void> | void
  saving?: boolean
}

// ── A1-coord helpers ────────────────────────────────────────────

function colLabel(c: number): string {
  let s = ''
  c = c | 0
  do {
    s = String.fromCharCode(65 + (c % 26)) + s
    c = Math.floor(c / 26) - 1
  } while (c >= 0)
  return s
}

function parseA1(ref: string): { row: number; col: number } | null {
  const m = /^([A-Z]+)([0-9]+)$/.exec(ref.trim())
  if (!m) return null
  let col = 0
  for (const ch of m[1]) col = col * 26 + (ch.charCodeAt(0) - 64)
  const row = parseInt(m[2], 10)
  if (!row) return null
  return { row: row - 1, col: col - 1 }
}

function cellKey(row: number, col: number): string {
  return `${colLabel(col)}${row + 1}`
}

// ── Workbook shape normalization ───────────────────────────────

function makeSheet(name = '估值表', id = 'sheet-1'): Sheet {
  return { id, name, rows: 10, cols: 6, cells: {}, col_widths: null }
}

export function normalizeWorkbook(raw: any): Workbook {
  if (raw && Array.isArray(raw.sheets) && raw.sheets.length > 0) {
    const sheets: Sheet[] = raw.sheets.map((s: any, idx: number) => ({
      id: String(s?.id ?? `sheet-${idx + 1}`),
      name: String(s?.name ?? `Sheet${idx + 1}`),
      rows: Math.max(1, Math.min(500, Number(s?.rows) || 1)),
      cols: Math.max(1, Math.min(50, Number(s?.cols) || 1)),
      cells: (s?.cells && typeof s.cells === 'object') ? { ...s.cells } : {},
      col_widths: Array.isArray(s?.col_widths) ? s.col_widths.slice() : null,
    }))
    const active = raw.active_sheet_id && sheets.some((s) => s.id === raw.active_sheet_id)
      ? String(raw.active_sheet_id)
      : sheets[0].id
    return { active_sheet_id: active, sheets }
  }
  // Legacy flat shape.
  if (raw && (raw.cells || raw.rows)) {
    const s: Sheet = {
      id: 'sheet-1',
      name: '估值表',
      rows: Math.max(1, Math.min(500, Number(raw.rows) || 22)),
      cols: Math.max(1, Math.min(50, Number(raw.cols) || 8)),
      cells: (raw.cells && typeof raw.cells === 'object') ? { ...raw.cells } : {},
      col_widths: Array.isArray(raw.col_widths) ? raw.col_widths.slice() : null,
    }
    return { active_sheet_id: s.id, sheets: [s] }
  }
  const s = makeSheet()
  return { active_sheet_id: s.id, sheets: [s] }
}

// ── Formula evaluation ─────────────────────────────────────────
//
// Tokenizer + mini recursive-descent parser handling:
//   - literals: 1, 2.5, -3, .5
//   - cell refs: A1, BC23
//   - range refs: A1:A10 (only valid inside a function arg)
//   - functions: SUM, AVG, AVERAGE, MIN, MAX, COUNT, PRODUCT, MEDIAN,
//     STDEV (ranges); IF, IFERROR, ROUND, ABS, SQRT, POWER, LOG, LN, EXP
//     (scalars, nested OK)
//   - operators: + - * / (), and comparison <, <=, >, >=, =, <>
//   - unary minus
//
// Cycles are detected by carrying a ``visiting`` set through resolveCell.

type Token =
  | { kind: 'num'; value: number }
  | { kind: 'ident'; value: string }
  | { kind: 'op'; value: string }
  | { kind: 'lparen' }
  | { kind: 'rparen' }
  | { kind: 'comma' }
  | { kind: 'colon' }
  | { kind: 'eof' }

function tokenize(src: string): Token[] {
  const tokens: Token[] = []
  let i = 0
  while (i < src.length) {
    const ch = src[i]
    if (ch === ' ' || ch === '\t') { i++; continue }
    if (ch === '(') { tokens.push({ kind: 'lparen' }); i++; continue }
    if (ch === ')') { tokens.push({ kind: 'rparen' }); i++; continue }
    if (ch === ',') { tokens.push({ kind: 'comma' }); i++; continue }
    if (ch === ':') { tokens.push({ kind: 'colon' }); i++; continue }
    if (ch === '<' || ch === '>' || ch === '=') {
      // 2-char: <=, >=, <>
      if (src[i + 1] === '=') { tokens.push({ kind: 'op', value: ch + '=' }); i += 2; continue }
      if (ch === '<' && src[i + 1] === '>') { tokens.push({ kind: 'op', value: '<>' }); i += 2; continue }
      tokens.push({ kind: 'op', value: ch === '=' ? '==' : ch }); i++; continue
    }
    if (ch === '+' || ch === '-' || ch === '*' || ch === '/' || ch === '^' || ch === '%') {
      tokens.push({ kind: 'op', value: ch }); i++; continue
    }
    // Number literal (allow leading decimal like ".5").
    if ((ch >= '0' && ch <= '9') || (ch === '.' && src[i + 1] >= '0' && src[i + 1] <= '9')) {
      let j = i
      while (j < src.length && /[0-9._eE+\-]/.test(src[j])) {
        // Don't consume a '+' or '-' unless preceded by 'e' / 'E' (scientific).
        if ((src[j] === '+' || src[j] === '-') && j > i && src[j - 1].toLowerCase() !== 'e') break
        j++
      }
      const text = src.slice(i, j)
      const n = Number(text)
      tokens.push({ kind: 'num', value: Number.isFinite(n) ? n : NaN })
      i = j
      continue
    }
    // Identifier: [A-Za-z_][A-Za-z0-9_]*
    if (/[A-Za-z_]/.test(ch)) {
      let j = i + 1
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j])) j++
      tokens.push({ kind: 'ident', value: src.slice(i, j).toUpperCase() })
      i = j
      continue
    }
    // Unknown char — skip (simpler than surfacing a parser error mid-edit).
    i++
  }
  tokens.push({ kind: 'eof' })
  return tokens
}

type Resolver = (row: number, col: number) => number | string

function expandRange(a: string, b: string, resolve: Resolver): number[] {
  const aa = parseA1(a)
  const bb = parseA1(b)
  if (!aa || !bb) return []
  const r0 = Math.min(aa.row, bb.row)
  const r1 = Math.max(aa.row, bb.row)
  const c0 = Math.min(aa.col, bb.col)
  const c1 = Math.max(aa.col, bb.col)
  const values: number[] = []
  for (let r = r0; r <= r1; r++) {
    for (let c = c0; c <= c1; c++) {
      const v = resolve(r, c)
      if (typeof v === 'number' && !Number.isNaN(v)) values.push(v)
    }
  }
  return values
}

function isRangeFn(name: string): boolean {
  return ['SUM', 'AVG', 'AVERAGE', 'MIN', 'MAX', 'COUNT', 'PRODUCT',
    'MEDIAN', 'STDEV'].includes(name)
}

function applyRangeFn(name: string, values: number[]): number {
  if (values.length === 0) return 0
  switch (name) {
    case 'SUM': return values.reduce((s, x) => s + x, 0)
    case 'AVG':
    case 'AVERAGE': return values.reduce((s, x) => s + x, 0) / values.length
    case 'MIN': return Math.min(...values)
    case 'MAX': return Math.max(...values)
    case 'COUNT': return values.length
    case 'PRODUCT': return values.reduce((s, x) => s * x, 1)
    case 'MEDIAN': {
      const sorted = values.slice().sort((a, b) => a - b)
      const mid = Math.floor(sorted.length / 2)
      return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2
    }
    case 'STDEV': {
      if (values.length < 2) return 0
      const mean = values.reduce((s, x) => s + x, 0) / values.length
      const variance = values.reduce((s, x) => s + (x - mean) ** 2, 0) / (values.length - 1)
      return Math.sqrt(variance)
    }
    default: return 0
  }
}

class Parser {
  tokens: Token[]
  pos = 0
  resolve: Resolver
  constructor(tokens: Token[], resolve: Resolver) {
    this.tokens = tokens
    this.resolve = resolve
  }
  peek() { return this.tokens[this.pos] }
  eat() { return this.tokens[this.pos++] }
  expect(kind: string, value?: string) {
    const t = this.eat()
    if (t.kind !== kind || (value !== undefined && (t as any).value !== value)) {
      throw new Error(`Expected ${kind}${value ? ` '${value}'` : ''}`)
    }
    return t
  }
  // Grammar:
  //   expr   → cmp ( (<|<=|>|>=|==|<>) cmp )*
  //   cmp    → sum
  //   sum    → term ( ('+'|'-') term )*
  //   term   → factor ( ('*'|'/') factor )*
  //   factor → unary ( '^' factor )?
  //   unary  → '-' unary | atom
  //   atom   → num | cellref | fn '(' args ')' | '(' expr ')'
  parse(): number {
    const v = this.expr()
    if (this.peek().kind !== 'eof') {
      throw new Error('Unexpected trailing tokens')
    }
    return v
  }
  expr(): number { return this.cmp() }
  cmp(): number {
    let left = this.sum()
    while (true) {
      const t = this.peek()
      if (t.kind === 'op' && ['<', '<=', '>', '>=', '==', '<>'].includes(t.value)) {
        this.eat()
        const right = this.sum()
        switch (t.value) {
          case '<': left = left < right ? 1 : 0; break
          case '<=': left = left <= right ? 1 : 0; break
          case '>': left = left > right ? 1 : 0; break
          case '>=': left = left >= right ? 1 : 0; break
          case '==': left = left === right ? 1 : 0; break
          case '<>': left = left !== right ? 1 : 0; break
        }
      } else break
    }
    return left
  }
  sum(): number {
    let left = this.term()
    while (true) {
      const t = this.peek()
      if (t.kind === 'op' && (t.value === '+' || t.value === '-')) {
        this.eat()
        const right = this.term()
        left = t.value === '+' ? left + right : left - right
      } else break
    }
    return left
  }
  term(): number {
    let left = this.factor()
    while (true) {
      const t = this.peek()
      if (t.kind === 'op' && (t.value === '*' || t.value === '/' || t.value === '%')) {
        this.eat()
        const right = this.factor()
        if (t.value === '*') left = left * right
        else if (t.value === '/') left = right === 0 ? NaN : left / right
        else left = right === 0 ? NaN : left % right
      } else break
    }
    return left
  }
  factor(): number {
    const left = this.unary()
    const t = this.peek()
    if (t.kind === 'op' && t.value === '^') {
      this.eat()
      const right = this.factor()
      return Math.pow(left, right)
    }
    return left
  }
  unary(): number {
    const t = this.peek()
    if (t.kind === 'op' && t.value === '-') { this.eat(); return -this.unary() }
    if (t.kind === 'op' && t.value === '+') { this.eat(); return this.unary() }
    return this.atom()
  }
  atom(): number {
    const t = this.peek()
    if (t.kind === 'num') { this.eat(); return t.value }
    if (t.kind === 'lparen') {
      this.eat()
      const v = this.expr()
      this.expect('rparen')
      return v
    }
    if (t.kind === 'ident') {
      // Is it a function call?
      const next = this.tokens[this.pos + 1]
      if (next?.kind === 'lparen') {
        const name = t.value
        this.eat(); this.eat()  // ident, '('
        const args = this.args()
        this.expect('rparen')
        return this.callFn(name, args)
      }
      // Otherwise — cell ref. Must look like A1.
      this.eat()
      const m = /^([A-Z]+)([0-9]+)$/.exec(t.value)
      if (!m) throw new Error(`Unknown identifier: ${t.value}`)
      const parsed = parseA1(t.value)
      if (!parsed) throw new Error(`Invalid cell ref: ${t.value}`)
      const v = this.resolve(parsed.row, parsed.col)
      return typeof v === 'number' ? v : Number(v) || 0
    }
    throw new Error('Unexpected token')
  }
  args(): Arg[] {
    const out: Arg[] = []
    if (this.peek().kind === 'rparen') return out
    out.push(this.arg())
    while (this.peek().kind === 'comma') {
      this.eat()
      out.push(this.arg())
    }
    return out
  }
  arg(): Arg {
    // A range arg is an ident followed by ':' ident. Otherwise it's an expr
    // which we evaluate now.
    const t = this.peek()
    if (t.kind === 'ident' && this.tokens[this.pos + 1]?.kind === 'colon') {
      const a = this.eat() as Extract<Token, { kind: 'ident' }>
      this.eat()  // ':'
      const b = this.expect('ident') as Extract<Token, { kind: 'ident' }>
      return { kind: 'range', a: a.value, b: b.value }
    }
    return { kind: 'scalar', value: this.expr() }
  }
  callFn(name: string, args: Arg[]): number {
    if (isRangeFn(name)) {
      const vals: number[] = []
      for (const a of args) {
        if (a.kind === 'range') vals.push(...expandRange(a.a, a.b, this.resolve))
        else vals.push(a.value)
      }
      return applyRangeFn(name, vals)
    }
    // Scalar fns below expect explicit numeric args.
    const nums = args.map((a) => a.kind === 'scalar' ? a.value : 0)
    switch (name) {
      case 'IF':
        if (nums.length < 2) return 0
        return nums[0] ? nums[1] : (nums[2] ?? 0)
      case 'IFERROR':
        // ``IFERROR(expr, fallback)`` — because we evaluated expr eagerly
        // above, a NaN/Infinity result (divide-by-zero, bad cast) is what
        // we treat as the "error" signal. Good enough for a DCF sheet.
        if (Number.isFinite(nums[0])) return nums[0]
        return nums[1] ?? 0
      case 'ROUND': {
        const digits = Math.max(0, Math.min(10, Math.floor(nums[1] ?? 0)))
        const k = Math.pow(10, digits)
        return Math.round(nums[0] * k) / k
      }
      case 'ABS': return Math.abs(nums[0])
      case 'SQRT': return nums[0] < 0 ? NaN : Math.sqrt(nums[0])
      case 'POWER': return Math.pow(nums[0], nums[1])
      case 'LOG': {
        // LOG(x) = log10(x); LOG(x, base) = log_base(x)
        if (nums.length < 2) return Math.log10(nums[0])
        return Math.log(nums[0]) / Math.log(nums[1])
      }
      case 'LN': return Math.log(nums[0])
      case 'EXP': return Math.exp(nums[0])
      case 'MAX': return Math.max(...nums)
      case 'MIN': return Math.min(...nums)
      case 'SUM': return nums.reduce((s, x) => s + x, 0)
      default: return 0
    }
  }
}

type Arg = { kind: 'range'; a: string; b: string } | { kind: 'scalar'; value: number }

function evalFormula(
  raw: string,
  sheet: Sheet,
  visiting: Set<string> = new Set(),
): number | string {
  if (!raw.startsWith('=')) return raw
  const expr = raw.slice(1).trim()
  if (!expr) return 0
  const tokens = tokenize(expr)
  const resolve: Resolver = (row, col) => resolveCell(cellKey(row, col), sheet, visiting)
  const parser = new Parser(tokens, resolve)
  try {
    const v = parser.parse()
    if (!Number.isFinite(v)) return '#ERR'
    return v
  } catch {
    return '#ERR'
  }
}

function resolveCell(
  key: string,
  sheet: Sheet,
  visiting: Set<string>,
): number | string {
  const cell = sheet.cells[key]
  if (!cell) return 0
  if (cell.f) {
    if (visiting.has(key)) return '#CYCLE'
    visiting.add(key)
    const v = evalFormula(cell.f, sheet, visiting)
    visiting.delete(key)
    return v
  }
  const raw = cell.v
  if (raw === undefined || raw === null || raw === '') return 0
  const n = Number(raw)
  return Number.isFinite(n) ? n : String(raw)
}

function displayCell(cell: SpreadsheetCell | undefined, sheet: Sheet): string {
  if (!cell) return ''
  if (cell.f) {
    const v = evalFormula(cell.f, sheet)
    if (typeof v === 'number') {
      if (!Number.isFinite(v)) return '#ERR'
      if (Math.abs(v) >= 1000) return v.toFixed(0)
      return v.toFixed(2).replace(/\.00$/, '').replace(/(\.\d)0$/, '$1')
    }
    return String(v)
  }
  return cell.v ?? ''
}

// ── Cell styles ─────────────────────────────────────────────────

const cellStyle: React.CSSProperties = {
  height: 28,
  borderRight: '1px solid var(--ws-grid-line, #e1e4e8)',
  borderBottom: '1px solid var(--ws-grid-line, #e1e4e8)',
  padding: '4px 6px',
  fontSize: 13,
  cursor: 'cell',
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
  userSelect: 'none',
  position: 'relative',
}

const colHeaderStyle: React.CSSProperties = {
  height: 26,
  padding: '4px 6px',
  fontSize: 12,
  fontWeight: 600,
  color: 'var(--ws-text-secondary, #475569)',
  textAlign: 'center',
  borderRight: '1px solid var(--ws-border-strong, #d1d5db)',
  background: 'var(--ws-grid-header-bg, #f1f3f5)',
  flexShrink: 0,
  position: 'relative',
}

const rowHeaderStyle: React.CSSProperties = {
  width: 40,
  height: 28,
  padding: '4px 6px',
  fontSize: 12,
  fontWeight: 600,
  color: 'var(--ws-text-secondary, #475569)',
  textAlign: 'center',
  borderRight: '1px solid var(--ws-border-strong, #d1d5db)',
  borderBottom: '1px solid var(--ws-grid-line, #e1e4e8)',
  background: 'var(--ws-grid-header-bg, #f1f3f5)',
  position: 'sticky',
  left: 0,
  zIndex: 1,
  flexShrink: 0,
}

const DEFAULT_COL_WIDTH = 110
const FIRST_COL_WIDTH = 160

// ── Undo/redo ring buffer ──────────────────────────────────────

const HISTORY_LIMIT = 100

function sheetsEqual(a: Sheet[], b: Sheet[]): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (JSON.stringify(a[i]) !== JSON.stringify(b[i])) return false
  }
  return true
}

// ── Component ──────────────────────────────────────────────────

export default function SpreadsheetEditor({
  value,
  onChange,
  readonly = false,
  title,
  onSave,
  saving = false,
}: SpreadsheetEditorProps) {
  const workbook = useMemo(() => normalizeWorkbook(value), [value])
  const activeSheet = useMemo(() => {
    return workbook.sheets.find((s) => s.id === workbook.active_sheet_id)
      ?? workbook.sheets[0]
  }, [workbook])

  const [selected, setSelected] = useState<{ row: number; col: number } | null>(null)
  const [selEnd, setSelEnd] = useState<{ row: number; col: number } | null>(null)
  const [editing, setEditing] = useState<{ row: number; col: number } | null>(null)
  const [editValue, setEditValue] = useState('')
  const [editingSheetId, setEditingSheetId] = useState<string | null>(null)
  const [editingSheetName, setEditingSheetName] = useState('')
  const gridRef = useRef<HTMLDivElement>(null)

  // Reset selection when the active sheet changes.
  useEffect(() => {
    setSelected(null); setSelEnd(null); setEditing(null); setEditValue('')
  }, [activeSheet.id])

  // Undo / redo stacks of sheet snapshots (full sheets array — simpler than
  // diff-based and small enough for 100 steps).
  const undoRef = useRef<Sheet[][]>([])
  const redoRef = useRef<Sheet[][]>([])
  const lastPushedRef = useRef<Sheet[] | null>(null)

  const pushHistory = useCallback(() => {
    const snap = JSON.parse(JSON.stringify(workbook.sheets)) as Sheet[]
    if (lastPushedRef.current && sheetsEqual(lastPushedRef.current, snap)) return
    undoRef.current.push(snap)
    if (undoRef.current.length > HISTORY_LIMIT) undoRef.current.shift()
    redoRef.current = []
    lastPushedRef.current = snap
  }, [workbook.sheets])

  const emit = useCallback((nextSheets: Sheet[], activeId?: string) => {
    onChange({
      active_sheet_id: activeId ?? workbook.active_sheet_id,
      sheets: nextSheets,
    })
  }, [onChange, workbook.active_sheet_id])

  const mutateActive = useCallback((patch: (s: Sheet) => Sheet) => {
    pushHistory()
    const next = workbook.sheets.map(
      (s) => s.id === activeSheet.id ? patch(s) : s,
    )
    emit(next)
  }, [pushHistory, workbook.sheets, activeSheet.id, emit])

  const setCell = useCallback((row: number, col: number, next: SpreadsheetCell | null) => {
    if (readonly) return
    const key = cellKey(row, col)
    mutateActive((s) => {
      const cells = { ...s.cells }
      if (next === null || (!next.v && !next.f)) delete cells[key]
      else cells[key] = next
      return { ...s, cells }
    })
  }, [mutateActive, readonly])

  const beginEdit = useCallback((row: number, col: number, seed?: string) => {
    if (readonly) return
    const cell = activeSheet.cells[cellKey(row, col)]
    const initial = seed ?? (cell?.f ?? cell?.v ?? '')
    setEditing({ row, col })
    setEditValue(initial)
  }, [activeSheet.cells, readonly])

  const commitEdit = useCallback((opts?: { advance?: 'right' | 'down'; cancel?: boolean }) => {
    if (!editing) return
    if (opts?.cancel) {
      setEditing(null); setEditValue(''); return
    }
    const raw = editValue
    if (raw.startsWith('=')) setCell(editing.row, editing.col, { f: raw })
    else setCell(editing.row, editing.col, raw === '' ? null : { v: raw })
    setEditing(null); setEditValue('')
    if (opts?.advance === 'right') {
      setSelected({ row: editing.row, col: Math.min(activeSheet.cols - 1, editing.col + 1) })
    } else if (opts?.advance === 'down') {
      setSelected({ row: Math.min(activeSheet.rows - 1, editing.row + 1), col: editing.col })
    }
  }, [editing, editValue, setCell, activeSheet.rows, activeSheet.cols])

  // ── Undo / redo ──────────────────────────────────────────────

  const undo = useCallback(() => {
    if (undoRef.current.length === 0) return
    const prev = undoRef.current.pop()!
    redoRef.current.push(JSON.parse(JSON.stringify(workbook.sheets)))
    lastPushedRef.current = prev
    emit(prev)
  }, [emit, workbook.sheets])

  const redo = useCallback(() => {
    if (redoRef.current.length === 0) return
    const next = redoRef.current.pop()!
    undoRef.current.push(JSON.parse(JSON.stringify(workbook.sheets)))
    lastPushedRef.current = next
    emit(next)
  }, [emit, workbook.sheets])

  // ── Selection range helpers ──────────────────────────────────

  const selBounds = useMemo(() => {
    if (!selected) return null
    const end = selEnd ?? selected
    return {
      r0: Math.min(selected.row, end.row),
      r1: Math.max(selected.row, end.row),
      c0: Math.min(selected.col, end.col),
      c1: Math.max(selected.col, end.col),
    }
  }, [selected, selEnd])

  const selectionStats = useMemo(() => {
    if (!selBounds) return null
    const vals: number[] = []
    let count = 0
    for (let r = selBounds.r0; r <= selBounds.r1; r++) {
      for (let c = selBounds.c0; c <= selBounds.c1; c++) {
        const cell = activeSheet.cells[cellKey(r, c)]
        if (!cell) continue
        count++
        const v = cell.f ? evalFormula(cell.f, activeSheet) : (cell.v ?? '')
        const n = typeof v === 'number' ? v : Number(v)
        if (Number.isFinite(n)) vals.push(n)
      }
    }
    const sum = vals.reduce((s, x) => s + x, 0)
    return {
      count,
      sum,
      avg: vals.length > 0 ? sum / vals.length : null,
    }
  }, [selBounds, activeSheet])

  // ── Keyboard handling (selection nav + undo/redo + delete + copy/paste) ──

  useEffect(() => {
    const onKeyDown = (e: KeyboardEvent) => {
      // Ignore when an input (edit-in-place, formula bar, modal) has focus.
      const target = e.target as HTMLElement | null
      if (target?.tagName === 'INPUT' || target?.tagName === 'TEXTAREA' || target?.isContentEditable) {
        // But catch the global Ctrl+S save shortcut even from the formula bar.
        if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 's') {
          e.preventDefault(); onSave?.()
        }
        return
      }
      if ((e.ctrlKey || e.metaKey)) {
        const k = e.key.toLowerCase()
        if (k === 'z' && !e.shiftKey) { e.preventDefault(); undo(); return }
        if (k === 'y' || (k === 'z' && e.shiftKey)) { e.preventDefault(); redo(); return }
        if (k === 's') { e.preventDefault(); onSave?.(); return }
        if (k === 'c' || k === 'x') {
          if (!selBounds) return
          const rows: string[] = []
          for (let r = selBounds.r0; r <= selBounds.r1; r++) {
            const row: string[] = []
            for (let c = selBounds.c0; c <= selBounds.c1; c++) {
              const cell = activeSheet.cells[cellKey(r, c)]
              row.push(displayCell(cell, activeSheet))
            }
            rows.push(row.join('\t'))
          }
          try { navigator.clipboard.writeText(rows.join('\n')) } catch { /* ignore */ }
          if (k === 'x' && !readonly) {
            pushHistory()
            const nextCells = { ...activeSheet.cells }
            for (let r = selBounds.r0; r <= selBounds.r1; r++) {
              for (let c = selBounds.c0; c <= selBounds.c1; c++) {
                delete nextCells[cellKey(r, c)]
              }
            }
            emit(workbook.sheets.map(
              (s) => s.id === activeSheet.id ? { ...s, cells: nextCells } : s,
            ))
          }
          e.preventDefault()
          return
        }
        if (k === 'v' && !readonly && selected) {
          e.preventDefault()
          ;(async () => {
            try {
              const text = await navigator.clipboard.readText()
              if (!text) return
              pushHistory()
              const lines = text.replace(/\r\n?/g, '\n').split('\n')
              const nextCells = { ...activeSheet.cells }
              let maxRow = activeSheet.rows
              let maxCol = activeSheet.cols
              for (let i = 0; i < lines.length; i++) {
                const cells = lines[i].split('\t')
                for (let j = 0; j < cells.length; j++) {
                  const rr = selected.row + i
                  const cc = selected.col + j
                  if (rr >= 500 || cc >= 50) continue
                  maxRow = Math.max(maxRow, rr + 1)
                  maxCol = Math.max(maxCol, cc + 1)
                  const raw = cells[j]
                  if (raw === '') {
                    delete nextCells[cellKey(rr, cc)]
                  } else if (raw.startsWith('=')) {
                    nextCells[cellKey(rr, cc)] = { f: raw }
                  } else {
                    nextCells[cellKey(rr, cc)] = { v: raw }
                  }
                }
              }
              emit(workbook.sheets.map(
                (s) => s.id === activeSheet.id
                  ? { ...s, rows: maxRow, cols: maxCol, cells: nextCells }
                  : s,
              ))
            } catch {
              /* clipboard denied — ignore silently */
            }
          })()
          return
        }
      }
      if (!selected) return
      let { row, col } = selected
      const shift = e.shiftKey
      if (e.key === 'ArrowUp') row = Math.max(0, row - 1)
      else if (e.key === 'ArrowDown') row = Math.min(activeSheet.rows - 1, row + 1)
      else if (e.key === 'ArrowLeft') col = Math.max(0, col - 1)
      else if (e.key === 'ArrowRight' || e.key === 'Tab') col = Math.min(activeSheet.cols - 1, col + 1)
      else if (e.key === 'Enter' || e.key === 'F2') {
        beginEdit(selected.row, selected.col)
        e.preventDefault(); return
      }
      else if (e.key === 'Delete' || e.key === 'Backspace') {
        if (readonly) return
        if (selBounds) {
          pushHistory()
          const nextCells = { ...activeSheet.cells }
          for (let r = selBounds.r0; r <= selBounds.r1; r++) {
            for (let c = selBounds.c0; c <= selBounds.c1; c++) {
              delete nextCells[cellKey(r, c)]
            }
          }
          emit(workbook.sheets.map(
            (s) => s.id === activeSheet.id ? { ...s, cells: nextCells } : s,
          ))
        }
        e.preventDefault(); return
      }
      else if (e.key.length === 1 && !e.metaKey && !e.ctrlKey && !e.altKey) {
        beginEdit(selected.row, selected.col, e.key)
        e.preventDefault(); return
      }
      else return
      e.preventDefault()
      setSelected({ row, col })
      if (!shift) setSelEnd(null)
      else setSelEnd({ row, col })
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [
    selected, selEnd, selBounds, activeSheet, workbook.sheets, readonly,
    beginEdit, undo, redo, onSave, emit, pushHistory,
  ])

  // ── Column widths (with drag-resize) ─────────────────────────

  const colWidths: number[] = useMemo(() => {
    const given = activeSheet.col_widths
    const widths: number[] = []
    for (let c = 0; c < activeSheet.cols; c++) {
      widths.push(
        (given && given[c]) || (c === 0 ? FIRST_COL_WIDTH : DEFAULT_COL_WIDTH),
      )
    }
    return widths
  }, [activeSheet.col_widths, activeSheet.cols])

  const resizeRef = useRef<{ col: number; startX: number; startW: number } | null>(null)
  const beginResize = (col: number, e: React.MouseEvent) => {
    if (readonly) return
    e.preventDefault(); e.stopPropagation()
    resizeRef.current = {
      col, startX: e.clientX, startW: colWidths[col] ?? DEFAULT_COL_WIDTH,
    }
    const onMove = (ev: MouseEvent) => {
      if (!resizeRef.current) return
      const dx = ev.clientX - resizeRef.current.startX
      const w = Math.max(40, Math.min(600, resizeRef.current.startW + dx))
      const widths = colWidths.slice()
      widths[resizeRef.current.col] = w
      // Don't push history for every mousemove — snapshot once at drag end.
      emit(workbook.sheets.map(
        (s) => s.id === activeSheet.id ? { ...s, col_widths: widths } : s,
      ))
    }
    const onUp = () => {
      resizeRef.current = null
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
      pushHistory()
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }

  // ── Sheet ops ────────────────────────────────────────────────

  const addSheet = () => {
    if (readonly) return
    pushHistory()
    const id = `sheet-${Math.random().toString(36).slice(2, 10)}`
    const next: Sheet = {
      id, name: `Sheet${workbook.sheets.length + 1}`,
      rows: 10, cols: 6, cells: {}, col_widths: null,
    }
    emit([...workbook.sheets, next], id)
  }
  const removeSheet = (id: string) => {
    if (readonly) return
    if (workbook.sheets.length <= 1) return
    pushHistory()
    const next = workbook.sheets.filter((s) => s.id !== id)
    const active = workbook.active_sheet_id === id ? next[0].id : workbook.active_sheet_id
    emit(next, active)
  }
  const renameSheet = (id: string, name: string) => {
    if (readonly) return
    pushHistory()
    emit(workbook.sheets.map(
      (s) => s.id === id ? { ...s, name: name.trim().slice(0, 64) || s.name } : s,
    ))
  }
  const switchSheet = (id: string) => {
    if (workbook.sheets.some((s) => s.id === id)) emit(workbook.sheets, id)
  }

  // ── Row/col CRUD ─────────────────────────────────────────────

  const addRow = () => {
    if (readonly) return
    mutateActive((s) => ({ ...s, rows: Math.min(500, s.rows + 1) }))
  }
  const removeRow = () => {
    if (readonly) return
    mutateActive((s) => {
      if (s.rows <= 1) return s
      const nextCells = { ...s.cells }
      for (const k of Object.keys(nextCells)) {
        const parsed = parseA1(k)
        if (parsed && parsed.row === s.rows - 1) delete nextCells[k]
      }
      return { ...s, rows: s.rows - 1, cells: nextCells }
    })
  }
  const addCol = () => {
    if (readonly) return
    mutateActive((s) => ({ ...s, cols: Math.min(50, s.cols + 1) }))
  }
  const removeCol = () => {
    if (readonly) return
    mutateActive((s) => {
      if (s.cols <= 1) return s
      const nextCells = { ...s.cells }
      for (const k of Object.keys(nextCells)) {
        const parsed = parseA1(k)
        if (parsed && parsed.col === s.cols - 1) delete nextCells[k]
      }
      return { ...s, cols: s.cols - 1, cells: nextCells }
    })
  }

  const totalWidth = 40 + colWidths.reduce((s, x) => s + x, 0)

  // ── Render ──────────────────────────────────────────────────

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Toolbar */}
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 8,
          padding: '8px 12px',
          borderBottom: '1px solid var(--ws-border, #e6e8eb)',
          background: 'var(--ws-surface-alt, #f7f9fb)',
          flexShrink: 0,
        }}
      >
        <div style={{ flex: 1, minWidth: 0 }}>{title}</div>
        {!readonly && (
          <Space size={4} wrap>
            <Tooltip title="撤销 (Ctrl+Z)">
              <Button size="small" icon={<UndoOutlined />} onClick={undo}
                disabled={undoRef.current.length === 0} />
            </Tooltip>
            <Tooltip title="重做 (Ctrl+Y)">
              <Button size="small" icon={<RedoOutlined />} onClick={redo}
                disabled={redoRef.current.length === 0} />
            </Tooltip>
            <Tooltip title="增加一行"><Button size="small" icon={<PlusOutlined />} onClick={addRow}>行</Button></Tooltip>
            <Tooltip title="删除末行"><Button size="small" icon={<DeleteOutlined />} onClick={removeRow}>行</Button></Tooltip>
            <Tooltip title="增加一列"><Button size="small" icon={<PlusOutlined />} onClick={addCol}>列</Button></Tooltip>
            <Tooltip title="删除末列"><Button size="small" icon={<DeleteOutlined />} onClick={removeCol}>列</Button></Tooltip>
            <InputNumber
              size="small" style={{ width: 72 }} min={1} max={500}
              value={activeSheet.rows}
              onChange={(v) => v && mutateActive((s) => ({ ...s, rows: Math.max(1, Math.min(500, Number(v))) }))}
              addonBefore="行"
            />
            <InputNumber
              size="small" style={{ width: 72 }} min={1} max={50}
              value={activeSheet.cols}
              onChange={(v) => v && mutateActive((s) => ({ ...s, cols: Math.max(1, Math.min(50, Number(v))) }))}
              addonBefore="列"
            />
            {onSave && (
              <Button
                size="small" type="primary" icon={<SaveOutlined />}
                loading={saving} onClick={() => onSave()}
              >
                保存
              </Button>
            )}
          </Space>
        )}
      </div>

      {/* Formula bar */}
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 8,
          padding: '6px 12px',
          borderBottom: '1px solid var(--ws-border, #e6e8eb)',
          background: '#fff', flexShrink: 0,
        }}
      >
        <Text code style={{ minWidth: 60 }}>
          {selected ? cellKey(selected.row, selected.col) : '—'}
        </Text>
        <Input
          size="small"
          value={
            editing && selected
              && editing.row === selected.row && editing.col === selected.col
              ? editValue
              : selected
                ? (activeSheet.cells[cellKey(selected.row, selected.col)]?.f
                  ?? activeSheet.cells[cellKey(selected.row, selected.col)]?.v
                  ?? '')
                : ''
          }
          placeholder="= formula or raw value"
          readOnly={readonly || !selected}
          onFocus={() => {
            if (!selected || editing) return
            beginEdit(selected.row, selected.col)
          }}
          onChange={(e) => setEditValue(e.target.value)}
          onPressEnter={() => commitEdit({ advance: 'down' })}
          onBlur={() => commitEdit()}
        />
      </div>

      {/* Grid */}
      <div
        ref={gridRef}
        style={{ flex: 1, overflow: 'auto', background: '#fff' }}
      >
        <div style={{ width: totalWidth, minWidth: '100%' }}>
          {/* Column header */}
          <div
            style={{
              display: 'flex', position: 'sticky', top: 0, zIndex: 2,
              background: 'var(--ws-grid-header-bg, #f1f3f5)',
              borderBottom: '1px solid var(--ws-border-strong, #cbd5e1)',
            }}
          >
            <div style={headerCornerStyle}></div>
            {Array.from({ length: activeSheet.cols }, (_, c) => (
              <div key={c} style={{ ...colHeaderStyle, width: colWidths[c] }}>
                {colLabel(c)}
                <div
                  onMouseDown={(e) => beginResize(c, e)}
                  style={{
                    position: 'absolute',
                    right: -3, top: 0, bottom: 0, width: 6,
                    cursor: 'col-resize',
                    // Subtle: only visible on hover via the browser's cursor.
                  }}
                />
              </div>
            ))}
          </div>

          {/* Body */}
          {Array.from({ length: activeSheet.rows }, (_, r) => (
            <div key={r} style={{ display: 'flex' }}>
              <div style={rowHeaderStyle}>{r + 1}</div>
              {Array.from({ length: activeSheet.cols }, (_, c) => {
                const key = cellKey(r, c)
                const cell = activeSheet.cells[key]
                const inBounds = selBounds
                  && r >= selBounds.r0 && r <= selBounds.r1
                  && c >= selBounds.c0 && c <= selBounds.c1
                const isAnchor = selected?.row === r && selected?.col === c
                const isEditing = editing?.row === r && editing?.col === c
                const display = displayCell(cell, activeSheet)
                const isFormula = !!cell?.f
                const isHeaderRow = r === 0
                const isLabelCol = c === 0
                return (
                  <div
                    key={c}
                    onMouseDown={(e) => {
                      if (editing) commitEdit()
                      if (e.shiftKey && selected) {
                        setSelEnd({ row: r, col: c })
                      } else {
                        setSelected({ row: r, col: c })
                        setSelEnd(null)
                      }
                    }}
                    onDoubleClick={() => beginEdit(r, c)}
                    style={{
                      ...cellStyle,
                      width: colWidths[c],
                      background: inBounds
                        ? (isAnchor ? 'var(--ws-accent-soft, #eff6ff)' : 'var(--ws-grid-selection, #dbeafe)')
                        : isHeaderRow
                          ? 'var(--ws-grid-header-bg, #f8fafc)'
                          : isLabelCol
                            ? 'var(--ws-surface-alt, #fafbfd)'
                            : '#fff',
                      fontWeight: isHeaderRow || isLabelCol ? 500 : 400,
                      color: isFormula ? 'var(--ws-accent-hover, #0369a1)' : 'var(--ws-text-primary, #1e293b)',
                      outline: isAnchor ? '2px solid var(--ws-accent, #2563eb)' : 'none',
                      outlineOffset: -2,
                      textAlign:
                        !isLabelCol && !isHeaderRow && /^-?\d/.test(display) ? 'right' : 'left',
                    }}
                  >
                    {isEditing ? (
                      <input
                        autoFocus
                        value={editValue}
                        onChange={(e) => setEditValue(e.target.value)}
                        onBlur={() => commitEdit()}
                        onKeyDown={(e) => {
                          if (e.key === 'Enter') {
                            e.preventDefault()
                            commitEdit({ advance: 'down' })
                          } else if (e.key === 'Tab') {
                            e.preventDefault()
                            commitEdit({ advance: 'right' })
                          } else if (e.key === 'Escape') {
                            commitEdit({ cancel: true })
                          }
                        }}
                        style={{
                          width: '100%', height: '100%', border: 'none',
                          outline: 'none', padding: '4px 6px',
                          fontSize: 13, fontFamily: 'inherit', background: '#fff',
                        }}
                      />
                    ) : (
                      display
                    )}
                  </div>
                )
              })}
            </div>
          ))}
        </div>
      </div>

      {/* Sheet tabs + status bar */}
      <div
        style={{
          display: 'flex', alignItems: 'center', gap: 8,
          padding: '4px 10px', flexShrink: 0,
          background: 'var(--ws-surface-alt, #f8fafc)',
          borderTop: '1px solid var(--ws-border, #e6e8eb)',
          minHeight: 32,
        }}
      >
        <Space size={2} style={{ overflow: 'auto', flex: 1 }}>
          {workbook.sheets.map((s) => (
            <SheetTab
              key={s.id}
              sheet={s}
              active={s.id === activeSheet.id}
              readonly={readonly}
              editingName={editingSheetId === s.id ? editingSheetName : null}
              onSwitch={() => switchSheet(s.id)}
              onBeginRename={() => { setEditingSheetId(s.id); setEditingSheetName(s.name) }}
              onChangeName={(n) => setEditingSheetName(n)}
              onCommitRename={() => {
                if (editingSheetId === s.id) renameSheet(s.id, editingSheetName)
                setEditingSheetId(null); setEditingSheetName('')
              }}
              onDelete={() => removeSheet(s.id)}
              canDelete={workbook.sheets.length > 1 && !readonly}
            />
          ))}
          {!readonly && (
            <Button
              size="small" type="text" icon={<PlusOutlined />} onClick={addSheet}
              style={{ marginLeft: 4 }}
            />
          )}
        </Space>
        <div style={{ fontSize: 11, color: 'var(--ws-text-secondary, #64748b)', whiteSpace: 'nowrap' }}>
          {selectionStats && selectionStats.count > 0 ? (
            <>
              Sum={selectionStats.sum.toFixed(2)}
              {selectionStats.avg !== null && <> · Avg={selectionStats.avg.toFixed(2)}</>}
              <> · Count={selectionStats.count}</>
            </>
          ) : (
            <>双击或回车编辑 · Tab 右移 · Enter 下移 · Ctrl+Z 撤销 · Ctrl+V 粘贴</>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Sheet tab ───────────────────────────────────────────────────

function SheetTab({
  sheet, active, readonly, editingName,
  onSwitch, onBeginRename, onChangeName, onCommitRename,
  onDelete, canDelete,
}: {
  sheet: Sheet
  active: boolean
  readonly: boolean
  editingName: string | null
  onSwitch: () => void
  onBeginRename: () => void
  onChangeName: (n: string) => void
  onCommitRename: () => void
  onDelete: () => void
  canDelete: boolean
}) {
  return (
    <Dropdown
      trigger={readonly ? [] : ['contextMenu']}
      menu={{
        items: [
          { key: 'rename', label: '重命名', onClick: onBeginRename },
          ...(canDelete ? [{ key: 'delete', label: '删除', danger: true, onClick: onDelete }] : []),
        ],
      }}
    >
      <div
        onClick={onSwitch}
        onDoubleClick={readonly ? undefined : onBeginRename}
        style={{
          padding: '4px 10px',
          borderRadius: 4,
          fontSize: 12,
          fontWeight: active ? 600 : 400,
          background: active ? '#fff' : 'transparent',
          color: active ? 'var(--ws-accent, #2ec98a)' : 'var(--ws-text-secondary, #64748b)',
          border: active ? '1px solid var(--ws-border, #e6e8eb)' : '1px solid transparent',
          cursor: 'pointer',
          userSelect: 'none',
          display: 'flex', alignItems: 'center', gap: 4,
          whiteSpace: 'nowrap',
        }}
      >
        {editingName !== null ? (
          <input
            autoFocus
            value={editingName}
            onChange={(e) => onChangeName(e.target.value)}
            onBlur={onCommitRename}
            onKeyDown={(e) => {
              if (e.key === 'Enter' || e.key === 'Escape') onCommitRename()
            }}
            style={{
              width: 90, border: '1px solid var(--ws-accent, #2ec98a)',
              borderRadius: 2, outline: 'none', padding: '0 4px',
              fontSize: 12,
            }}
          />
        ) : (
          <>
            <span>{sheet.name}</span>
            {!readonly && (
              <MoreOutlined
                style={{ fontSize: 11, opacity: active ? 1 : 0.4 }}
              />
            )}
          </>
        )}
      </div>
    </Dropdown>
  )
}

// ── Styles ──────────────────────────────────────────────────────

const headerCornerStyle: React.CSSProperties = {
  width: 40,
  height: 26,
  borderRight: '1px solid var(--ws-border-strong, #cbd5e1)',
  borderBottom: '1px solid var(--ws-border-strong, #cbd5e1)',
  background: 'var(--ws-border, #e2e8f0)',
  position: 'sticky',
  left: 0,
  zIndex: 3,
  flexShrink: 0,
}
