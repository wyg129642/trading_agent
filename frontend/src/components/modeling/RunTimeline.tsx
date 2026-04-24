/**
 * Real-time event timeline for a running RecipeRun.
 */
import { Tag, Timeline } from 'antd'

interface Event {
  type: string
  data: any
  ts: string
}

const COLORS: Record<string, string> = {
  step_started: 'blue',
  step_progress: 'gray',
  step_completed: 'green',
  cell_update: 'purple',
  verify_flag: 'orange',
  step_failed: 'red',
  run_completed: 'green',
}

export default function RunTimeline({ events }: { events: Event[] }) {
  const items = events.slice(-30).map((e, i) => ({
    key: i,
    color: COLORS[e.type] || 'gray',
    children: (
      <div>
        <Tag color={COLORS[e.type] || 'gray'}>{e.type}</Tag>
        {e.data?.step_id && <code style={{ marginLeft: 6 }}>{e.data.step_id}</code>}
        {e.data?.payload?.label && <span style={{ marginLeft: 6 }}>{e.data.payload.label}</span>}
        {e.data?.payload?.cells_written != null && (
          <span style={{ marginLeft: 6, color: '#64748b' }}>
            ({e.data.payload.cells_written} cells written)
          </span>
        )}
        {e.data?.payload?.cell_path && (
          <code style={{ marginLeft: 6, color: '#64748b', fontSize: 11 }}>
            {e.data.payload.cell_path}
          </code>
        )}
        {e.data?.payload?.error && <span style={{ color: '#ef4444', marginLeft: 6 }}>{e.data.payload.error}</span>}
        <span style={{ color: '#94a3b8', fontSize: 11, marginLeft: 10 }}>
          {new Date(e.ts).toLocaleTimeString('zh-CN')}
        </span>
      </div>
    ),
  }))
  return <Timeline items={items} style={{ maxHeight: 260, overflow: 'auto' }} />
}
