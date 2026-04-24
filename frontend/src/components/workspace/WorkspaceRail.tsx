/**
 * App rail shown on the left edge of the workspace.
 *
 * Mirrors the PaiWork reference: a narrow vertical column with large icons
 * linking to the sibling apps inside the research workbench. Clicking one
 * jumps to its route; the active item gets an accent bar + soft tint.
 *
 * Scoped to `/my-knowledge` for now — the plan keeps the existing global
 * sidebar intact outside this route.
 */

import { useNavigate, useLocation } from 'react-router-dom'
import { Tooltip } from 'antd'
import {
  MessageOutlined, AppstoreOutlined, DatabaseOutlined, LineChartOutlined,
} from '@ant-design/icons'

interface RailItem {
  key: string
  title: string
  icon: React.ReactNode
  to: string
}

const ITEMS: RailItem[] = [
  {
    key: 'ask',
    title: 'Ask PaiPai',
    icon: <MessageOutlined style={{ fontSize: 20 }} />,
    to: '/ai-chat',
  },
  {
    key: 'workspace',
    title: 'PaiWork 工作台',
    icon: <AppstoreOutlined style={{ fontSize: 20 }} />,
    to: '/my-knowledge',
  },
  {
    key: 'portfolio',
    title: '持仓概览',
    icon: <LineChartOutlined style={{ fontSize: 20 }} />,
    to: '/portfolio',
  },
  {
    key: 'news',
    title: '新闻',
    icon: <DatabaseOutlined style={{ fontSize: 20 }} />,
    to: '/',
  },
]

export default function WorkspaceRail() {
  const nav = useNavigate()
  const loc = useLocation()
  return (
    <div
      style={{
        width: 56,
        flexShrink: 0,
        background: 'var(--ws-surface, #fff)',
        borderRight: '1px solid var(--ws-border, #e6e8eb)',
        display: 'flex', flexDirection: 'column',
        alignItems: 'center',
        padding: '8px 0',
        gap: 4,
      }}
    >
      {ITEMS.map((item) => {
        const active = loc.pathname === item.to
          || (item.to !== '/' && loc.pathname.startsWith(item.to + '/'))
        return (
          <Tooltip key={item.key} title={item.title} placement="right">
            <div
              onClick={() => nav(item.to)}
              role="button"
              aria-label={item.title}
              style={{
                width: 40, height: 40,
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                borderRadius: 8,
                cursor: 'pointer',
                background: active ? 'var(--ws-accent-soft, #e8f8f0)' : 'transparent',
                color: active ? 'var(--ws-accent, #2ec98a)' : 'var(--ws-text-secondary, #6b7280)',
                position: 'relative',
                transition: 'background 0.15s',
              }}
              onMouseEnter={(e) => {
                if (!active) (e.currentTarget.style.background = 'var(--ws-surface-alt, #f7f9fb)')
              }}
              onMouseLeave={(e) => {
                if (!active) (e.currentTarget.style.background = 'transparent')
              }}
            >
              {active && (
                <div style={{
                  position: 'absolute', left: -4, top: 8, bottom: 8, width: 3,
                  borderRadius: 2,
                  background: 'var(--ws-accent, #2ec98a)',
                }} />
              )}
              {item.icon}
            </div>
          </Tooltip>
        )
      })}
    </div>
  )
}
