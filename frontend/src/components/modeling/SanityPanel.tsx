/**
 * Panel showing SanityIssue rows for a model.
 *
 * Keeps a clear severity-first ordering; one-click "jump to cell" for
 * the first mentioned cell_path.
 */
import { Alert, Button, Empty, List, Space, Tag } from 'antd'
import type { SanityIssue } from '../../services/modeling'

interface Props {
  issues: SanityIssue[]
  onSelectCell: (path: string) => void
}

export default function SanityPanel({ issues, onSelectCell }: Props) {
  if (issues.length === 0) {
    return <Empty description="未发现健全性问题 ✅" style={{ padding: 48 }} />
  }
  const sorted = [...issues].sort((a, b) => {
    const order = { error: 0, warn: 1, info: 2 } as any
    return (order[a.severity] ?? 3) - (order[b.severity] ?? 3)
  })
  return (
    <List
      dataSource={sorted}
      renderItem={(i) => (
        <List.Item>
          <List.Item.Meta
            title={
              <Space>
                <Tag color={i.severity === 'error' ? 'red' : i.severity === 'warn' ? 'orange' : 'blue'}>
                  {i.severity.toUpperCase()}
                </Tag>
                <Tag>{i.issue_type}</Tag>
                <span>{i.message}</span>
              </Space>
            }
            description={
              <>
                {i.suggested_fix && <div>💡 {i.suggested_fix}</div>}
                {i.cell_paths.length > 0 && (
                  <Space wrap size={4} style={{ marginTop: 4 }}>
                    {i.cell_paths.slice(0, 10).map((p) => (
                      <Button size="small" key={p} onClick={() => onSelectCell(p)}>
                        <code style={{ fontSize: 11 }}>{p}</code>
                      </Button>
                    ))}
                    {i.cell_paths.length > 10 && <span>...+{i.cell_paths.length - 10}</span>}
                  </Space>
                )}
              </>
            }
          />
        </List.Item>
      )}
    />
  )
}
