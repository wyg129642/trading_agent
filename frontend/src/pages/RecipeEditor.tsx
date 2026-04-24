/**
 * Recipe editor — list, fork, and tweak recipes.
 *
 * Researchers can:
 *   * see all available recipes filtered by industry
 *   * fork a public recipe to create their own version
 *   * edit the prompt template / tools / confidence threshold per node
 */
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Button, Card, Empty, Input, List, Modal, Select, Space, Table, Tag, Typography, message,
} from 'antd'
import { ArrowLeftOutlined, BranchesOutlined, PlusOutlined, ReloadOutlined } from '@ant-design/icons'
import {
  modelingApi, playbookApi, recipeApi, type PackInfo, type Recipe,
} from '../services/modeling'

const { TextArea } = Input
const { Paragraph } = Typography

export default function RecipeEditor() {
  const nav = useNavigate()
  const [recipes, setRecipes] = useState<Recipe[]>([])
  const [packs, setPacks] = useState<PackInfo[]>([])
  const [editing, setEditing] = useState<Recipe | null>(null)
  const [loading, setLoading] = useState(false)

  const reload = async () => {
    setLoading(true)
    try {
      const [r, p] = await Promise.all([recipeApi.list(), playbookApi.listPacks().catch(() => [])])
      setRecipes(r)
      setPacks(p)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { reload() }, [])

  const fork = async (r: Recipe) => {
    try {
      const f = await recipeApi.fork(r.id)
      message.success(`已 fork: ${f.name}`)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const importPack = async (slug: string) => {
    try {
      const res = await recipeApi.importPack(slug)
      message.success(`已导入 ${res.imported.length} 个 recipe`)
      reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  return (
    <div style={{ padding: '16px 24px' }}>
      <Card
        title={<Space>
          <Button icon={<ArrowLeftOutlined />} onClick={() => nav('/modeling')}>返回</Button>
          <span style={{ fontSize: 17 }}>Recipe 管理</span>
        </Space>}
        extra={
          <Space>
            <Button icon={<ReloadOutlined />} onClick={reload}>刷新</Button>
            {packs.map(p => (
              <Button key={p.slug} onClick={() => importPack(p.slug)}>
                导入 {p.name} pack
              </Button>
            ))}
          </Space>
        }
      >
        <Paragraph type="secondary">
          Recipe 是可编辑的工作流 DAG。修改 prompt 模板或工具开关 → fork 出你自己的版本 → 下次运行即生效。
        </Paragraph>
        <Table
          rowKey="id"
          loading={loading}
          dataSource={recipes}
          pagination={{ pageSize: 20 }}
          columns={[
            { title: '名称', dataIndex: 'name', key: 'name',
              render: (n: string, r: Recipe) => (
                <Space>
                  <strong>{n}</strong>
                  <Tag>v{r.version}</Tag>
                  {r.is_public ? <Tag color="blue">公开</Tag> : <Tag>私有</Tag>}
                  {r.pack_ref && <Tag color="purple">{r.pack_ref}</Tag>}
                </Space>
              ),
            },
            { title: 'slug', dataIndex: 'slug', key: 'slug' },
            { title: '行业', dataIndex: 'industry', key: 'industry' },
            { title: '步骤数', key: 'steps', render: (_: any, r: Recipe) => r.graph.nodes?.length || 0 },
            { title: '标签', dataIndex: 'tags', key: 'tags',
              render: (ts: string[]) => ts?.map(t => <Tag key={t}>{t}</Tag>) },
            { title: '操作', key: 'actions', render: (_: any, r: Recipe) => (
              <Space>
                <Button size="small" type="primary"
                        onClick={() => nav(`/modeling/recipes/${r.id}`)}>可视化编辑</Button>
                <Button size="small" onClick={() => setEditing(r)}>查看</Button>
                <Button size="small" icon={<BranchesOutlined />} onClick={() => fork(r)}>Fork</Button>
              </Space>
            )},
          ]}
        />
        {recipes.length === 0 && !loading && (
          <Empty description="暂无 recipe。使用 '导入 pack' 按钮从 Industry Pack 加载标准 recipe。" />
        )}
      </Card>

      {editing && (
        <Modal
          title={`Recipe: ${editing.name}`}
          open
          width={860}
          onCancel={() => setEditing(null)}
          footer={null}
        >
          <Paragraph type="secondary">{editing.description}</Paragraph>
          <List
            header={<strong>Steps ({editing.graph.nodes?.length || 0})</strong>}
            bordered
            dataSource={editing.graph.nodes || []}
            renderItem={(n: any) => (
              <List.Item>
                <Space direction="vertical" style={{ width: '100%' }}>
                  <Space>
                    <Tag color="blue">{n.type}</Tag>
                    <strong>{n.id}</strong>
                    <span>{n.label}</span>
                  </Space>
                  {n.config?.prompt_template && (
                    <details>
                      <summary style={{ cursor: 'pointer', color: '#64748b' }}>Prompt template</summary>
                      <pre style={{
                        background: '#f8fafc', padding: 8, borderRadius: 4,
                        fontSize: 11, whiteSpace: 'pre-wrap',
                      }}>{n.config.prompt_template}</pre>
                    </details>
                  )}
                  {n.config?.tools && (
                    <span>Tools: {(n.config.tools as string[]).map(t => <Tag key={t}>{t}</Tag>)}</span>
                  )}
                </Space>
              </List.Item>
            )}
          />
          <div style={{ marginTop: 12, color: '#64748b', fontSize: 12 }}>
            💡 完整编辑器 (prompt 模板修改、工具开关、置信度阈值) 在 Phase 4 接入 reactflow canvas。
            当前版本可通过 API 修改：<code>PATCH /api/recipes/:id</code>.
          </div>
        </Modal>
      )}
    </div>
  )
}
