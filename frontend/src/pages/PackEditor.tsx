/**
 * Pack Editor — visual YAML editor for an industry pack.
 *
 * Three tabs (one per file):
 *   - pack.yaml
 *   - segments_schema.yaml
 *   - sanity_rules.yaml
 *
 * Plus overview/lessons/rules markdown. Uses a plain textarea with syntax
 * hint + "Validate YAML" button (parses via js-yaml on save).
 */
import { useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Alert, Button, Card, Input, Space, Spin, Tabs, Tag,
  Typography, message,
} from 'antd'
import { ArrowLeftOutlined, SaveOutlined, ReloadOutlined } from '@ant-design/icons'
import yaml from 'js-yaml'
import api from '../services/api'

const { Title, Paragraph } = Typography

const PACK_TABS = [
  { key: 'pack.yaml', label: 'pack.yaml', hint: 'slug / name / tickers / default_periods / units' },
  { key: 'segments_schema.yaml', label: 'segments_schema.yaml', hint: 'list of { slug, label_zh, kind, volume_unit, asp_unit, ... }' },
  { key: 'sanity_rules.yaml', label: 'sanity_rules.yaml', hint: 'margin / yoy / ratio range + severity' },
  { key: 'overview.md', label: 'overview.md', hint: 'pack overview markdown (injected into system prompt)' },
  { key: 'rules.md', label: 'rules.md', hint: 'industry rules for the analyst' },
  { key: 'lessons.md', label: 'lessons.md', hint: 'accumulated lessons (editable, but usually appended automatically)' },
]


export default function PackEditor() {
  const { slug = '' } = useParams()
  const nav = useNavigate()
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [active, setActive] = useState('pack.yaml')
  // content keyed by filename
  const [contents, setContents] = useState<Record<string, string>>({})
  const [dirty, setDirty] = useState<Record<string, boolean>>({})
  const [error, setError] = useState<string>('')

  const reload = async () => {
    setLoading(true)
    try {
      // The server-side read endpoint returns only the three md files today;
      // we extend it by reading YAML via a new admin endpoint. Fall back to
      // "empty" if the file doesn't exist yet (pack was bootstrapped without it).
      const [mdResp, yamlResp] = await Promise.all([
        api.get<Record<string, string>>(`/api/playbook/packs/${slug}`),
        api.get<Record<string, string>>(`/api/playbook/packs/${slug}/files`),
      ])
      setContents({ ...mdResp.data, ...yamlResp.data })
      setDirty({})
    } catch (e: any) {
      setError(e?.response?.data?.detail || String(e))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { if (slug) reload() }, [slug])

  const isYaml = (file: string) => file.endsWith('.yaml') || file.endsWith('.yml')

  const validate = (file: string, body: string): string => {
    if (!isYaml(file)) return ''
    try {
      yaml.load(body)
      return ''
    } catch (e: any) {
      return e?.message || String(e)
    }
  }

  const save = async () => {
    setSaving(true)
    try {
      for (const [file, body] of Object.entries(contents)) {
        if (!dirty[file]) continue
        const err = validate(file, body)
        if (err) {
          throw new Error(`${file}: ${err}`)
        }
      }
      const dirtyFiles = Object.keys(dirty).filter(f => dirty[f])
      for (const file of dirtyFiles) {
        await api.patch(`/api/playbook/packs/${slug}/files`, {
          filename: file,
          body: contents[file],
        })
      }
      message.success(`已保存 ${dirtyFiles.length} 个文件`)
      setDirty({})
    } catch (e: any) {
      message.error(e?.response?.data?.detail || e?.message || String(e))
    } finally {
      setSaving(false)
    }
  }

  const reindexVectors = async () => {
    try {
      const r = await api.post(`/api/playbook/packs/${slug}/reindex-vectors`)
      message.success(`已重建向量索引: ${JSON.stringify(r.data)}`)
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const autoArchive = async () => {
    try {
      const r = await api.post(`/api/playbook/packs/${slug}/auto-archive`)
      message.success(`已归档 ${(r.data as any).archived ?? 0} 条过期 lesson`)
      await reload()
    } catch (e: any) {
      message.error(e?.response?.data?.detail || String(e))
    }
  }

  const currentContent = contents[active] ?? ''
  const currentError = useMemo(() => validate(active, currentContent), [active, currentContent])
  const totalDirty = Object.values(dirty).filter(Boolean).length

  return (
    <div style={{ padding: 24, maxWidth: 1400, margin: '0 auto' }}>
      <Space style={{ marginBottom: 12 }}>
        <Button icon={<ArrowLeftOutlined />} onClick={() => nav('/modeling/playbook')}>返回</Button>
        <Title level={3} style={{ margin: 0 }}>📦 Pack 编辑器: {slug}</Title>
      </Space>

      {error && <Alert type="error" message={error} closable onClose={() => setError('')} style={{ marginBottom: 12 }} />}

      <Card
        size="small"
        style={{ marginBottom: 12 }}
        bodyStyle={{ padding: '8px 12px' }}
      >
        <Space wrap>
          <Button icon={<ReloadOutlined />} onClick={reload} loading={loading}>刷新</Button>
          <Button
            type="primary"
            icon={<SaveOutlined />}
            onClick={save}
            loading={saving}
            disabled={totalDirty === 0}
          >
            保存 {totalDirty > 0 ? `(${totalDirty})` : ''}
          </Button>
          <Button onClick={reindexVectors}>重建向量索引</Button>
          <Button onClick={autoArchive}>自动归档过期 lesson</Button>
        </Space>
      </Card>

      {loading ? <div style={{ padding: 40, textAlign: 'center' }}><Spin /></div> : (
        <Card size="small">
          <Tabs
            activeKey={active}
            onChange={setActive}
            items={PACK_TABS.map(t => ({
              key: t.key,
              label: (
                <Space size={4}>
                  <span>{t.label}</span>
                  {dirty[t.key] && <Tag color="orange">已修改</Tag>}
                </Space>
              ),
              children: (
                <div>
                  <div style={{ fontSize: 12, color: '#64748b', marginBottom: 8 }}>
                    {t.hint}
                  </div>
                  {currentError && (
                    <Alert
                      type="error"
                      showIcon
                      style={{ marginBottom: 8 }}
                      message="YAML 语法错误"
                      description={<code>{currentError}</code>}
                    />
                  )}
                  <Input.TextArea
                    value={contents[t.key] || ''}
                    autoSize={{ minRows: 24, maxRows: 60 }}
                    onChange={e => {
                      setContents(prev => ({ ...prev, [t.key]: e.target.value }))
                      setDirty(prev => ({ ...prev, [t.key]: true }))
                    }}
                    style={{ fontFamily: 'SFMono-Regular, Consolas, monospace', fontSize: 13 }}
                    spellCheck={false}
                  />
                </div>
              ),
            }))}
          />
        </Card>
      )}
    </div>
  )
}
