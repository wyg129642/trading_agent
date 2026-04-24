/**
 * Skills tab in the workspace panel.
 *
 * A *skill* is a reusable bundle of folders + markdown + workbook files.
 * Users browse the list and install into a target folder (the currently
 * selected folder in the workspace tree). System skills are shipped with
 * the app (DCF / 三张报表 / 敏感性 / 同业对比 / 研报纪要); users can
 * additionally see public skills published by admins and their own
 * personal skills.
 */

import { useCallback, useEffect, useState } from 'react'
import {
  Alert, Button, Empty, List, Spin, Tag, Tooltip, Typography,
  message as antdMessage, Popconfirm,
} from 'antd'
import {
  ThunderboltOutlined, DownloadOutlined, ReloadOutlined,
  LineChartOutlined, TableOutlined, RadarChartOutlined,
  DeploymentUnitOutlined, FileMarkdownOutlined,
} from '@ant-design/icons'
import api from '../../services/api'

const { Text } = Typography

interface Skill {
  id: string
  owner_id: string | null
  scope: 'system' | 'public' | 'personal'
  name: string
  description: string
  icon: string
  target_types: string[]
  slug: string | null
  spec: any
  is_published: boolean
  installs_count: number
  created_at: string
  updated_at: string
}

const ICON_MAP: Record<string, React.ReactNode> = {
  ThunderboltOutlined: <ThunderboltOutlined />,
  LineChartOutlined: <LineChartOutlined style={{ color: 'var(--ws-accent, #2ec98a)' }} />,
  TableOutlined: <TableOutlined style={{ color: '#0ea5e9' }} />,
  RadarChartOutlined: <RadarChartOutlined style={{ color: '#8b5cf6' }} />,
  DeploymentUnitOutlined: <DeploymentUnitOutlined style={{ color: '#f59e0b' }} />,
  FileMarkdownOutlined: <FileMarkdownOutlined style={{ color: '#6366f1' }} />,
}

function iconFor(name: string) {
  return ICON_MAP[name] || <ThunderboltOutlined style={{ color: 'var(--ws-accent, #2ec98a)' }} />
}

export interface SkillsPanelProps {
  targetFolderId: string | null
  targetFolderType: 'stock' | 'industry' | 'general' | null
  canWrite: boolean
  onInstalled?: (result: {
    created_folders: string[]
    created_documents: string[]
  }) => void
}

export default function SkillsPanel({
  targetFolderId, targetFolderType, canWrite, onInstalled,
}: SkillsPanelProps) {
  const [skills, setSkills] = useState<Skill[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [installing, setInstalling] = useState<string | null>(null)

  const fetchSkills = useCallback(async () => {
    setLoading(true); setError(null)
    try {
      const res = await api.get<Skill[]>('/user-kb/skills')
      setSkills(res.data || [])
    } catch (err: any) {
      setError(err?.response?.data?.detail || err.message || 'load failed')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchSkills() }, [fetchSkills])

  const install = useCallback(async (skill: Skill) => {
    if (!targetFolderId) {
      antdMessage.warning('请先在左侧选择一个目标目录')
      return
    }
    setInstalling(skill.id)
    try {
      const res = await api.post<{
        skill_id: string; folder_id: string;
        created_folders: string[]; created_documents: string[];
        skipped_existing: number;
      }>(
        `/user-kb/folders/${targetFolderId}/install-skill/${skill.id}`,
      )
      antdMessage.success(
        `已安装 "${skill.name}" — 新增 ${res.data.created_documents.length} 个文件，${res.data.created_folders.length} 个目录`,
      )
      onInstalled?.({
        created_folders: res.data.created_folders,
        created_documents: res.data.created_documents,
      })
    } catch (err: any) {
      antdMessage.error(`安装失败: ${err?.response?.data?.detail || err.message}`)
    } finally {
      setInstalling(null)
    }
  }, [targetFolderId, onInstalled])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{
        padding: '8px 12px', display: 'flex', alignItems: 'center', gap: 8,
        borderBottom: '1px solid var(--ws-border, #e6e8eb)',
        background: 'var(--ws-surface, #ffffff)',
      }}>
        <Text strong style={{ flex: 1 }}>Skills 广场</Text>
        <Tooltip title="刷新">
          <Button size="small" icon={<ReloadOutlined />} onClick={fetchSkills} />
        </Tooltip>
      </div>

      {targetFolderId ? null : (
        <Alert
          type="info" showIcon
          message="在左侧选中一个目录后即可安装"
          style={{ margin: 8 }}
        />
      )}
      {error && (
        <Alert type="error" showIcon message={error} style={{ margin: 8 }} />
      )}

      <div style={{ flex: 1, overflow: 'auto', padding: 8 }}>
        {loading ? (
          <div style={{ textAlign: 'center', padding: 20 }}><Spin /></div>
        ) : skills.length === 0 ? (
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无可用 Skill" />
        ) : (
          <List
            size="small"
            dataSource={skills}
            renderItem={(s) => {
              const canInstall = canWrite && !!targetFolderId
                && (s.target_types.length === 0
                  || (targetFolderType && s.target_types.includes(targetFolderType)))
              const disabledReason = !targetFolderId
                ? '先选中一个目录'
                : !canWrite
                  ? '没有写权限'
                  : (targetFolderType && !s.target_types.includes(targetFolderType))
                    ? `此 Skill 只能安装到 ${s.target_types.join('/')} 目录`
                    : ''
              return (
                <List.Item
                  style={{ padding: '8px 4px' }}
                  actions={[
                    canInstall ? (
                      <Popconfirm
                        key="install"
                        title={`安装 "${s.name}" 到当前目录？`}
                        description="同名文件会被跳过而不会覆盖。"
                        onConfirm={() => install(s)}
                        okText="安装" cancelText="取消"
                      >
                        <Button
                          size="small" type="primary" icon={<DownloadOutlined />}
                          loading={installing === s.id}
                        >
                          安装
                        </Button>
                      </Popconfirm>
                    ) : (
                      <Tooltip title={disabledReason} key="disabled">
                        <Button size="small" icon={<DownloadOutlined />} disabled>
                          安装
                        </Button>
                      </Tooltip>
                    ),
                  ]}
                >
                  <List.Item.Meta
                    avatar={iconFor(s.icon)}
                    title={
                      <span>
                        <Text strong>{s.name}</Text>
                        <Tag
                          color={
                            s.scope === 'system' ? 'purple'
                              : s.scope === 'public' ? 'blue' : 'default'
                          }
                          style={{ marginLeft: 6, fontSize: 10 }}
                        >
                          {s.scope === 'system' ? '内置' : s.scope === 'public' ? '公共' : '个人'}
                        </Tag>
                        {s.installs_count > 0 && (
                          <Text type="secondary" style={{ marginLeft: 6, fontSize: 10 }}>
                            {s.installs_count} 次使用
                          </Text>
                        )}
                      </span>
                    }
                    description={
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        {s.description}
                      </Text>
                    }
                  />
                </List.Item>
              )
            }}
          />
        )}
      </div>
    </div>
  )
}
