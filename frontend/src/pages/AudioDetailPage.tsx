/**
 * AudioDetailPage — dedicated full-page view for an audio document in the
 * personal knowledge base. Mirrors AlphaPai's self-summary-detail layout:
 * a split transcript (bubbles, left) + AI-generated summary tabs (right)
 * with the audio player docked at the bottom.
 *
 * Route: /my-knowledge/audio/:documentId
 *
 * The page owns navigation (header breadcrumb back to 我的知识库) and the
 * document fetch; all rendering is delegated to AudioTranscriptViewer.
 */
import { useCallback, useEffect, useMemo, useState } from 'react'
import { useNavigate, useParams } from 'react-router-dom'
import {
  Alert, Button, ConfigProvider, Descriptions, Space, Spin, Tag, Typography,
  message as antdMessage,
} from 'antd'
import {
  ArrowLeftOutlined, DownloadOutlined, SoundOutlined,
} from '@ant-design/icons'
import api from '../services/api'
import { useAuthStore } from '../store/auth'
import AudioTranscriptViewer, {
  AudioMeta, isAudioDoc,
} from '../components/AudioTranscriptViewer'

const { Text, Title } = Typography

interface DocumentResponse {
  id: string
  user_id: string
  title: string
  description: string
  original_filename: string
  file_extension: string
  content_type: string
  file_size_bytes: number
  upload_status: string
  parse_status: string
  parse_error: string | null
  parse_progress_percent: number
  parse_phase: string
  extracted_char_count: number
  num_chunks: number
  folder_id: string | null
  scope: 'personal' | 'public'
  doc_type: string
  created_at: string
  updated_at: string
  audio: AudioMeta | null
}

// Match MyKnowledgeBase theme so the page doesn't clash with the rest of
// the workspace when the user navigates between them.
const workspaceTheme = {
  token: {
    colorPrimary: '#2ec98a',
    colorLink: '#2ec98a',
    colorLinkHover: '#23b579',
    borderRadius: 6,
  },
}

function humanSize(bytes: number): string {
  if (!bytes) return '0 B'
  const k = 1024
  const units = ['B', 'KB', 'MB', 'GB']
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(k)), units.length - 1)
  return `${(bytes / Math.pow(k, i)).toFixed(i === 0 ? 0 : 1)} ${units[i]}`
}

export default function AudioDetailPage() {
  const { documentId = '' } = useParams<{ documentId: string }>()
  const navigate = useNavigate()
  const currentUser = useAuthStore((s) => s.user)
  const isAdminOrBoss =
    currentUser?.role === 'admin' || currentUser?.role === 'boss'

  const [doc, setDoc] = useState<DocumentResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string>('')

  // Refetch loop while the doc is still being parsed (ASR in progress).
  // The backend synthesizes pseudo-segments for legacy audio docs on read,
  // so the detail page can count on ``doc.audio.segments`` being populated
  // once ``parse_status === completed`` — no client-side fallback needed.
  const fetchDoc = useCallback(async () => {
    if (!documentId) return
    try {
      const res = await api.get<DocumentResponse>(
        `/user-kb/documents/${documentId}`,
      )
      setDoc(res.data)
      setError('')
    } catch (err: any) {
      setError(
        err?.response?.data?.detail || err?.message || '读取文档失败',
      )
    } finally {
      setLoading(false)
    }
  }, [documentId])

  useEffect(() => { fetchDoc() }, [fetchDoc])

  // Poll while parsing so the ASR progress bar updates live.
  useEffect(() => {
    if (!doc) return
    if (doc.parse_status === 'completed' || doc.parse_status === 'failed') return
    const t = window.setInterval(fetchDoc, 3000)
    return () => window.clearInterval(t)
  }, [doc?.parse_status, fetchDoc, doc])

  const canEdit = useMemo(() => {
    if (!doc || !currentUser) return false
    if (doc.user_id === String(currentUser.id)) return true
    // Public-scope editing by admin/boss is allowed at the data layer for
    // some operations, but transcript text edits are restricted to the
    // uploader on the backend. Match that here.
    return false
  }, [doc, currentUser])

  const downloadOriginal = useCallback(async () => {
    if (!doc) return
    try {
      const res = await api.get(`/user-kb/documents/${doc.id}/file`, {
        responseType: 'blob',
      })
      const blob = new Blob([res.data], { type: doc.content_type })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = doc.original_filename
      document.body.appendChild(a); a.click(); document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (err: any) {
      antdMessage.error(`下载失败: ${err?.response?.data?.detail || err.message}`)
    }
  }, [doc])

  // Guard: route param must be audio. Everything else redirects.
  const notAudio = !!doc && !isAudioDoc(doc.file_extension)
  useEffect(() => {
    if (notAudio) {
      antdMessage.info('该文档不是音频，已返回知识库')
      navigate('/my-knowledge')
    }
  }, [notAudio, navigate])

  return (
    <ConfigProvider theme={workspaceTheme}>
      <div style={{
        height: 'calc(100vh - 60px)', display: 'flex', flexDirection: 'column',
        background: '#f1f5f9', padding: 16, gap: 12,
      }}>
        {/* Header */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: 12,
          background: '#fff', padding: '10px 16px', borderRadius: 8,
          border: '1px solid #e2e8f0',
        }}>
          <Button
            type="text" icon={<ArrowLeftOutlined />}
            onClick={() => navigate('/my-knowledge')}
          >
            返回知识库
          </Button>
          <div style={{ height: 20, width: 1, background: '#e2e8f0' }} />
          <Space size={8} style={{ flex: 1, minWidth: 0 }}>
            <SoundOutlined style={{ color: '#2ec98a', fontSize: 18 }} />
            <Title level={5} style={{
              margin: 0, fontSize: 15, color: '#0f172a',
              whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
              maxWidth: 520,
            }}>
              {doc?.title || doc?.original_filename || '音频详情'}
            </Title>
            {doc?.scope === 'public' && (
              <Tag color="purple" style={{ margin: 0 }}>公共</Tag>
            )}
            {doc && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                {humanSize(doc.file_size_bytes)}
              </Text>
            )}
          </Space>
          {doc && doc.parse_status === 'completed' && (
            <Button
              size="small" icon={<DownloadOutlined />}
              onClick={downloadOriginal}
            >
              下载原文件
            </Button>
          )}
        </div>

        {/* Body */}
        <div style={{ flex: 1, minHeight: 0 }}>
          {loading && !doc ? (
            <div style={{
              height: '100%', display: 'flex', alignItems: 'center',
              justifyContent: 'center', background: '#fff', borderRadius: 8,
              border: '1px solid #e2e8f0',
            }}>
              <Spin size="large" tip="加载中…" />
            </div>
          ) : error ? (
            <Alert
              type="error" showIcon
              message="加载失败" description={error}
              action={
                <Button size="small" onClick={fetchDoc}>重试</Button>
              }
            />
          ) : doc && doc.parse_status !== 'completed' ? (
            <div style={{
              background: '#fff', borderRadius: 8, padding: 32,
              border: '1px solid #e2e8f0',
            }}>
              <Space direction="vertical" size={12} style={{ width: '100%' }}>
                <Alert
                  type="info" showIcon
                  message={
                    doc.parse_status === 'failed'
                      ? '音频转写失败'
                      : '音频转写中，请稍候…'
                  }
                  description={
                    doc.parse_status === 'failed'
                      ? (doc.parse_error || '未知错误')
                      : `${doc.parse_phase || 'transcribing'} · ${doc.parse_progress_percent || 0}%`
                  }
                />
                <Descriptions column={1} size="small" bordered>
                  <Descriptions.Item label="原始文件">
                    {doc.original_filename} ({humanSize(doc.file_size_bytes)})
                  </Descriptions.Item>
                  <Descriptions.Item label="上传时间">
                    {new Date(doc.created_at).toLocaleString('zh-CN')}
                  </Descriptions.Item>
                  <Descriptions.Item label="状态">
                    {doc.parse_status}
                  </Descriptions.Item>
                </Descriptions>
              </Space>
            </div>
          ) : doc && doc.audio && (doc.audio.segments?.length ?? 0) > 0 ? (
            <AudioTranscriptViewer
              documentId={doc.id}
              title={doc.title || doc.original_filename}
              audio={doc.audio}
              filename={doc.original_filename}
              canEdit={canEdit}
              onDownload={downloadOriginal}
            />
          ) : (
            <Alert
              type="warning" showIcon
              message="该文档没有可用的音频转写数据"
              description="可能是旧版解析结果，尝试重新上传以生成段落级转写。"
            />
          )}
        </div>
      </div>
    </ConfigProvider>
  )
}
