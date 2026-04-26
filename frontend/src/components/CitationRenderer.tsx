import { useMemo } from 'react'
import { Popover } from 'antd'
import { LinkOutlined } from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { oneLight } from 'react-syntax-highlighter/dist/esm/styles/prism'

interface Source {
  index: number
  title: string
  url: string
  website: string
  date: string
  /** 'web' | 'alphapai' | 'jinmen' | 'kb' — discriminator for rendering style */
  source_type?: string
  /** Human-readable document type for alphapai/jinmen/kb (e.g. '路演纪要', '券商研报', '业绩会纪要') */
  doc_type?: string
  /** Internal knowledge-base document id, set when source_type === 'kb'. */
  doc_id?: string
}

function isExternalLink(url: string): boolean {
  return !!url && /^https?:\/\//i.test(url)
}

interface CitationRendererProps {
  content: string
  sources: Source[]
}

// ── Trailing source section stripping ────────────────────────────

/**
 * Strip trailing source/reference sections that LLMs append to their responses.
 * The UI renders its own source section via CitationRenderer, so we remove
 * LLM-generated duplicates.
 *
 * Matches patterns like:
 *   ---\n**搜索来源:**\n1. [Title](url)...
 *   **来源引用：**\n- [1] Title...
 *   ### References\n1. ...
 */
function stripTrailingSources(content: string): string {
  return content
    .replace(
      /\n+(?:---\n+)?(?:\*{0,2}#{0,3}\s*)(?:搜索来源|来源引用|来源|引用来源|参考来源|参考文献|参考资料|References?|Sources?)\s*(?:：|:)?\s*\*{0,2}\n[\s\S]*$/,
      '',
    )
    .trimEnd()
}

// ── Tilde normalization (prevent number-range strikethrough) ─────

/**
 * Normalize runs of `~~` (or longer) into a single `~` when they sit next to
 * digits. The LLM frequently uses `~~` to denote numeric ranges
 * (`60~~75%`, `$14,000~~17,000`), but GFM parses `~~text~~` as strikethrough,
 * which renders the range as deleted text.
 *
 * Three patterns are normalized:
 *   1. digit ~~ digit  →  digit~digit       (range between two numbers)
 *   2. boundary ~~ digit →  boundary~digit  (leading "approx" prefix)
 *   3. digit ~~ boundary →  digit~boundary  (trailing tilde before space/punct)
 *
 * Combined with `singleTilde: false` on remark-gfm below, this guarantees
 * neither single nor double tildes around numbers render as strikethrough.
 */
function normalizeTildeRanges(content: string): string {
  return content
    .replace(/(\d)\s*~{2,}\s*(\d)/g, '$1~$2')
    .replace(/(^|[\s>([{,，。；：:、])~{2,}(?=\d)/g, '$1~')
    .replace(/(\d)~{2,}(?=[\s,，。；：:)\]}、]|$)/g, '$1~')
}

// ── Citation processing (code-block-aware) ───────────────────────

/**
 * Replace [N] citation markers with sentinel markdown links (#cite-N),
 * skipping fenced code blocks and inline code spans to avoid false matches.
 * Only replaces citations whose index exists in the source map.
 */
function processCitations(content: string, sourceMap: Map<number, Source>): string {
  // Split content preserving fenced code blocks and inline code
  const CODE_RE = /(```[\s\S]*?```|`[^`\n]+`)/g
  let result = ''
  let lastIdx = 0
  let m: RegExpExecArray | null

  while ((m = CODE_RE.exec(content)) !== null) {
    // Replace citations in text before this code segment
    result += replaceCitationMarkers(content.slice(lastIdx, m.index), sourceMap)
    // Preserve code segment as-is
    result += m[0]
    lastIdx = m.index + m[0].length
  }
  // Process remaining text after the last code segment
  result += replaceCitationMarkers(content.slice(lastIdx), sourceMap)

  return result
}

/** Replace [N] markers with sentinel links, validating indices against sourceMap. */
function replaceCitationMarkers(text: string, sourceMap: Map<number, Source>): string {
  // Match [N] but NOT [N]( which is already a markdown link
  return text.replace(/\[(\d+)\](?!\()/g, (match, num) => {
    const idx = parseInt(num)
    if (sourceMap.has(idx)) {
      // Sentinel URL detected by the custom <a> component in ReactMarkdown
      return `[${idx}](#cite-${idx})`
    }
    return match // Leave unmatched indices as plain text
  })
}

// ── Favicon helper ───────────────────────────────────────────────

function getFaviconUrl(website: string): string {
  if (!website) return ''
  const domain = website.replace(/^www\./, '')
  return `https://www.google.com/s2/favicons?domain=${domain}&sz=16`
}

// ── Main component ───────────────────────────────────────────────

/**
 * Production-grade citation renderer.
 *
 * Features:
 * - Strips LLM-generated trailing source sections (avoids duplicates)
 * - Converts inline [N] to hoverable superscript badges with Popover preview
 * - Code-block-aware: skips [N] inside fenced code blocks and inline code
 * - Validates citation indices against available sources
 * - Only shows actually-cited sources in the reference list
 * - Favicon display for source items
 */
export default function CitationRenderer({ content, sources }: CitationRendererProps) {
  if (!content) return null

  // No sources → plain markdown, no stripping (preserves Gemini grounding links).
  // We still normalize tildes so numeric ranges render correctly.
  if (!sources || sources.length === 0) {
    return <MarkdownBase content={normalizeTildeRanges(content)} />
  }

  const sourceMap = useMemo(() => {
    const map = new Map<number, Source>()
    for (const s of sources) map.set(s.index, s)
    return map
  }, [sources])

  const processedContent = useMemo(() => {
    const cleaned = stripTrailingSources(content)
    const tildeFixed = normalizeTildeRanges(cleaned)
    return processCitations(tildeFixed, sourceMap)
  }, [content, sourceMap])

  // Only show sources that are actually cited in the text
  const citedSources = useMemo(() => {
    const used = new Set<number>()
    const re = /\[(\d+)\]/g
    let match: RegExpExecArray | null
    while ((match = re.exec(content)) !== null) {
      const idx = parseInt(match[1])
      if (sourceMap.has(idx)) used.add(idx)
    }
    return sources.filter((s) => used.has(s.index))
  }, [content, sources, sourceMap])

  return (
    <div>
      <ReactMarkdown
        remarkPlugins={[[remarkGfm, { singleTilde: false }]]}
        components={{
          a({ href, children, title, ...props }) {
            // Detect citation sentinel links: #cite-N
            if (href?.startsWith('#cite-')) {
              const idx = parseInt(href.slice(6))
              const source = sourceMap.get(idx)
              if (source) {
                return <InlineCitation index={idx} source={source} />
              }
            }
            // Regular external links
            return (
              <a href={href} target="_blank" rel="noopener noreferrer" title={title} {...props}>
                {children}
              </a>
            )
          },
          ...markdownComponents,
        }}
      >
        {processedContent}
      </ReactMarkdown>

      {citedSources.length > 0 && (
        <div
          style={{
            marginTop: 16,
            paddingTop: 12,
            borderTop: '1px solid #e8eaed',
          }}
        >
          <div
            style={{
              fontSize: 12,
              fontWeight: 600,
              color: '#5f6368',
              marginBottom: 8,
              display: 'flex',
              alignItems: 'center',
              gap: 4,
            }}
          >
            <LinkOutlined />
            来源引用
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            {citedSources.map((s) => (
              <SourceItem key={s.index} source={s} />
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

// ── Inline citation badge with hover Popover ─────────────────────

function InlineCitation({ index, source }: { index: number; source: Source }) {
  const clickable = isExternalLink(source.url)
  const sourceType = source.source_type || 'web'

  // Color tokens per source type
  const palette =
    sourceType === 'alphapai'
      ? { bg: '#fdf4ff', bgHover: '#fae8ff', fg: '#9333ea' }
      : sourceType === 'jinmen'
      ? { bg: '#fef3c7', bgHover: '#fde68a', fg: '#b45309' }
      : sourceType === 'kb'
      ? { bg: '#ecfdf5', bgHover: '#d1fae5', fg: '#047857' }
      : { bg: '#eff6ff', bgHover: '#dbeafe', fg: '#2563eb' }

  const popContent = (
    <div style={{ maxWidth: 340 }}>
      <div
        style={{
          fontSize: 13,
          fontWeight: 500,
          color: '#1a1a1a',
          lineHeight: 1.4,
          marginBottom: 4,
        }}
      >
        {source.title || source.website || source.url || '来源'}
      </div>
      <div
        style={{
          fontSize: 11,
          color: '#6b7280',
          display: 'flex',
          alignItems: 'center',
          gap: 4,
          flexWrap: 'wrap',
        }}
      >
        {sourceType === 'web' && source.website && (
          <img
            src={getFaviconUrl(source.website)}
            alt=""
            width={14}
            height={14}
            style={{ borderRadius: 2 }}
            onError={(e) => {
              ;(e.target as HTMLImageElement).style.display = 'none'
            }}
          />
        )}
        {source.doc_type && (
          <span
            style={{
              padding: '1px 6px',
              borderRadius: 3,
              fontSize: 10,
              background: palette.bg,
              color: palette.fg,
            }}
          >
            {source.doc_type}
          </span>
        )}
        {source.website && <span>{source.website}</span>}
        {source.date && <span style={{ color: '#9ca3af' }}>· {source.date}</span>}
      </div>
    </div>
  )

  const badgeStyle: React.CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    minWidth: 18,
    height: 18,
    padding: '0 4px',
    fontSize: 11,
    fontWeight: 600,
    lineHeight: '18px',
    borderRadius: 4,
    backgroundColor: palette.bg,
    color: palette.fg,
    textDecoration: 'none',
    verticalAlign: 'super',
    marginLeft: 1,
    marginRight: 1,
    cursor: clickable ? 'pointer' : 'default',
    transition: 'background-color 0.15s',
  }

  const badge = clickable ? (
    <a
      href={source.url}
      target="_blank"
      rel="noopener noreferrer"
      style={badgeStyle}
      onMouseEnter={(e) => {
        ;(e.currentTarget as HTMLElement).style.backgroundColor = palette.bgHover
      }}
      onMouseLeave={(e) => {
        ;(e.currentTarget as HTMLElement).style.backgroundColor = palette.bg
      }}
    >
      {index}
    </a>
  ) : (
    <span style={badgeStyle}>{index}</span>
  )

  return (
    <Popover
      content={
        clickable ? (
          <a
            href={source.url}
            target="_blank"
            rel="noopener noreferrer"
            style={{ textDecoration: 'none', color: 'inherit', display: 'block' }}
          >
            {popContent}
          </a>
        ) : (
          popContent
        )
      }
      trigger="hover"
      placement="top"
      mouseEnterDelay={0.15}
      mouseLeaveDelay={0.1}
      overlayInnerStyle={{ padding: '8px 12px' }}
    >
      {badge}
    </Popover>
  )
}

// ── Source list item with favicon ─────────────────────────────────

function SourceItem({ source }: { source: Source }) {
  const clickable = isExternalLink(source.url)
  const sourceType = source.source_type || 'web'

  // Color palette per source type (matches InlineCitation)
  const palette =
    sourceType === 'alphapai'
      ? { bg: '#fdf4ff', fg: '#9333ea' }
      : sourceType === 'jinmen'
      ? { bg: '#fef3c7', fg: '#b45309' }
      : sourceType === 'kb'
      ? { bg: '#ecfdf5', fg: '#047857' }
      : { bg: '#eff6ff', fg: '#2563eb' }

  const rowStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '6px 8px',
    borderRadius: 6,
    textDecoration: 'none',
    transition: 'background-color 0.15s',
    cursor: clickable ? 'pointer' : 'default',
  }

  const content = (
    <>
      <span
        style={{
          display: 'inline-flex',
          alignItems: 'center',
          justifyContent: 'center',
          minWidth: 20,
          height: 20,
          fontSize: 11,
          fontWeight: 600,
          borderRadius: 4,
          backgroundColor: palette.bg,
          color: palette.fg,
          flexShrink: 0,
        }}
      >
        {source.index}
      </span>
      {sourceType === 'web' ? (
        <img
          src={getFaviconUrl(source.website)}
          alt=""
          width={14}
          height={14}
          style={{ borderRadius: 2, flexShrink: 0 }}
          onError={(e) => {
            ;(e.target as HTMLImageElement).style.display = 'none'
          }}
        />
      ) : (
        source.doc_type && (
          <span
            style={{
              padding: '1px 6px',
              borderRadius: 3,
              fontSize: 10,
              fontWeight: 500,
              background: palette.bg,
              color: palette.fg,
              flexShrink: 0,
            }}
          >
            {source.doc_type}
          </span>
        )
      )}
      <span
        style={{
          fontSize: 12,
          color: '#374151',
          flex: 1,
          minWidth: 0,
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}
      >
        {source.title || source.website}
      </span>
      <span style={{ fontSize: 11, color: '#9ca3af', flexShrink: 0 }}>
        {source.website}
        {source.date ? ` · ${source.date}` : ''}
      </span>
    </>
  )

  const hoverHandlers = {
    onMouseEnter: (e: React.MouseEvent<HTMLElement>) => {
      ;(e.currentTarget as HTMLElement).style.backgroundColor = '#f3f4f6'
    },
    onMouseLeave: (e: React.MouseEvent<HTMLElement>) => {
      ;(e.currentTarget as HTMLElement).style.backgroundColor = 'transparent'
    },
  }

  return clickable ? (
    <a
      href={source.url}
      target="_blank"
      rel="noopener noreferrer"
      style={rowStyle}
      {...hoverHandlers}
    >
      {content}
    </a>
  ) : (
    <div style={rowStyle} {...hoverHandlers}>
      {content}
    </div>
  )
}

// ── Shared markdown component overrides ──────────────────────────

const markdownComponents: Record<string, React.ComponentType<any>> = {
  code({ className, children, ...props }: any) {
    const match = /language-(\w+)/.exec(className || '')
    const codeStr = String(children).replace(/\n$/, '')
    if (match) {
      return (
        <SyntaxHighlighter
          style={oneLight}
          language={match[1]}
          PreTag="div"
          customStyle={{
            fontSize: 13,
            borderRadius: 6,
            margin: '8px 0',
            maxWidth: '100%',
            overflowX: 'auto',
          }}
        >
          {codeStr}
        </SyntaxHighlighter>
      )
    }
    return (
      <code
        style={{
          background: '#f1f5f9',
          padding: '1px 4px',
          borderRadius: 3,
          fontSize: 13,
          wordBreak: 'break-word',
        }}
        {...props}
      >
        {children}
      </code>
    )
  },
  table({ children }: any) {
    return (
      <div style={{ overflowX: 'auto', margin: '8px 0' }}>
        <table style={{ borderCollapse: 'collapse', width: '100%', fontSize: 13 }}>
          {children}
        </table>
      </div>
    )
  },
  th({ children }: any) {
    return (
      <th
        style={{
          border: '1px solid #e2e8f0',
          padding: '6px 10px',
          background: '#f8fafc',
          fontWeight: 600,
          textAlign: 'left' as const,
        }}
      >
        {children}
      </th>
    )
  },
  td({ children }: any) {
    return (
      <td style={{ border: '1px solid #e2e8f0', padding: '6px 10px' }}>
        {children}
      </td>
    )
  },
  // Safety net: if GFM still parses something as strikethrough (e.g. user
  // intentionally typed `~~text~~`), render without the line-through so an
  // accidental match never silently hides text. Range tildes are already
  // normalized upstream by `normalizeTildeRanges`.
  del({ children }: any) {
    return <span>{children}</span>
  },
}

/** Plain markdown renderer (no citation handling). */
function MarkdownBase({ content }: { content: string }) {
  return (
    <ReactMarkdown
      remarkPlugins={[[remarkGfm, { singleTilde: false }]]}
      components={markdownComponents}
    >
      {content}
    </ReactMarkdown>
  )
}
