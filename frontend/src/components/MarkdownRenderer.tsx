import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { oneLight } from 'react-syntax-highlighter/dist/esm/styles/prism'

interface MarkdownRendererProps {
  content: string
}

export default function MarkdownRenderer({ content }: MarkdownRendererProps) {
  if (!content) return null

  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        code({ className, children, ...props }) {
          const match = /language-(\w+)/.exec(className || '')
          const codeStr = String(children).replace(/\n$/, '')
          if (match) {
            return (
              <SyntaxHighlighter
                style={oneLight}
                language={match[1]}
                PreTag="div"
                customStyle={{ fontSize: 13, borderRadius: 6, margin: '8px 0' }}
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
              }}
              {...props}
            >
              {children}
            </code>
          )
        },
        table({ children }) {
          return (
            <div style={{ overflowX: 'auto', margin: '8px 0' }}>
              <table
                style={{
                  borderCollapse: 'collapse',
                  width: '100%',
                  fontSize: 13,
                }}
              >
                {children}
              </table>
            </div>
          )
        },
        th({ children }) {
          return (
            <th
              style={{
                border: '1px solid #e2e8f0',
                padding: '6px 10px',
                background: '#f8fafc',
                fontWeight: 600,
                textAlign: 'left',
              }}
            >
              {children}
            </th>
          )
        },
        td({ children }) {
          return (
            <td
              style={{
                border: '1px solid #e2e8f0',
                padding: '6px 10px',
              }}
            >
              {children}
            </td>
          )
        },
      }}
    >
      {content}
    </ReactMarkdown>
  )
}
