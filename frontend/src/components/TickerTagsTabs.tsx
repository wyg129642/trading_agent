import { Space, Tabs, Tag, Typography } from 'antd'
import dayjs from 'dayjs'

const { Text } = Typography

export interface TickerTags {
  raw: any[]
  canonical: string[]
  canonical_source: string | null
  canonical_at: string | null
  unmatched_raw: string[]
  llm_canonical: string[]
  llm_source: string | null
  llm_at: string | null
  llm_unmatched_raw: string[]
}

interface Props {
  tags?: TickerTags | null
  /** override the wrapper margin (default: marginBottom 8) */
  style?: React.CSSProperties
}

/**
 * Three-tab widget showing ticker provenance for one document, surfaced
 * uniformly across all 8 platform DB pages:
 *  - 上游原标 (raw)              — platform-native ticker field (per-platform schema, see TICKER_AGGREGATION.md §4.1)
 *  - 规则 canonical              — `_canonical_tickers` from rule path
 *  - LLM                         — `_llm_canonical_tickers` from realtime tagger
 *
 * Backend builds `ticker_tags` via `build_ticker_tags(doc, source, collection)`
 * in backend/app/services/ticker_tags_builder.py — keep the prop shape in sync.
 */
export default function TickerTagsTabs({ tags, style }: Props) {
  if (!tags) return null

  return (
    <Tabs
      size="small"
      style={{ marginBottom: 8, ...style }}
      items={[
        {
          key: 'raw',
          label: `上游原标 (${tags.raw.length})`,
          children: (
            <Space wrap size={[4, 4]}>
              {tags.raw.length === 0 ? (
                <Text type="secondary">(空)</Text>
              ) : (
                tags.raw.map((r: any, i: number) => {
                  const code =
                    typeof r === 'string'
                      ? r
                      : r?.code ||
                        r?.ticker ||
                        r?.fullCode ||
                        r?.gtsCode ||
                        r?.gts_code ||
                        r?.stockcode ||
                        ''
                  const name =
                    typeof r === 'object'
                      ? r?.name ||
                        r?.scrAbbr ||
                        r?.scr_abbr ||
                        r?.stockname ||
                        r?.label ||
                        ''
                      : ''
                  const display = name && code ? `${name} · ${code}` : (name || code || JSON.stringify(r))
                  return (
                    <Tag key={`${code || name}-${i}`} color="default">
                      {display}
                    </Tag>
                  )
                })
              )}
              {tags.unmatched_raw.length > 0 && (
                <Text type="secondary" style={{ fontSize: 11 }}>
                  · normalizer-未匹配: {tags.unmatched_raw.join(', ')}
                </Text>
              )}
            </Space>
          ),
        },
        {
          key: 'canonical',
          label: `规则 canonical (${tags.canonical.length})`,
          children: (
            <div>
              <Space wrap size={[4, 4]}>
                {tags.canonical.length === 0 ? (
                  <Text type="secondary">(规则路径未命中)</Text>
                ) : (
                  tags.canonical.map((t) => (
                    <Tag key={t} color="geekblue">
                      {t}
                    </Tag>
                  ))
                )}
              </Space>
              {tags.canonical_source && (
                <div style={{ fontSize: 11, color: '#888', marginTop: 4 }}>
                  来源: <code>{tags.canonical_source}</code>
                  {tags.canonical_at &&
                    ` · ${dayjs(tags.canonical_at).format('YYYY-MM-DD HH:mm')}`}
                </div>
              )}
            </div>
          ),
        },
        {
          key: 'llm',
          label: `LLM (${tags.llm_canonical.length})`,
          children: (
            <div>
              <Space wrap size={[4, 4]}>
                {tags.llm_canonical.length === 0 ? (
                  <Text type="secondary">
                    {tags.llm_source ? '(LLM 跑过但判定无个股)' : '(LLM 未处理)'}
                  </Text>
                ) : (
                  tags.llm_canonical.map((t) => (
                    <Tag key={t} color="purple">
                      {t}
                    </Tag>
                  ))
                )}
              </Space>
              {tags.llm_source && (
                <div style={{ fontSize: 11, color: '#888', marginTop: 4 }}>
                  来源: <code>{tags.llm_source}</code>
                  {tags.llm_at && ` · ${dayjs(tags.llm_at).format('YYYY-MM-DD HH:mm')}`}
                </div>
              )}
              {tags.llm_unmatched_raw.length > 0 && (
                <div style={{ fontSize: 11, color: '#888', marginTop: 4 }}>
                  丢弃: {tags.llm_unmatched_raw.join(', ')}
                </div>
              )}
            </div>
          ),
        },
      ]}
    />
  )
}
