# Trading Agent Open API

API for external agents (e.g. OpenClaw) to search and retrieve trading intelligence data.

**Base URL:** `http://192.168.31.97:8000` (configure via `OPEN_API_BASE_URL` env var)

---

## Authentication

All requests require an API key in the `X-API-Key` header.

```
X-API-Key: ta_your_api_key_here
```

API keys are created by the admin:

```bash
python scripts/create_api_key.py --name "Alice's agent" --rate-limit 60
```

Rate limit: 60 requests/minute by default (configurable per key).

---

## Endpoints

### 1. Stock Suggest (Autocomplete)

Fuzzy-match a stock name or code. Use this first to confirm the stock before searching.

```
GET /api/open/stock/suggest?q={query}&limit=10
```

**Parameters:**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `q` | string | yes | Stock name or code (Chinese/English, min 1 char) |
| `limit` | int | no | Max results (default 10, max 30) |

**Example:**

```bash
curl -H "X-API-Key: ta_xxx" "http://192.168.31.97:8000/api/open/stock/suggest?q=英伟"
```

**Response:**

```json
{
  "suggestions": [
    {
      "name": "英伟达",
      "code": "NVDA",
      "market": "美股",
      "label": "英伟达(NVDA)"
    }
  ]
}
```

---

### 2. Search (Core Endpoint)

Search across all data sources by stock name or code. Returns a list of items with title, summary, tickers, sentiment, and URLs.

```
GET /api/open/search?q={query}
```

**Parameters:**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `q` | string | yes | - | Stock name or code (auto fuzzy-expanded) |
| `hours` | int | no | 168 | Time window in hours (max 720 = 30 days) |
| `page` | int | no | 1 | Page number |
| `page_size` | int | no | 20 | Items per page (max 50) |
| `source` | string | no | `all` | Filter by source: `all`, `news`, `alphapai`, `jiuqian` |
| `sentiment` | string | no | - | Filter: `bullish`, `bearish`, `very_bullish`, `very_bearish` |

**Example:**

```bash
curl -H "X-API-Key: ta_xxx" "http://192.168.31.97:8000/api/open/search?q=NVDA&hours=72&page_size=5"
```

**Response:**

```json
{
  "query": "NVDA",
  "resolved_stock": {
    "name": "英伟达",
    "code": "NVDA",
    "market": "美股"
  },
  "total": 42,
  "page": 1,
  "page_size": 5,
  "has_next": true,
  "items": [
    {
      "id": "abc123",
      "source_type": "news",
      "source_label": "资讯中心",
      "title": "英伟达Q4财报超预期，数据中心收入创新高",
      "title_zh": "英伟达Q4财报超预期，数据中心收入创新高",
      "summary": "英伟达发布2025Q4财报，营收同比增长78%，数据中心业务收入达到...",
      "tickers": ["NVDA", "AMD", "TSM"],
      "sectors": ["半导体", "AI"],
      "sentiment": "very_bullish",
      "impact_magnitude": "high",
      "concept_tags": ["AI芯片", "数据中心"],
      "published_at": "2026-03-28T14:30:00+00:00",
      "detail_url": "http://192.168.31.97:8000/api/open/detail/news/abc123",
      "site_url": "http://192.168.31.97:8000/news/abc123",
      "original_url": "https://reuters.com/article/nvidia-earnings-q4"
    },
    {
      "id": "wx_20260327_001",
      "source_type": "alphapai_wechat",
      "source_label": "AlphaPai公众号",
      "title": "英伟达产业链深度解读：谁是下一个十倍股？",
      "summary": "本文从供应链角度分析英伟达上下游...",
      "tickers": ["NVDA", "002049.SZ"],
      "sectors": ["半导体"],
      "sentiment": "bullish",
      "concept_tags": ["产业链"],
      "published_at": "2026-03-27T08:00:00+00:00",
      "detail_url": "http://192.168.31.97:8000/api/open/detail/alphapai_wechat/wx_20260327_001",
      "site_url": "http://192.168.31.97:8000/alphapai/feed?detail=wx_20260327_001",
      "original_url": "https://mp.weixin.qq.com/s/xxxx"
    }
  ],
  "source_counts": {
    "news": 15,
    "alphapai_wechat": 12,
    "alphapai_comment": 5,
    "alphapai_roadshow_cn": 3,
    "jiuqian_minutes": 5,
    "jiuqian_forum": 2
  }
}
```

**Key fields in each item:**

| Field | Description |
|-------|-------------|
| `detail_url` | Call this URL to get full content (see Endpoint 3) |
| `site_url` | Human-readable URL to view on our web platform |
| `original_url` | Link to original news source (may be null for non-news items) |
| `source_type` | Data source identifier (see Source Types below) |
| `sentiment` | AI-analyzed sentiment: `very_bullish`, `bullish`, `bearish`, `very_bearish` |
| `impact_magnitude` | Impact level: `critical`, `high`, `medium`, `low` (news only) |

---

### 3. Detail (Full Content)

Get the full content of a specific item. Use the `detail_url` returned from the search endpoint.

```
GET /api/open/detail/{source_type}/{item_id}
```

**Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `source_type` | path | One of the source types listed below |
| `item_id` | path | The item ID from search results |

**Example:**

```bash
curl -H "X-API-Key: ta_xxx" "http://192.168.31.97:8000/api/open/detail/news/abc123"
```

**Response:**

```json
{
  "id": "abc123",
  "source_type": "news",
  "title": "NVIDIA Q4 earnings beat expectations, data center revenue hits record",
  "title_zh": "英伟达Q4财报超预期，数据中心收入创新高",
  "content": "Full article text content here...\n\nNVIDIA Corporation reported record quarterly revenue of $22.1 billion for Q4 FY2025, up 78% from a year ago...",
  "published_at": "2026-03-28T14:30:00+00:00",
  "original_url": "https://reuters.com/article/nvidia-earnings-q4",
  "site_url": "http://192.168.31.97:8000/news/abc123",
  "tickers": ["NVDA", "AMD", "TSM"],
  "sectors": ["半导体", "AI"],
  "analysis": {
    "sentiment": "very_bullish",
    "impact_magnitude": "high",
    "impact_timeframe": "short_term",
    "summary": "英伟达Q4营收同比增78%，远超预期。数据中心收入创新高，AI需求持续强劲。",
    "key_facts": [
      "Q4营收221亿美元，同比+78%",
      "数据中心收入184亿美元，同比+93%",
      "Q1指引250亿美元，高于市场预期"
    ],
    "bull_case": "AI基础设施投资持续加速，英伟达作为GPU垄断者持续受益...",
    "bear_case": "估值已充分反映增长预期，客户自研芯片可能构成长期威胁...",
    "ticker_sentiments": {
      "NVDA": "very_bullish",
      "AMD": "bullish",
      "TSM": "bullish"
    },
    "surprise_factor": 0.82,
    "concept_tags": ["AI芯片", "数据中心", "GPU"],
    "industry_tags": ["半导体", "云计算"]
  },
  "research": {
    "executive_summary": "英伟达Q4财报全面超预期，多项指标创历史新高...",
    "context": "本季财报发布在AI投资持续加速的背景下...",
    "historical_precedent": "过去4个季度，英伟达连续大幅超预期...",
    "bull_scenario": "若AI资本开支继续翻倍增长...",
    "bear_scenario": "若主要云客户削减Capex...",
    "recommended_actions": "维持超配，关注Q1指引执行情况...",
    "risk_factors": "估值拉伸、客户自研芯片、地缘政治出口管制...",
    "confidence": 0.88,
    "citations": [
      {
        "title": "NVIDIA Earnings Release",
        "url": "https://investor.nvidia.com/...",
        "snippet": "Record quarterly revenue of $22.1 billion..."
      }
    ]
  }
}
```

**Notes:**
- `news` source provides the richest data with `analysis` + `research` sections
- Other sources (`alphapai_*`, `jiuqian_*`) provide `analysis` with sentiment/summary from LLM enrichment
- `content` contains the full text (may be long for research reports and roadshow transcripts)
- `research` is only available for `news` items that have been through the deep research pipeline

---

## Source Types

| source_type | Label | Description |
|-------------|-------|-------------|
| `news` | 资讯中心 | Global financial news with 3-phase AI analysis |
| `alphapai_wechat` | AlphaPai公众号 | WeChat public account articles |
| `alphapai_comment` | AlphaPai券商点评 | Broker analyst comments and reports |
| `alphapai_roadshow_cn` | AlphaPai路演纪要(A股) | A-share company roadshow transcripts |
| `alphapai_roadshow_us` | AlphaPai路演纪要(美股) | US company roadshow transcripts |
| `jiuqian_forum` | 久谦专家访谈 | Expert interview transcripts |
| `jiuqian_minutes` | 久谦研究纪要 | Research meeting minutes |
| `jiuqian_wechat` | 久谦公众号 | WeChat articles from Jiuqian |

---

## Typical Agent Workflow

```
Step 1: Suggest  →  Confirm which stock to look up
Step 2: Search   →  Get overview list with titles + summaries
Step 3: Detail   →  Fetch full content for items of interest
```

**Example flow for an OpenClaw-like agent:**

```python
import httpx

BASE = "http://192.168.31.97:8000"
HEADERS = {"X-API-Key": "ta_your_key"}

# 1. User asks about "茅台" → confirm the stock
resp = httpx.get(f"{BASE}/api/open/stock/suggest", params={"q": "茅台"}, headers=HEADERS)
stock = resp.json()["suggestions"][0]
# → {"name": "贵州茅台", "code": "600519.SH", "market": "A股"}

# 2. Search for related intelligence
resp = httpx.get(f"{BASE}/api/open/search", params={"q": stock["code"], "hours": 72}, headers=HEADERS)
results = resp.json()
# → 42 items across 7 sources, with titles, summaries, sentiment

# 3. Agent reads summaries, picks interesting items to deep-dive
for item in results["items"][:3]:
    detail = httpx.get(item["detail_url"], headers=HEADERS).json()
    # → full content, analysis, research data
    # Agent can now reason over the full text
```

---

## Error Responses

| Status | Meaning |
|--------|---------|
| 401 | Missing or invalid API key |
| 400 | Invalid source_type or parameters |
| 404 | Item not found |
| 429 | Rate limit exceeded |

Error body:

```json
{
  "detail": "Invalid or inactive API key"
}
```

---

## Rate Limiting

Default: **60 requests per minute** per API key. Sliding window via Redis.

When rate-limited, you receive a `429` response. Wait and retry after a few seconds.

---

## Setup

1. Set the `OPEN_API_BASE_URL` environment variable to your domain:
   ```
   OPEN_API_BASE_URL=https://trading.yourcompany.com
   ```

2. Run the migration:
   ```bash
   alembic upgrade head
   ```

3. Create an API key:
   ```bash
   python scripts/create_api_key.py --name "Partner Agent" --rate-limit 120
   ```

4. Share the key with your colleague. The key format is `ta_<48 hex chars>`.

5. Interactive docs available at `http://192.168.31.97:8000/docs` (scroll to "Open API" section).
