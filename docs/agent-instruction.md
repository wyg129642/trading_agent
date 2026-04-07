# Trading Intelligence API — Agent 调用指南

你可以通过以下 API 搜索和获取股票相关的金融资讯、研报、路演纪要、专家访谈等内容。

## 认证

所有请求必须携带 `X-API-Key` 头：

```
X-API-Key: ta_cbbce2fa1032a5fdc0cb13d3b1440cbe501c5b8fe6b122bd
```

## Base URL

```
http://192.168.31.97:8000
```

---

## 可用接口

### 1. 股票模糊搜索（确认股票代码）

当你不确定股票的准确代码时，先调用此接口。

```
GET /api/open/stock/suggest?q={关键词}&limit=10
```

示例请求：
```
GET http://192.168.31.97:8000/api/open/stock/suggest?q=英伟&limit=5
```

示例响应：
```json
{
  "suggestions": [
    {"name": "英伟达公司", "code": "NVDA", "market": "美股", "label": "英伟达公司(NVDA)"}
  ]
}
```

支持：中文名、英文名、股票代码、代码前缀。覆盖 A股、美股、港股。

---

### 2. 搜索资讯列表（核心接口）

输入股票名称或代码，返回关联的所有资讯摘要。

```
GET /api/open/search?q={股票名或代码}&hours=168&page=1&page_size=20&source=all&sentiment=
```

| 参数 | 必填 | 默认值 | 说明 |
|------|------|--------|------|
| q | 是 | - | 股票名称或代码，会自动模糊展开（输入 NVDA 会同时搜 "英伟达"） |
| hours | 否 | 168 | 搜索时间窗口（小时），默认7天，最大720（30天） |
| page | 否 | 1 | 页码 |
| page_size | 否 | 20 | 每页条数，最大50 |
| source | 否 | all | 数据源筛选：`all` / `news` / `alphapai` / `jiuqian` |
| sentiment | 否 | - | 情绪筛选：`very_bullish` / `bullish` / `bearish` / `very_bearish` |

示例请求：
```
GET http://192.168.31.97:8000/api/open/search?q=NVDA&hours=72&page_size=10
```

示例响应（简化）：
```json
{
  "query": "NVDA",
  "resolved_stock": {"name": "英伟达公司", "code": "NVDA", "market": "美股"},
  "total": 44,
  "page": 1,
  "page_size": 10,
  "has_next": true,
  "items": [
    {
      "id": "d305fe6e24864e28",
      "source_type": "news",
      "source_label": "资讯中心",
      "title": "OpenAI完成1220亿美元融资...",
      "summary": "OpenAI完成1220亿美元融资（估值8520亿美元），由亚马逊（500亿）、英伟达（300亿）、软银（300亿）领投...",
      "tickers": ["英伟达公司(NVDA)", "亚马逊公司(AMZN)", "苹果公司(AAPL)"],
      "sectors": ["人工智能", "科技股", "半导体"],
      "sentiment": "bullish",
      "impact_magnitude": "high",
      "concept_tags": [],
      "published_at": "2026-04-01T01:01:55+00:00",
      "detail_url": "http://192.168.31.97:8000/api/open/detail/news/d305fe6e24864e28",
      "site_url": "http://192.168.31.97:8000/news/d305fe6e24864e28",
      "original_url": "https://www.cls.cn/detail/2331334"
    }
  ],
  "source_counts": {"news": 22, "alphapai_wechat": 14, "jiuqian_minutes": 6, "jiuqian_forum": 2}
}
```

**每条结果的关键字段：**
- `title` + `summary`：标题和AI生成的摘要，用于快速判断是否值得深入阅读
- `tickers`：关联的股票列表
- `sentiment`：AI分析的情绪（very_bullish/bullish/bearish/very_bearish）
- `impact_magnitude`：影响程度（critical/high/medium/low），仅资讯中心有
- `detail_url`：**调用此URL获取全文内容**（见接口3）
- `site_url`：我们网站的页面链接，可提供给人类用户查看
- `original_url`：新闻原文链接

**数据源类型（source_type）：**
- `news`：全球金融资讯（含3阶段AI深度分析）
- `alphapai_wechat`：公众号文章
- `alphapai_comment`：券商分析师点评
- `alphapai_roadshow_cn`：A股路演纪要
- `alphapai_roadshow_us`：美股路演纪要
- `jiuqian_forum`：专家访谈纪要
- `jiuqian_minutes`：研究纪要
- `jiuqian_wechat`：久谦公众号

---

### 3. 获取全文详情

根据搜索结果中的 `detail_url` 获取完整内容。

```
GET /api/open/detail/{source_type}/{item_id}
```

直接使用搜索结果里的 `detail_url` 即可，例如：
```
GET http://192.168.31.97:8000/api/open/detail/news/d305fe6e24864e28
```

示例响应（简化）：
```json
{
  "id": "d305fe6e24864e28",
  "source_type": "news",
  "title": "OpenAI完成1220亿美元融资...",
  "title_zh": "...",
  "content": "完整新闻正文内容...",
  "published_at": "2026-04-01T01:01:55+00:00",
  "original_url": "https://www.cls.cn/detail/2331334",
  "site_url": "http://192.168.31.97:8000/news/d305fe6e24864e28",
  "tickers": ["英伟达公司(NVDA)", "亚马逊公司(AMZN)"],
  "sectors": ["人工智能", "半导体"],
  "analysis": {
    "sentiment": "bullish",
    "impact_magnitude": "high",
    "impact_timeframe": "short_term",
    "summary": "AI生成的摘要...",
    "key_facts": ["要点1", "要点2", "要点3"],
    "bull_case": "看多逻辑...",
    "bear_case": "看空逻辑...",
    "ticker_sentiments": {"NVDA": "bullish", "AMZN": "bullish"},
    "surprise_factor": 0.55,
    "concept_tags": ["AI芯片"],
    "industry_tags": ["半导体"]
  },
  "research": {
    "executive_summary": "深度研究摘要...",
    "context": "背景分析...",
    "historical_precedent": "历史先例...",
    "bull_scenario": "乐观情景...",
    "bear_scenario": "悲观情景...",
    "recommended_actions": "建议操作...",
    "risk_factors": "风险因素...",
    "confidence": 0.88,
    "citations": [{"title": "来源标题", "url": "https://...", "snippet": "引用片段"}]
  }
}
```

**说明：**
- `content` 字段包含完整的文章正文（纯文本）
- `analysis` 包含AI分析结果：情绪、影响、多空观点、关联股票情绪
- `research` 包含深度研究报告（仅 `news` 类型有）：含历史先例、情景分析、引用来源
- 其他数据源（alphapai_*、jiuqian_*）只有 `analysis` 中的 sentiment 和 summary

---

## 推荐调用流程

```
1. 用户提到一只股票 → 调用 /api/open/stock/suggest 确认代码
2. 用确认的代码调用 /api/open/search 获取资讯列表
3. 根据 title 和 summary 判断哪些值得深入阅读
4. 对感兴趣的条目调用 detail_url 获取全文和完整分析
5. 向用户呈现信息时，附带 site_url（网页链接）和 original_url（原文链接）
```

## 错误码

| HTTP状态码 | 含义 |
|-----------|------|
| 401 | API Key 缺失或无效 |
| 400 | 参数错误（如 source_type 不合法） |
| 404 | 条目不存在 |
| 429 | 请求频率超限（当前限制：120次/分钟） |

## 限频

120 次请求/分钟。超限时返回 429，等待几秒后重试即可。
