# 跨平台股票标签使用指南

所有爬虫文档都带有规范化股票标签 `_canonical_tickers`。本文档说明**怎么用这个字段做股票关联检索和模型训练**。

> 字段在文档落库时就写入,实时 / 回填 / 历史数据一视同仁。当前覆盖率:1 887 052 / 1 887 052 = 100%。

---

## 1. 字段 schema

每份内容文档都有这 4 个派生字段(不改原字段):

```js
{
  _id: "...",
  title: "...",
  // ... 原始字段保持不变 ...

  _canonical_tickers: ["NVDA.US", "AVGO.US"],   // ← 查询主字段,有索引
  _canonical_tickers_at: ISODate("..."),         // ← 打标时间戳
  _unmatched_raw: ["OpenAI"],                     // ← 有提及但未命中别名(可忽略)
  _canonical_extract_source: "alphapai"           // ← 来源平台
}
```

**三种语义,必须分清**:

| 值 | 含义 | 典型场景 |
|---|---|---|
| `["NVDA.US", ...]` | 文档涉及这些具体股票 | 研报、点评、业绩会纪要 |
| `[]` | **已扫描,上游本身无个股** | 行业纪要、宏观策略、话题调研 |
| 字段缺失 | **不应该出现** | 若看到,说明是极新的文档尚未被 cron 追上 |

训练时若只要"有个股"的文档,过滤条件是 `{"_canonical_tickers": {"$ne": []}}`,**不是** `{"$exists": true}`。

---

## 2. Canonical 格式

`<CODE>.<MARKET>`,两字母大写 MARKET:

```
SH 上交所   SZ 深交所   BJ 北交所   HK 港交所
US NASDAQ/NYSE   DE Xetra   JP 东京
KS Korea   TW Taiwan   AU ASX   CA TSX
GB LSE   FR Paris   CH SIX   NL Euronext
```

**示例**:`NVDA.US` · `603061.SH` · `01211.HK`(HK 5 位补零)· `BABA.US` · `09988.HK`

**同一公司多地上市**:目前不做实体归并。中芯国际 `688981.SH` 和 `0981.HK` 会分成两份查询。要覆盖全股就用 `$in`:

```python
{"_canonical_tickers": {"$in": ["688981.SH", "00981.HK"]}}
```

---

## 3. 数据源 → Mongo DB 路由表

**一定要按这张表查,DB 名不是平台名**:

| 平台 | Mongo DB | 集合 |
|---|---|---|
| AlphaPai | `alphapai-full` | `comments` / `roadshows` / `reports` / `wechat_articles` |
| 进门财经 | `jinmen-full` | `meetings` / `reports` / `oversea_reports` |
| 久谦中台(meritco) | **`jiuqian-full`** | `forum` / `research` |
| 高临咨询(third_bridge) | **`third-bridge`** | `interviews` |
| Funda | **`funda`** | `posts` / `earnings_reports` / `earnings_transcripts` / `sentiments` |
| 岗底斯(gangtise) | `gangtise-full` | `summaries` / `researches` / `chief_opinions` |
| AceCamp | **`acecamp`** | `articles` |
| 阿尔法引擎(alphaengine) | **`alphaengine`** | `summaries` / `china_reports` / `foreign_reports` / `news_items` |

**连接串**(本机 ta-mongo-crawl 容器, 2026-04-26 起从远端 ops cluster 迁回本机):

```
mongodb://127.0.0.1:27018/
```

历史远端连接串(应急回滚用,见 `.env` 注释): `mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin`。
本机连接无需代理设置,直连 loopback;远端连接才需要 `NO_PROXY=192.168.31.176,localhost,127.0.0.1`。

---

## 4. 股票关联检索

### 4.1 单票跨源 —— MongoDB 直查(训练/批量推荐)

```python
from pymongo import MongoClient

c = MongoClient("mongodb://127.0.0.1:27018/")

ROUTE = [
    ("alphapai-full",  ["comments", "roadshows", "wechat_articles", "reports"]),
    ("jinmen-full",    ["meetings", "reports", "oversea_reports"]),
    ("jiuqian-full",   ["forum", "research"]),
    ("third-bridge",   ["interviews"]),
    ("funda",          ["posts", "earnings_reports", "earnings_transcripts", "sentiments"]),
    ("gangtise-full",  ["summaries", "researches", "chief_opinions"]),
    ("acecamp",        ["articles"]),
    ("alphaengine",    ["summaries", "china_reports", "foreign_reports", "news_items"]),
]

# NVDA.US 跨 8 平台所有提及
for dbn, colls in ROUTE:
    for coll in colls:
        cur = c[dbn][coll].find(
            {"_canonical_tickers": "NVDA.US"},
            projection={"_id": 1, "title": 1, "release_time": 1, "_canonical_tickers": 1}
        )
        for doc in cur:
            process(doc)
```

### 4.2 篮子查询 —— `$in` + 聚合管道

```python
basket = ["NVDA.US", "AAPL.US", "600519.SH"]

pipeline = [
    {"$match": {
        "_canonical_tickers": {"$in": basket},
        "release_time": {"$gte": "2026-03-01"}
    }},
    {"$project": {"title": 1, "release_time": 1,
                  "_canonical_tickers": 1, "url": 1}},
    {"$sort": {"release_time": -1}},
]

# 并发扫所有 collection
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
mc = AsyncIOMotorClient("mongodb://127.0.0.1:27018/")

async def run():
    tasks = [mc[dbn][coll].aggregate(pipeline).to_list(None)
             for dbn, cols in ROUTE for coll in cols]
    all_rows = [row for batch in await asyncio.gather(*tasks) for row in batch]
    return sorted(all_rows, key=lambda x: x.get("release_time", ""), reverse=True)
```

### 4.3 跨源聚合 HTTP API(线上应用)

```bash
# 单票聚合
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8000/api/unified/by-symbol/NVDA.US?limit=20"

# 限定来源 + 日期
curl "...&sources=alphapai,meritco&from_date=2026-03-01&to_date=2026-04-24"

# 模糊别名搜索(前端自动补全)
curl "/api/unified/symbols/search?q=英伟"

# 看某个原始字符串会规范化成什么
curl "/api/unified/normalize?q=阳光电源"
# → {"matched": ["300274.SZ"], "unmatched": []}
```

响应 shape:

```json
{
  "canonical_id": "NVDA.US",
  "total": 1234,
  "by_source": {
    "alphapai": 245, "meritco": 67, "thirdbridge": 12,
    "funda": 89, "gangtise": 21, "jinmen": 800
  },
  "items": [
    {
      "source": "alphapai",
      "collection": "comments",
      "source_label": "AlphaPai · 点评",
      "id": "HCMT00000001036234",
      "title": "...",
      "release_time": "2026-04-17 14:33",
      "url": "https://alphapai-web.../detail?...",
      "preview": "前 260 字节正文...",
      "tickers": ["NVDA.US", "AVGO.US"]
    }
  ]
}
```

### 4.4 加常用过滤条件

```python
# 排除"无个股"的行业纪要
{"_canonical_tickers": "NVDA.US", "_canonical_tickers": {"$ne": []}}

# 近 30 天
{"_canonical_tickers": "NVDA.US",
 "release_time": {"$gte": (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")}}

# 多票任一命中
{"_canonical_tickers": {"$in": ["NVDA.US", "AMD.US", "INTC.US"]}}

# 多票全部命中(罕见,一般用聚合 + $size)
{"_canonical_tickers": {"$all": ["NVDA.US", "AAPL.US"]}}

# 取正文字段(各平台不一,常见有 content_md / summary_md / transcript_md)
projection = {"title": 1, "release_time": 1, "_canonical_tickers": 1,
              "content_md": 1, "summary_md": 1, "transcript_md": 1}
```

---

## 5. 模型训练:一个 `fetch_all_mentions()` 即可

```python
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta

MONGO_URI = "mongodb://127.0.0.1:27018/"
ROUTE = [
    ("alphapai-full",  ["comments", "roadshows", "wechat_articles", "reports"]),
    ("jinmen-full",    ["meetings", "reports", "oversea_reports"]),
    ("jiuqian-full",   ["forum", "research"]),
    ("third-bridge",   ["interviews"]),
    ("funda",          ["posts", "earnings_reports", "earnings_transcripts", "sentiments"]),
    ("gangtise-full",  ["summaries", "researches", "chief_opinions"]),
    ("acecamp",        ["articles"]),
    ("alphaengine",    ["summaries", "china_reports", "foreign_reports", "news_items"]),
]

async def fetch_all_mentions(tickers: list[str], since_days: int = 365,
                              min_text_len: int = 100):
    """Fan-out 所有平台,按 canonical ticker 收集训练样本."""
    c = AsyncIOMotorClient(MONGO_URI, tz_aware=True)
    cutoff = (datetime.now() - timedelta(days=since_days)).strftime("%Y-%m-%d")
    q = {"_canonical_tickers": {"$in": tickers},
         "release_time": {"$gte": cutoff}}
    proj = {"_id": 1, "title": 1, "release_time": 1, "_canonical_tickers": 1,
            "content_md": 1, "summary_md": 1, "transcript_md": 1, "insight_md": 1}

    async def one(dbn, coll):
        docs = await c[dbn][coll].find(q, projection=proj).to_list(None)
        return [{
            "source": dbn.replace("-full", ""),
            "collection": coll,
            "id": str(d["_id"]),
            "date": d.get("release_time"),
            "title": d.get("title") or "",
            "text": (d.get("content_md") or d.get("summary_md")
                     or d.get("transcript_md") or d.get("insight_md") or ""),
            "tickers": d.get("_canonical_tickers") or [],
        } for d in docs]

    batches = await asyncio.gather(*[one(dbn, coll)
                                      for dbn, cols in ROUTE for coll in cols])
    rows = [r for b in batches for r in b if len(r["text"]) >= min_text_len]
    c.close()
    return rows

# 用:
rows = asyncio.run(fetch_all_mentions(["NVDA.US", "AMD.US"], since_days=180))
print(f"拿到 {len(rows)} 条训练样本")
# → 按日期 / 按 ticker / 按 source 切分即可喂给模型
```

关键字段说明:

| 字段 | 哪些集合会有 | 说明 |
|---|---|---|
| `content_md` | alphapai / acecamp / jinmen / gangtise(部分) | 正文 Markdown |
| `summary_md` | alphapai.roadshows / jinmen.meetings / alphaengine.summaries | 纪要/摘要 |
| `transcript_md` | jinmen.meetings / meritco.research | 逐字转写 |
| `insight_md` | alphapai.comments / gangtise.chief_opinions | 点评/观点 |
| `pdf_rel_path` | alphapai / jinmen / gangtise / meritco / alphaengine / acecamp | PDF 存 `/home/ygwang/crawl_data/*_pdfs/` 下,要全文需读 PDF |
| `release_time` | 所有集合 | 字符串,格式不统一(`YYYY-MM-DD HH:MM` / `YYYY-MM-DD` / ms 时间戳都有) |

---

## 6. 未匹配原始字符串 / 扩 alias

若你发现某个公司在 `_unmatched_raw` 里反复出现但没进 `_canonical_tickers`,可以扩别名表:

```bash
# 看 Top 50 未匹配
cd /home/ygwang/trading_agent
PYTHONPATH=. python3 scripts/enrich_tickers.py --dry-run --report-unmatched 50
```

编辑 `backend/app/services/ticker_data/aliases.json`:

```json
{
  "Palantir": "PLTR.US",
  "谷歌/Google": "GOOGL.US",
  "Temu": null
}
```

- `null` = 已知无法映射(未上市 / 不是股票),避免反复出现在 unmatched
- 改完等 03:00 cron 自动把历史 `_canonical_tickers: []` 重新匹配捡回,或手动:

```bash
PYTHONPATH=. python3 scripts/enrich_tickers.py --reload-aliases
```

---

## 7. 几个注意事项

- **排除 `_state` / `account` 集合**:每个 DB 里都有 checkpoint / daily stats / account probe 等元数据集合,它们没有 `_canonical_tickers` 字段。按 §3 的集合列表取数据即可。
- **`release_time` 格式不统一**:不同平台 string 格式不同,做时间过滤时统一 parse 到 datetime 再比,或用 `release_time_ms`(部分集合有)。
- **覆盖率低 ≠ 漏打**:`alphapai.reports`(0%)、`alphapai.wechat_articles`(9.5%)是**上游本身没有股票字段**,不是系统问题,文档仍带 `_canonical_tickers: []`。要从正文挖 ticker 需要额外 NER,不在本体系内。
- **sentimentrader 不在体系内**:它是宏观情绪指数(Smart/Dumb、Fear/Greed、QQQ Optix),不涉及个股。
- **索引是 single-field `{_canonical_tickers: 1}`**:multikey index,命中 `$in` / `$eq` 高效;但复合查询(如 `ticker + date`)不会用复合索引,若查询量大且慢,可按需加 `{_canonical_tickers: 1, release_time: -1}`。
