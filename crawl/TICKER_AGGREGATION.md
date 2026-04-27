# 跨平台股票标签:字段说明与提取方式

每份爬虫文档落库时打两套独立的派生字段:**规则路径(rule)** 和 **LLM 路径(llm)**。两者互不覆写,下游可分开取或 `$or` 合并。

---

## 1. 字段总览

| 字段 | 类型 | 路径 | 含义 / 例子 |
|---|---|---|---|
| `_raw_tickers` | `list` | rule | 上游平台原生 ticker 字段的**原样快照**(归一化前)。每个平台 shape 不同 —— 见 §4.1。例(alphapai):`[{"code":"INV.US","name":"Innventure, Inc..US"}]`;例(funda.posts):`["MSFT","PLTR"]` |
| `_canonical_tickers` | `list[str]` | rule | `_raw_tickers` 经 normalizer 后的 canonical ticker 列表;`[]` = 已扫描无命中。例:`["600282.SH"]` |
| `_canonical_tickers_at` | `datetime` | rule | 上次规则打标 / 重扫的 UTC 时间戳 |
| `_canonical_extract_source` | `str` | rule | `<source>` = 结构化字段命中;`<source>_title` = 标题正则兜底。例:`alphapai` / `alphapai_title` |
| `_unmatched_raw` | `list[str]` | rule | 规则路径见到的 ticker-like 字符串但被 normalizer 拒收。例:`["821018.CI"]`(中信一级行业指数,非个股) |
| `_llm_canonical_tickers` | `list[str]` | llm | LLM 输出经 normalizer 后的 canonical 列表;**仅当 `_canonical_tickers: []` 时才会写**。例:`["LPPSY.US"]` |
| `_llm_canonical_tickers_at` | `datetime` | llm | LLM 路径打标时间戳 |
| `_llm_extract_source` | `str` | llm | `<source>_llm:<model_key>`。例:`alphapai_llm:qwen-plus` |
| `_llm_unmatched_raw` | `list[str]` | llm | LLM 编出但 normalizer 拒收的字符串 |

**字段缺失语义:** 落库后 cron 自动补齐;`_canonical_tickers` 字段在场即"已扫描"。`_raw_tickers` 总是与 `_canonical_tickers` 一起写入,空数组也会落字段。`_llm_*` 字段仅在 LLM 路径处理过的文档上才存在(默认是 `_canonical_tickers: []` 的子集)。

**`_raw_tickers` vs `_canonical_tickers` 的差别:** 前者保留上游格式与元信息(`name` / `rating` / `country` 等),用于审计、按公司原名查询、或调试 normalizer;后者是统一规范化的查询主字段。下游做训练 / 检索默认用 canonical;追溯哪里来 / 上游怎么标的就看 `_raw_tickers`。

---

## 2. Canonical 格式

`<CODE>.<MARKET>`,大写 2 字母 MARKET。51 个交易所枚举见 `backend/app/services/ticker_normalizer.py::_KNOWN_MARKETS`,常用:

```
SH/SZ/BJ 沪深北   HK 港   US NASDAQ/NYSE   JP 东京   KS Korea   TW Taiwan
GB LSE   DE Xetra   FR Paris   CH SIX   NL Amsterdam   IT Milano
IN NSE/BSE   AU ASX   CA Toronto   BR São Paulo   SG SGX   ...
```

格式细节:A 股 6 位 + `.SH/.SZ/.BJ`(`600519.SH`);港股 5 位补零 + `.HK`(`09988.HK`);US 用 ticker symbol(`NVDA.US`、`BRK.B.US`)。

`ticker_normalizer.py::_EXCHANGE_SUFFIX_MAP` 把上游异型后缀(Bloomberg `.N→US` / Refinitiv `.SA→BR` / 进门财经自造 `.JPN→JP`)归一到 canonical;`.A` / `.P` / `.V` / `.CA` 故意不映射(避开 `S.p.A.` / `J.P.` / `N.V.` 公司法律形式后缀和 Cairo/Canada 歧义)。

---

## 3. Mongo 路由表

| 平台 | DB | 集合 |
|---|---|---|
| AlphaPai | `alphapai-full` | `comments` / `roadshows` / `reports` / `wechat_articles` |
| 进门财经 | `jinmen-full` | `meetings` / `reports` / `oversea_reports` |
| 久谦中台 | `jiuqian-full` | `forum` / `research` |
| 高临咨询 | `third-bridge` | `interviews` |
| Funda | `funda` | `posts` / `earnings_reports` / `earnings_transcripts` / `sentiments` |
| 岗底斯 | `gangtise-full` | `summaries` / `researches` / `chief_opinions` |
| AceCamp | `acecamp` | `articles` |
| 阿尔法引擎 | `alphaengine` | `summaries` / `china_reports` / `foreign_reports` / `news_items` |

连接串:`mongodb://127.0.0.1:27018/`(本机 ta-mongo-crawl,无 auth)。

`alphapai.wechat_articles` 信噪比过低,**不在打标体系内**(规则会写空 `[]`,LLM 路径不处理)。

---

## 4. 规则路径 — `scripts/enrich_tickers.py`

无 LLM 调用,跑遍 Mongo 集合,对每条文档:**(a)** 调 per-platform extractor 拿到 raw payload 写入 `_raw_tickers`;**(b)** 把 raw payload 喂 normalizer 做归一化,写入 `_canonical_tickers` / `_unmatched_raw`;**(c)** 结构化空时再走标题正则兜底。

### 4.1 `_raw_tickers` 怎么来 — per-platform extractor

`backend/app/services/ticker_normalizer.py::EXTRACTORS[<source>]` 路由到对应 `extract_from_<platform>(doc, collection)`,返回的 raw payload 经 `normalize_raw_for_storage()`(扁平化 + 过滤 `None` / 空串)后**原样**落 `_raw_tickers`。每个平台从不同的原生字段抽:

| 平台 | 原生字段(`extract_from_<platform>`) | `_raw_tickers` 形式 |
|---|---|---|
| **AlphaPai** | `roadshows`: `doc.stock` ; `comments`/`reports`/`wechat_articles`: `list_item.stock` (reports 缺时回退 `detail.stock`) | `[{code: "INV.US", name: "Innventure, Inc..US", isLinked?: bool}]` |
| **进门财经** | `meetings` / `oversea_reports`: `doc.stocks` ; `reports`: `doc.companies` | `[{name, code, fullCode: "sz000559", market: "sz", stockcode?, currentRating?, ...}]` |
| **岗底斯** | `summaries` / `researches`: `doc.stocks` + `list_item.emoSecurities` + `labelDisplays[].extra` + `aflScr.detail` + (summaries) `list_item.stock` 全部聚合 ; `chief_opinions`: 同上 | `[{code: "603755.SH", scr_id: "F1...", name}, {scrAbbr, gtsCode, scrId, ...}]` |
| **久谦中台 (meritco)** | `forum`: `doc.related_targets` + `list_item.tag1` ; `research`: 仅 `list_item.tag1` | `["云顶新耀", "驯鹿医疗"]`(纯中文公司名) |
| **高临 (third_bridge)** | `doc.target_companies` + `doc.relevant_companies` 拼接 | `[{id, label, ticker: "LTBR US", public, country, sector}]`(Bloomberg-style 空格分隔 ticker) |
| **Funda** | `posts`: `entities.tickers` ; `earnings_reports` / `earnings_transcripts`: 顶层 `ticker`(标量) | `["MSFT", "PLTR"]`(裸 US ticker) |
| **AceCamp** | 优先 `list_item.corporations`(含 `ticker: "SZ.000568"`),回退顶层 `corporations`(只有 `name`) | `[{id, ticker: "SZ.000568", name: "泸州老窖", logo}]` |
| **阿尔法引擎 (alphaengine)** | `doc.company_codes[]` / `doc.company_names[]` 平行拼成 dict 列表 | `[{code: "ARJO.ST", name: "ARJO"}]` |
| **SemiAnalysis** | 标题 / subtitle / preview 里的 cashtag(`$AAPL` 形式) | `["$AAPL", ...]` |

`_raw_tickers` 字段保留**所有**上游元信息(`rating` / `country` / `sector` / `scrId` 等),不做 schema 统一 —— 下游若要按这些信息切分,直接从这里读。

### 4.2 `_canonical_tickers` 怎么来 — normalizer 归一化

`_raw_tickers` 中每个元素(dict 或 str)送 `ticker_normalizer.normalize_with_unmatched()`:

1. **dict 优先按 `code` / `fullCode` / `ticker` 字段解**(`_from_alphapai_stock` / `_from_jinmen_stock` / `_from_gangtise_stock` / `_from_acecamp_inner_corp` / `_from_tb_company` 一组 per-platform helper);进门财经 `fullCode: "sh601339"` 走 `_parse_jinmen_fullcode`(2 字母前缀 `sh/sz/hk/us` → canonical 后缀),AceCamp `ticker: "SZ.000568"` 走 `_parse_reverse_dotted`,高临 `ticker: "LTBR US"` 走 `_parse_tb_ticker`,普通 `code: "INV.US"` 走 `_parse_dotted`
2. **再走 alias 表**(`aliases.json` ~560 curated + `aliases_bulk.json` ~49k Tushare 自动)—— 落地拿到中文 / 英文公司名也能解析(`"伊利股份" → "600887.SH"`,`"Innventure, Inc."` 走法律后缀剥离 stem)
3. **后缀白名单卡死**:`_KNOWN_MARKETS` 51 个国家代码 + `_EXCHANGE_SUFFIX_MAP` 把上游异型后缀(Bloomberg `.N→US`、进门财经 `.JPN→JP`、`.SA→BR`)归一
4. 命中 → `_canonical_tickers`;后缀不识别 / dict 既无 code 又无 alias-命中 name → `_unmatched_raw`

### 4.3 标题正则兜底 → `_canonical_extract_source: <source>_title`

结构化为空时,扫 `title` / `title_cn` / `title_en`,用 `extract_tickers_from_text(text)` 找带括号的 `(CODE.MARKET)` / `(CODE:MARKET)`(半角全角括号都认):

```
"Kakaku.com Inc.(2371.JPN)"      →  2371.JP   ← .JPN→JP 走 _EXCHANGE_SUFFIX_MAP
"Mountain Province (MPVD:CA)"     →  MPVD.CA  ← 冒号形式走 _parse_colon_suffix
```

只匹配带括号形式,**不**匹配裸 `CODE.MARKET`(否则 `S.p.A` 里 `.A` 会误命中)。

### 4.4 Cron

```cron
*/10 * * * * ... enrich_tickers.py --incremental ...     # 近 N 小时新文档
0 3 * * *   ... enrich_tickers.py --reload-aliases ...   # 全库重扫,扩 alias 后捞回历史空标
```

`--only-empty` 选项专扫 `_canonical_tickers: []`(加新后缀 / 新 alias 后用这个最划算)。

### 4.5 别名表

`backend/app/services/ticker_data/aliases_bulk.json` (~49k 条,Tushare 自动生成 + 法律后缀剥离 stem) + `aliases.json` (~560 条手工 curated,优先级高于 bulk)。重建 bulk:

```bash
TUSHARE_TOKEN=<hex64> python3 scripts/rebuild_aliases_bulk.py
```

---

## 5. LLM 路径 — `scripts/llm_tag_tickers.py`

按量付费,只处理 `_canonical_tickers: []` 且 `_llm_canonical_tickers` 不存在的文档。

### 5.1 提取流程

```
title(≤240 char)+ body excerpt(≤1500 char)
  → SYSTEM_PROMPT(JSON 输出 + canonical 格式约束 + 严格规则)
  → qwen-plus chat.completions(默认;`MODELS` dict 里有 8 个模型可选)
  → JSON parse 拿到 ["CODE.MARKET", ...]
  → ticker_normalizer.normalize_with_unmatched()
     · 命中 canonical → _llm_canonical_tickers
     · 后缀不识别 → _llm_unmatched_raw
  → bulk_write 落 Mongo(BATCH=100)
```

正文字段优先级(取第一个非空):`summary_md` → `content_md` → `transcript_md` → `insight_md` → `oversea_content_md` → `chief_opinion_md` → `article_md` → `body_md` → `subtitle` → `truncated_body_text` → `summary` → `content`。

### 5.2 SYSTEM_PROMPT 关键约束

- canonical 格式枚举(51 markets)+ 6 位 / 5 位 / ADR ticker 格式规则
- "Tag ONLY the document's primary analysis subject(s)" — 排除 casual mentions
- "If macro / industry / strategy with no single-stock subject → return `{tickers:[]}`"
- "NEVER invent CODE.MARKET pairs — output empty list when uncertain"
- 输出严格 JSON,不允许 markdown 围栏 / 散文

完整 prompt 见脚本 `SYSTEM_PROMPT` 常量。

### 5.3 模型 / 成本(2026-04 list)

| key | provider | $/1M in | $/1M out | qps | 适用 |
|---|---|---:|---:|---:|---|
| `qwen-plus` | DashScope | 0.11 | 0.27 | 8 | 默认;中文集合甜点 |
| `claude-haiku` | OpenRouter | 1.00 | 5.00 | 8 | 英文研报性价比 |
| `claude-sonnet` | OpenRouter | 3.00 | 15.00 | 5 | 英文 NER 最稳 |
| `gemini-flash` | OpenRouter | 0.30 | 2.50 | 10 | 海量集合(jinmen.oversea) |
| `gpt-5-mini` / `gpt-5` | OpenAI | 0.40/2.50 | 1.60/10.00 | 10/8 | 需 Clash 代理 |
| `gemini-pro` / `deepseek-v3` | OpenRouter | 1.25/0.27 | 10.00/1.10 | 6/8 | 备选 |

实测 alphapai roadshows 10k(qwen-plus):in 8.28M / out 140k tokens / **$0.95** / **18 min**,命中率 49.3%。

### 5.4 CLI

```bash
# 默认方案:全部 alphapai 空标用 qwen-plus 跑(预算硬停 $20,跳过确认)
PYTHONPATH=. python scripts/llm_tag_tickers.py \
    --model qwen-plus --source alphapai \
    --max-cost-usd 20 --yes

# 单 collection smoke test
PYTHONPATH=. python scripts/llm_tag_tickers.py \
    --model qwen-plus --collection gangtise.chief_opinions \
    --max-docs 50

# 重跑已 LLM 标过的(用更准模型)
PYTHONPATH=. python scripts/llm_tag_tickers.py \
    --model claude-haiku --collection alphapai.reports \
    --force-relabel --max-cost-usd 30 --yes
```

Run log 落 `logs/llm_tag/<ts>_<model_key>.json`(in/out tokens、cost、failures)。

---

## 6. 审计 / 回滚

```python
# 标题正则兜底命中(规则路径弱命中)
db.<coll>.find({"_canonical_extract_source": {"$regex": "_title$"}}, ...)

# LLM 命中
db.<coll>.find({"_llm_canonical_tickers": {"$ne": [], "$exists": True}}, ...)

# 联合查询(rule + LLM 任一命中)
db.<coll>.find({"$or": [
    {"_canonical_tickers": "NVDA.US"},
    {"_llm_canonical_tickers": "NVDA.US"},
]})

# 回滚某次 LLM 打标(规则字段不动)
db.<coll>.update_many(
    {"_llm_extract_source": {"$regex": "_llm:qwen-plus$"}},
    {"$unset": {
        "_llm_canonical_tickers": "",
        "_llm_canonical_tickers_at": "",
        "_llm_unmatched_raw": "",
        "_llm_extract_source": "",
    }},
)
```

`_canonical_tickers` 上有 single-field multikey index,`$in` / `$eq` 高效。`_llm_canonical_tickers` 暂未建索引;查询量大可加 `db.<coll>.create_index([("_llm_canonical_tickers", 1)])`。
