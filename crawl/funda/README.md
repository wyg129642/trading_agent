# funda_crawl — funda.ai

Funda AI (`https://funda.ai`) 的 tRPC 接口爬虫 — 研究文章 / 8-K / 财报电话会逐字稿 → MongoDB。

- 鉴权: Cookie (`session-token`) (§5.3 场景 C)
- API: `/api/trpc/*.procedure?batch=1&input=...` (Next.js + tRPC 10)
- 响应: 常规 JSON (不要加 `trpc-accept: application/jsonl` 头, 否则变成流式 JSONL)
- 反爬: 平台看起来未设重度 WAF, 默认用 `antibot` 的 `3s±2s` 节奏 + daily cap 500

---

## 1. 工作流 (浏览器登录到入库)

1. 浏览器登录 `https://funda.ai`, 进入 `/reports` 页面
2. F12 → Application → Cookies → 把关键 cookie 拼成 `k1=v1; k2=v2` (或 Console 敲 `document.cookie` 整行复制)
   - 必需: `session-token=<64 hex>`
   - 可选 (但有就更像浏览器): `__Secure-x-geo-country`, `NEXT_LOCALE`, `cc_prefs`, `_ga`, `_ga_*`
3. 粘贴到 `credentials.json` 的 `cookie` 字段 (该文件已被 `.gitignore`)
4. 启动本地 MongoDB (默认 `mongodb://localhost:27017`)
5. 首次:
   ```bash
   python3 scraper.py --show-state     # 凭证健康 + 空 checkpoint
   python3 scraper.py --max 20         # 各分类各抓 20 条 + account 元数据
   ```
6. 增量:
   ```bash
   python3 scraper.py --watch --resume --interval 600
   ```

---

## 2. 接口说明

所有接口走 `GET /api/trpc/<procedure>?batch=1&input=<URL-encoded JSON>`。

`input` 格式 (tRPC superjson):
```json
{"0": {"json": <真正的 body>, "meta": {"values": {...}, "v": 1}}}
```

其中 `meta.values.*` 用来标注 JSON 里的特殊类型:
- `"cursor.publishedAt": ["Date"]` — 该字段序列化时按 ISO 日期
- `"tag": ["undefined"]` — 与 `null` 区分 (tRPC 前端习惯用 undefined 省略)

响应格式 (常规批量 tRPC):
```json
[{"result":{"data":{"json":{...真正的 data}}}}]
```

### 2.1 三个列表接口

| 分类 | procedure | cursor 类型 | 返回字段 |
|---|---|---|---|
| `post` | `post.fetchInfinite` | object `{id, publishedAt}` | `items[], nextCursor, ...` |
| `earnings_report` | `companyEarning.fetchEightKReports` | string (cuid) | `items[], counts{all,today,yesterday,week}, nextCursor, totalCount` |
| `earnings_transcript` | `companyEarning.fetchTranscripts` | string (cuid) | 同上 |

列表 body (`input.0.json`):
```jsonc
// post.fetchInfinite
{"limit":20,"sortBy":"latest","tag":null,"tickers":null,"enterpriseOnly":null,"direction":"forward","cursor":{"id":"...","publishedAt":"..."}?}

// companyEarning.fetch{EightKReports,Transcripts}
{"limit":20,"dateFilter":"all","customDate":null,"ticker":null,"tickers":null,"industry":"","searchQuery":null,"direction":"forward","cursor":"cmlhs..."?}
```

### 2.2 两个详情接口

| 分类 | procedure | input |
|---|---|---|
| `post` | `post.fetchBySlug` | `{"0":{"json":"<slug>"}}` (吃 string, 不是 {id}!) |
| `earnings_report` / `earnings_transcript` | `companyEarning.fetchById` | `{"0":{"json":{"id":"<cuid>"}}}` |

详情返回:
- **post**: `body`(markdown/富文本正文), `visibility`, `attachments`, `comments`, `totalComments`, `likesCount`, `previewBody`, ...
- **earnings**: `content`(8-K 是 HTML; transcript 是纯文本), `type`(EIGHT_K / TRANSCRIPT), `createdAt`, `updatedAt`

### 2.3 账户 / 元数据接口 (写入 `account`)

| _id | procedure | 说明 |
|---|---|---|
| `user-profile` | `user.getUserProfile` | `orgId, tier(FREE/ENTERPRISE), region, appMode` |
| `cms-access` | `cmsAccess.getCurrentAccess` | 后台权限 |
| `scaling-up-config` | `appConfig.fetchScalingUpConfig` | 配置 |
| `post-available-tickers` | `post.fetchAvailableTickers` | 研究文章的可筛选 ticker 列表 |
| `post-tag-counts` | `post.fetchTagCounts` | 各 tag 文章数 |
| `earning-available-tickers` | `companyEarning.fetchAvailableTickers` | 财报的可筛选 ticker |
| `earning-industries` | `companyEarning.fetchAvailableIndustries` | 可筛选行业 |

---

## 3. 故障排查清单

| 症状 | 优先排查 |
|---|---|
| 所有请求 HTTP 401/403 | `session-token` 过期 → 浏览器重登, 更新 `credentials.json` |
| 响应体是分块 JSONL (4 行 `{"json":...}`) | 不小心设了 `trpc-accept: application/jsonl` 头 → 去掉 |
| `tRPC error: {code:"BAD_REQUEST",...}` | `meta.values` 缺 `undefined` / `Date` 标记, 或 body 字段类型错 |
| `post.fetchBySlug` 报 "Input should be string" | 记得传 string 不是 `{id}`; 参考 `build_detail_input` |
| `companyEarning.fetchById` 空 | 需要传 `{"id": "<cuid>"}`, 不是 slug |
| 列表 `nextCursor=null` 但前端还能翻 | 说明到头了, 正常退出; 不要循环 |
| `--today` 扫不到今天 | funda.ai 的时间是 UTC; 本机时区可能对应前一天 |
| HTML 正文里有 `&#160;` / `&#58;` 等实体 | `html_to_text` 只 strip 标签不做 unescape — 要查时用 `html.unescape()` |

---

## 4. 数据模型

### 4.1 `posts` / `earnings_reports` / `earnings_transcripts`

```js
{
  _id: <platform id>,            // UUID (post) 或 cuid (earnings)
  id: <same>,
  category: "post" | "earnings_report" | "earnings_transcript",
  title,
  release_time: "YYYY-MM-DD HH:MM",    // UTC, 人类可读
  release_time_ms: <long>,              // ms since epoch
  web_url: "https://funda.ai/reports/...",
  slug, ticker, year, period, industry, sourceUrl,
  accessLevel, coverImageUrls, excerpt, subtitle, tags, entities, views,
  content_md:     "...",                // 主要正文 (post.body / transcript.content / 8-K 的 stripped text)
  content_html:   "...",                // 仅 8-K: 原始 HTML
  type:           "EIGHT_K" | "TRANSCRIPT",      // earnings only
  createdAt, updatedAt, visibility, attachments, totalComments, likesCount, previewBody,
  list_item:     {...},                 // 原始列表响应
  detail_result: {...},                 // 原始详情响应
  stats: {chars, html_chars},
  crawled_at: ISODate,
}
```

**索引**: `title`, `release_time`, `release_time_ms`, `ticker`, `category`, `crawled_at`。

### 4.2 `_state`

checkpoint — 每分类一条:
```js
{
  _id: "crawler_post" | "crawler_earnings_report" | "crawler_earnings_transcript",
  top_id,                    // 本轮第 1 条 id, 下轮 --resume 的锚点
  last_processed_id,
  last_processed_at,
  in_progress,               // 上轮是否跑完
  last_run_end_at,
  last_run_stats: {added, updated, skipped, failed},
  updated_at,
}
```

日统计 (`--today`):
```js
{
  _id: "daily_YYYY-MM-DD",
  date,
  post: {platform_count, in_db, missing, scanned},
  earnings_report: {...},
  earnings_transcript: {...},
  scanned_at,
}
```

### 4.3 `account`

见 §2.3, 每条 `{_id, procedure, response, updated_at}`。

---

## 5. 命令行参数

### 5.1 通用 (crawl/README.md §4)

| 参数 | 语义 |
|---|---|
| `--category {all,post,earnings_report,earnings_transcript}` | 指定分类 (默认 all) |
| `--max N` | 最多抓 N 条 (单分类) |
| `--page-size N` | 每页大小 (默认 20, funda 前端就是这个值) |
| `--force` | 强制重爬已入库 + 刷新 `account` |
| `--resume` | 增量模式: 遇到 `top_id` 即停 |
| `--watch --interval N` | 实时轮询 |
| `--since-hours N` | 只抓过去 N 小时内 (基于 `time_field`) |
| `--today [--date YYYY-MM-DD]` | 当日统计 (UTC) |
| `--show-state` | 凭证 + checkpoint 健康 |
| `--reset-state` | 清空 checkpoint |
| `--auth COOKIE` | 覆盖 credentials.json |
| `--mongo-uri`, `--mongo-db` | 默认 `localhost:27017` + `funda` |
| `--clean-posts` / `--clean-earnings-reports` / `--clean-earnings-transcripts` | 清空某集合 |

### 5.2 反爬 (crawl/antibot.py)

| 参数 | 默认 |
|---|---|
| `--throttle-base` | 3s (基础请求间隔) |
| `--throttle-jitter` | 2s (抖动) |
| `--burst-size` | 40 (每 N 条长冷却) |
| `--burst-cooldown-min/max` | 30~60s |
| `--daily-cap` | 500 (0=无限) |

### 5.3 典型用法

```bash
# 凭证 + 库状态
python3 scraper.py --show-state

# 首次小试 (各分类 10 条, 小节流)
python3 scraper.py --max 10 --throttle-base 1 --throttle-jitter 0.5

# 全量 (daily-cap 500 保护)
python3 scraper.py --max 500

# 增量轮询 (默认反爬参数, 10 分钟一轮)
python3 scraper.py --watch --resume --interval 600

# 单分类增量
python3 scraper.py --category post --resume --max 50

# 今日统计 (UTC)
python3 scraper.py --today

# 被风控怀疑时 (保守参数)
python3 scraper.py --throttle-base 8 --throttle-jitter 5 --burst-size 20 --daily-cap 100
```

---

## 6. 已知坑

1. **tRPC `meta.values` 不能省** — 对 `null` 但语义是 "undefined" 的字段必须标 `["undefined"]`, 否则 zod 校验会报 "Expected undefined, received null". 见 `CATEGORIES[*].list_meta_undef`.

2. **`post.fetchBySlug` 的 input 是 `string`, 不是 `{id}`** — 其他 procedure 都吃对象, 这一个吃原始字符串. 别踩.

3. **cursor 类型两种** —
   - `post.fetchInfinite`: `{"id":"...","publishedAt":"2026-..."}` (带 Date meta)
   - `companyEarning.*`: 一个 cuid 字符串
   见 `CATEGORIES[*].cursor_kind`.

4. **时间是 UTC** — 列表/详情里的 `publishedAt`/`date` 字段都是 ISO UTC (带 `Z`). `--today` 按 UTC 自然日切分, 本机时区若是 +08 会少算一天的晚间内容, 必要时用 `--date YYYY-MM-DD` 显式指定.

5. **8-K 的 `content` 是 HTML** — `content_md` 存 stripped 纯文本 (最多 20 万字符), `content_html` 存原件. 做 LLM 训练用 `content_md`; 做精确引用 / 图片渲染用 `content_html`.

6. **ID 两种格式** — `post` 里新文章是 UUID (带 `-`), 老文章是纯数字 (Substack 遗留 ID); `earnings` 全是 cuid (cmlhpfpdo00eci4tp1uom1f8c). 全都稳定, 直接用做 `_id`.

7. **反爬怀疑** — funda.ai 目前没看到 WAF 重度干预, 但:
   - `__Secure-x-geo-country` cookie 记国家, 跨地域请求可能被挡
   - `_ga*` cookie 没有的话浏览器 telemetry 会比真人少一大截
   - 连续几百次 API 不穿插页面加载, 会很显眼
   所以默认 3±2s + 每 40 条冷却 + daily cap 500. 被怀疑时降到 `--throttle-base 8`.

---

## 7. 依赖

```bash
pip install httpx pymongo tqdm
```

Python 3.9+。`httpx` 为了 `trust_env=False` 绕系统代理 (依赖服务器若走公司代理会出毛病); 不需要 `h2` (HTTP/2 关了).

---

## 8. 目录结构

```
funda/
├── README.md                 本文档
├── scraper.py                单文件爬虫
├── credentials.json          cookie (gitignored)
├── promts.md                 原始抓包笔记 (调试期参考)
└── .gitignore
```

---

## 9. 安全

- `credentials.json` 已在 `.gitignore`, 不要提交
- 一个账号一个 scraper 进程, 避免多开触发风控
- MongoDB 默认无鉴权, 公网部署一定防火墙 / SSH 隧道
- funda 研究文章 `accessLevel=ENTERPRISE` 的数据要求企业账号, 失去 ENTERPRISE 权限后 `post.fetchBySlug` 的 `body` 字段会被截断或替换成 `previewBody`; `user-profile.org.tier` 可以用来探测当前是否仍是 ENTERPRISE.
