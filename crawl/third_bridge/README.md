# Third Bridge Forum 爬虫

从 [forum.thirdbridge.com](https://forum.thirdbridge.com/zh/home/all) (Third Bridge) 爬取专家访谈 (含逐字稿 + 议程 + 专家信息 + 关联公司) 并存入 MongoDB。

`scraper.py` 跟 `crawl/meritco_crawl/scraper.py` / `crawl/jinmen/scraper.py` 同风格, 共享 `crawl/README.md` 第 3-4 节的数据模型 + CLI 约定。

---

## 1. 工作流

```
① 浏览器登录 https://forum.thirdbridge.com
② F12 → Console → 输入 `document.cookie` → 复制整行输出
   (或 Application → Cookies → forum.thirdbridge.com → 手动拼)
③ 粘到 credentials.json {"cookie": "..."}   (已 gitignore)
④ python3 scraper.py --show-state         # 检查 token 健康
⑤ python3 scraper.py --max 100             # 首次入库
⑥ python3 scraper.py --watch --resume --interval 600   # 实时增量
```

Cookie 也可通过 `--auth` 或环境变量 `TB_AUTH` 传入。

---

## 2. 接口说明

### 2.1 列表 (POST `/api/interview/search`)

body 对应前端搜索组件 (`groups=[], filters=[]` 就是不加任何筛选):

```json
{
  "lang": "zh",
  "groups": [],
  "sortBy": {"field": "startAt", "order": "desc"},
  "showNeuralSearch": false,
  "filters": [],
  "pageSize": 32,
  "pageFrom": 0
}
```

- `pageFrom` 是 **0-based 偏移** (不是 page number), 翻第 2 页传 `pageFrom=32`
- `sortBy.field=startAt order=desc` = 按访谈开始时间倒序 (最新在前, 包括未来的)
- `pageSize` 建议 32 (前端默认), 最大可以到 40+ 但没意义

**响应 shape:**

```json
{
  "requestId": "...",
  "count": 95708,                       // 平台总数
  "extendedCount": 0,
  "results": [ {interview 简化对象}, ... ],
  "tags": [...],                        // 本次查询命中的 tag 汇总
  "threadId": "", "entities": []
}
```

**每个 result 关键字段:**

| 字段 | 用途 |
|---|---|
| `uuid` | 32-hex 主键 (稳定, URL 也是这个) |
| `title` | 访谈标题 |
| `start` | ISO-8601 UTC, 访谈开始时间 |
| `status` | `READY` / `SCHEDULED` / ... |
| `language`, `contentType` | `{id, label, idPath}` 分类 |
| `agenda` | 议程 list\[str\] |
| `specialists` | 专家 (含 title/firstName/lastName/specialistType/合规问答/工作经历) |
| `moderators` | 研究员 |
| `targetCompanies` / `relevantCompanies` | 目标/相关公司 (含 ticker/country/sector) |
| `themes`, `sectors`, `geographies` | taxonomy, 每项 `{id, label, idPath}` |
| `transcripts` | `{en, zh, jp}` dict, value=true 表示有该语种逐字稿 |
| `pdfAvailableLanguages` | `{en, zh, jp}` dict, value=true 表示有 PDF |
| `audio`, `hasCommentary`, `expertCommentaryCount` | flags |
| `entitlements.granted` | 当前账号是否有权限 |

⚠️ 列表里**没有**逐字稿内容本身 —— 要拿逐字稿必须调详情接口。

### 2.2 详情 (GET `/api/interview/<lang>/<uuid>?source=&withTranscript=true`)

`<lang>` = `zh` / `en` / `jp`。`withTranscript=true` 必须带, 否则 `transcript: []`。

返回结构跟列表 item 类似, 额外多了:

| 字段 | 用途 |
|---|---|
| `transcript` | list\[{timestamp, discussionItem:\[{id, content, ...}\]}\] — **主菜, 逐字稿** |
| `introduction` | 同 transcript shape, 访谈开场白 (合规声明等) |
| `highlights` / `wordCounts` | 摘要高亮 / 按发言人字数统计 (新访谈可能为空) |

**时间戳格式**: `[HH:MM:SS]` 带方括号, 可直接拼到 md 里。

**访谈还没发生 / 不在权限内的情况:** transcript 可能为空 list, `entitlements.granted` 可能为 false。脚本不报错, 如实入库。

### 2.3 账户级接口 (写入 `account` collection, 一次性)

| 路径 | 方法 | _id in `account` | 用途 |
|---|---|---|---|
| `/api/client-users/account-management` | GET | `account-management` | 用户信息 (uuid, email, company, forumStatus, clientType, ...) |
| `/api/feature-manager` | GET | `feature-manager` | 前端 feature flag 列表 (可用于检测账号 AI 搜索等权限) |
| `/api/interview/filters` | GET → fallback POST | `filters` | 平台 taxonomy 字典 (行业 / 主题 / 地域 / 专家类型 / 内容类型 ...). **⚠ 某些账号会 hang** — 脚本会超时 8s 再 fallback POST, 两者都失败就存 `{}` |

### 2.4 专家点评 (per-interview, 自动抓)

| 路径 | 方法 | 触发条件 | 写入位置 |
|---|---|---|---|
| `/api/expert-commentary/specialist-commentary-api/v1/comment-data/by-interview-uuids` | POST | 详情里 `hasCommentary=true` | `interviews.{commentary_items, commentary_md}` + `stats.{点评条数, 点评字数}` |

响应 shape 可变 (直接 `{<uuid>:[...]}` / `{"data":{<uuid>:[...]}}` / `{"commentaryByInterview":{<uuid>:[...]}}`), 脚本三种都接受. 失败静默, 单条点评拉不到不影响整条 interview 入库.

### 2.5 已探测但未使用

| 路径 | 用途 | 情况 |
|---|---|---|
| `/api/interview/autocomplete` | 搜索联想 | 不用爬数据 |
| `/api/preferences/banner` | UI banner | 无业务价值 |
| `/_next/data/<BUILD_ID>/zh/home/*.json` | Next.js SSR 数据 | BUILD_ID 每次发布变, 不稳定, 走 `/api/*` 即可 |

### 2.5 PDF 下载

目前**未实现**。可能的接口:

- `/api/interview/pdf/<uuid>?language=zh` → HTTP 200 但 Content-Type 是 **JSON** (返回的是同一份 metadata+transcript), 不是 PDF
- 可能前端"下载 PDF"是客户端用 react-pdf 之类按 transcript 数据 **本地渲染**出来的

TODO: 如果确认有真实 PDF 下载接口, 再按 `alphapai_crawl` 研报 PDF 的套路加 `--pdf-dir`。现在逐字稿走 `transcript_md` 字段即可。

---

## 3. 鉴权

Third Bridge 用 **AWS Cognito + AWS WAF**, 不是 JWT 拿出来就行 —— 必须整份 cookie。

**关键 cookie** (其他是追踪, 可有可无, 全复制最稳):

| cookie | 作用 |
|---|---|
| `tb_forum_authenticated_prod` | JWE 格式的会话凭证 (最关键) |
| `AWSELBAuthSessionCookie-0` | AWS ELB + Cognito 会话 |
| `proda-forum-session-id` | 后端 session |
| `aws-waf-token` | AWS WAF token (会动态刷新, 过期后请求会被挡) |
| `NEXT_LOCALE` | `en` / `zh` 影响默认语言 |

**过期表现:**
- 请求返回 302 → 重定向 `login`
- 请求直接 401/403
- `aws-waf-token` 过期时可能回 403 / 挑战页

脚本在 `_raise_auth_or_http()` 里识别这些并抛 `AuthExpired`, 建议重登浏览器, 复制新 cookie。

---

## 4. MongoDB 数据模型

数据库默认 `thirdbridge`, 三个 collection:

### 4.1 `interviews`

```js
{
  _id: <uuid>,                          // 32-hex, 稳定主键
  uuid,
  title,
  release_time: "YYYY-MM-DD HH:MM",     // 本机时区可读
  release_time_ms: <long>,              // ms 时间戳 (基于 detail.start, 用于 --today / --since-hours)
  is_future: Bool,                      // start > now 时为 true (仅 --include-future 下才会出现 true)
  status,                                // READY / SCHEDULED / ...
  language_id, language_label,          // 原始语言
  content_type_id, content_type_label,

  agenda: [...], agenda_md,             // 议程原始 + markdown
  target_companies: [{id,label,ticker,public,country,sector}],
  relevant_companies: [...],
  specialists: [...], specialists_md,   // 原始 + 一行式
  moderators: [...],
  researcher_email,
  themes, sectors, geographies,         // taxonomy 名称 list

  transcripts_available: ["en","zh","jp"],  // 有哪些语种逐字稿
  pdf_available:          ["en","zh","jp"],  // 有哪些语种 PDF
  audio: Bool,
  has_commentary: Bool,
  expert_commentary_count: Int,

  // 可读文本
  transcript_md,                        // 主菜: 逐字稿 markdown
  introduction_md,                      // 开场白 (合规声明等)
  transcript_items: [...],              // transcript 原始 (timestamp + discussionItem)
  introduction_items: [...],

  // 原始 API 返回
  list_item, detail_result,
  entitlements, rules,

  stats: {转录段数, 转录字数, 议程条数, 专家数, 目标公司, 相关公司},
  crawled_at: ISODate
}
```

索引: `title`, `release_time`, `release_time_ms`, `is_future`, `crawled_at`。

**查询示例:**
```js
// 只要有 transcript 的
db.interviews.find({"stats.转录字数": {$gt: 0}})

// 过去 30 天内完成 + 有 transcript
db.interviews.find({
  is_future: false,
  "stats.转录字数": {$gt: 100},
  release_time_ms: {$gt: new Date(Date.now() - 30*86400000).getTime()},
}).sort({release_time_ms: -1})

// 涉及某公司的 (按 ticker 过滤)
db.interviews.find({"target_companies.ticker": "NVDA US"})
```

### 4.2 `account`

一次性元数据 (`account-management`, `feature-manager`)。`--force` 可刷新。

### 4.3 `_state`

- `_id = crawler_interviews` — checkpoint (`top_uuid`, `last_processed_uuid`, `last_run_stats`, 时间戳)
- `_id = daily_YYYY-MM-DD` — `--today` 快照

---

## 5. 命令行参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--max N` | 无限 | 最多爬 N 条 |
| `--page-size N` | `32` | 每页大小 (pageSize, 与前端一致) |
| `--force` | off | 强制重爬已入库 + 刷新 account |
| `--resume` | off | 增量模式, 遇到已知 `top_uuid` 停 |
| `--watch` | off | 定时轮询 (Ctrl+C 退出) |
| `--interval N` | `600` | `--watch` 间隔秒数 |
| `--since-hours N` | 无 | 只抓过去 N 小时内的访谈 (基于 start 时间) |
| `--include-future` | off | 保留已排期但尚未发生的访谈 (默认跳过, 因为 transcript 都是空) |
| `--lang zh/en/jp` | `zh` | 详情/搜索用哪种语言 |
| `--show-state` | off | 打印 checkpoint + token 健康检查, 退出 |
| `--reset-state` | off | 清空所有 checkpoint / daily 统计, 退出 |
| `--today` | off | 统计今日平台访谈 vs 本地库, 存 `_state` |
| `--date YYYY-MM-DD` | 今天 | 配合 `--today` |
| `--auth TOKEN` | `credentials.json` / env `TB_AUTH` | 覆盖 cookie |
| `--mongo-uri URI` | `mongodb://localhost:27017` | 或 env `MONGO_URI` |
| `--mongo-db NAME` | `thirdbridge` | 或 env `MONGO_DB` |

### 示例

```bash
# 首次入库前 200 条 (列表是 startAt desc, 前面会是未来 / 刚发布的访谈)
python3 scraper.py --max 200

# token 健康检查 + 账号信息
python3 scraper.py --show-state

# 增量监听, 每 10 分钟
python3 scraper.py --watch --resume --interval 600

# 今日统计
python3 scraper.py --today

# 拉英文逐字稿
python3 scraper.py --max 50 --lang en

# 只抓过去 7 天发生的访谈
python3 scraper.py --since-hours 168

# 远程 MongoDB
python3 scraper.py --mongo-uri mongodb://user:pass@host:27017 --mongo-db mydb
```

---

## 6. 已知坑 / 注意事项

### 6.1 列表按 startAt 倒序 ⇒ 前排全是未来访谈

Third Bridge 会提前**几周 / 几个月**公布即将做的访谈 (title + agenda + 专家 + 公司都有, 只是没 transcript)。实测 top-100 条**全部**是 2026-04~07 的未来访谈 (`start > now, status=READY, transcripts全false`)。

**默认行为: 跳过这些**。`fetch_items_paginated()` 里 `skip_future=True` (由 CLI `--include-future` 反转),
遇到 `start > now` 的 item 直接跳过, **不计入 --max、不入库**, 继续翻页直到遇到 past items。打印的
`future跳过 N/本次 M` 告诉你跳了多少, 累计多少 (`M` 随未来预约数增长会慢慢变大, 几百条起步)。

- `--resume` 的 `top_uuid` 锚点**永远记录最近一条 past 访谈**, 这样下次跑不会被未来访谈的不断新增干扰
- 如果需要未来访谈的元数据 (title/agenda/公司, 比如用来监控谁将接受调研), 加 `--include-future`
- 入库 doc 里无论如何都会标 `is_future: Bool`, 用 `{is_future: false}` query 过滤

### 6.1b 刚结束的访谈 transcript 还没出

平台的访谈流程: 预约 → (到 start 时间) 开谈 → 录音 → 处理 → 几小时~几天后 transcript 上线。所以:

- `start` 刚刚过去 (比如今天早上的访谈), `transcripts={en:false,zh:false,jp:false}` 是正常的
- `entitlements.granted` 里会出现 `subscribeToTranscriptAvailableNotification` 表示"可订阅 transcript 就绪通知"
- 脚本仍然会入库 (元数据还是有用), `stats.转录字数=0`
- 过几天再跑 `--force` 重爬这些 uuid, transcript 就会补上

### 6.2 `pageFrom` 不是 page number

API 是**偏移分页**, 不是页码分页。分页过程中如果平台又新增了访谈, 下一页起点会前移 1 条, 可能产生重复 (脚本里按 uuid dedup) 或漏抓 (小概率, 对短批量影响不大)。

### 6.3 transcript 为空的情况

1. 访谈还没发生 (start 在未来)
2. 账号权限不够 (`entitlements.granted=false`)
3. 指定 lang 的逐字稿没出 (比如 `--lang zh` 但平台只有 en 版)

脚本不报错, 如实入库 `transcript_md=""`。query 时 `stats.转录字数 > 0` 即可筛出有效的。

### 6.4 AWS WAF token 会过期

`aws-waf-token` cookie 有刷新机制, 一般账号活跃时前端自动刷新。**后端脚本不会自动刷新**, 所以长时间跑之后可能被挡。

- 正常 `--watch` 每 10 分钟跑一次, 轻度访问通常 24h+ 不掉
- 如果出现 `AuthExpired` 就重登浏览器, 复制新 cookie

### 6.5 `/api/interview/pdf/<uuid>` 返回 JSON 不是 PDF

实测结果: 这个端点返回的是**访谈详情 JSON**, 不是 PDF 文件。真正的 PDF 导出应该是前端用 react-pdf 本地渲染。所以本爬虫暂不提供 PDF 下载能力, 逐字稿走 `transcript_md` 字段 (markdown) 即可。

### 6.6 限流节流

每条访谈 1 次列表请求 + 1 次详情请求, 加 `TB_DELAY=0.8s` (env 可调) 节流。平台到目前没报过 429, 但一次跑几千条建议分批 / 夜里跑。

---

## 7. 依赖

```bash
pip install httpx pymongo tqdm
```

Python 3.9+。比 `meritco_crawl` 少一个 `pycryptodome` (不需要 RSA 签名), 比 `jinmen/` 也少 `pycryptodome` (不需要 AES 响应解密)。

---

## 8. 目录结构

```
third_bridge/
├── scraper.py          # 主脚本
├── credentials.json    # cookie (gitignored)
├── README.md
├── promts.md           # 平台接口参考 (原始抓包笔记)
├── logs/               # watch 模式 stdout 归档 (自建)
└── .gitignore
```

---

## 9. 安全

- `credentials.json` 已 gitignore, 不要提交到仓库
- 默认 `mongodb://localhost:27017` 无认证; 公网部署必须用防火墙 / SSH 隧道
- 一个账号只跑一个 scraper 进程, 避免触发风控 / 会话被清
