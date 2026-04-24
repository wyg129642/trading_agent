# `crawl/` — 爬虫集主文档 (Master Ops Guide)

> **一页上手**:本文档是全部 **8 个平台 / 18 条并行爬虫** + 后端 / 前端接入
> + 运维的**唯一权威入口**。子目录各自的 `README.md` 仍保留作为平台内部
> 详细说明,**日常运维看这里就够**。

---

## 1. 全景图

```
浏览器手动登录拷贝 token (或走 /data-sources 凭证管理 UI 自动登录)
           │
           ▼
┌──────────────────────────────────────────────────────────────┐
│  crawl/                                                       │
│  ├── alphapai_crawl/    Alpha派 (国内券商研报/纪要/点评/微信)  │
│  ├── jinmen/            进门财经 (AI 会议纪要 + 研报 PDF)      │
│  ├── meritco_crawl/     久谦中台 (论坛: 纪要/研报/久谦自研)    │
│  ├── third_bridge/      Third Bridge (英文专家访谈)            │
│  ├── funda/             funda.ai (美股研究/8-K/业绩会/情绪)    │
│  ├── gangtise/          港推 (港股纪要/研报/首席观点 — 7 类)   │
│  ├── AceCamp/           AceCamp (观点/纪要/调研)               │
│  ├── sentimentrader/    SentimenTrader (情绪指标 daily)        │
│  │                                                             │
│  ├── antibot.py         共享节流 / 日限 / 会话死亡处理         │
│  ├── auto_login_common.py  Playwright 通用登录骨架             │
│  ├── crawler_monitor.py    跨爬虫监控面板 (Web :8080 + 飞书)   │
│  ├── tools/dedup_urls.py   URL 去重小工具 (调研抓包用)         │
│  ├── CRAWLERS.md           ← 你在这里                          │
│  ├── README.md             共用架构 + 新爬虫 playbook + antibot│
│  ├── BOT_USAGE.md          antibot.py 使用手册 (详细版)        │
│  └── TICKER_AGGREGATION.md 跨平台股票字段统一 (训练准备)       │
└──────────────────────────────────────────────────────────────┘
           │
           ▼
MongoDB:每平台一个 DB (`localhost:27017`,容器名 `crawl_data`)
每个内容类型一个 collection
           │
           ▼
backend/app/api/{alphapai,jinmen,meritco,thirdbridge,funda,
                 gangtise,acecamp,sentimentrader}_db.py
           │
           ▼
frontend/src/pages/{AlphaPai*,Jinmen*,Meritco*,ThirdBridge*,
                    Funda*,Gangtise*,AceCamp*}.tsx
           │
           ▼
侧边栏「深度研究」分组 · 统一命名 (研报 / 纪要 / 观点 / 微信)
```

---

## 2. 平台一览 (入库数据,2026-04-22)

| 平台 | DB | 核心内容 · 数量 | 进程数 | 鉴权 | 反爬难度 | 子 README |
|---|---|---|---|---|---|---|
| **AlphaPai** (Alpha 派) | `alphapai` | roadshows 557 · reports 3 425 · comments 11 266 · wechat 21 068 | **4** (`--category`) | localStorage JWT | ⭐ 普通 | [alphapai_crawl/README.md](alphapai_crawl/README.md) |
| **Jinmen** (进门财经) | `jinmen` | meetings 10 058 · reports 10 068 · oversea_reports (外资 / 实时) | **3** (默认 + `--reports` + `--oversea-reports`) | localStorage base64 JSON + AES 响应解密 | ⭐⭐ 中 | [jinmen/README.md](jinmen/README.md) |
| **Meritco** (久谦中台) | `meritco` | forum 2 281 (type 2 专业 + 3 久谦自研) · research 68 (久谦研究) · PDF 附件 121 | 1 (`--type 2,3`) | Network token + **RSA X-My-Header 签名** | ⭐⭐⭐ 难 | [meritco_crawl/README.md](meritco_crawl/README.md) |
| **Third Bridge** (高临) | `thirdbridge` | interviews 148 | 1 (低频) | 完整 Cookie 串 (AWS Cognito + WAF) | ⭐⭐⭐⭐ 最难 | [third_bridge/README.md](third_bridge/README.md) |
| **Funda** (funda.ai) | `funda` | posts 354 · earnings_reports 886 · earnings_transcripts 2 480 · sentiments 10 487 | **3** (`--category`) | Cookie `session-token` + tRPC superjson | ⭐⭐ 中 | [funda/README.md](funda/README.md) |
| **Gangtise** (港推) | `gangtise` | summaries 656 · researches 480 · chief_opinions 711 | **3** (`--type`) — `--type summary` 内部再轮询 **7 个 classify** | localStorage `G_token` (UUID) | ⭐ 普通 (CDN 禁代理 + chief 端点特殊) | [gangtise/README.md](gangtise/README.md) |
| **AceCamp** | `acecamp` | articles 765 (观点 + 纪要) · events 111 | **2** (`--type`) | Cookie 三件套 (`user_token` JWT + Rails session + `aceid`) | ⭐⭐ 中 | [AceCamp/README.md](AceCamp/README.md) |
| **SentimenTrader** | `sentimentrader` | indicators 4 (Smart/Dumb · Fear/Greed · QQQ Optix) | 1 (cron 06:00 CST) | email + 密码 (Playwright) | ⭐⭐ 中 (Highcharts JS 内嵌) | [sentimentrader/README.md](sentimentrader/README.md) |
| **合计** | 8 DB | **76 000+ 文档** | **18 进程并行** | | | |

每份文档都带 `_canonical_tickers` 字段 (跨平台检索),`crawler_<sub>._state`
checkpoint 独立管理,各分类进程互不干扰。

---

## 3. 一条命令跑起来

### 3.1 常规增量 (推荐,cron 每小时一次)

```bash
cd /home/ygwang/trading_agent
for d in alphapai_crawl jinmen meritco_crawl third_bridge funda gangtise AceCamp; do
  ( cd crawl/$d && /home/ygwang/miniconda3/envs/agent/bin/python scraper.py --resume --max 200 ) \
    >> logs/crawler_${d}.log 2>&1
done
PYTHONPATH=. /home/ygwang/miniconda3/envs/agent/bin/python scripts/enrich_tickers.py --incremental
```

### 3.2 实时模式 (watch,后台常驻) — 推荐用 `crawler_monitor.py` 一键启动

```bash
# 跨平台监控面板 + 一键启停 (Web :8080)
cd crawl && nohup python3 -u crawler_monitor.py --web --port 8080 \
  > logs/logs_monitor_8080.log 2>&1 & disown

# 浏览器打开 http://127.0.0.1:8080 → 「🚀 实时」按钮
# 等价于 POST http://127.0.0.1:8080/api/start-all?mode=realtime
```

`ALL_SCRAPERS` (定义在 `crawler_monitor.py`) 列出每个 watcher 的额外参数:

```python
ALL_SCRAPERS = [
    ("meritco",  ["--type", "2,3"],                        "watch.log"),
    ("jinmen",   [],                                        "watch_meetings.log"),
    ("jinmen",   ["--reports"],                             "watch_reports.log"),
    ("jinmen",   ["--oversea-reports"],                     "watch_oversea_reports.log"),
    ("alphapai", ["--category", "roadshow"],                "watch_roadshow.log"),
    ("alphapai", ["--category", "comment"],                 "watch_comment.log"),
    ("alphapai", ["--category", "report"],                  "watch_report.log"),
    ("alphapai", ["--category", "wechat"],                  "watch_wechat.log"),
    ("funda",    ["--category", "post"],                    "watch_post.log"),
    ("funda",    ["--category", "earnings_report"],         "watch_earnings_report.log"),
    ("funda",    ["--category", "earnings_transcript"],     "watch_earnings_transcript.log"),
    ("gangtise", ["--type", "summary"],                     "watch_summary.log"),
    ("gangtise", ["--type", "research"],                    "watch_research.log"),
    ("gangtise", ["--type", "chief"],                       "watch_chief.log"),
    ("acecamp",  ["--type", "articles"],                    "watch_articles.log"),
    ("acecamp",  ["--type", "events"],                      "watch_events.log"),
]
```

`mode=realtime` 注入的公共参数:

```
--watch --resume --since-hours 24 --interval 60
--throttle-base 1.5 --throttle-jitter 1.0
--burst-size 0 --daily-cap 0           ← 实时档关掉 burst cooldown / daily cap
```

> **为什么实时档要关 cap?**`burst-size 40` 会每 30~60s 喘息一次,
> `daily-cap 500` 会让单进程当日封顶 ——爬虫看着"一阵一阵的"。在
> watch + since-hours 24 模式下,网页本来就只取昨天到今天的新增,
> 风控压力远低于全量回灌,关掉它们才能做到"网页发布 1~2min 内入库"。

**手动拉单一 watcher** (例如只重启 gangtise summary):

```bash
cd crawl/gangtise && nohup python3 -u scraper.py \
  --watch --resume --since-hours 24 --interval 60 \
  --throttle-base 1.5 --throttle-jitter 1.0 \
  --type summary --skip-pdf \
  >> logs/watch_summary.log 2>&1 & disown
```

### 3.3 历史补齐 (backfill)

补长账 (某分类断档 / 平台改了 API 结构丢了几千条历史) 走
`crawler_monitor.start_all("historical")` 或浏览器按钮「📚 历史补齐」,注入:

```
--watch --resume --interval 600          ← 慢轮询降负载
# 不加 --since-hours, 吃到上次 top_id 的所有历史
# 反爬保留默认 (base 3s · burst 40 · cap 500)
```

scraper 输出写到 `logs/weekend_backfill/<source>.log`,`crawler_monitor`
的 `effective_log_path` 会自动优先选该文件。补齐跑完一轮自然退出 (遇
`hit_known=True`),不会持续占资源。**不再有独立的 shell 脚本**(2026-04-17
加的 `weekend_*.sh` 已在 2026-04-22 整理时删除,逻辑搬到 `crawler_monitor.py`)。

### 3.3.1 当日对齐审计 (`audit_today.sh`) — 每 30 分钟一次

即使 watcher + `daily_catchup.sh` 跑得稳, 日内发布高峰还是会临时出现 "平台 N
条 / DB M 条" 的小缺口 (研报是重灾区, 因为 `--resume` 按 top_id 早停, 新条目被
挤到 page 2 时会整批漏). 这个脚本每半小时一次**精确定位缺失 IDs + 单条 force
补抓**, 既不等到次日 05:30 `daily_catchup`, 也不撞 `--resume` 盲区:

```
*/30 * * * * flock -n /tmp/ta_audit_today.lock /home/ygwang/trading_agent/crawl/audit_today.sh \
             >> /home/ygwang/trading_agent/logs/audit_today.log 2>&1
```

内部逻辑 (各平台各一个 `backfill_today*.py`):

1. 对 `alphapai.{report, roadshow, comment, wechat}` 走 `list_path` — 对
   `report` 注入 `startDate/endDate` 过滤 (=`--sweep-today` 端点路径),
   其他分类老老实实翻页到 `dt < today`.
2. 对 `gangtise.{summary, research, chief}` 走各自 list API,
   `summary`/`chief` 跨 classify 去重.
3. 算 `make_dedup_id` / `dedup_id_*`, `col.find({_id: {$in: ids}})` 一发查重.
4. 缺失的条目**逐条** `force=True` 调 `dump_one` / `dump_research` — 独立 detail
   抓取, bypass watcher 的 `--resume` top-id 盲点.

每轮约 3~4 min (read-only scan ~60-70% / 实际 force 抓取 ~30%). 全程只命中
list + detail, 不动 PDF (`--skip-pdf`), 不跟 watcher 抢 session (Mongo `replace_one`
幂等).

日期全部按 **CST 零点** 切天 (`datetime.now(Asia/Shanghai).replace(hour=0,...)`),
防止 UTC 跨天导致"今日"错位.

**手动跑 dry-run (只报告缺失, 不写 DB)**:

```bash
bash crawl/audit_today.sh --dry-run
```

**平台 vs DB 对齐体系 (三段式)**:

| 频率 | 工具 | 作用 |
|---|---|---|
| 实时 (60s) | `crawler_monitor.py --web :8080` 起的 watcher | `--watch --resume --since-hours 24` |
| 半小时 | `crawl/audit_today.sh` (新) | 扫今日 + 精确补漏 (不含 PDF) |
| 每日 05:30 | `crawl/daily_catchup.sh` | 过去 36h + `--force` 全量重扫 |

三层相互幂等, Mongo `upsert by _id` 天然容忍重复写入.

### 3.4 状态检查

```bash
# 每个爬虫 token + checkpoint
for d in alphapai_crawl jinmen meritco_crawl third_bridge funda gangtise AceCamp; do
  echo "=== $d ==="
  ( cd crawl/$d && python3 scraper.py --show-state )
done

# 跨 DB 入库进度
docker exec crawl_data mongosh --quiet --eval '
  ["alphapai","jinmen","meritco","thirdbridge","funda","gangtise","acecamp","sentimentrader"].forEach(dbn => {
    const d = db.getSiblingDB(dbn);
    d.getCollectionNames().filter(c => !c.startsWith("_") && c !== "account").forEach(c => {
      print(dbn + "." + c + ": " + d[c].countDocuments());
    });
  });
'
```

### 3.5 前端预览

浏览器打开 `http://127.0.0.1:8000`,左侧「**深度研究**」分组下 7 个专区都能看到最新数据。
管理员可在 `/data-sources` 页面看 8 个爬虫的 token 健康 / 启停 / 凭证管理 + 每日入库柱状图
(详见 [Daily Ingestion Chart](#52-daily-ingestion-chart))。

---

## 4. 统一 CLI 约定 (所有 scraper 支持)

**必支持** (详见 [README.md §4](README.md#4-统一-cli-约定)):

| 参数 | 含义 |
|---|---|
| `--max N` | 最多抓 N 条 |
| `--resume` | 增量,遇 `top_id` 停 |
| `--watch --interval N` | 实时模式 |
| `--force` | 强制重抓已入库 |
| `--today [--date YYYY-MM-DD]` | 当日统计对比,写 `_state` |
| `--show-state` | checkpoint + token 健康 |
| `--reset-state` | 清 checkpoint |
| `--auth TOKEN` | 覆盖硬编码 token |
| `--mongo-uri/--mongo-db` | 自定义 Mongo |
| `--since-hours N` | 仅抓过去 N 小时内的内容 |

**反爬** (由 `antibot.add_antibot_args` 注入):

| 参数 | 默认 (按平台) | 实时档常用 | 说明 |
|---|---|---|---|
| `--throttle-base` | 3s (3rd 4s · acecamp 2.5s) | 1.5s | 请求间隔基值 |
| `--throttle-jitter` | 2s (3rd 3s · acecamp 1.5s) | 1.0s | 间隔抖动 ± |
| `--burst-size` | 40 (3rd 30) | **0 (关闭)** | 每 N 条一次长冷却,0 = 不冷却 |
| `--daily-cap` | 500 (3rd 300) | **0 (关闭)** | 单轮上限,0 = 无限 |

**额外** (仅部分爬虫):

| 参数 | 适用 | 说明 |
|---|---|---|
| `--category <key>` | alphapai (`roadshow`/`comment`/`report`/`wechat`)<br>funda (`post`/`earnings_report`/`earnings_transcript`/`sentiment`) | 仅抓某类 (生产推荐分进程) |
| `--type <key>` | meritco (`2`/`3`/`2,3`) · gangtise (`summary`/`research`/`chief`) · acecamp (`articles`/`events`) | 同上 |
| `--reports` | jinmen | 切到 `reports` 抓研报 (默认 `meetings`) |
| `--lang zh\|en\|jp` | third_bridge | 访谈语言 |
| `--pdf-dir` / `--skip-pdf` / `--force-pdf` / `--pdf-only` | alphapai · jinmen · gangtise · meritco | PDF 下载控制 (gangtise 建议 `--skip-pdf`,白名单 403 较多;meritco `--pdf-only` 是 backfill 模式) |
| `--retry-until-fresh` | sentimentrader | 等到 EOD 数据更新才退出 (cron 用) |

**生产推荐**:用 `--category <key>` / `--type <key>` 把多分类平台拆成
独立进程并行,配合 `--watch --resume --since-hours 24 --interval 60
--burst-size 0 --daily-cap 0` 能做到新发布 1~2min 内入库。

---

## 5. 数据模型 + 监控面

### 5.1 文档 schema (每个平台共享)

```js
// <platform_db>.<content_collection>[]
{
  _id: <平台稳定 ID>,
  title,
  release_time: "YYYY-MM-DD HH:MM",            // 人类可读 (本地 CST)
  release_time_ms: <long>,                      // 毫秒戳 (UTC),排序/增量用
  organization / source / author,               // 发布方
  content_md | summary_md | transcript_md | insight_md | ...,
  list_item: {...},                             // 原始列表响应
  detail_result: {...},                         // 原始详情响应
  stats: {字数, 专家数, ...},
  crawled_at: ISODate,                          // UTC tz-aware (BSON Date)

  // gangtise.summaries 独有 (2026-04-22 起)
  classify_id, classify_name,                   // 7 个分类标签

  // 若有 PDF
  pdf_rel_path, pdf_local_path, pdf_size_bytes, pdf_download_error,

  // TICKER_AGGREGATION.md 派生字段
  _canonical_tickers: ["INTC.US", ...],
  _canonical_tickers_at: ISODate,
  _unmatched_raw: [],
  _canonical_extract_source: "<platform>",
}

// <platform_db>._state
{ _id: "crawler_<type>", top_id, last_processed_id, in_progress,
  last_run_stats: {added, skipped, failed, ...}, ... }
{ _id: "daily_<type>_YYYY-MM-DD", total_on_platform, in_db, not_in_db, ... }

// <platform_db>.account  (一次性元数据缓存)
{ _id: "<endpoint_name>", endpoint, response, updated_at }
```

所有 collection 都有 `_canonical_tickers_1` + `crawled_at_1` 单字段索引。

### 5.2 Daily Ingestion Chart

`/data-sources` 页面顶部一个 ECharts 堆叠柱状图 (后端
`GET /api/data-sources/ingestion-daily?days=7|14|30`),按 **CST 午夜**
桶聚合 `crawled_at`,直接对照各平台每日实际入库节奏。

后端入口在 `backend/app/services/credential_manager.py::ingestion_daily_series`,
用 MongoDB aggregation `$dateToString { timezone: "Asia/Shanghai" }` 桶化。

**注意时区**:`crawled_at` 在 Mongo 里是 UTC BSON Date;
Motor 客户端必须 `tz_aware=True`,否则前端 `dayjs` 会按本地解析得 8h 偏差。
2026-04-21 修过这个 bug,所有 `*_db.py` 已统一带 `tz_aware=True`。

### 5.3 Crawler Monitor (`crawler_monitor.py`)

CLI Rich 仪表盘 + Web :8080 + 飞书推送 (5min watchdog)。模式:

```bash
python3 crawler_monitor.py                          # CLI live
python3 crawler_monitor.py --web --port 8080        # HTTP dashboard
python3 crawler_monitor.py --json                   # 一次性快照
python3 crawler_monitor.py --push-feishu            # state 变化才推送
```

显示:`今日入库`、`最新入库时间`、`checkpoint`、`进程状态 / PID`、
`auth health (从日志末尾推断)`。Web UI 还有「🚀 实时」/「📚 历史补齐」
两个一键按钮。

---

## 6. 前端接入约定

### 6.1 侧边栏结构 (「深度研究」组)

```
深度研究
├── 每日简报                       (/alphapai/digest)
├── AlphaPai 专区
│   ├── 研报   (/alphapai/reports)
│   ├── 纪要   (/alphapai/roadshows)
│   ├── 点评速递 (/alphapai/comments)
│   └── 微信公众号 (/alphapai/feed)
├── 久谦专区 (Meritco)
│   ├── 纪要 (/meritco/minutes  · forum_type=2)
│   └── 研究 (/meritco/research · forum_type=3)
├── 进门专区 (Jinmen)
│   ├── 纪要 (/jinmen/meetings)
│   └── 研报 (/jinmen/reports)
├── 高临专区 (Third Bridge)
│   └── 专家访谈 (/thirdbridge/interviews)
├── 港推专区 (Gangtise)
│   ├── 纪要 (/gangtise/summary)        ← 7 类 classify 全量
│   ├── 研报 (/gangtise/research)
│   └── 首席观点 (/gangtise/chief)
├── Funda 专区
│   ├── 研报 (/funda/posts)
│   ├── 财报 (/funda/earnings-reports)
│   ├── 业绩会 (/funda/earnings-transcripts)
│   └── 情绪因子 (/funda/sentiment)
└── AceCamp 专区
    ├── 纪要 (/acecamp/minutes)
    ├── 观点 (/acecamp/viewpoint)
    └── 调研 (/acecamp/event)
```

**统一命名原则**:
- **研报 / 纪要 / 观点 / 微信 / 财报** 五类基础词汇跨平台复用
- 特殊保留:「首席观点」(港推) /「专家访谈」(高临) /「点评速递」(AlphaPai)
- 避免 Research / Reports / Earnings 等英文混杂

### 6.2 每个页面的极简布局

```
┌──────────────────────────────────────────┐
│  标题栏                       [刷新]    │
├──────────────────────────────────────────┤
│  [今日新增 N 条]  最近发布时间          │
├──────────────────────────────────────────┤
│  筛选条   搜索 / 机构 / Ticker / 行业    │
├──────────────────────────────────────────┤
│  列表 (分页 20)                          │
│    · 类型 Tag + 机构 + 标题              │
│    · 时间 + ticker + 行业                │
│    · 预览 / 字数                         │
└──────────────────────────────────────────┘
  点击 → Drawer 详情 (Tabs: 核心观点 / 正文 / 逐字稿 / 议程 / 专家 …)
```

### 6.3 后端 API 一览

```
GET  /api/{platform}-db/stats                    # 顶部小卡数据
GET  /api/{platform}-db/{items}[?category=...]   # 列表 + 筛选
GET  /api/{platform}-db/{items}/{id}             # 详情 + 完整内容
GET  /api/{platform}-db/{items}/{id}/pdf         # PDF 流式 (alphapai/jinmen/gangtise/meritco)

GET  /api/unified/by-symbol/{canonical_id}       # 跨平台聚合
GET  /api/unified/symbols/search?q=...           # 别名搜索

GET  /api/data-sources                           # 8 平台凭证 + 健康
GET  /api/data-sources/ingestion-daily?days=14   # 每日入库柱状图
POST /api/data-sources/{key}/crawler/start       # 启停单个 watcher
```

---

## 7. 跨平台股票字段统一 (训练准备)

每份文档入库后,由 `scripts/enrich_tickers.py` 自动加两个派生字段:

```js
_canonical_tickers: ["INTC.US", "0700.HK", "603061.SH"],   // 规范化跨平台 ID
_unmatched_raw: ["OpenAI"],                                 // 未匹配上的原始字符串
```

**查询 (训练取样)**:
```python
async for doc in coll.find({"_canonical_tickers": "NVDA.US"}): ...
```

**cron 接入 (每 10 分钟跑一次增量)**:
```cron
*/10 * * * * cd /home/ygwang/trading_agent && \
  PYTHONPATH=. /home/ygwang/miniconda3/envs/agent/bin/python \
  scripts/enrich_tickers.py --incremental \
  >> logs/enrich_tickers.log 2>&1
```

**扩充别名**:编辑 `backend/app/services/ticker_data/aliases.json` 加条目,
跑 `--reload-aliases` 重刷。

详见: **[TICKER_AGGREGATION.md](TICKER_AGGREGATION.md)**

---

## 8. 反爬 / 节流策略 (antibot.py)

> **2026-04-24 大修**:把 antibot 从"节流 + 401 退出"升级到完整的多层防护栈,
> 18 个 watcher 不再是同一指纹 / 同一节奏 / 同一账号 / 同一时段的"机器人合奏"。

### 8.1 完整组件清单

| 组件 | 用途 | 跨进程? |
|---|---|---|
| `AdaptiveThrottle` | **Gaussian** base±jitter (σ≈jitter/2) + 每 N 条 burst cooldown + 退避 + 5% long-tail 阅读停留 + 时段倍增 | 进程内 |
| `DailyCap` | 单进程单轮上限,主循环 `if cap.exhausted(): break` | 进程内 |
| **`AccountBudget`** (新) | 跨进程账号 24h 滚动总闸,Redis backed | ✅ |
| **`SoftCooldown`** (新) | 软警告 → 同平台所有 watcher 全局静默 30~60min | ✅ |
| `SessionDead` | 401/403 统一异常,主循环 catch → 退出让用户重登 | 进程内 |
| `parse_retry_after` / `is_auth_dead` | HTTP 响应助手 | — |
| **`detect_soft_warning`** (新) | 检测 `hasPermission:False/code=7` / 限流关键词 / WAF cookie / 软 429 | — |
| **`pick_user_agent`** + **`headers_for_platform`** (新) | 按 `CRAWLER_PROCESS_LABEL` env hash 到 5-8 个 Chrome 122-126 UA,locale + sec-ch-ua hints 跟平台 user base 对齐 | — |
| **`time_of_day_multiplier`** (新) | 23:00-07:00 ×2.5 / 周末 ×1.8 / 12:00-13:30 ×1.3 — 工时形态 | — |
| **`log_config_stamp`** (新) | scraper 启动时打印一行配置 stamp (UA / budget / cooldown 状态),监控可 grep | — |

### 8.2 平台默认参数

| 爬虫 | base | jitter | burst | daily_cap (单进程) | acct_budget (跨进程 24h) |
|---|---|---|---|---|---|
| alphapai_crawl | 3s | 2s | 40 | 500 | **3000** |
| jinmen | 3s | 2s | 40 | 500 | **2500** |
| meritco_crawl | 3s | 2s | 40 | 500 | **1200** |
| **third_bridge** | **4s** | **3s** | **30** | **300** | **300** (反爬最严) |
| funda | 3s | 2s | 40 | 500 | **2000** |
| gangtise | 3s | 2s | 40 | 500 | **3000** |
| AceCamp | 2.5s | 1.5s | 30 | 500 | **1500** |
| alphaengine | 3s | 2s | 40 | 500 | **1500** |

`acct_budget` 是估算的"日入库 ×2 余量",看入库情况调整。`--account-budget 0` 关闭。

### 8.3 实时档 (`start_all mode=realtime`)

| 参数 | 旧值 | 新值 | 说明 |
|---|---|---|---|
| `--interval` | 30s | 30s | 每轮轮询间隔不变 |
| `--throttle-base` | 1.5s | 1.5s | 基础节流不变,但分布从 uniform → Gaussian |
| `--throttle-jitter` | 1.0s | 1.0s | σ ≈ 0.5s |
| `--since-hours` | 24 | 24 | 增量边界不变 |
| **`--burst-size`** | **0 (关闭)** | **80** | 关键修正: 实时档每 80 条仍喘息一次, 防异常没人接 |
| `--burst-cooldown-min/max` | 30~60s | **10~25s** | 比 historical 短, 实时档延迟不能太长 |
| **`--daily-cap`** | **0 (关闭)** | **600** | 单进程兜底, 防跑飞 |

`--start-offset` 由 `crawler_monitor.start_all` 自动注入 0~min(interval, 60)s 随机偏移,
18 个 watcher tick 散开,不再每分钟 :00 同步打闪。

### 8.4 历史回填档 (`mode=historical`)

| 参数 | 值 | 说明 |
|---|---|---|
| `--interval` | 600s | 慢轮询降负载 |
| `--throttle-base` | 3.0s | 默认节流 |
| `--throttle-jitter` | 2.0s | |
| `--burst-size` | 40 | 每 40 条 30-60s 冷却 |
| `--daily-cap` | 500 | 单轮上限 |

不限 `--since-hours` → 吃到 checkpoint 上次位置的所有历史。

### 8.5 凌晨低峰档 (`mode=dawn`,新)

`crawler_monitor /api/start-all?mode=dawn` 或前端「🌙 凌晨低峰档」按钮触发。
配合 cron 02:00 启动,日间手工触发也行 (会自动按 time_of_day_multiplier ×2.5 减速)。

| 参数 | 值 |
|---|---|
| `--interval` | 300s (5 min) |
| `--throttle-base` | 2.5s |
| `--throttle-jitter` | 1.5s |
| `--burst-size` | 60 |
| `--burst-cooldown-min/max` | 20~45s |
| `--daily-cap` | 400 |
| `--since-hours` | 36 |

### 8.6 软冷却 (SoftCooldown) 触发条件

| 信号 | 静默时长 | 来源 |
|---|---|---|
| `http_429` | 45 min (thirdbridge 60 min) | `api_call` 检测 |
| `quota_code_7` (`hasPermission:False`) | 30 min | `detect_soft_warning` body 层 |
| `text:` / `msg:` 包含 "请求过于频繁" / "rate limit" / "captcha" 等 | 60 min | `detect_soft_warning` |
| `waf_cookie:datadome/_pxvid/_abck/ak_bmsc/captcha/geetest` | 60 min | `detect_soft_warning` cookie 层 |
| `REFRESH_LIMIT` (alphaengine 专属) | 30 min | scraper 内部 |

冷却期间, **同平台所有 watcher** 在 `_THROTTLE.sleep_before_next()` 顶部读 Redis flag,
睡到清除。手动清: `redis-cli del crawl:soft_cooldown:<platform>`。

### 8.7 时段倍增 (time_of_day_multiplier)

`--no-time-of-day` 禁用,默认开启:

| 时段 (CST) | 倍数 |
|---|---|
| 23:00 ~ 07:00 | × 2.5 (深夜) |
| 12:00 ~ 14:00 | × 1.3 (午休) |
| 周六 / 周日 (整天) | × 1.8 (叠乘以上) |
| 其它 | × 1.0 |

理由:bot 的特征之一是 24/7 平摊请求密度。把 throttle 按真人作息加权,后端按
hour-of-day 分桶判别就把我们的轨迹推向"工时活跃用户"这一族。

### 8.8 进程级 UA 池

`headers_for_platform("alphapai")` 返回的 UA 取自 `_UA_POOL` (5-8 个 Chrome 122-126
Win/Mac),按 `CRAWLER_PROCESS_LABEL` 环境变量 hash 稳定映射:

- 同一 watcher 重启 → 同一 UA (像同一个真人单设备)
- 不同 watcher (不同 `--category` / `--type` / `--market-type`) → 不同 UA
- `crawler_monitor.start_all` 在 spawn 时把 label 写到 env, scraper 读

每 watcher 还顺带配齐了 `Accept-Language` / `sec-ch-ua` / `sec-ch-ua-platform` /
`Sec-Fetch-*` 这些 modern Chrome 必带的头 — 缺这些是另一个 WAF 信号。

### 8.9 被风控 (401) 处置 5 步

1. 停 scraper (它应该自己已 SessionDead 退出)
2. 浏览器重登 → 复制新 token / cookie / auth-info
3. 写到 `credentials.json` 或走 `/data-sources` 凭证管理 UI
4. `--show-state` 验证 (会顺便打印 antibot stamp 确认配置)
5. 不要立刻起实时档 — 等 24h 让 AccountBudget 滚动窗清掉,再用 `mode=realtime` 重启

### 8.10 软冷却卡住的处置

如果某平台被一次警告卡了 60min, 但你已经修好问题了 (e.g. token 刷新了, IP 切了):

```bash
# 看有没有冷却 flag
redis-cli get crawl:soft_cooldown:alphapai

# 手动清掉, watcher 立即解除睡眠
redis-cli del crawl:soft_cooldown:alphapai
```

详见: **[BOT_USAGE.md](BOT_USAGE.md)** + **[README.md §7](README.md#7-反爬--反封号-antibotpy)**

---

## 9. 每个平台一句话知识点

| 平台 | 你必须知道的 1 件事 |
|---|---|
| **AlphaPai** | 部分"日报/期权早报/晨会纪要"只有 PDF 没有抽取文本 (`content=null` 正常)。<br>**2026-04-21 起** scraper 会把 `release_time > now` 的发布时间钳到 `now`,防止平台预发明天的报告污染前端列表。 |
| **Jinmen** | 响应体是 AES-CBC 加密的,`decrypt_response()` 用 salt `039ed7d8...`。<br>**2026-04-21 起** scraper 跳过 `was_skip` 时不做 throttle (大幅加速 dedup phase)。 |
| **Meritco** | 2026-04 起 `forum/select/list` + `forum/select/id` 必须带 **X-My-Header RSA 签名**,否则 `code=500`。PDF 附件下载端点 `POST /matrix-search/forum/pdfDownloadWatermark` body `{"pdfOSSUrlEncoded": <pdf_url.url>}` 直接返 PDF 流,**无 5 天预览限制** (前端 UI 层限制而已) |
| **Third Bridge** | 列表按 startAt desc,前面几百条全是**未发生的未来访谈** (transcript 必为空);用 `--since-hours 2400` 过滤出已完成的 |
| **Funda** | tRPC superjson `meta.values` 的 `undefined` 标记不能省,否则 zod 校验 `code=BAD_REQUEST` |
| **Gangtise** | **Clash 代理会中断 CN CDN**,`session.trust_env=False` 强制直连才不超时。<br>**2026-04-22 起** `summary` 内部轮询 7 个 classify (`SUMMARY_CLASSIFIES`):帕米尔研究 / A股 / 港股 / 美股 / 专家会议 / 投关活动记录 / 网络资源 — 每条 doc 写入 `classify_id` + `classify_name`。比之前单 source-union 多抓 4-7×。 |
| **Gangtise · chief** | 2026-04 起 `/chief/v2/queryOpinionList` 整条死 (`code=10019999`)。`fetch_chief_list` 改用 `/chief/foreign/queryOpinionList` + `bizParams.foreignType=independent`,这是前端 chief 页真实路径 |
| **Gangtise · summary** | 部分纪要 `summary/download` 会返 **HTTP 403 "中泰证券白名单"**,这是 **per-doc** 限制。`fetch_summary_text` 现已吞掉该 403 返回空串,让上层回退到 brief |
| **AceCamp** | Cookie 必须同时包含 `user_token` (JWT) + `_ace_camp_tech_production_session` (Rails) + `aceid`,缺一就 401。`_id` 用 `a<id>` / `e<id>` 前缀区分 articles / events |
| **SentimenTrader** | 走 Playwright 读 `window.Highcharts.charts[0].series` (无 XHR 可逆向)。EOD 数据 16:00 ET 后才更新,cron 06:00 CST + `--retry-until-fresh` 重试到拿到当日 |

---

## 9.5. 反爬破解 / API 反编译自动化流程

新平台接入,或存量平台"爬不全/内容乱"排查时的**标准流程**。全部工具都在仓库里,不需要额外账号/代理。

**核心工具 3 件套**:

| 工具 | 文件 | 用途 |
|---|---|---|
| **实时浏览器查看器** | `backend/app/services/cdp_screencast_session.py` | 数据源管理页"实时查看" — 用已保存 cookie/localStorage 在 headless Chromium 里打开对应平台页面,前端 JPEG 流实时看 |
| **Network capture** | `ScreencastSession._network_log` | Playwright `request`/`response` 监听,XHR/fetch 全部落 ring buffer (200 条),带 status + response_body + post_data |
| **credential_manager 健康探针** | `backend/app/services/credential_manager.py::_probe_<platform>` | 直接 HTTP 调后端 API (users/me / account / sessions) 判定 "真实登陆 vs 匿名 session" |

**为什么这三个组合起来能解问题**: 一般反爬是 **SPA 在 JS 里构造 body → axios 拦截器加 header → 后端按独立格式校验**。光看爬虫代码只能看到"请求没返回预期数据"这一步;只有把 UI 实际打出的包完整抓下来,才知道 body 差了哪些字段 / 签名差了哪个 header / endpoint 是不是换路径了。

### 9.5.1 Step 1 — 直接 API 探针 (先排除环境问题)

用爬虫自己的 `create_session()` + `api_call()` 跑几个最简单的 request, 看:
- 返回 code (200 OK 还是 401/403/500?)
- `total` 字段 (是否符合平台对外展示的规模?)
- 反复 poll 同一端点,返回的 ID 是否在变 (=实时 feed) 还是不变 (=被缓存)

```bash
cd crawl/<platform>
python3 -c "
import os
for k in ['http_proxy','https_proxy','HTTP_PROXY','HTTPS_PROXY']: os.environ.pop(k,None)
import sys; sys.path.insert(0,'.'); sys.path.insert(0,'..')
from scraper import _load_token_from_file, create_session, api_call
sess = create_session(_load_token_from_file())
r = api_call(sess, 'POST', '/application/glory/research/v2/queryByCondition',
             json_body={'pageNum':1,'pageSize':10})
print('code:', r.get('code'), 'total:', r.get('total'))
print('first:', (r.get('data') or [{}])[0].get('title','')[:60])
"
```

### 9.5.2 Step 2 — 看看 SPA 实际发的 body (Playwright + Network capture)

如果直接 API 能调通但数据量不对,多半是 body 少字段。用 viewer session 让 SPA 自己跑一遍,captured XHR 把真实 body 吐出来:

```python
import asyncio, json, sys
from pathlib import Path
sys.path.insert(0, '/home/ygwang/trading_agent')
from backend.app.services import cdp_screencast_session as s
from backend.app.api.data_sources import _viewer_inject_for

async def main():
    creds = json.loads(open('crawl/gangtise/credentials.json').read())
    sess = await s.create_session(
        platform='gangtise',
        login_url='https://open.gangtise.com/research/',
        credentials_path=Path('crawl/gangtise/credentials.json'),
        extract_fn=None,
        inject_fn=_viewer_inject_for('gangtise', creds),
        mode='viewer',
    )
    await asyncio.sleep(15)   # 等 SPA 自己打完所有 init XHR
    # 精确到 /application/ 的真实 XHR
    for e in sess._network_log:
        if 'application/glory' in e['url']:
            print(f"{e['method']} {e['url'][-80:]}")
            if e.get('post_data'): print('  body:', e['post_data'][:400])
            if e.get('response_preview'): print('  resp:', e['response_preview'][:200])
    await s.drop_session(sess.session_id)

asyncio.run(main())
```

**实战案例** — 2026-04-22 修 Gangtise research "数据量不够":
- 直接 API: `pageNum=1..100` 全部返回同样 10 条. 老 body 被服务端降级到 "top-10 feed" 路径.
- SPA 拦截看 body 发现是 **Elasticsearch 风格** `{from, size, searchType, orgList, ...}`,不是 `{pageNum, pageSize}`. 服务端对两种 body 走完全不同的 handler,老 body 进降级,新 body 才分页.
- 改 `fetch_research_list` 的 body 生成逻辑,pageNum→from 换算,单 tick 拉到 500 条,日抓量从 77→485。

### 9.5.3 Step 3 — 啃 SPA Vue bundle (Network capture 不够时)

如果 SPA 是 Vue + 按需加载 chunk, 上面的 capture 可能缺东西 (某些 chunk 还没加载)。直接下 bundle 反编译更快:

```bash
# 抓主 bundle
curl -s https://<spa>/static/js/app.xxx.js -o /tmp/main.js

# 在 minified JS 里找端点 / payload shape / axios 拦截器
grep -oE '"/(?:api/)?[a-zA-Z0-9/_-]*(?:login|session|signin|queryBy|Condition|List|me|profile)[a-zA-Z0-9/_-]*"' /tmp/main.js | sort -u

# 找请求体字段 (看 SPA 是怎么构造 body 的)
python3 -c "
import re
src = open('/tmp/main.js').read()
# 找 R(e), A(t) 这种调用,窥探 e/t 的组装
for m in re.finditer(r'postUsersLogin|queryByCondition|\b[A-Z]\(\w+\)', src):
    s = max(0, m.start() - 400)
    e = min(len(src), m.end() + 400)
    ctx = src[s:e]
    if 'pageSize' in ctx or 'pageNum' in ctx or 'from:' in ctx:
        print(ctx[:700], '\n---')
        break
"
```

**实战案例** — Gangtise axios 拦截器:
```javascript
I.interceptors.request.use(e => {
  let t = localStorage.getItem("token");
  t && (e.headers["Authorization"] = "Bearer " + t);
  let a = {}, i = localStorage.getItem("activeProduct");
  // ↓ 多了一个爬虫完全没注意过的 header
  if (i && i !== "{}") {
    i = JSON.parse(i);
    a.productCode = i.productCode;
    a.tenantId = i.tenantId || "";
  }
  e.headers["GTerminal"] = JSON.stringify(a);
});
```
找到 `GTerminal` header 后,试着加进 `create_session()` 的默认头,排除"少头所以被限流"的可能性。

### 9.5.4 Step 4 — 判定 "匿名 session" 陷阱 (credential_manager 兜底)

Rails / Next.js / Hono 等框架**给任何首次访客分发 session cookie**,爬虫拿到 cookie 不代表真实登陆。必须用**平台专用的 `users/me` 等效端点**判真实登陆。

- 2026-04-22 发现 AceCamp 被这个坑:`_extract` 只检查 `_ace_camp_tech_production_session` 存在,Rails 给匿名访客也发这个 cookie → 爬虫把**匿名 session 当成功登陆**写回 credentials.json → 付费内容拉到的是 100 字的 title-only preview。修法:`_extract` 增加 `users/me` 探针,`data: null` 即拒绝写入。

**每个平台的 "真实登陆" 验证端点** (credential_manager.py::\_probe\_\*):

| 平台 | 端点 | 成功信号 |
|---|---|---|
| alphapai | `POST /reading/comment/list` | `code: 200000` |
| gangtise | `GET /userCenter/api/account` | `data.uid` 非空 |
| funda | `POST tRPC user.getUserProfile` | `result.data.id` 存在 |
| meritco | `POST /account/me` | 有 `uid` + `email` + role 权限 |
| thirdbridge | AWS Cognito refresh | 200 + 新 access_token |
| jinmen | base64 JWT exp 未到期 + `/user/detail` HTTP 200 | |
| acecamp | `GET /users/me` | `data.id` 非空 (匿名 `data: null`) |
| sentimentrader | Playwright storage_state 存在 + `users.*` 域名跳转成功 | |

### 9.5.5 Step 5 — locale / fingerprint 对齐 (防"机器人"判定)

Playwright 默认 `en-US` + UTC,我们服务器 IP 上海电信 → **地理/语言不一致是 TDC/Akamai 典型指纹信号**。所有 Playwright 上下文都要用 `auto_login_common.context_opts_for(platform)` 取对齐配置:

- CN 站 (alphapai/gangtise/jinmen/meritco/acecamp/thirdbridge) → `zh-CN` + `Asia/Shanghai`
- US 站 (funda/sentimentrader) → `en-US` + `America/New_York`
- `apply_stealth(page, platform=<key>)` 额外覆写 `navigator.languages` (Playwright `locale=` 不管这个)

### 9.5.6 Step 6 — 健康状态同步回监控面 (闭环)

`crawler_monitor.py` 每 60s 调 `credential_manager.status_with_health(key)` 聚合到 dashboard。`status: expired` → 对应平台 dot 变黄,徽章显示"✗ 未真实登陆 · <detail>"。研究员一眼就知道哪个平台需要重登。

### 9.5.7 排障清单

| 症状 | 下一步查什么 |
|---|---|
| 直接 API 能调通,数据量远少于平台显示 | Step 2: SPA body 和我们差多少字段;看 `from/size` vs `pageNum/pageSize` |
| API 返回 200 但 `data: null` / `data: []` | Step 4: 是不是匿名 session;加 `users/me` 探针 |
| API 返回 200 但 code/msg 提示"无权访问" | Step 3: 反编译找缺失 header (GTerminal / X-My-Header / 签名) |
| Playwright 登陆每次都触发 TDC slider | Step 5: locale + UA 一致性;看 `PLATFORM_LOCALE` 配置 |
| SPA 白屏不触发任何 XHR | inject_fn 没写对 localStorage key;去 Step 3 看 SPA 第一行读什么 key 决定 auth |
| 内容被 quota 限流返回试读 (部分字段全但 content 截到 200 字) | Step 9.5.8 "list-vs-detail 配额不对称" — 换 `.../detail?id=X` 端点 / 加 `Range` 头直连 OBS |

### 9.5.8 绕过"每日查看 N 篇上限"配额 (list-vs-detail 不对称)

许多平台给 list 端点加了 "每日独立查看数" 硬墙 (alphapai roadshow 100/天, gangtise
summary 60/天, meritco 白名单 per-broker), 触发后 list 响应返回 `hasPermission:False`
+ `content` 字段被截到 200-500 字 "试读"。常见字段签名:

```json
{"hasPermission": false,
 "noPermissionReason": {"code": 7, "message": "用户访问纪要次数已达上限"}}
```

**关键观察**:同一条目的 list 响应走配额闸, 但是 **detail 端点通常不走** 同一个闸。
逆向 SPA 经常会发现:
- 有一条 **轻量 detail** (`/.../detail?id=X` 只含 metadata) — UI 渲染列表时用
- 有一条 **重量 detail** (`/.../SUBTYPE/detail?id=X` 含完整 content) — UI 点进详情页才调

平台把配额加在 list 上 (防刷爬), 往往**忘了同步加在 detail 上**。所以 detail 端点
通常 bypass 整个 100/天 quota, 返回 `hasPermission:True` + 完整 `aiSummary.content`
/ `usSummary.content` / PDF rel_path。

**操作步骤**:

1. **确认有配额墙**: list 端点扫完, 统计 `hasPermission=False + noPermissionReason.code=7`
   的条目数 — 这些就是配额截断的;content 字段看看是否普遍 150-300 字。
2. **枚举 detail 候选端点**:
   ```
   /.../detail?id=X             # 轻量, 通常没 content
   /.../<subtype>/detail?id=X   # 重量, 带 content (e.g. roadshow/summary/detail)
   /.../detail/full?id=X
   /.../segment/list?summaryId=X
   /storage/s3/download/<bucket>/<path>?access_token=X + Range: bytes=0-
   ```
3. **用 scraper 自己的 session 直接打每条端点**, 拦 `noPermissionReason` 是否还在:
   ```python
   from scraper import create_session, api_call, _load_token_from_file
   sess = create_session(_load_token_from_file())
   sess.trust_env = False; sess.proxies = {'http': None, 'https': None}
   r = api_call(sess, 'GET', f'reading/roadshow/summary/detail?id={rid}')
   # 看 r['data']['hasPermission'] 是否 True; r['data']['aiSummary']['content'] 是否完整
   ```
4. **一旦确认配额 bypass**, 在 scraper 里改两处:
   - `CATEGORIES[key]['detail_path']` 指向重量 detail 端点
   - `dump_item` dedup check 增加: 已入库但 `content_truncated=True` 的条目,
     允许 fall-through 重拉 (日额度重置 / 或通过 detail 补全时触发)
5. **回填存量**: 把所有 `hasPermission=False + code=7` 的条目标记 `content_truncated=True`,
   下一次 walker 扫到时会通过 detail 端点重拉:
   ```python
   col.update_many({'hasPermission': False, 'detail.noPermissionReason.code': 7},
                   {'$set': {'content_truncated': True}})
   ```
6. **写入时自动打标**: `if hasPermission=False and noPermissionReason.code=7:
   doc["content_truncated"] = True` — 之后每次 list 拿到新的试读, 同一时刻
   detail 端点已经把它"救"下来;watch 循环每 60s 一次, 基本实时补齐。

**已验证案例 A** (AlphaPai roadshow, 2026-04-22):
- list 端点 (`reading/roadshow/summary/list`): 100/天 quota 触发后返回
  220 字 `content` + `hasPermission:False`
- detail 端点 (`reading/roadshow/summary/detail?id=X`): **不计 quota**, 返回
  `aiSummary.content` 3-8k 字 + `usSummary.content` 20k+ 词 (美股 earnings call
  原稿), `hasPermission:True`
- 存量 59 条 quota-blocked 条目里, 26 条能通过 detail 补回 (5 万-9 万字/条),
  33 条 detail 也无内容 (本就没 AI 纪要 — 调研未到场 / 未转写)。

**已验证案例 B** (Jinmen 外资研报 preview-vs-detail, 2026-04-23):

同一 pattern 的另一种变体 — 付费墙 (field-level ACL) 下,**preview 端点比 detail 端点少一层 ACL**:

- 旧端点 `json_research_detail` + `{id: rid}`:原本打算复用国内研报端点。但 oversea
  rid 与国内 rid **数字碰撞** (e.g. oversea rid=1669095 → 国内 "信达生物 2019 年"),
  返回的是完全错误的记录,`originalUrl` / `content` 要么是错的要么为空。
- `json_oversea-research_detail` + `{researchId: rid}`:返回 `code:500 msg:"外资研报未解锁"`
  对所有 rid 都 500 (包括有 PDF 的),这条路被付费墙挡死。
- **`json_oversea-research_preview` + `{researchId: rid}`:绕开付费墙**。对 1635 条
  oversea 条目全部返回 `code=0 + homeOssPdfUrl`,URL 指向 `database.comein.cn/
  original-data/pdf/mndj_report/<md5>.pdf`,直接流式下载,`%PDF-1.7` magic 正常。
- Pattern 总结:**detail 端点走授权粒度 ACL;preview / list-entry / 缩略图等
  端点往往只查"是否登录"**。逆向时除了 `detail`, 也要枚举
  `_preview` / `_summary-only` / `_info` / `_query-info` 等可能 bypass ACL 的端点。

根因:服务端把 pay-wall 加在 `_detail` 端点上, 但 preview / 翻译 / 封面预览等轻量端
点为前端渲染列表卡片时复用,**权限粒度没对齐**,同样的 rid 在 preview 里照样给 PDF URL。

**回填操作** (Jinmen 外资研报):
```bash
# 新 preview 端点修补在 scraper.py: fetch_oversea_report_detail 已切到
# json_oversea-research_preview + researchId 键; dump_oversea_report 先读
# detail.homeOssPdfUrl, 回落 detail.originalUrl.
cd crawl/jinmen && python3 backfill_oversea_pdfs.py --sleep 0.6
# 1635 条 rid 逐个重拉 preview + 重下 PDF, 耗时约 20 分钟.
```

**已入库但 PDF 下错的情况** (必须清理):旧 `json_research_detail` 给的 `originalUrl`
指向国内 rid 碰撞的完全错误 PDF (~434 条)。Backfill 前必须:
1. 删除 `/home/ygwang/crawl_data/jinmen_pdfs/mndj_rtime_*.pdf` (oversea 专属前缀)
2. `oversea_reports.update_many({}, {$set: {pdf_local_path:'', pdf_size_bytes:0}})`
3. 然后跑 backfill_oversea_pdfs.py,`force=True` 重爬。

**不能一劳永逸的情况**:
- 部分条目是"仅音频未转写"或"活动预约未开场", detail 也没 content — 服务端根本
  就没生成文本。这类只能等平台后处理。
- 少数平台 (e.g. 早期 meritco 某些 broker) 把 quota 同时加在 list + detail — 那
  只剩 `/storage/s3/download/<bucket>/<path>` 直连 OBS 存储层这一条路 (需要先通过
  SPA bundle 反编译拿到 `PRIVATEOBSMAPPINGID` / `bucket` 编号, 见 §9.5.3)。

---

---

## 10. 故障速查 (5 秒定位)

| 症状 | 根因 | 处置 |
|---|---|---|
| 全量请求 401/403 | token 过期 / 会话被撤 | 浏览器重登,复制新 auth |
| `code=500 "参数错误"` | 请求签名缺 / body 字段缺 | 检查 `X-My-Header` / `meta.values` |
| 响应乱码 | AES 加密 | 看响应头 `k` / `sig`,解密 |
| PDF 下载不是 PDF | WAF 拦截或 URL 过期 | 重新登录,用新 token 重抓 |
| `content_md` 为空 | 平台本身无文本 (仅 PDF) | 正常 — 期权早报等日报最常见 |
| 同条目 `content` 只有 200-500 字 + `hasPermission=False` | 平台日查看上限触发 (list quota) | §9.5.8 — 换 detail 端点 (`/.../SUBTYPE/detail?id=X` 通常不计 quota);回填 `content_truncated=True` 让 watcher 自动补 |
| 爬到一半 ReadTimeout | 代理问题 / WAF challenge | `trust_env=False` / 降速 |
| `_canonical_tickers` 为空 | 文档还没富化 | 跑 `enrich_tickers.py --incremental` |
| `--resume` 重复抓 | `top_id` 没更新 | 检查 `save_state(top_id=...)` 是否落库 |
| `/data-sources` 显示"已停止" 但实际在跑 | Redis PID 过期未自愈 | 已修 (`crawler_manager.status` 走 `/proc/*/cwd` 自愈),刷新即可 |
| `/data-sources` 卡片时间晚 8 小时 | 后端返 naive ISO 字符串 | 已修 (Motor `tz_aware=True` + 兜底 `Z` 后缀) |
| 爬虫"一阵一阵的"入库 | 实时档 burst-size 没关 | `--burst-size 0 --daily-cap 0` |

---

## 11. 依赖 / 环境

```bash
# 爬虫侧 (Python 3.9+)
pip install httpx requests pymongo motor tqdm pycryptodome brotli playwright

# Playwright (auto_login + sentimentrader 需要)
playwright install chromium

# MongoDB (Docker, 容器名 crawl_data)
docker run -d --name crawl_data -p 27017:27017 \
  -v crawl_data:/data/db docker.1ms.run/library/mongo:7

# Web UI (可选, mongo-express)
docker run -d --name crawl_data_ui --network host \
  -e ME_CONFIG_MONGODB_URL=mongodb://localhost:27017 \
  -e ME_CONFIG_BASICAUTH_USERNAME=admin \
  -e ME_CONFIG_BASICAUTH_PASSWORD=admin \
  docker.1ms.run/library/mongo-express
# → http://127.0.0.1:8081
```

**Backend**: `./start_web.sh restart` 重启 FastAPI,自动加载所有 `*_db.py` 路由。
**Frontend**: `cd frontend && npm run build` 后刷新浏览器即可。

---

## 12. 安全 (一定要看)

- **凭证不进 git**: `credentials.json` / `.env` / 硬编码 token 的 scraper 副本 — 全部在 `.gitignore`
- **一个账号一个 scraper 进程**: 多开会触发风控批量清会话 (2026-04-17 全家桶掉过一次)
- **MongoDB 默认无鉴权**: 本地 OK,公网部署必须防火墙 / SSH 隧道
- **PDF 体量**: 一年 10w+ 文件常见,按 `pdfs/YYYY-MM/` 分月归档,提前规划磁盘

---

## 13. 文件地图 (哪里改什么)

| 想做什么 | 去改这里 |
|---|---|
| 加一个新平台爬虫 | 复制最像的一个子目录,按 [README.md §6](README.md#6-添加一个新平台--playbook) playbook 做 8 件事 |
| 调整某平台的 CLI 参数 | `crawl/<platform>/scraper.py` 里 `parse_args()` |
| 加节流 / 退避规则 | `crawl/antibot.py` (所有爬虫共享) |
| 改 Playwright 自动登录 | `crawl/<platform>/auto_login.py` (共享 `auto_login_common.py`) |
| 改后端 API 响应 | `backend/app/api/<platform>_db.py` |
| 改前端页面 | `frontend/src/pages/<Platform>*.tsx` |
| 改侧边栏命名 | `frontend/src/components/AppLayout.tsx` + `frontend/src/i18n/zh.json` 的 `nav.*` |
| 扩充 ticker 别名 | `backend/app/services/ticker_data/aliases.json` 然后 `--reload-aliases` |
| 跨平台按股票训练取样 | `_canonical_tickers` 索引查询 (见第 7 节) |
| 查平台会话状态 | `python3 crawl/<platform>/scraper.py --show-state` |
| 查跨爬虫汇总 | `python3 crawl/crawler_monitor.py` 或 `/data-sources` |
| 改实时档节流参数 | `crawler_monitor.py::RESTART_CONFIG` (七爬虫共享 default) |

---

## 14. 数据目录分离 (2026-04-17)

大体积 PDF 归档从项目树移出,单独落在 `/home/ygwang/crawl_data/`,避免 git / 备份
/ 卷积占用:

```
/home/ygwang/crawl_data/
├── pdf_full/               706 GB  历史全量 jinmen 研报归档 (用户手工管理)
├── alphapai_pdfs/          ~440 MB  AlphaPai 研报 PDF (2026-04 起增量)
├── gangtise_pdfs/          ~16 MB   港推研报 PDF
├── jinmen_pdfs/            ~9 MB    进门增量 PDF
├── meritco_pdfs/           ~263 MB  久谦 PDF 附件
└── sentimentrader_images/  ~5 MB    SentimenTrader 图表 PNG
```

**后向兼容**: `crawl/<platform>/pdfs` → `/home/ygwang/crawl_data/<platform>_pdfs`
的**软链接**保留,老代码/老脚本不受影响。

**涉及改动**:
- `backend/app/config.py` 的 `alphapai_pdf_dir` / `jinmen_pdf_dir` / `gangtise_pdf_dir` 默认值
- 各 scraper 的 `PDF_DIR_DEFAULT` 常量
- Backend PDF 流式端点验证过 (`%PDF` magic 命中 200)

**改回旧位置** (磁盘紧时回退): 设置环境变量,例 `export ALPHAPAI_PDF_DIR=/data/pdfs`,
或改 `config.py`。

---

## 15. 最近改动

- **2026-04-24 · 反爬全栈大修 (antibot.py 升级 + crawler_monitor 启动改造)** — 现状诊断: 旧 antibot 只覆盖了"节流 + 401 退出", 缺指纹多样化 / 跨进程账号闸 / 软警告全局冷却 / 工时形态 / 启动错峰这些关键防御层. 18 个 watcher 同 IP 同 UA 同账号同分钟撞 tick + uniform 抖动 = 教科书级 bot signature. 新增内容: (a) `AdaptiveThrottle` 抖动从 uniform → **Gaussian**, 加 5% 概率 5-30s long-tail "阅读停留"; (b) `time_of_day_multiplier` — 23:00-07:00 ×2.5 / 周末 ×1.8 / 午休 ×1.3, 工时形态拉低 24/7 平摊机器特征; (c) **`AccountBudget`** Redis-backed 跨进程 24h 滚动窗账号闸 (各平台默认 300~3000), 防止 4 个 alphapai watcher 单账号 ×4 ×500=2000/天; (d) **`SoftCooldown`** Redis-backed 同平台全局静默 — 任一 watcher 触发 `hasPermission:False/code=7` / 软 429 / WAF cookie / 限流关键词 / REFRESH_LIMIT → 该平台所有 watcher 静默 30~60min, 不等到 401/403 才退; (e) **`pick_user_agent`** 5-8 个 Chrome 122-126 UA 池 + `headers_for_platform` 按 `CRAWLER_PROCESS_LABEL` env 稳定 hash 映射 — 18 个 watcher 自动分到不同 UA + 同 watcher 重启不变; (f) `headers_for_platform` 一并配齐 `sec-ch-ua` / `Sec-Fetch-*` / `Accept-Language` modern Chrome 必带头. crawler_monitor 改动: (g) `start_all` spawn 时给每个 watcher 注入 0~min(interval, 60)s 随机偏移, tick 散开不再每分钟 :00 撞死; (h) 透传 `CRAWLER_PROCESS_LABEL` 环境变量给 scraper, antibot 据此选 UA; (i) **新增 `dawn` 模式** (interval=300s, base=2.5s, burst=60, daily_cap=400) 配合 cron 02:00-06:00 凌晨低峰回填; (j) realtime 档 `--burst-size` 从 0 → 80 + `--daily-cap` 从 0 → 600, 实时档恢复防跑飞保险, 异常有人接. 实现文件: `crawl/antibot.py` 重写 + `crawl/crawler_monitor.py::_mode_args/start_all` 改造 + 8 个 scraper.py (alphapai/jinmen/meritco/funda/gangtise/AceCamp/alphaengine/third_bridge) 全部接入新 API. 每个 scraper 启动会打印一行 `[antibot] platform=X label=Y base=...` stamp, grep 即可确认配置. 前端 `/data-sources` 监控页加了「🌙 凌晨低峰档」按钮.
- **2026-04-23 · daily_catchup.sh + 每日 05:30 cron — 系统性防漏** (新增 `crawl/daily_catchup.sh`):实时档 watcher (`--watch --resume --since-hours 24 --interval 60`) 在日常发布高峰 (早盘/晚盘/美股 earnings call) 会漏条目 —— 当一批新条目瞬间涌上 list 页, 未扫的旧条目被挤到 page 2, watcher 到 `top_dedup_id` 就早停, 漏掉的条目一过 24h 窗口就再也捡不回来。`--today --date 2026-04-22` 全平台扫一遍得到昨日漏损:**jinmen 研报 190/366 (52%)** · **jinmen 纪要 68/261 (26%)** · **alphapai roadshow 58/234 (26%)** · **alphapai report 28/226 · comment 26/596** · gangtise summary 7/313 · acecamp 1/75 · meritco/thirdbridge/funda/gangtise_chief 0%。修复两步:(1) 当场用 `--since-hours 36 --force --max N` 跑一轮补回所有漏条;(2) `crawl/daily_catchup.sh` 把 18 条 ALL_SCRAPERS 全部按 `--since-hours 36 --force` 重扫一次, `30 5 * * *` cron 每早 05:30 CST 跑 (早盘研报发布窗口之后), 写日志到 `logs/daily_catchup/`。跟实时 watcher 互不干扰, Mongo 层 `upsert` 天然幂等。alphaengine 单独跳过 —— 账号 token 挂了 (`biz code=401 用户状态发生变更`),需要先通过 `/data-sources` 走一遍 auto_login 重登。
- **2026-04-23 · Jinmen 外资研报 preview 端点绕开付费墙** (§9.5.8 案例 B):账号无 "外资研报解锁" 权限时,原本 scraper 用 `json_research_detail` + `{id: rid}` 拉 detail;但 oversea rid 与国内 rid 数字碰撞,返回的是错误记录 (e.g. rid=1669095 返回 "信达生物 2019 年报告"),1635 条 oversea_reports 里 1201 条 `originalUrl` 空、434 条 `originalUrl` 指向错 PDF。Playwright + API 枚举 7 个候选端点后发现 `json_oversea-research_preview` + `{researchId: rid}` **对所有 rid 返回 `homeOssPdfUrl`** (`database.comein.cn/original-data/pdf/mndj_report/<md5>.pdf`),绕过 "外资研报未解锁" 付费墙。改动:(a) `scraper.py` 的 `OVERSEA_REPORT_DETAIL_API` 切到 `json_oversea-research_preview`;(b) `fetch_oversea_report_detail` 用 `researchId` 参数名;(c) `dump_oversea_report` 先读 `detail.homeOssPdfUrl`,回落 `detail.originalUrl`;(d) 删除 434 条错 PDF (`find .../jinmen_pdfs/ -name 'mndj_rtime_*.pdf' -delete`) + 清零 `pdf_local_path` 字段 + 跑 `backfill_oversea_pdfs.py` 重爬全部 1635 条。
- **2026-04-22 · AlphaPai 100/天 quota 彻底绕过** (§9.5.8):AlphaPai roadshow list 端点 `reading/roadshow/summary/list` 超过 100 条/天后返回 `hasPermission:False + code=7` + 220 字试读。Playwright 拦包发现 **detail 端点 `reading/roadshow/summary/detail?id=X` 不计同一 quota**, 返回完整 `aiSummary.content` (3-8k 字) + `usSummary.content` (美股 earnings call 原稿 20k+ 词)。改动:(a) `CATEGORIES['roadshow']['detail_path']` 从 `reading/summary/detail` → `reading/roadshow/summary/detail`;(b) `_extract_roadshow_content` 增加 usSummary 拼接;(c) `dump_item` dedup 允许 `content_truncated=True` 条目 re-fetch;(d) 回写 59 条存量 blocked 记录, 跑一遍 detail-bypass 拿回 26 条 (5 万-9 万字/条), 剩 33 条 detail 也无内容 (本就无 AI 纪要)。这套 "list-vs-detail 配额不对称" 模式作为通用方法论写进 §9.5.8。
- **2026-04-22 · 反爬破解自动化流程**:新增 §9.5, 把近期 5 次排障 (AceCamp 匿名 session / Gangtise research from/size / Gangtise chief attachment / locale 对齐 / GTerminal header) 抽成可复用的 6 步流程。新平台接入或"爬不全"问题,按流程排一遍基本能定位。
- **2026-04-22 · Gangtise research 真实分页**:`fetch_research_list` 从 `pageNum/pageSize` 改成 ES 风格 `from/size`,单 tick 从 top-10 feed 变成真实翻页,日抓量 77 → 485+。通过 Playwright viewer + `ScreencastSession._network_log` 拦截 SPA 实际 XHR 反推参数。
- **2026-04-22 · Gangtise chief 图片/PDF 附件识别**:`dump_chief` 增加 `is_attachment` 判定 (ext 或 title 匹配 hash.jpg 模式),避免 OCR 股票页噪声污染 `content_md`;title 改为 `[图片/PDF] <发布者> · <时间>`。migrate 74 条存量。
- **2026-04-22 · AceCamp 匿名 session 修复**:`_extract` 增加 `/users/me` 真实登陆校验 (之前只要有 `_ace_camp_tech_production_session` 就算成功,Rails 给任何访客都发这个 cookie → 爬到的付费内容只有 title-only preview)。`credential_manager._probe_acecamp` 同步改走 `users/me`,监控面板 `expired` 徽章能正确标出"未真实登陆"。
- **2026-04-22 · Playwright context locale 全平台对齐**:新增 `auto_login_common.PLATFORM_LOCALE` 映射,CN 站 `zh-CN/Asia/Shanghai`,US 站 `en-US/America/New_York`。`apply_stealth(page, platform=key)` 额外覆写 `navigator.languages` (Playwright `locale=` 不管这个)。修复 viewer / login / sentimentrader 3 处浏览器上下文 en-US/UTC 默认 — IP↔locale 错配是典型 TDC 指纹信号。
- **2026-04-22 · 监控平台真实登陆徽章**:`crawler_monitor.py` 直接 import `credential_manager.status_with_health`,60s 缓存,每平台渲染绿/红/灰徽章 (✓ 已登陆 / ✗ 未真实登陆 / ? 未知)。数据源管理页 + 监控面板共享同一个 health 源。
- **2026-04-22 · Gangtise 7 分类**:`gangtise/scraper.py` 的 `fetch_summary_list` 接受
  `classify_param`,主循环按 `SUMMARY_CLASSIFIES` 轮询 7 个分类 (帕米尔 / A股 / 港股 /
  美股 / 专家 / 投关 / 网络资源 — 来自服务端 `summary/getClassifyList` 缓存)。每条 doc
  落 `classify_id` + `classify_name`。比之前单 source-union 多抓 4-7×。
- **2026-04-22 · AlphaPai 未来日期保护**:`scraper.py` 在写 doc 前 `release_time_ms` 与
  当前时间比对,大于 `now` 一律钳到 `now`。修复了 26/27 榨季 招商期货报告显示 2026-04-22
  污染列表的 bug。
- **2026-04-21 · Jinmen 全量补齐**:启动 `--burst-size 0 --daily-cap 0` 双进程,
  reports 1 440 → 10 049 (+8 609 / 0 失败),meetings 持续补齐到 10 058。根因:旧
  daily_cap=500 默认值让 watcher 每天硬封 500 条,长期累积约 2 周空窗。
- **2026-04-21 · 时区 + UI bug 三联修**:
  (a) `credential_manager._probe_data_freshness` Motor 加 `tz_aware=True`,前端不再
      把 UTC ISO 误读为本地时间 (差 8h);
  (b) `data_sources.crawler_status` 扫 `/proc/*/cwd` 收养未注册 scraper PID,Redis
      自愈,「实际在跑显示已停止」修掉;
  (c) `/data-sources` 页面顶部新增「每日入库」ECharts 堆叠柱图,后端
      `ingestion_daily_series` 用 MongoDB `$dateToString { timezone: "Asia/Shanghai" }`
      按 CST 午夜桶聚合。
- **2026-04-21 · 实时档 antibot 关 cap**:`crawler_monitor::_mode_args('realtime')` 默认
  追加 `--burst-size 0 --daily-cap 0`,实时档不再"一阵一阵"。`antibot.py` 的
  `AdaptiveThrottle` 同步支持 `burst_size <= 0` 跳过 burst 冷却。
- **2026-04-21 · Meritco UI**:重写 `MarkdownContent` (移除灰底 + 加 typography CSS),
  drawer 加 PDF 附件卡 (`pdf_files: [{name, size_bytes}]`)。
- **2026-04-21 · Meritco PDF 下载**:逆向出 `POST /matrix-search/forum/pdfDownloadWatermark`
  body `{"pdfOSSUrlEncoded": <enc>}` 端点 (从前端 `/forumPDF` 路由 + be3b axios wrapper
  反编译)。后端 API 无 5 天时间窗口限制,前端 UI 的预览/下载按钮是显示层限制。
  scraper.py 新增 `--pdf-dir` / `--skip-pdf` / `--force-pdf` / `--pdf-only`。
  存量 115 条有 PDF 的 forum 文档全部下载成功 (263 MB)。
- **2026-04-21 · Meritco research 导入**:`scripts/import_meritco_research.py` 把本地
  `details/*.json` + `lists/<category>/page_*.json` + `pdfs/*.pdf` 共 68 条导入
  `meritco.research` collection,与 forum 同 schema (50 条带 detail / 18 条 list-only / 6 条带 PDF)。
- **2026-04-20 · Per-category 并行重构**:`ALL_SCRAPERS` 从每平台 1 条展开成
  每分类 1 条,共 **15 个并发 watcher** (thirdbridge 除外)。解决之前单进程
  CATEGORY_ORDER 串行导致 alphapai roadshow 被 report+wechat 阻塞。`start_all` 也重构为
  先 bulk-kill 所有目标 cwd 下的 scraper,再批量 spawn,避免多条同 cwd 进程启动时误杀。
- **2026-04-20 · Gangtise chief 端点修复**:`/chief/v2/queryOpinionList` 整条死,
  从 chief 页 Vue bundle 反编译找出 `/chief/foreign/queryOpinionList` + body 加
  `bizParams.foreignType=independent`。backfill 一次性把 chief_opinions 从 150 → 711。
- **2026-04-20 · 实时抓取速率提升**:`--interval` 默认从 300s → **60s**;
  `--throttle-base` 从 3s → **1.5s**;新增 `acecamp` 到 `RESTART_CONFIG` + `ALL_SCRAPERS`。
- **2026-04-20 · monitor `classify_health` 放宽**:"无轮次" 阈值从 `3*interval` (15min)
  放宽到 `24*interval` (24min @ interval=60s),避免 meritco 这种低频平台被误标 warn。
- **2026-04-20 · AceCamp 接入**:第 7 个平台爬虫,DB `acecamp` 含 `articles` (观点/纪要) +
  `events` (调研/专家会);backend `acecamp_db.py` 挂在 `/api/acecamp-db`;前端侧边栏 AceCamp 专区。
- **2026-04-17 · 数据分离**:3 个 PDF 目录迁移到 `/home/ygwang/crawl_data/`,配置 + scraper + backend 全部更新。
- **2026-04-17 · 跨平台 ticker**:`_canonical_tickers` + `enrich_tickers.py`,6 平台 3600+ 文档全部带规范化标签。
- **2026-04-17 · 一键启动**:`crawler_monitor.py` 加 `start_all(mode)` + Web UI 按钮 (🚀 实时 / 📚 历史补齐),7 个 watcher 并发。
- **2026-04-17 · third_bridge**:补全 `/api/interview/filters` 和 `hasCommentary` 专家点评抓取。
- **2026-04-17 · alphapai reports**:加 `core_viewpoint` 启发式提取 (前端红卡高亮)。
- **2026-04-17 · 前端**:Funda 专区英文标签改中文,今日新增卡去冗余。
