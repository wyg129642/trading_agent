# crawl/ — 财经数据爬虫集合

此目录下的每个子目录是一个独立的平台爬虫。它们共享同一套架构 + CLI + **反爬机制** (§7),
**新平台按第 6 节 playbook 套模板,一两小时内上线**。

---

## 1. 已接入的平台

> **8 个平台 / 17 条并行 watcher**。跨平台综合视图见
> [CRAWLERS.md](CRAWLERS.md);本 README 聚焦共用架构 + 新爬虫 playbook + antibot。

| 目录 | 平台 | 核心内容 | 存储 | 鉴权 |
|---|---|---|---|---|
| [`alphapai_crawl/`](alphapai_crawl/README.md) | alphapai-web.rabyte.cn (Alpha派) | roadshow / comment / report (含 PDF) / wechat | Mongo `alphapai` + PDF | localStorage JWT |
| [`jinmen/`](jinmen/README.md) | brm.comein.cn (进门财经) | AI 纪要 + 研报 (含 PDF) | Mongo `jinmen` + PDF | localStorage base64 JSON + AES 响应解密 |
| [`meritco_crawl/`](meritco_crawl/README.md) | research.meritco-group.com (久谦中台) | 论坛 (纪要/专家/业绩/路演/研报/活动) + 久谦研究 | Mongo `meritco` + PDF | Network 32-hex token + RSA X-My-Header |
| [`third_bridge/`](third_bridge/README.md) | forum.thirdbridge.com (Third Bridge) | 专家访谈 (议程/逐字稿/专家/关联公司) | Mongo `thirdbridge` | Cookie 串 (AWS Cognito + WAF) |
| [`funda/`](funda/README.md) | funda.ai | 研报 / 8-K / 业绩会 / 情绪因子 | Mongo `funda` | Cookie `session-token` + tRPC superjson |
| [`gangtise/`](gangtise/README.md) | open.gangtise.com (岗底斯) | 纪要 (7 分类) / 研报 / 首席观点 | Mongo `gangtise` + PDF | localStorage `G_token` (UUID) |
| [`AceCamp/`](AceCamp/README.md) | api.acecamptech.com | 观点 / 纪要 / 调研 | Mongo `acecamp` | Cookie 三件套 |
| [`sentimentrader/`](sentimentrader/README.md) | users.sentimentrader.com | Smart/Dumb · Fear/Greed · QQQ Optix | Mongo `sentimentrader` + PNG | email + 密码 (Playwright) |
| `tools/` | `dedup_urls.py` (调研抓包 URL 去重) | — | — | — |
| [`antibot.py`](antibot.py) | **共享反爬 / 节流模块** (`AdaptiveThrottle`, `DailyCap`, `SessionDead`) — 所有 scraper 导入 | — | — | — |
| [`auto_login_common.py`](auto_login_common.py) | **Playwright 通用登录骨架** (凭证管理 UI 用) | — | — | — |
| `crawler_monitor.py` | 总控监控 (Mongo 汇总 / 进程 / 日志,CLI live / Web :8080 / 飞书推送) | — | — | — |

---

## 2. 共用架构

```
┌─────────────────┐    ①鉴权 token (浏览器手动拷贝)
│  平台 (Web UI)  │
└────────┬────────┘
         │ HTTPS (可能 RSA 签名 / AES 响应加密)
         ▼
┌──────────────────────────┐       ┌───────────────┐
│  scraper.py (Python)     │──────▶│   本地 PDF     │  可选: pdfs/YYYY-MM/<id>.pdf
│    - requests / httpx    │       └───────────────┘
│    - antibot.AdaptiveThrottle 节流 (§7)
│    - antibot.DailyCap 单轮上限
│    - 分页列表 + 详情
│    - 增量 checkpoint     │       ┌───────────────┐
│    - tqdm 进度条         │──────▶│   MongoDB      │  <platform>.{items, account, _state}
│    - --watch 轮询        │       └───────────────┘
└──────────────────────────┘               ▲
                                            │
                                   ┌─────────────────────┐
                                   │  crawler_monitor.py  │
                                   └─────────────────────┘
```

**关键设计决定**:

- **一个平台 = 一个 MongoDB 数据库**, 数据库名 = 目录名去掉 `_crawl`
- **脚本单文件**: 每个爬虫就一个 `scraper.py`, 直接 `python3 scraper.py` 跑
- **共享反爬**: 所有爬虫 `from antibot import ...` (通过 `sys.path.insert` 到 `crawl/`)
- **一份 `README.md`** 每爬虫; 调试期笔记可放 `promts.md`
- **`--watch --interval N` 就是调度器本身**, 不用 Celery / 不起 HTTP 服务

---

## 3. MongoDB 数据模型约定

### 3.1 内容集合 (`items` / `meetings` / `reports` / `forum` / `interviews` 等)

```js
{
  _id: <platform_primary_key>,            // 平台返回的稳定 ID; 如果会话级变就 SHA1(title|time)
  id: <same>,
  title,
  release_time: "YYYY-MM-DD HH:MM",       // 人类可读
  release_time_ms: <long>,                // 毫秒戳, 用于增量 / --today / --since-hours
  organization / source / author,
  content_md | summary_md | transcript_md | ...,   // 清洗后 markdown
  list_item: {...}, detail_result: {...},  // 原始两份
  stats: {字数, ...},
  crawled_at: ISODate,
  // 可选 PDF
  pdf_local_path, pdf_size_bytes, pdf_download_error,
}
```

索引: `title`, `release_time`, `organization`, `crawled_at`。**详情版 summary 覆盖列表版**
(列表通常 100-200 字预览, 详情才是完整正文)。

### 3.2 `_state` 集合

**checkpoint**:

```js
{ _id: "crawler_<type>", top_id, last_processed_id, last_processed_at,
  in_progress, last_run_end_at,
  last_run_stats: {added, skipped, failed, ...} }
```

**日统计** (`--today` 的结果):

```js
{ _id: "daily_<type>_YYYY-MM-DD", date, total_on_platform, in_db, not_in_db,
  by_organization_top10, ..., scanned_at }
```

### 3.3 `account` 集合

一次性用户 / 平台元数据 (会员卡 / 分类字典 / 筛选项等), 每条 `{_id, endpoint, response, updated_at}`。

---

## 4. 统一 CLI 约定

**所有 scraper 必须支持** (新爬虫照抄):

| 参数 | 语义 |
|---|---|
| `--max N` | 最多抓 N 条, 默认翻到尽头 |
| `--page-size N` | 每页大小 (默认 40) |
| `--force` | 强制重抓已入库 + 刷新 `account` |
| `--resume` | 增量模式: 翻页遇到 `top_id` 即停 |
| `--watch --interval N` | 实时模式: 每 N 秒轮询, Ctrl+C 退出 |
| `--today [--date YYYY-MM-DD]` | 平台当日产出 vs 本地库对比, 存 `_state` |
| `--show-state` | 打印 checkpoint + token 健康检查 |
| `--reset-state` | 清空 checkpoint + 日统计 |
| `--auth TOKEN` | 覆盖硬编码 / env 里的 token |
| `--mongo-uri` / `--mongo-db` | Mongo 连接 (默认 localhost + 平台名) |
| `--since-hours N` | 仅抓过去 N 小时内的内容 (基于 release_time_ms) |

**反爬节流** (由 `antibot.add_antibot_args()` 注入, §7):

| 参数 | 语义 |
|---|---|
| `--throttle-base N` | 基础请求间隔秒数 |
| `--throttle-jitter N` | 间隔抖动 ± 秒 |
| `--burst-size N` | 每 N 条请求后冷却一次 |
| `--burst-cooldown-min/max N` | 突发冷却区间 (30-60s) |
| `--daily-cap N` | 单轮最多抓 N 条, 防风控 |

**额外** (如 PDF):

| 参数 | 语义 |
|---|---|
| `--pdf-dir PATH` / `--skip-pdf` | PDF 下载目录 / 跳过下载 |
| `--clean-<type>` | 清空某内容集合 + 对应 checkpoint |

---

## 5. 鉴权手册

三种常见 token 场景:

### 5.1 场景 A: localStorage 里的 token
**代表**: alphapai (JWT), jinmen (base64 JSON)

浏览器登录 → F12 → Application → Local Storage → 复制 key Value → 粘进 scraper.py / env / `--auth`。

### 5.2 场景 B: Network 面板的请求头 token
**代表**: meritco (32-hex token)

浏览器登录 → F12 → Network → 任一 XHR → Request Headers → 复制 `token` → 写 `credentials.json`。

### 5.3 场景 C: Cookie 串 (AWS Cognito / Akamai 等)
**代表**: third_bridge

浏览器 Console 里敲 `document.cookie` → 整行复制 → 写 `credentials.json` 的 `cookie` 字段。

### 5.4 请求签名 / 响应加密

| 症状 | 机制 | 处理 |
|---|---|---|
| 所有请求 `code=500 "参数错误"` | 请求签名 (RSA/HMAC over 字段) | 前端 JS 找 `setPublicKey`/interceptor (meritco 就是这个) |
| 响应 base64 乱码 + header 有 key | AES-CBC 响应加密 | 前端 JS 找 salt (jinmen 就是这个) |
| 返回 HTML / Captcha | WAF (Akamai / AWS WAF / Cloudflare) | 见 §7 |

### 5.5 Token 有效 ≠ 有权限

- HTTP 200 + user-info 返回正常 **不等于**有数据权限
- alphapai + meritco 最近踩过: `researchRole=0, userMenus=[]`, 业务接口全 401
- `--show-state` 必须调一个**真实业务接口**探测, 不能只看 user-info
- 别信 `expireDate`, 真到期时间在 `userMenus.roles[].roleExpireTimeStr`

---

## 6. 添加一个新平台 — Playbook

### 6.1 抓包 (30 分钟)

1. 浏览器登录, F12 → Network
2. 列出所有 XHR URL, 用 `tools/dedup_urls.py` 去重筛出有用端点
3. 在新建的 `<platform>/README.md` 头部标: 列表接口 / 详情接口 / PDF / 账户接口 / 鉴权方式 / 是否加密签名 (参考已接入 7 个平台的 README 结构)

### 6.2 搭骨架 (10 分钟)

从最相近的一家 copy:

| 新平台特点 | 参考 |
|---|---|
| 稳定 ID + 简单鉴权 + 有 PDF | `alphapai_crawl/scraper.py` |
| 多 type, 严格 body 字段, 请求签名 | `meritco_crawl/scraper.py` |
| AES 响应加密, 多内容模式 | `jinmen/scraper.py` |
| Cookie 串 + AWS WAF + 异步 transcript | `third_bridge/scraper.py` |

**必做 8 件事**:

1. `parse_auth()` / `load_creds()` 解析 token
2. `create_session()` 带完整 header/cookie
3. `fetch_list(...)` / `fetch_detail(...)` 规范化分页响应
4. `dump_one(...)` 拼 doc, `replace_one({_id}, upsert=True)`
5. `fetch_items_paginated()` 翻页 + stop conditions
6. `count_today()` 实现 `--today`
7. `run_once()` 主循环 + tqdm + per-item checkpoint
8. `parse_args()` + `main()` 统一 CLI (包含 antibot args, §7)

### 6.3 **接入反爬** (必做, §7)

在 scraper.py 顶部 import, main() 初始化 `_THROTTLE`, 主循环里 `sleep_before_next()` + `cap`。

### 6.4 写 README.md

照 meritco/third_bridge 的目录: 工作流 → 接口 → 故障排查 → 数据模型 → CLI → 已知坑 → 依赖 → 目录结构 → 安全。

### 6.5 上线前 checklist

```bash
python3 scraper.py --show-state           # token 健康
python3 scraper.py --max 3                # 小批试跑
python3 scraper.py --resume --max 10      # resume 停止
python3 scraper.py --today                # 日统计
python3 scraper.py --watch --resume --interval 600   # 5 分钟无崩
```

### 6.6 接入 `crawler_monitor.py`

把新爬虫的 DB 名 / collection / 日志路径 / 进程 regex 填进去, 看跨爬虫仪表盘。

---

## 7. **反爬 / 反封号 (antibot.py)**

**问题**: 爬虫跑到几十~几百条时会被平台"**会话级撤销**" — HTTP 200 变 401,
要求用户重登浏览器复制新 cookie. 今天 (2026-04-17) alphapai / meritco / third_bridge 三家
同一账号一起掉, 根因是平台风控检测到异常访问模式 (见 §7.3).

### 7.1 共享模块 `crawl/antibot.py`

所有 scraper 顶部都有:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from antibot import (
    AdaptiveThrottle, DailyCap, SessionDead,
    parse_retry_after, is_auth_dead,
    add_antibot_args, throttle_from_args, cap_from_args,
)
_THROTTLE: AdaptiveThrottle = AdaptiveThrottle(base_delay=3.0, jitter=2.0, burst_size=40)
```

4 个关键组件:

| 组件 | 用途 |
|---|---|
| `AdaptiveThrottle` | 节流: 基础 + 抖动, **每 N 条后长冷却**, 支持退避 |
| `DailyCap` | 单轮上限, 主循环里 `if cap.exhausted(): break` |
| `SessionDead` | 401/403 统一异常, 主循环 catch → 退出让用户重登 |
| `parse_retry_after` / `is_auth_dead` | 响应解析助手 |

### 7.2 `AdaptiveThrottle` 节流策略

**三档 sleep**:

1. **Backoff** (一次性, 上次 `on_retry()` 注入): 尊重 `Retry-After` 或指数退避
2. **Burst cooldown** (每 `burst_size` 条请求后): 随机 30-60s, 模拟用户"读一下再继续"
3. **Normal** (其余时间): `base ± jitter` 秒 (**默认 3s ± 2s, 即 1-5s**)

**per-scraper 默认参数** (反爬紧的平台用更保守值):

| 爬虫 | base | jitter | burst_size | daily_cap | 备注 |
|---|---|---|---|---|---|
| alphapai_crawl | 3s | 2s | 40 | 500 | JWT 鉴权, 风控普通 |
| jinmen | 3s | 2s | 40 | 500 | 之前 0.3s 硬节流, 改这个 |
| meritco_crawl | 3s | 2s | 40 | 500 | 已有 with_retry, 现用 antibot 退避 |
| third_bridge | **4s** | **3s** | **30** | **300** | **最严**, AWS Cognito + WAF, 已经被烧过 |

### 7.3 平台怎么检测爬虫

```
① 请求速率: 5 分钟 >60 个 API 请求 → 高风险
② 间距均匀: 2s 一个周期性请求 → 不像真人
③ 缺浏览器 telemetry: 没有 FullStory / NewRelic / Akamai heartbeat
④ 缺页面级加载: 没有 *.css / *.js / _next/data/*.json 穿插
⑤ WAF token 过期没刷: aws-waf-token 10min TTL 到了没走 challenge 续命
⑥ UA + IP + cookie 指纹过于稳定 (正常用户会偶尔换标签页 / 鼠标抖一下)
```

**重点**: 真实浏览器每分钟会发**几十个** telemetry beacon (fullstory, newrelic, mpulse,
userpilot), 而我们爬虫 API 请求只有 1-2 个. 这个比例就很显眼.

### 7.4 Mitigation 按有效性排序

| 等级 | 做法 | 实现 |
|---|---|---|
| **A** | **大 base delay + 大 jitter** | `AdaptiveThrottle` 默认 3±2s, third_bridge 4±3s |
| **A** | **burst cooldown 模拟读书** | 每 30-40 条 sleep 30-60s |
| **A** | **daily-cap 硬停** | 单轮 300-500 条自动停, 人工再启 |
| **B** | **401 立刻退出** | `SessionDead` 抛出, 不要继续烧 |
| **B** | **尊重 `Retry-After`** | `parse_retry_after` → `on_retry` |
| **B** | **指数退避 429/5xx** | `on_retry(attempt=N)` → 2^N 秒 |
| **C** | 每批次间加长 idle | 用 `--watch --interval 1800` (30 分钟) |
| **C** | 避开风控敏感时段 | 凌晨 3-5 点反而最显眼, 白天+晚上更自然 |
| **D** | **Playwright 浏览器模式** | 终极方案: 真 Chrome 里 `window.fetch()`, telemetry 全自动发 |

### 7.5 典型用法 — 稳扎稳打的增量模式

```bash
# 首次: daily-cap 500 保护, 分多天慢慢扫
python3 scraper.py --max 500

# 增量监听: 每 30 分钟一轮, 每轮最多 50 条, 总 daily-cap 300
python3 scraper.py --watch --resume --interval 1800 --max 50 --daily-cap 300

# 被怀疑时 (撞上 401 后重登 cookie) 再保守一点
python3 scraper.py --throttle-base 8 --throttle-jitter 5 --burst-size 20 --daily-cap 100 --max 100
```

### 7.6 会话被杀了怎么办

症状:
```
[错误] 会话失效: HTTP 401: "Not authorized"
  → 浏览器重登 <platform>, 更新 credentials.json (或对应 localStorage key)
```

步骤:
1. **立刻停** scraper (它应该自己已经退出)
2. **浏览器重登** 对应平台
3. **复制新 token / cookie / auth-info** → 覆盖 `credentials.json` 或 CLI `--auth`
4. **跑 `--show-state` 验证**
5. **用更保守参数重启**: `--throttle-base 5 --daily-cap 200`
6. 等 24 小时再恢复默认参数

### 7.7 新爬虫接入反爬 — 5 步

1. `scraper.py` 顶部 import antibot (见 §7.1 片段)
2. `parse_args()` 末尾加 `add_antibot_args(p, default_base=3.0, default_jitter=2.0, default_burst=40, default_cap=500)`
3. `main()` 开头 `global _THROTTLE; _THROTTLE = throttle_from_args(args)`
4. 所有请求间的 `time.sleep(...)` 替换成 `_THROTTLE.sleep_before_next()`
5. 主循环里加 `cap = cap_from_args(args)` + `if cap.exhausted(): break` + 成功后 `cap.bump()`
6. **(推荐)** retry wrapper 里用 `_THROTTLE.on_retry(retry_after_sec=...)` 注入退避
7. **(推荐)** HTTP response 层用 `is_auth_dead(r.status_code)` 抛 `SessionDead`

---

## 8. 故障速查

| 症状 | 优先排查 |
|---|---|
| 所有请求 `401/403` | token 过期 or 会话被撤 → 浏览器重登 |
| 所有 `code=500 "参数错误"` | 请求签名 (X-My-Header 等); 其次 body 字段缺失 |
| 响应 base64 乱码 | AES 响应加密, 看 response header 里的 `k` / `sig` |
| 响应 HTML / Captcha | WAF, 检查 cookie 完整性 + 降速 (§7) |
| PDF 下载内容是 HTML | `%PDF` 魔数校验不过, 记 `pdf_download_error` |
| `--resume` 重复抓同一批 | `top_id` 没更新, 检查 `save_state(top_id=...)` |
| `--today` 扫不到今日新数据 | 可能 token 降级成"免费视图", 见 §5.5 |
| `brotli` 解码报错 | `pip install brotli` 或去掉 `accept-encoding: br` |
| httpx 系统代理失败 | `httpx.Client(trust_env=False)` |
| MongoDB 数据被外部拿走 | 默认 `localhost:27017` 无鉴权, 公网必须防火墙 |

---

## 9. 依赖

```bash
pip install requests httpx pymongo motor tqdm pycryptodome brotli playwright
playwright install chromium      # auto_login + sentimentrader 需要
```

- `pycryptodome`: AES 解密 / RSA 签名
- `brotli`: 部分站点强制 br 编码
- `httpx`: `trust_env=False` 绕系统代理
- `tqdm`: 进度条
- `motor`: backend `*_db.py` 异步 Mongo
- `playwright`: `auto_login.py` 浏览器自动化 + sentimentrader chart scrape

Python 3.9+。

**MongoDB** (容器名 `crawl_data`):

```bash
docker run -d --name crawl_data -p 27017:27017 \
  -v crawl_data:/data/db docker.1ms.run/library/mongo:7

# 可选 Web UI
docker run -d --name crawl_data_ui --network host \
  -e ME_CONFIG_MONGODB_URL=mongodb://localhost:27017 \
  docker.1ms.run/library/mongo-express
# → http://127.0.0.1:8081
```

---

## 10. 安全

- **凭证不进 git**: `credentials.json`, `.env`, 硬编码 token 的 scraper 副本 — 都在子目录 `.gitignore`
- **一个账号一个 scraper 进程**: 多开触发风控批量清会话 (4/17 全家桶掉线)
- **MongoDB 默认无认证**: 本地 OK, 公网必须防火墙 / SSH 隧道
- **本地 PDF 体量**: 一年 10w+ 文件常见, 提前规划磁盘
