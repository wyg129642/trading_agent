# 反反爬虫机制总览 (Antibot)

> **版本**:2026-04-25 (v2.2 — 实时档去量闸 + 浏览器模拟加强)
> **代码**:`crawl/antibot.py` (~1200 行) + `crawl/crawler_monitor.py::start_all` 改造
> **覆盖**:8 个 HTTP 爬虫 (alphapai / jinmen / meritco / funda / gangtise / acecamp / alphaengine / third_bridge) + 8 个 backfill 脚本。sentimentrader 走 Playwright 单跑不参与。

---

## 一句话总览

`crawl/antibot.py` 提供 **10 层基础防御 + 6 层回填增强 = 16 层** 组件,18 个 watcher 不再共享指纹/节奏/账号闸,backfill 不再 24/7 平摊也不会饿死 realtime。任一 watcher 探测到软警告 → 该平台所有 watcher Redis 共享 flag 全局静默 30~60min,把"撞墙等吊销"改成"早一步收手"。

**v2.2 变更**:
- **实时档去数量闸** — 撤掉 `DailyCap` / `AccountBudget` rt 主桶的默认启用。WAF 抓的是节奏和指纹,不是 24h 总数;撞顶只会漏抓增量(alphapai report 单日 881 条撞 3000 的顶就是这么来的)。
- **浏览器模拟加强** — `headers_for_platform` 补齐 Chrome 126 真实指纹(`Priority: u=1, i` + `sec-ch-ua-arch/bitness/full-version-list/model/platform-version`);`AdaptiveThrottle` 加 `idle_window_prob` 模拟切 tab;新增 `warmup_session` 在首次建连时先 GET landing HTML 再发 XHR。
- 数量闸仅 backfill 保留(单进程兜底 + bg→rt floor 让位逻辑)。

---

## Part A — 基础 9 层防御 (适用所有 watcher / scraper)

### ① 节奏抖动 (Gaussian + Long-tail + Idle-window)
- 分布:`random.gauss(base, jitter/2)` (旧版 uniform 是机器特征)
- 截断:`max(0.2, min(base + jitter*2, delay))`
- 尾部概率分支 (**互斥**,避免一次停 3+ min):
  - `idle_window_prob` (默认 0.0, 实时档注入 0.03) → 60-180s "切 tab 离开一会儿"
  - `elif long_tail_prob` (默认 0.05) → 5-30s "读完一条停一下"
- CLI:`--throttle-base/-jitter`,`--no-long-tail`,`--idle-window-prob N`

### ② 突发冷却 (Burst Cooldown)
- 每 N 条请求后 `random.uniform(burst_cooldown_min, burst_cooldown_max)` 长冷却
- **实时档关键修正**:从旧 `--burst-size 0`(关闭)→ 新 **80 条 + 10-25s 冷却**
- CLI:`--burst-size`, `--burst-cooldown-min/max`

### ③ 时段倍增 (Time-of-Day Multiplier)
按 CST 工时形态加权 throttle:

| 时段 | 倍数 |
|---|---|
| 23:00 ~ 07:00 (深夜) | × 2.5 |
| 12:00 ~ 14:00 (午休) | × 1.3 |
| 周六 / 周日 | × 1.8 (叠乘) |
| 工作日工时 | × 1.0 |

`--no-time-of-day` 关闭。

### ④ 进程级 UA 池 (Per-process UA)
- 5-8 个 Chrome 122-126 (Win10/11 + macOS Sonoma)
- `pick_user_agent(label)` 按 `CRAWLER_PROCESS_LABEL` env **稳定 hash**
- 同一 watcher 重启 → 同一 UA(像同一个真人单设备)
- 不同 watcher → 不同 UA(18 个进程自动散到 5-8 个)

### ⑤ Chrome 126 完整 Browser-like Headers (2026-04-25 v2.2 升级)
`headers_for_platform(platform)` 一并配齐完整 XHR 指纹:
- `User-Agent` (从池里选)
- `Accept-Language` (CN 站 zh-CN / US 站 en-US)
- `Accept-Encoding: gzip, deflate, br` (不加 zstd — requests/httpx 不原生支持解压)
- `Priority: u=1, i` (RFC 9218 HTTP/2 priority hint, Chrome 126+ 必带)
- Client Hints 全套:
  - `sec-ch-ua` (简版)
  - `sec-ch-ua-full-version-list` (详细 build, e.g. `126.0.6478.127`)
  - `sec-ch-ua-arch` / `sec-ch-ua-bitness` (`"x86"` / `"64"`)
  - `sec-ch-ua-mobile` / `sec-ch-ua-model` (`?0` / `""`)
  - `sec-ch-ua-platform` / `sec-ch-ua-platform-version` (Win10/Mac + 版本号)
- `Sec-Fetch-Dest/Mode/Site`
- `Referer` / `Origin` (平台正确 landing 域)

缺这些在现代 Chrome UA 下是硬指纹 (Akamai/Datadome/Cloudflare Turnstile 都会查)。

### ⑥ 进程内单轮上限 (DailyCap) — **实时档默认禁用 (2026-04-25 v2.2)**
- 单进程单轮抓 N 条就退出 (`if cap.exhausted(): break`)
- 实时档 `--daily-cap 0`(不传)— 旧版 600 被 80 条 burst 喘息 + tod 倍增实际覆盖
- **backfill 档仍保留**,作为单进程跑飞兜底(见「四档运行配置」)
- CLI:`--daily-cap N` (0=无限)

### ⑦ 跨进程账号 24h 滚动闸 (AccountBudget) — **rt 主桶默认禁用 (2026-04-25 v2.2)**
- Redis sorted set 记录账号最近 24h **全部** 请求时间戳
- `account_id` 从 token JWT 解出 uid (或 hash)
- 同一账号下所有 watcher **共享同一预算桶**
- **role="rt"** (主桶,**默认 0 禁用**) / **role="bg"** (后台桶, 给 backfill 用 — 见 Part B)

**2026-04-25 变更**:rt 主桶旧版 300~20000/24h 作为硬封顶,实战检验:
- alphapai report 财报季单日 881 条撞 3000 的顶 → 漏抓当日增量
- jinmen reports 17k+ 历史回填一波把 2500 吃光 → meetings/oversea 饿死
- 先是拆子模块分桶(加复杂度),再是各桶上调到 10000~20000(撞不到,等于没开)
- 结论:**rt 量闸反爬价值≈0**,WAF 抓的是节奏/指纹/WAF cookie,不是 24h 总数

现行策略:rt 默认 0 不启用,靠 SoftCooldown + 节奏抖动 + UA 池 + Chrome 126
header 联合防护。`_DEFAULT_ACCOUNT_BUDGET` 字典保留仅作为 bg 桶 `realtime_floor_pct=70` 让位逻辑的参考值。

| 平台 | rt floor 参考 (**不再作为 rt 硬闸**) | 仅用途 |
|---|---|---|
| alphapai | 3000 / 模块 | bg→rt 让位对比基准 |
| jinmen | 1500 / 模块 | 同上 |
| funda | 2500 | 同上 |
| alphaengine | 1500 / 模块 | 同上 |
| acecamp | 800 | 同上 |
| meritco | 1200 | 同上 |
| thirdbridge | 300 | 同上 |
| gangtise | 20000 | 同上 |

**子模块 account_id 后缀保留**(`account_id_for_alphapai` / `_for_jinmen` / `_for_alphaengine`),因为 bg 桶的子模块 floor 对比也用这套分桶 key。

CLI:`--account-budget N` (0=禁用, 默认) / `--account-role rt|bg` (默认 rt;
backfill 脚本 `add_backfill_args` 会覆盖成 bg)
紧急限流仍可 CLI 传 `--account-budget 500` 自保。
Redis key:`crawl:budget:<platform>:<account_id>` (rt) / `:<account_id>:bg` (bg)

### ⑧ 软警告全局冷却 (SoftCooldown,**核心**)
任一 watcher 触发软信号 → 该平台所有 watcher Redis 共享 flag 全局静默:

| 触发信号 | 静默时长 | 检测来源 |
|---|---|---|
| HTTP 429 | 45 min (thirdbridge 60 min) | `api_call` |
| `hasPermission:False / code=7` | 30 min | `detect_soft_warning` body 层 |
| 限流关键词 ("请求过频繁" / "rate limit" / "captcha" 等) | 60 min | body / text_preview 检测 |
| WAF cookie (datadome / _pxvid / _abck / ak_bmsc / captcha / geetest) | 60 min | `r.cookies` 检测 |
| `REFRESH_LIMIT` (alphaengine 专属) | 30 min | scraper 业务层 |

冷却期间所有 `_THROTTLE.sleep_before_next()` 顶部读 Redis flag → 睡到清除。
**手动清**:`redis-cli del crawl:soft_cooldown:<platform>`
CLI:`--no-soft-cooldown` 调试时关闭

### ⑨ 启动错峰 + 进程标签透传
`crawler_monitor.start_all` spawn 时:
- shell wrapper `sleep $((RANDOM % min(interval, 60))) && exec scraper.py ...`
- 18 个 tick 散到整个 60s 窗口,**不再每分钟 :00 集体打闪**
- 同时透传 `CRAWLER_PROCESS_LABEL` env → antibot 据此选 UA

### ⑩ 会话 warmup(新 2026-04-25 v2.2)
真人打开 SPA 先 GET 一次 HTML 主页,再由浏览器发 XHR。直接干 API 是教科书级 bot 指纹。

`warmup_session(session, platform)` 在 scraper 的 `create_session` / `create_client` 末尾调一次:
- 用 navigate-style headers(`Accept: text/html...`, `Sec-Fetch-Dest: document`, `Sec-Fetch-Mode: navigate`)GET 平台 landing(`_PLATFORM_HEADERS[platform]["referer"]`)
- 停 2-5s(模拟页面加载 + 人类看一眼)
- 把 set-cookie 自动收进 session,后续 XHR 自带
- 幂等(`session._antibot_warmed` flag),失败不抛异常不影响调用方

接入平台:
- ✅ alphapai / jinmen / meritco / funda / gangtise / acecamp / alphaengine / semianalysis
- ❌ third_bridge(AWS Cognito cookie jar 敏感,warmup 可能触发 challenge 或覆盖认证 cookie,故意跳过)

---

## Part B — 回填特有 6 层 (Backfill v1)

> 回填的封号风险跟实时不同 — **稳态高密度持续多小时** 才是 cron 特征,单次 burst 反而不是。
> 核心策略:**"挤碎成短任务、躲到深夜跑、永远让位 realtime"**。

### ⑩ BackfillWindow — 强制工时禁跑(最大单笔收益)

```python
BackfillWindow.wait_until_allowed("alphapai")  # 工时段直接 sleep 到当晚 22:00
```

每平台允许窗口(CST):

| 平台 | 工作日窗口 | 周末 |
|---|---|---|
| alphapai / jinmen / meritco / gangtise / acecamp / alphaengine | **22:00 ~ 08:00** (次日) | 全天允许 |
| **thirdbridge** | **23:00 ~ 07:00** (反爬最严) | 全天允许 |
| funda / sentimentrader (US) | any-time(美股活跃在 CN 凌晨,反正不冲突) | — |

`--no-backfill-window` 关闭。

### ⑪ AccountBudget(role="bg") — realtime 让位

`bg.exhausted()` 双重判定:
1. **自身配额 24h 用尽**:`count_24h() >= daily_limit`
2. **rt 兄弟桶用量 >= floor%**(默认 70%)→ 让位 realtime,backfill 暂停

平台 bg 桶默认值:

| 平台 | bg_budget |
|---|---|
| alphapai / gangtise | 1500 |
| jinmen | 1200 |
| funda | 1000 |
| alphaengine | 750 |
| **acecamp** | **500** (2026-04-24 下调, 回填强制 --skip-detail) |
| meritco | 600 |
| **thirdbridge** | **150** |

CLI:`--account-role bg` / `--bg-budget N` / `--realtime-floor-pct 70`
所有 scraper.py(realtime + backfill)都接受 `--account-role bg`,backfill orchestrator 注入它让子 scraper 自动走后台桶。

### ⑫ BackfillSession — 每 N 条强制 5-15 min idle

`AdaptiveThrottle` 的 5% long-tail 概率上不保证 — 跑 10 万条可能 5000 个 long-tail 全在凌晨 3 点 burst。
**确定性触发** 才能切碎稳态密度:

| pace | 每 N 条停 | 适用 |
|---|---|---|
| `fast` | 60-180s (1-3 min) | PDF 字节流 |
| `normal` | 300-600s (5-10 min) | 常规 list+detail |
| `slow` | 600-1500s (10-25 min) | 敏感平台 (thirdbridge / meritco) |

`bf_session.page_done()` 翻页 / sweep 切换日期时也注入 30-90s 间隔。

### ⑬ BackfillCheckpointBackoff — 启动 / 中断恢复后慢起

```python
BackfillCheckpointBackoff(throttle, warm_up=30, factor=3.0).arm()
# 前 30 条节奏 ×3 (~12s/条),之后回到正常
```

中断恢复后并发请求是 backfill 最常见事故源(真人重新打开页面会先停一下)。

### ⑭ BackfillLock — 平台级单实例锁

```python
BackfillLock.acquire("gangtise", role="pdf_backfill", ttl_min=30)
# Redis key: crawl:bf_lock:gangtise:pdf_backfill
# 心跳每 5-10 min 一次, TTL 30 min, 进程死了自动清
```

防 13 个 `gangtise/backfill_pdfs.py` 同时跑这种事故。`--bf-no-lock` 禁用,`--bf-force-lock` 强制夺锁。

### ⑮ `_PLATFORM_BACKFILL_DEFAULTS` — 平台默认配置表

| 平台 | base | jitter | burst | daily_cap | bg_budget | pace | break_every |
|---|---|---|---|---|---|---|---|
| alphapai | 4.0s | 2.5s | 30 | 400 | 1500 | normal | 50 |
| jinmen | 4.0s | 2.5s | 30 | 400 | 1200 | normal | 50 |
| meritco | 5.0s | 3.0s | 25 | 300 | 600 | slow | 30 |
| **thirdbridge** | **8.0s** | **4.0s** | **20** | **100** | **150** | **slow** | **20** |
| funda | 4.0s | 2.5s | 30 | 400 | 1000 | normal | 50 |
| gangtise | 4.0s | 2.5s | 30 | 400 | 1500 | normal | 50 |
| **acecamp** | **4.5s** | **2.5s** | **20** | **250** | **500** | **slow** | **30** |
| alphaengine | 4.0s | 2.5s | 30 | 400 | 750 | normal | 40 |

`add_backfill_args(parser, platform="alphapai")` 自动用这套默认值。

---

## 四档运行配置 (crawler_monitor `_mode_args`)

| 参数 | **realtime** (2026-04-25) | **backfill** (推荐) | historical (老兼容) | dawn |
|---|---|---|---|---|
| `--interval` | 30s | **1200s** | 600s | 300s |
| `--throttle-base` | 1.5s | **4.0s** | 3.0s | 2.5s |
| `--throttle-jitter` (σ≈/2) | 1.0s | **2.5s** | 2.0s | 1.5s |
| `--burst-size` | 80 | **30** | 40 | 60 |
| `--burst-cooldown-min/max` | 10~25s | **60~180s** | 30~60s | 20~45s |
| `--daily-cap` | **0 (不传)** | **400** | 500 | 400 |
| `--account-budget` | **0 (不传)** | 0 (bg 桶接管) | 0 | 0 |
| `--idle-window-prob` | **0.03** | — | — | — |
| `--account-role` | rt 默认 (主桶 0 等效禁用) | **`bg`** | rt | rt |
| `--since-hours` | 24 | (不限) | (不限) | 36 |
| 工时禁跑 | ❌ | **✅ (各 backfill 脚本主循环执行)** | ❌ | ❌ |
| 用途 | 实时入库 (靠节奏+指纹+SoftCooldown+warmup) | **推荐回填,bg 桶+工时禁跑** | 紧急回填 (跟 realtime 抢主桶) | cron 02:00-06:00 |

**触发**:

```bash
# Web 按钮
http://127.0.0.1:8080
  →  「🚀 实时」/ 「📦 回填安全档」/ 「⚠️ 历史紧急档」/ 「🌙 凌晨低峰档」

# HTTP API
curl -X POST 'http://127.0.0.1:8080/api/start-all?mode=realtime'
curl -X POST 'http://127.0.0.1:8080/api/start-all?mode=backfill'
curl -X POST 'http://127.0.0.1:8080/api/start-all?mode=historical'
curl -X POST 'http://127.0.0.1:8080/api/start-all?mode=dawn'

# 单 backfill 脚本启动 (推荐;含完整 v2 + backfill v1 stack)
cd crawl/gangtise && python3 backfill_pdfs.py --loop                           # PDF 长期补齐
cd crawl/alphapai_crawl && python3 perday_backfill.py --watch --days-sliding 7 # 7 天滚动 sweep
cd crawl/jinmen && python3 backfill_oversea_pdfs.py --only-missing             # oversea 缺 PDF 补
cd crawl/alphapai_crawl && python3 backfill_today_reports.py --category report # 今日缺漏精确补
cd crawl/gangtise && python3 backfill_today.py --type all                       # gangtise 当日全补
cd crawl/alphaengine && python3 backfill_roadshow_events.py --watch            # roadshow_events 全量
python3 crawl/backfill_by_date.py --from 2025-10-23 --to yesterday              # 多平台按日 sweep
python3 crawl/backfill_6months.py --cutoff 2025-10-23                            # 6 月历史回灌 orchestrator
```

---

## 老栈保留(向后兼容)

- `SessionDead` — 401/403 立即抛,主循环 catch → 退出让用户重登(不要重试,只会延长封禁)
- `parse_retry_after` / `is_auth_dead` — HTTP 助手
- `AdaptiveThrottle.on_retry()` — 429/5xx 指数退避 + 尊重 `Retry-After`
- `AdaptiveThrottle.on_warning()` — 任一警告后 20 条节奏 ×2 (`_preemptive_factor`)

---

## 启动配置可视化

每个 scraper 启动会打印一行 `[antibot]` stamp,backfill 脚本额外打印 `[backfill]`:

```
[antibot]  platform=alphapai label=alphapai_crawl|--watch_--resume_--category_roadshow
           base=1.5s jitter±1.0s(σ=0.50) burst=80 tod=on soft_cd=on longtail=0.05
           daily_cap=600 acct_budget=3000/24h(rt) ua=Mozilla/5.0 (Windows NT...)
           acct=u_1244342170433880064

[backfill] platform=gangtise pace=normal break_every=50 page_pause=30-90s
           window=[22:00~08:00 CST 工作日 + 周末全天] BLOCKED, 10.4h 后开窗
           lock_holder=2604113:1761275465.123
```

```bash
# 查每个 watcher 实际加载的配置
grep "^\[antibot\]\|^\[backfill\]" logs/<platform>/watch_*.log
```

---

## 平台接入状态

| 平台 | 实时 watcher | UA 池 | SoftCooldown | rt 桶 | bg 桶 | backfill 脚本 | 工时禁跑 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| alphapai | ✅ | ✅ | ✅ HTTP+body+text+cookie | acct=u_124... | ✅ 1500 | `backfill_today_reports.py` + `perday_backfill.py` | ✅ 22-08 |
| jinmen | ✅ × 3 (meetings / reports / oversea_reports) | ✅ | ✅ AES 解密后过滤 + 401→SessionDead (2026-04-24) | acct=u_4210838:`<module>` (1500/桶) | ✅ 1200 | `backfill_oversea_pdfs.py` | ✅ 22-08 |
| meritco | ✅ httpx | ✅ | ✅ with_retry 内 + 业务层 | acct=hash | ✅ 600 | (无独立 backfill) | ✅ 22-08 |
| third_bridge | ✅ httpx | ✅ | ✅ 429→60min(AWS WAF 保守) | acct=hash | ✅ **150** | (无独立 backfill) | ✅ 23-07 |
| funda | ✅ httpx | ✅ | ✅ tRPC 双层 (HTTP + biz error) | acct=hash | ✅ 1000 | (无独立 backfill) | ❌ any-time (US) |
| gangtise | ✅ requests | ✅ | ✅ | acct=hash | ✅ 1500 | `backfill_pdfs.py` (常驻) + `backfill_today.py` | ✅ 22-08 |
| acecamp | ✅ requests | ✅ | ✅ | acct=u_50531199 | ✅ 750 | (无独立 backfill) | ✅ 22-08 |
| alphaengine | ✅ requests | ✅ | ✅ + REFRESH_LIMIT 专属 | acct=u_4531490 | ✅ 750 | `backfill_roadshow_events.py` | ✅ 22-08 |
| sentimentrader | (Playwright cron) | — | — | — | — | — | — |

跨平台 orchestrator:`crawl/backfill_by_date.py` (按日 sweep alphapai/gangtise/funda) + `crawl/backfill_6months.py` (6 月回灌 alphapai/jinmen/meritco/gangtise),都自带 orchestrator-级 BackfillLock + 注入 `--account-role bg` 给子 scraper。

---

## 关键 Redis Keys (15 层完整清单)

```
crawl:budget:<platform>:<account_id>       # rt 主桶 (sorted set, 24h zcard)
crawl:budget:<platform>:<account_id>:bg    # bg 后台桶
crawl:soft_cooldown:<platform>             # 软冷却 flag (rt + bg 共享, TTL = 静默剩余)
crawl:bf_lock:<platform>:<role>            # 回填单实例锁 (TTL 30 min, 心跳续期)
```

Redis 不可用时,自动降级为单进程 in-memory dict(开发机 / CI 友好,不强制依赖 Redis)。

---

## 常用运维命令

### 启停
```bash
# 启动监控 + 全部 watcher (启动随机偏移自动注入)
./start_web.sh crawl start

# 浏览器面板
http://127.0.0.1:8080

# 一键启停
curl -X POST 'http://127.0.0.1:8080/api/start-all?mode=realtime'
curl -X POST 'http://127.0.0.1:8080/api/start-all?mode=backfill'

# 全停
./start_web.sh crawl stop
```

### 状态排查
```bash
# 看每个 scraper / backfill 的 antibot stamp
grep -h "^\[antibot\]\|^\[backfill\]" /home/ygwang/trading_agent/logs/*.log /home/ygwang/trading_agent/crawl/*/logs/*.log | tail -30

# 看哪个平台被软冷却了
redis-cli --scan --pattern 'crawl:soft_cooldown:*' | xargs -I{} redis-cli get {}

# 看哪个平台有 backfill 在跑 (锁)
for k in $(redis-cli --scan --pattern 'crawl:bf_lock:*'); do
  echo "$k = $(redis-cli get $k)"
done

# 看账号 24h 用量 (rt + bg)
for p in alphapai jinmen meritco funda gangtise acecamp alphaengine thirdbridge; do
  for k in $(redis-cli --scan --pattern "crawl:budget:$p:*"); do
    n=$(redis-cli zcard "$k")
    echo "$k: $n / 24h"
  done
done

# 看是否在 backfill 窗口
python3 -c "
import sys; sys.path.insert(0, 'crawl')
from antibot import _in_backfill_window
for p in ('alphapai','jinmen','meritco','gangtise','acecamp','alphaengine','thirdbridge','funda'):
    allowed, secs = _in_backfill_window(p)
    print(f'{p:14s} {\"ALLOWED\" if allowed else \"BLOCKED\"}  (next change in {secs/3600:.1f}h)')
"

# 手动清除某平台软冷却
redis-cli del crawl:soft_cooldown:alphapai

# 手动清除某 backfill 锁 (确认前一进程已死)
redis-cli del crawl:bf_lock:gangtise:pdf_backfill

# 单 scraper 验证 + 打 stamp
cd crawl/alphapai_crawl && python3 scraper.py --show-state
```

### 被风控 (401) 处置 5 步
1. 停 scraper (它应该自己已 SessionDead 退出)
2. 浏览器重登 → 复制新 token / cookie / auth-info
3. 写到 `credentials.json` 或走 `/data-sources` 凭证管理 UI
4. `--show-state` 验证 (会顺便打印 antibot stamp 确认配置)
5. 不要立刻起实时档 — 等 24h 让 AccountBudget 滚动窗清掉,再用 `mode=realtime` 重启

### 紧急回填
```bash
# 工时段强制启动回填 (绕开窗口, 但保留 bg 桶 + 锁)
cd crawl/gangtise && python3 backfill_pdfs.py --loop --no-backfill-window

# 完全裸跑 (绕开窗口 + 锁 + bg 桶让位规则)
cd crawl/gangtise && python3 backfill_pdfs.py --loop \
    --no-backfill-window --bf-no-lock --account-role rt
# ⚠️ 慎用 — 会跟 realtime 抢主桶, 可能耗光 24h 配额
```

---

## 文件地图

| 想做什么 | 改这里 |
|---|---|
| 调反爬全局逻辑 | `crawl/antibot.py` |
| 改四档默认参数 | `crawl/crawler_monitor.py::_mode_args` |
| 加新平台 rt 账号预算默认值 | `crawl/antibot.py::_DEFAULT_ACCOUNT_BUDGET` |
| 加新平台 bg/backfill 默认值 | `crawl/antibot.py::_PLATFORM_BACKFILL_DEFAULTS` |
| 加新平台 backfill 窗口 | `crawl/antibot.py::_PLATFORM_BACKFILL_WINDOW` |
| 加新平台 referer / locale | `crawl/antibot.py::_PLATFORM_HEADERS` |
| 扩 UA 池 | `crawl/antibot.py::_UA_POOL` |
| 扩软警告检测词 | `crawl/antibot.py::_SOFT_WARNING_BODY_KEYWORDS` |
| 接入新 scraper / backfill | 见下文 |

---

## 接入清单

### 新 scraper (realtime / 通用) 接入

```python
# 1. import
from antibot import (
    AdaptiveThrottle, DailyCap, SessionDead,
    parse_retry_after, is_auth_dead,
    add_antibot_args, throttle_from_args, cap_from_args,
    AccountBudget, SoftCooldown, detect_soft_warning,
    headers_for_platform, log_config_stamp, budget_from_args,
)

# 2. 模块级
_THROTTLE = AdaptiveThrottle(base_delay=3.0, jitter=2.0, burst_size=40,
                              platform="myplatform")
_BUDGET = AccountBudget("myplatform", "default", 0)
_PLATFORM = "myplatform"

# 3. create_session 用 headers_for_platform
def create_session(token):
    s = requests.Session()
    h = headers_for_platform("myplatform")
    h["Authorization"] = f"Bearer {token}"
    s.headers.update(h)
    return s

# 4. api_call 检测软警告
reason = detect_soft_warning(r.status_code, body=body, cookies=dict(r.cookies))
if reason:
    SoftCooldown.trigger(_PLATFORM, reason=reason, minutes=45)
    _THROTTLE.on_warning()

# 5. 主循环检 budget
if cap.exhausted() or _BUDGET.exhausted():
    break
cap.bump(); _BUDGET.bump()

# 6. parse_args 透传 platform
add_antibot_args(p, platform="myplatform")  # 含 --account-role flag

# 7. main 重建 throttle + budget + 打 stamp
_THROTTLE = throttle_from_args(args, platform="myplatform")
_BUDGET = budget_from_args(args, account_id=acct_id, platform="myplatform")
# (role 自动取 args.account_role; orchestrator 注入 --account-role bg 时自动走 bg 桶)
log_config_stamp(_THROTTLE, cap=cap_from_args(args), budget=_BUDGET)

# 8. crawler_monitor::_DEFAULT_ACCOUNT_BUDGET 加一项默认值
```

### 新 backfill 脚本接入

```python
from antibot import (
    SessionDead, SoftCooldown,
    add_antibot_args, throttle_from_args, cap_from_args,
    add_backfill_args, backfill_session_from_args,
    budget_from_args, log_config_stamp,
    BackfillWindow, BackfillLock, BackfillCheckpointBackoff,
)
PLATFORM = "myplatform"

# 1. parse_args 加两组 flag (顺序: antibot 在前, backfill 在后)
add_antibot_args(p, default_base=4.0, default_jitter=2.5,
                 default_burst=30, default_cap=400, platform=PLATFORM)
add_backfill_args(p, platform=PLATFORM)

# 2. main 顶部:锁 + 窗口
if not BackfillLock.acquire(PLATFORM, role="my_subtype",
                             force=args.bf_force_lock):
    sys.exit("已被占用")
BackfillWindow.wait_until_allowed(PLATFORM)

# 3. 构造 throttle / bg_budget / bf_session
throttle = throttle_from_args(args, platform=PLATFORM)
cap = cap_from_args(args)
bg_budget = budget_from_args(args, account_id=acct_id, platform=PLATFORM, role="bg")
bf_session = backfill_session_from_args(args, platform=PLATFORM)
BackfillCheckpointBackoff(throttle, warm_up=30, factor=3.0).arm()
log_config_stamp(throttle, cap=cap, budget=bg_budget,
                 bf_session=bf_session, bf_window_platform=PLATFORM)

# 4. 主循环
try:
    for item in items:
        if cap.exhausted() or bg_budget.exhausted():
            break
        if BackfillWindow.seconds_until_allowed(PLATFORM) > 0:
            break  # 窗口关了, 不要再硬干
        SoftCooldown.wait_if_active(PLATFORM)
        BackfillLock.heartbeat(PLATFORM, role="my_subtype")
        process(item)
        cap.bump(); bg_budget.bump()
        bf_session.step()                # 每 50 条强制 5-15min idle
        throttle.sleep_before_next()
    bf_session.page_done()              # 翻页/切日期时
finally:
    BackfillLock.release(PLATFORM, role="my_subtype")
```

---

## 平台专属微调 (per-platform tuning)

### jinmen (2026-04-24)

jinmen 三条业务线 — 纪要 (meetings) / 研报 (reports) / 外资研报 (oversea_reports) —
共享一个 2500/24h 账号预算, reports 的历史回填 (17k+ 条) 能单天把预算吃光;
401 被 scraper 各层 `except Exception` 吞, --watch 拿失效 token 每
`--interval` 秒硬打一次, AccountBudget 白白烧 + 延长封禁. 本次微调:

- **账号预算按子模块独立 1500/24h**:新 `account_id_for_jinmen()` 给 account_id
  追 `:meetings` / `:reports` / `:oversea_reports` 后缀,Redis sorted set key
  天然分离;`main()` 按 `args.oversea_reports / args.reports` 路由一次定下
  category. 单桶 1500 留 2-3× realtime 峰值 + 有限 backfill 余量. Stamp 示例:
  `acct=u_4210838:meetings mode=meetings`.
- **SessionDead 泄漏全面修复** (`jinmen/scraper.py`):
  - 新增 `_raise_for_status_safe(r, endpoint)` 把 HTTP 401/403 升格为 SessionDead
    (替换 9 处 `r.raise_for_status()` + `fetch_raw` 的合成 dict 分支).
  - 所有 runner + page-loop 的 `except Exception` 前加 `except SessionDead: raise`:
    `fetch_items_paginated` / `fetch_reports_paginated` /
    `fetch_oversea_reports_paginated` / `count_today` / `count_reports_today` /
    `run_once` / `run_once_streaming` (两处) / `run_reports_once` /
    `run_oversea_reports_once` / `unlock_and_refetch` / `--refetch-empty`.
  - `--watch` 外层 loop 显式捕获 SessionDead → 打印 "立即退出等重登" 后 break,
    由 credential_manager 重登后 crawler_monitor 自动重启此 watcher.
- **Referer 默认修正**:`_PLATFORM_HEADERS["jinmen"].referer` 从
  `https://www.comein.cn/` (C 端主站,基本不调私有 API) 改
  `https://brm.comein.cn/` (登录后台 SPA,所有业务 XHR 的真实发起源).
  scraper.create_session 已显式覆盖过了,此处同步默认值以防
  `headers_for_platform("jinmen")` 不覆盖的调用路径失真.

触发点一览 (jinmen 特有):

| 信号 | 现象 | antibot 动作 |
|---|---|---|
| HTTP 401/403 | webtoken 被吊销 / 账号被拒 | SessionDead → 4 层 runner + watch 立即 break, 不再重试 (新) |
| body `code != "0"` + msg 含 "请求过频繁" / "访问受限" / "限流" | AES 解密后的业务限流文本 | SoftCooldown 60 min (走 decrypt_response → detect_soft_warning) |
| cookie datadome / geetest / captcha | WAF 指纹 | SoftCooldown 60 min |
| `code=454` WAF OTP 发码冷却 | 60s 内重发 SMS | 仅 `--otp-send` 路径触发, 手工流不进 SoftCooldown |
| 429 / 5xx | 后端真正限流 | SoftCooldown 45 min |

### acecamp (2026-04-24)

AceCamp VIP 团队金卡账号被官方封控事故的应对策略 (见 memory `crawler_acecamp_quota`
+ `crawler_disable_gate`). 核心问题: `articles/article_info` detail 端点在
`balance:0` 状态下 ~12 次就返 code=10003/10040; `articles/article_list` 无 quota,
仍返摘要. realtime scraper `--force` 会反复 upsert 空壳 content_md 覆盖数据库,
dashboard "今日入库" 数字虚高 → 用户以为一切正常 → 账号继续被平台追踪 → 封控.

**配置收紧清单** (此次事故全部改动):

| 层 | 参数 | 旧 | 新 |
|---|---|---|---|
| `antibot.py::_DEFAULT_ACCOUNT_BUDGET` | rt 桶 | 1500/24h | **800/24h** |
| `antibot.py::_PLATFORM_BACKFILL_DEFAULTS` | base / jitter / burst / cap / bg_budget / pace / break_every | 3.5/2.0/30/400/750/normal/50 | **4.5/2.5/20/250/500/slow/30** |
| scraper 模块级 fallback throttle | base / jitter / burst | 3.0/2.0/40 | **4.0/2.5/20** |
| scraper `add_antibot_args` 默认 | base/jitter/burst/cap | 2.5/1.5/30/500 | **4.0/2.5/20/300** |
| `crawler_manager.SPECS.acecamp.articles` | interval/base/jitter/burst + `--skip-detail` | 60s/1.5/1.0/80 无 skip | **120s/3.0/2.0/20 + --skip-detail** |
| `crawler_manager.SPECS.acecamp.opinions` | interval/base/jitter/burst | 60s/1.5/1.0/80 | **180s/3.0/2.0/20** (保留 detail, 独立端点) |
| `crawler_monitor.ALL_SCRAPERS` | 同 SPECS articles/opinions | `--type articles` / `--type opinions` 裸传 | 显式 `--skip-detail` + 保守 interval/burst |
| `backfill_6months.TARGETS` | acecamp articles/opinions extra_args | `--page-size 50` | **`--page-size 30 --skip-detail`** (articles only) |
| `daily_catchup.sh` | acecamp maxn | articles 200 / events 100 / opinions 200 | **articles 120 (--skip-detail) / opinions 80 / events 移除** |

**核心原则**:

1. **realtime 永不拉 detail** — `--skip-detail` 强制走 list 元数据路径, article_info
   quota 完全留给用户 web 端 + 未来专门的补齐脚本.
2. **quota code 10003/10040 自动触发 SoftCooldown** — `scraper.api_call` 业务层
   code 检查后直接 `SoftCooldown.trigger(platform, reason, minutes=30)`,
   所有 watcher + backfill 同步静默, 不再一起硬打 quota.
3. **scraper 内部有 tripwire 兜底** — `_tripwire_record_detail` 连续 15 次 detail 空
   content 抛 SessionDead, 即使 cooldown 没兜住也不会无限灌空壳.
4. **DISABLED 文件全局闸** — 账号封控期间直接 `touch crawl/AceCamp/DISABLED`,
   scraper main()/crawler_monitor/daily_catchup/backfill/admin-UI 5 条路径都会拒绝
   spawn (见 memory `crawler_disable_gate`).
5. **信号与状态独立** — `credential_manager._probe_acecamp` 在 `users/me` 通过后
   再探 content_md 空壳比例 ≥50% → health=degraded (红色 "detail 被封"),
   跟爬虫停车 (DISABLED) 独立但互补, 前者告警后者止血.

**账号恢复后的分步恢复**:

```bash
# 1. 到 /data-sources 验证 health=ok (不是 degraded), content_empty_ratio < 30%
# 2. rm crawl/AceCamp/DISABLED  — 解除闸门
# 3. 先只开 opinions watcher, 观察 24h quota 消耗
curl -X POST 'http://127.0.0.1:8000/api/data-sources/acecamp/crawler/start'
# 4. quota 无异常后才开 articles (--skip-detail 模式)
# 5. detail 正文补齐用独立脚本 (未来待写), 限速 base 6+, --break-every 20,
#    只针对 content_md 为空的 older doc 跑, 速率 ≤50 doc/h
```

---

## 设计原则(给以后改的人)

1. **任何故障都别让单一 watcher 硬撑** — 软冷却跨进程联动是核心防护
2. **指纹多样化优先于节奏放慢** — UA 池 + Chrome 126 完整 client-hint + warmup 比把 base 调到 10s 更有效
3. **实时档不要靠数量闸防跑飞** (2026-04-25 v2.2) — 旧版 `--daily-cap 600` / rt 主桶 1500~20000 经实战验证反爬价值≈0,反而频频漏抓增量。实时档靠:`--burst-size 80` 每 80 条喘息 (这是节奏不是量闸) + Gaussian 抖动 + long_tail + idle_window + 时段倍增 + SoftCooldown + UA 池 + Chrome 126 header 对齐 + warmup。数量闸只在 backfill 脚本保留做单进程兜底。
4. **凌晨档独立配置** — dawn 模式按低峰时段语义命名,跟 historical 区分
5. **回填脚本默认走 `--account-role bg`** — 不抢 realtime 主桶,realtime 用量 ≥70% 时自动让位 (floor 对比读 `_DEFAULT_ACCOUNT_BUDGET` 参考值)
6. **回填默认强制工时禁跑** — `BackfillWindow.wait_until_allowed()` 是主循环顶部第一件事
7. **每个 backfill 进程要持锁** — `BackfillLock.acquire(platform, role=唯一)`,死了自动 TTL 清
8. **每 N 条强制阅读停留** — 切碎稳态密度,比单纯放慢 throttle 有效
9. **orchestrator (backfill_by_date / backfill_6months) 自己也持一把全局锁** — 防 2 个 orchestrator 同时跑
10. **子 scraper 默认接受 `--account-role`** — `add_antibot_args` 已加,所有 scraper 都接受 bg flag
11. **Redis 不可用要降级** — 不能强制依赖,开发机和 CI 不该跑 Redis
12. **每次启动打 stamp** — 出问题第一手 grep `\[antibot\]` / `\[backfill\]` 就能知道当时配置。rt 桶禁用后 stamp 不再打 `acct_budget=...`,这是正常的
13. **dry-run 跳过 antibot stack** — 调试时不污染 Redis 桶
14. **warmup 是幂等的** — 新 session 调一次就够;老 session `_antibot_warmed` 标记会阻止重复 warmup。敏感 cookie jar (如 third_bridge AWS Cognito) 故意跳过。
