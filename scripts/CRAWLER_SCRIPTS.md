# 爬虫一键启动脚本

两个独立的一键脚本,服务器重启后用来快速恢复爬虫状态。
所有进程都走 `nohup + disown`,SSH 断了、终端关了都继续跑。

- **`start_crawler_realtime.sh`** — 实时监控(24h 滚动窗口 watcher + 监控看板)
- **`start_crawler_backfill_1year.sh`** — 1 年历史回填(一次性补齐缺失数据)

两者独立工作,可以同时跑(backfill 不会被 realtime 按钮误杀,见 §6)。

---

## 1. 服务器重启后的完整恢复流程

```bash
cd /home/ygwang/trading_agent

# 1. 确认 MongoDB 容器在跑 (若不在, 先启动)
docker start crawl_data 2>/dev/null || true

# 2. 启动实时监控 (monitor + 22+ watcher)
bash scripts/start_crawler_realtime.sh

# 3. (可选) 启动 1 年历史回填
bash scripts/start_crawler_backfill_1year.sh

# 4. 浏览器打开看板验证
#    http://127.0.0.1:8080
```

脚本本身是幂等的 — 已经在跑就跳过或先 kill 再启,不会重复拉起。

---

## 2. 实时监控脚本 `start_crawler_realtime.sh`

### 功能

1. 启动 `crawler_monitor.py`(web :8080 + 飞书监听 + 健康告警)
2. 通过 `/api/start-all?mode=realtime` 拉起所有平台的 watcher

### 命令

```bash
bash scripts/start_crawler_realtime.sh           # 启动
bash scripts/start_crawler_realtime.sh --status  # 查看状态
bash scripts/start_crawler_realtime.sh --stop    # 停 watcher + 停 monitor
```

### 启动后你会看到

- 看板: <http://127.0.0.1:8080> —— 平台总览 + 实时/回填双流 + 每平台详情
- 约 22~25 个 watcher 进程(每平台每分类一个),PPID 指向 monitor
- 日志:
  - `logs/crawler_monitor.log` —— monitor 主日志
  - 每 watcher 的日志写在对应 `crawl/<平台>/logs/watch_*.log`
  - 实时入库通过看板左栏「🚀 实时入库流」展示

### watcher 的默认参数(monitor 注入)

```
--watch --resume
--since-hours 24     # 只抓过去 24h 的发布
--interval 60        # 每 60s 轮询
--throttle-base 1.5 --throttle-jitter 1.0
--burst-size 0 --daily-cap 0    # 实时档关掉 burst 冷却/日上限
```

---

## 3. 1 年历史回填脚本 `start_crawler_backfill_1year.sh`

### 功能

并行启动 8 个一次性 backfill 进程,每个覆盖一个平台/分组的过去 1 年:

| 名字 | 平台 | 参数 | 日志 |
|---|---|---|---|
| alphapai | Alpha派 4 分类串行 | `--category all --skip-pdf` | `alphapai.log` |
| jinmen_meetings | 进门纪要 | — | `jinmen.log` |
| jinmen_reports | 进门国内研报 | `--reports --skip-pdf` | `jinmen_reports.log` |
| jinmen_oversea | 进门外资研报 | `--oversea-reports --skip-pdf` | `jinmen_oversea_reports.log` |
| meritco | 久谦 type 2+3 | `--type 2,3 --skip-pdf` | `meritco.log` |
| funda | Funda 3 分类串行 | `--category all` | `funda.log` |
| gangtise | 港推 3 分类串行 | `--type all --skip-pdf` | `gangtise_summary.log` |
| acecamp | AceCamp | `--type all --skip-detail`(避开 10/session 额度) | `acecamp.log` |

通用参数:

```
--since-hours 8760   # 1 年 = 365 × 24
--throttle-base 5 --throttle-jitter 3     # 慢节流, 不冲击 watcher
--burst-size 0 --daily-cap 0              # 一跑到底
--skip-pdf                                  # PDF 太大, backfill 不下载
NOT --watch / NOT --resume                  # 一次性, 走完自然退出
```

跳过的平台:
- **Third Bridge** —— 账号 token 过期(AWS Cognito),先到 `/data-sources` UI 重登
- **SentimenTrader** —— 只是 daily indicator,不需要 backfill

### 命令

```bash
bash scripts/start_crawler_backfill_1year.sh           # 启动 (已在跑则报错)
bash scripts/start_crawler_backfill_1year.sh --force   # kill 旧 backfill 再起
bash scripts/start_crawler_backfill_1year.sh --status  # 状态
bash scripts/start_crawler_backfill_1year.sh --stop    # 只停不启
```

### 日志

全部落到 `logs/weekend_backfill/<平台>.log`,看板会自动识别(`_BACKFILL_LOG_MAP`)
并用这个路径替代 watcher 的 `watch.log`。

```bash
tail -f /home/ygwang/trading_agent/logs/weekend_backfill/*.log
```

### 预计运行时间

| 平台 | DB 现存 | 1 年预计新增 | 耗时 |
|---|---|---|---|
| alphapai | ~52k | 0~数千(watcher 已覆盖近 6 月) | 30 min - 2 h |
| jinmen meetings | ~14k | 数千 | 1~3 h |
| jinmen oversea | ~980k | 持续入库(firehose) | 10+ h |
| meritco | ~2.3k | 数百 | 30 min |
| funda | ~3.7k | 已覆盖,基本 0 | 10~30 min |
| gangtise | ~7.4k | 数千 | 2~5 h |
| acecamp | ~1.7k | 数百 | 30 min |

看板里「数据跨度」一列直接显示每平台 DB 实际覆盖的 `release_time` 最老→最新,
一眼看回填效果。

---

## 4. 双流入库看板(2026-04-22 新功能)

<http://127.0.0.1:8080> 上面板分两栏:

- **🟢 实时入库流**(左) —— `release_time` 在最近 24h 内的条目,即 watcher 刚抓到的新鲜内容
- **🟡 回填入库流**(右) —— `release_time` 超过 24h 的条目,即 backfill 挖回的历史

头部徽章显示 `实时 scraper: N · 回填 scraper: M · 合计: T`。

两栏各自 2s 轮询 `/api/recent?mode=realtime|backfill`,互不干扰。

---

## 5. 常见故障

| 症状 | 根因 | 处理 |
|---|---|---|
| `./start_crawler_realtime.sh` 报 `docker 容器 crawl_data 没在跑` | docker 没启 | `docker start crawl_data` |
| 看板打不开 (`curl localhost:8080` 502/超时) | 代理拦截 | `curl --noproxy '*' http://127.0.0.1:8080`;或 `unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY` |
| 点看板「🚀 实时」按钮后 backfill 消失 | 已修 (2026-04-22):`start_all` 只 kill `--watch` 进程,backfill (无 `--watch`) 免疫 | 老版本升级后重新跑本脚本即可 |
| 某平台 watcher 全部红 `✗ 已过期` | token 过期 | `/data-sources` 页面点该平台「实时查看」,浏览器里走一次登录;然后脚本不用重启,monitor 会在 60s 内重新探活 |
| backfill 启动后"入库=0" | 该平台近 1 年数据已经被 watcher 抓满,backfill 全跳过(正常) | 看板「数据跨度」列,如果跨度已经 >365 天就是正常;如果 <365 天但没动,看 `logs/weekend_backfill/<平台>.log` 找原因 |
| backfill 进程掉了 | 不挂守护,可能平台 401 风控死掉 | `--status` 看哪个没了;看日志排查;`--force` 重起 |

---

## 6. 关于"两个工程分开"

**实时 vs 回填一直是独立的两条管道**,脚本只是把它们打包成两个开关:

- watcher 的寿命 = 长期常驻,由 monitor 管理(PPID 指向 monitor)
- backfill 的寿命 = 一次性扫到头就退,由 `nohup + disown` 脱离终端(PPID=1 init)

**`start_all` bulk-kill 只杀 watcher(有 `--watch`),不动 backfill(无 `--watch`)**。
所以:

- 任何时候可以重跑 `start_crawler_realtime.sh`(等价于点看板「🚀 实时」按钮),
  backfill 依然在跑
- 任何时候可以重跑 `start_crawler_backfill_1year.sh`,watcher 不受影响
- 两个脚本真正"互斥"的只有 MongoDB 和各平台的 API 账号 —— 慢节流参数已经留
  够余量(watcher 1.5s / backfill 5s),双流同跑不会触发风控

---

## 7. 完整停掉所有爬虫

```bash
bash scripts/start_crawler_realtime.sh --stop
bash scripts/start_crawler_backfill_1year.sh --stop

# 验证
ps -ef | grep -E "scraper\.py|crawler_monitor\.py" | grep -v grep
# 应该空
```

---

## 8. 相关文件

```
scripts/
├── start_crawler_realtime.sh        # 本脚本 A
├── start_crawler_backfill_1year.sh  # 本脚本 B
└── CRAWLER_SCRIPTS.md               # 本文档

logs/
├── crawler_monitor.log              # monitor 主日志
├── crawler_monitor.pid              # monitor PID
└── weekend_backfill/                # 8 个 backfill 各自日志
    ├── alphapai.log
    ├── jinmen.log
    ├── jinmen_reports.log
    ├── jinmen_oversea_reports.log
    ├── meritco.log
    ├── funda.log
    ├── gangtise_summary.log
    └── acecamp.log

crawl/crawler_monitor.py             # 监控后端 + 一键按钮
crawl/<platform>/scraper.py          # 各平台爬虫入口
crawl/<platform>/logs/watch_*.log    # 各平台 watcher 日志
```

详见 `crawl/CRAWLERS.md`(§3 启动约定 / §5 监控看板 / §8 反爬)。
