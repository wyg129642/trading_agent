# 爬虫监控机器人使用指南

监控 8 个数据平台, 掉线自动告警, 凭证可热更新.

> **重要**:凭证管理的首选入口是后端 `/data-sources` 页面 (管理员可见)。
> 那里能可视化看到 8 平台 token 健康 / 每日入库柱图 / 一键启停 watcher /
> 走 Playwright 自动登录刷新凭证。飞书机器人仍然提供掉线告警和 `/token`
> 手动热更新,作为兜底。

---

## 一、告警群 (新 Webhook)

平台健康状态 **变化时** 机器人自动推送告警卡片到这个群:

- 🚨 红色: 爬虫进程挂了 (`stopped`)
- ⚠️ 橙色: 进程还在但 API 401/参数错误 (`warn`, 凭证失效常见)
- ✅ 绿色: 状态恢复正常 (`ok`)

告警卡片里会直接带上 **恢复方法** (发什么命令到指令群, 去哪儿拿新凭证).

**不会**收到整点定期播报或指令回复.

检查周期: 5 分钟.

---

## 二、指令群 (App 机器人 Trading_agent爬虫报告)

在群里直接发消息给机器人:

### 查看当前所有平台状态
发送任一关键词, 机器人立即回复状态卡片:
```
状态      报告      快照
status    /status   report    /report
```

### 更新凭证并自动重启某个爬虫 (掉线补救)
```
/token <平台名> <完整凭证>
```

机器人会: 校验凭证 → 写入 `credentials.json` → kill 旧进程 → 起新进程 → 回复新 PID.

---

## 三、八个平台一览

| 平台 | 子分类 | 凭证类型 | /token 名 |
|---|---|---|---|
| **meritco · 久谦** | 纪要 / 研究 | 请求头 token (32 hex) | `meritco` |
| **jinmen · 进门** | 纪要 / 研报 | base64 `JM_AUTH_INFO` | `jinmen` |
| **alphapai · Alpha派** | 路演 / 券商点评 / 券商研报 / 社媒微信 | JWT (`eyJ...`) | `alphapai` |
| **thirdbridge · 高临** | 专家访谈 | AWS Cognito Cookie 串 | `thirdbridge` |
| **funda · Funda.ai** | 研究 / 8-K / 业绩会 / 情绪 | 整条浏览器 Cookie | `funda` |
| **gangtise · 港推** | 纪要 (7 分类) / 研报 / 首席观点 | localStorage `G_token` (UUID) | `gangtise` |
| **acecamp** | 观点 / 纪要 / 调研 | Cookie 三件套 (`user_token`+Rails+`aceid`) | `acecamp` |
| **sentimentrader** | Smart/Dumb · Fear/Greed · QQQ Optix | email + 密码 (走 `/data-sources` UI 自动登录) | — |

---

## 四、凭证怎么拿

### Meritco (久谦)
1. 浏览器登录 https://research.meritco-group.com/forum
2. F12 → **Network** → 随便点一个 XHR (如 `forum/select/list`)
3. Request Headers 里的 **`token:`** 行, 复制后面的 32 位 hex
4. 格式: `03dc5c5312c44039ba086a4ac5497b63`
5. 命令: `/token meritco 03dc5c53...`

### AlphaPai
1. 浏览器登录 https://alphapai-web.rabyte.cn
2. F12 → **Application** → **Local Storage** → `https://alphapai-web.rabyte.cn`
3. 找 key **`USER_AUTH_TOKEN`**, 双击 Value 选中整串复制
4. 格式: `eyJhbGci...xxxx` (三段点分隔)
5. 命令: `/token alphapai eyJhbGci...`

### ThirdBridge (高临)
1. 浏览器登录 https://forum.thirdbridge.com
2. F12 → **Network** → 任意请求 → Request Headers → **`Cookie:`** → 复制整条
3. 格式: `AWSELBAuthSessionCookie-0=A3+Y...一直到末尾`
4. 命令: `/token thirdbridge AWSELBAuth...`

### Funda
1. 浏览器登录 https://www.funda.ai
2. F12 → **Application** → **Cookies** → `www.funda.ai`
3. 或 Network → 任意请求 → Request Headers → **`Cookie:`** 行 → 整条复制
4. 关键字段: `session-token=...`
5. 命令: `/token funda session-token=...`

---

## 五、网页仪表盘

实时可视化, **5 个平台 · 今日新增 · 最近更新 · 健康状态**:

```
http://192.168.31.97:8080/
```

功能:
- 平台卡片 · 点击 tab 切换子分类
- 每个子分类的最近 5 条入库、日志 tail、checkpoint 详情
- 页面每 10 秒自动刷新
- 健康状态 🟢 ok / 🟡 warn / 🔴 stopped

从办公网 (同 192.168.31.x 网段) 可直接访问. 外网需 SSH 端口转发:
```bash
ssh -L 8080:127.0.0.1:8080 -J <跳板> ygwang@milkt-n-01
# 浏览器打开 http://127.0.0.1:8080/
```

---

## 六、各环节刷新频率

| 层 | 频率 | 做什么 |
|---|---|---|
| **前端页面自刷新** | 10 秒 | 浏览器 `setTimeout(() => location.reload())`, 重新拉 HTML |
| **API 响应** | 按需 | 每次 `/` 或 `/api/status` 被访问才计算一次, 不缓存 |
| **snapshot() 查 DB+扫日志** | 跟随 API | 查各平台 `_state` collection + 读 log tail 80 行判健康 |
| **飞书告警 watchdog** | 5 分钟 | 每 5 分钟扫一次全平台, 只在状态变化 (ok ↔ warn/stopped) 时推告警 |
| **各爬虫轮询源网站** | 5 分钟 | 每个 watcher 自己的 `--interval 300`, 平台间错开几秒 |

**如何理解**:
- 打开仪表盘 → 每 10 秒刷一次看最新状态 (实时感最强)
- 飞书告警群 → 只在状态真实变化时发, 最细粒度 5 分钟 (噪音最少)
- 爬虫自己 → 每 5 分钟访问一次各源站, 有新内容才入库

改频率的方法:
- 改仪表盘刷新: 改 `HTML_TEMPLATE` 里的 `setTimeout(() => location.reload(), 10000)` 数字 (毫秒)
- 改 watchdog: `start_health_watchdog(..., check_interval_s=300)` 参数
- 改爬虫轮询: kill 后用新的 `--interval <秒数>` 重启

---

## 七、常见问题

**Q: 新 webhook 群最近收到 ⚠️ 告警, 怎么办**
A: 看告警卡片下方的"恢复方法"段 — 告诉你发什么命令、去哪取凭证. 按提示在**指令群** (不是告警群) 发 `/token ...`.

**Q: `/token` 为啥在新 webhook 群不生效**
A: webhook 是"单向推送"机器人, 收不到群消息. 所有指令只能发在指令群 (原 Trading_agent爬虫报告).

**Q: 今日新增为什么有时看起来少**
A: 用的是内容 **`release_time`/`publish_time`** (平台发布日), 不是入库时间. 今天发布的才算今天.

**Q: 告警说 `auth/401` 或 `参数错误`**
A: 凭证过期了. 按"四、凭证怎么拿"取新的, 发 `/token ...`.

**Q: 告警说 `已 Xm 无轮次`**
A: 爬虫进程活着但卡住了 (不轮询). 管理员需要 kill + 重启.

**Q: 想立即看一次**
A: 在指令群发 `状态`.

---

## 八、管理员参考

**服务路径**
- 监控服务: `/home/ygwang/trading_agent/crawl/crawler_monitor.py`
- 爬虫目录: `alphapai_crawl/` · `meritco_crawl/` · `third_bridge/` · `jinmen/` · `funda/` · `gangtise/` · `AceCamp/` · `sentimentrader/`
- 分板块日志: `<平台目录>/logs/watch_<category>.log` (如 `watch_roadshow.log` / `watch_reports.log`)
- 历史补齐日志: `logs/weekend_backfill/<platform>.log`
- 凭证: `<平台目录>/credentials.json`
- 监控服务日志: `crawl/logs/logs_monitor_8080.log`
- 健康基线 (防启动瞬间重复告警): `crawl/logs/monitor_health.json`

**关键配置 (`.env`)**
```
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/0c2248fd-feb7-498f-94e0-405862b602cb
FEISHU_APP_ID=cli_a96b12e7fff81cb1
FEISHU_APP_SECRET=M1DDKRJgSLsifGPjibjrxbWPsbAQYJ5l
FEISHU_RECEIVE_ID_TYPE=chat_id
FEISHU_RECEIVE_ID=          ← 留空, 告警走 webhook; 填 chat_id 则走 App API
```

**手动启爬虫 (模板 — 实时档参数)**
```bash
cd /home/ygwang/trading_agent/crawl/<平台目录>
setsid nohup python3 -u scraper.py --watch --resume \
  --since-hours 24 --interval 60 \
  --throttle-base 1.5 --throttle-jitter 1.0 \
  --burst-size 0 --daily-cap 0 \
  --category <cat>  \
  > logs/watch_<cat>.log 2>&1 < /dev/null &
```

推荐直接用 `crawler_monitor.py::start_all('realtime')`  (Web UI 「🚀 实时」按钮),
自动覆盖全部 17 个 watcher 并注入正确参数。

**重启监控服务**
```bash
cd /home/ygwang/trading_agent/crawl
fuser -k 8080/tcp
setsid nohup python3 -u crawler_monitor.py --web --port 8080 \
  > logs/logs_monitor_8080.log 2>&1 < /dev/null &
```

**查看所有 scraper 进程**
```bash
for pid in $(pgrep -f "python3 -u scraper.py"); do
  echo "pid=$pid cwd=$(readlink /proc/$pid/cwd)"
done
```

**CLI 仪表盘 (终端实时刷新)**
```bash
python3 crawler_monitor.py            # rich live UI
python3 crawler_monitor.py --json     # 一次性 JSON
```

**立刻推一张卡片 (绕过自动告警逻辑, 手动发)**
```bash
python3 crawler_monitor.py --push-feishu
```
