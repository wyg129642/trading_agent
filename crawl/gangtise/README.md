# gangtise/ — open.gangtise.com (港推) 爬虫

抓取港推 (Gangtise) 研究平台的 **纪要 / 研报 / 首席观点** 三类内容到 MongoDB,
并落地研报 PDF, 供 backend `/api/gangtise-db` 和 unified 跨平台检索复用.

- 单文件 `scraper.py` (~780 行), 与 `alphapai_crawl` / `jinmen` 同构.
- 接入 [`crawl/antibot.py`](../antibot.py) 统一节流 / 日限制 / 会话死亡处理.
- 接入 [`crawl/crawler_monitor.py`](../crawler_monitor.py) 实时监控面板.
- 接入 backend [`backend/app/api/gangtise_db.py`](../../backend/app/api/gangtise_db.py) 对外暴露 REST.

## 1. 快速开始

```bash
# 1) 浏览器登录 https://open.gangtise.com
# 2) F12 → Application → Local Storage → 复制 G_token 的值 (UUID)
# 3) 粘贴到 credentials.json
#    或 export GANGTISE_AUTH=<token>
#    或 python3 scraper.py --auth <token>
# 4) MongoDB (默认 localhost:27017)

python3 scraper.py --show-state              # 检查 checkpoint + token 健康
python3 scraper.py --max 10 --skip-pdf       # 各类各跑 10 条 (不下 PDF)
python3 scraper.py                           # 全量 (各类翻到尽头)
python3 scraper.py --type research           # 只抓研报
python3 scraper.py --watch --interval 600 --resume    # 实时增量
python3 scraper.py --today                   # 今日平台 vs 本地库对比
```

## 2. 接口清单

所有接口基址 `https://open.gangtise.com`。
鉴权: `Authorization: bearer <G_token>` + `access_token: <G_token>`。
**Clash 代理会中断 CN CDN, scraper 已 `session.trust_env=False` 强制直连**。

| 分类 | 端点 | 方法 | 关键 payload |
|---|---|---|---|
| 纪要列表 | `/application/summary/queryPage` | POST | `{pageNum, pageSize, sourceList:[100100178,100100263,100100262]}` |
| 纪要正文 | `/application/summary/download?id=<id>&path=<msgText.url>` | GET | 返回纯文本 |
| 纪要元 | `/application/summary/getSourceList` / `getClassifyList` | GET | 源 / 分类字典 |
| 研报列表 | `/application/glory/research/v2/queryByCondition` | POST | `{pageNum, pageSize}` |
| 研报详情 | `/application/glory/research/<rptId>` | GET | 额外带 author 明细 / aflScr |
| 研报 PDF | `/application/download/storage/s3/download/20002/<item.file>?access_token=<token>` | GET | 流式 `%PDF` |
| 观点列表 | `/application/glory/chief/v2/queryOpinionList` | POST | `{condition:{pageNum,pageSize,type:1,keywords:{},industryIds:[],partyIds:[]}}` |
| 观点元 | `/application/glory/chief/industryGroup` | POST | 研究行业字典 |
| 账户 | `/application/userCenter/userCenter/api/account` | GET | token 健康探针 |

**响应体约定**:

- `application/*` 端点: `{code:"000000", msg:"操作成功", status:true, data:{...}}`
- `glory/*` 端点: `{status:true, code:10010000, data:[...], total}`
- 其它失败: `code="999999"` 系统错误 / `code=10019999` Server error.

## 3. 字段 / 返回形状

### 3.1 纪要 (summary)

列表项 (`data.summList[]`):
```js
{
  id: 4927054, dataTable: "tb_3821", msgId: "12053892",
  msgTime: 1776413284621,  summTime: 1776096000000,
  title: "诺禾致源：2026年4月14日投资者关系活动记录表",
  brief: "...",                                 // 500-字截断预览 (含 HTML 残留)
  guest: "董事长、总经理：李瑞强...",
  researcher: null, location: "公司会议室",
  stock: [{gts_code:"688315.SH", scr_abbr:"诺禾致源", ...}],
  block: [...],                                 // 行业标签
  msgText: [{url:"gdp/text/.../*.txt", extension:".txt", ...}],
  source: 100100263, sourceName: "公司公告",
  columnNames: ["高管","业绩会"],
  canSeeAudio: 0, duration: null, ...
}
```

正文: `summary/download` 返回 UTF-8 纯文本 (Q&A 问答纪要).

### 3.2 研报 (research)

列表项 (`data[]`):
```js
{
  id: 20196850, rptId: "435722231424372736",
  title: "...", rptDate: 20260417, pubTime: 1776415502201,
  issuer: "C100000068", issuerStmt: "华创证券",
  rptType: 104801405, rptTypeStmt: "公司研究",
  author: {display:"姚蕾,刘玉洁", detail:[{id,name,extra},...]},
  brief: "...",                                 // 完整核心观点 (列表已全量, 不需二次拉)
  file: "jy_research_data_s1/2026-04-17/829750424143.pdf",
  extension: ".pdf", size: 590213, page: 6,
  headParty: 1, foreignParty: 0, firstCoverage: 0,
  aflScr: {display:"中国儒意(00136.HK)", detail:[{rate,change,scrId,gtsCode,scrAbbr}]},
  aflBlock: {display:"传媒", detail:[{block:[...]}]},
  ...
}
```

### 3.3 首席观点 (chief)

列表项 (`data[]`) — `msgText` 是 **JSON 字符串**, scraper 自动解析:
```js
{
  id: 1469287, msgId: "1330333", msgTime: 1753014103000,
  userId: "P200005850", username: "周尔双",
  partyId: "C100000016", partyName: "东吴证券",
  rsrchDir: "|机械|", rsrchSector: "|122000006|100800110|",
  msgType: -1,
  msgText: "{\"title\":\"拓荆科技...\",\"description\":\"...\",\"content\":null,...}"
}
```

## 4. MongoDB 数据模型

| Collection | 内容 | 主键 | 索引 |
|---|---|---|---|
| `summaries` | 纪要 | `s<id>` (int id 前加 s) | title, release_time, release_time_ms, crawled_at |
| `researches` | 研报 | `<rptId>` (string) | title, release_time, release_time_ms, organization, crawled_at |
| `chief_opinions` | 首席观点 | `c<id>` | title, release_time, release_time_ms, organization, crawled_at |
| `account` | 账户元数据 / 字典 | endpoint 名 | - |
| `_state` | checkpoint + 日统计 | `crawler_<type>` / `daily_YYYY-MM-DD` | - |

统一 doc schema (与 alphapai / jinmen 对齐, 便于 unified 跨平台聚合):

```js
{
  _id, category,
  title, release_time, release_time_ms,
  organization, analyst?/analysts?, stocks:[{code,name,...}], industries:[...],
  brief_md, content_md,
  pdf_local_path?, pdf_size_bytes?, pdf_download_error?,
  list_item, detail_result?,
  web_url, stats, crawled_at
}
```

## 5. CLI 参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--type` | `all` | 选 `summary` / `research` / `chief` / `all` |
| `--max N` | 无限 | 每类最多爬 N 条 |
| `--page-size N` | 40 | 每页大小 |
| `--force` | off | 强制重爬已入库的内容 |
| `--resume` | off | 增量: 翻页遇到 `top_dedup_id` 即停 |
| `--watch` | off | 实时模式, `--interval` 秒轮询一次 |
| `--interval N` | 600 | 实时轮询间隔 |
| `--since-hours N` | 无 | 只抓过去 N 小时内 |
| `--show-state` | off | 打印 checkpoint + token 健康后退出 |
| `--reset-state` | off | 清 crawler_* checkpoint |
| `--today` | off | 扫当日平台 vs 本地库, 存 `daily_YYYY-MM-DD` |
| `--date YYYY-MM-DD` | 今天 | 配合 `--today` |
| `--skip-pdf` | off | 研报只入库元数据, 不下 PDF |
| `--pdf-dir PATH` | `./pdfs` | PDF 落盘目录 |
| `--clean {summary,research,chief}` | - | 清空某类集合 + checkpoint |
| `--auth TOKEN` | credentials.json > env GANGTISE_AUTH > 脚本内 | G_token UUID |
| `--mongo-uri` / `--mongo-db` | `localhost:27017` / `gangtise` | Mongo 连接 |
| `--throttle-*` / `--burst-*` / `--daily-cap` | 见 [antibot](../README.md#7-反爬--反封号-antibotpy) | 节流参数 |

## 6. 后端 + 前端对接

**Config** (`backend/app/config.py`):
```python
gangtise_mongo_uri: str = "mongodb://localhost:27017"
gangtise_mongo_db: str = "gangtise"
gangtise_pdf_dir: str  = "<repo>/crawl/gangtise/pdfs"
```

**API 路由** (`backend/app/api/gangtise_db.py`), 挂在 `/api/gangtise-db`:

| 方法 | 路径 | 用途 |
|---|---|---|
| GET | `/items?category=summary/research/chief&page=1&page_size=20&q=&organization=&ticker=&industry=` | 列表 |
| GET | `/items/{category}/{item_id}` | 详情 (全量 content_md) |
| GET | `/items/research/{item_id}/pdf?download=0` | 研报 PDF 流 (目录穿越防御) |
| GET | `/stats` | 每类 total/today/last_7/top_orgs/crawler_state/daily_platform_stats |

**Unified 跨平台聚合** (`backend/app/api/unified.py`): 已注册 3 条 SourceSpec (summaries/researches/chief_opinions), `/api/unified/by-symbol/<CODE.MARKET>` 自动带入 gangtise 数据 (前提: enrich_tickers.py 跑过给每条 doc 打 `_canonical_tickers`).

**Crawler Monitor** (`crawl/crawler_monitor.py`): `PLATFORMS` 新增 `gangtise` 色条, `SOURCES` 新增三子源 (`gangtise_summary` / `gangtise_research` / `gangtise_chief`), 共用 `logs/watch.log` + 统一进程 regex `gangtise.*scraper\.py.*--watch`.

启动监控:
```bash
cd crawl && python3 crawler_monitor.py --live         # 终端彩色仪表盘
cd crawl && python3 crawler_monitor.py --serve 8080   # Web 仪表盘
```

## 7. 已知坑 / 故障排查

| 症状 | 原因 | 处理 |
|---|---|---|
| `SSLError: UNEXPECTED_EOF_WHILE_READING` | Clash 7890 代理劫持 TLS | 已 `session.trust_env=False`; shell 里也 `unset HTTP_PROXY` |
| `code=999999 "系统错误"` on `/summary/v2/queryPage` | 旧接口 502 变体, v2 对此用户不开放 | scraper 使用 **不带 v2** 的 `/application/summary/queryPage` |
| `code=10019999 "Server error"` on `/chief/v2/queryOpinionList` | 服务器间歇性 500 | scraper 连续空 2 页即停本轮; 下一轮 `--watch` 重试 |
| `queryById?id=X` 500 | 详情端点对所有 id 都挂 | scraper 不依赖它; 正文直接走 `msgText.url` + `/summary/download` |
| 研报 PDF HEAD 返回 206 | 服务端走 Range 断点续传 | scraper 认 `200/206` 都可以 |
| `brief` 有 `<br/>` / `&nbsp;` | 前端渲染时转义 | scraper `_strip_html()` 已清 |
| `--resume` 重复抓同一批 | `top_dedup_id` 没更新 | 检查 `_state.crawler_<type>.top_dedup_id` |
| `--today` 扫不到今日 | token 降级成免费视图 | `--show-state` 查业务接口能否返回正常 data |
| 多次跑撞 401 | 会话被平台撤销 (参见 README §7.6) | 重登浏览器, 更新 credentials.json, `--throttle-base 6 --daily-cap 200` 保守重启 |

## 8. 目录结构

```
gangtise/
├─ README.md          — 本文件
├─ promts.md          — 调试笔记 (URL / localStorage / cookie 样例)
├─ scraper.py         — 单文件爬虫 (~780 行)
├─ credentials.json   — token (不入 git)
├─ .gitignore         — credentials.json / logs/ / pdfs/
├─ logs/              — watch.log (crawler_monitor 读)
└─ pdfs/              — 研报 PDF 按 YYYY-MM 分子目录
```

## 9. 上线前 checklist

```bash
python3 scraper.py --show-state                      # 1. token 健康 ✓
python3 scraper.py --max 3 --skip-pdf                # 2. 小批各类试跑
python3 scraper.py --type research --max 1           # 3. PDF 下载成功
python3 scraper.py --resume --max 10                 # 4. 增量模式工作
python3 scraper.py --today                           # 5. 日统计入 _state

# 后端:
curl -s --noproxy '*' http://127.0.0.1:8000/openapi.json | jq '.paths | keys[] | select(contains("gangtise"))'
./start_web.sh restart                                # 重载 gangtise_db router

# 监控:
cd crawl && python3 crawler_monitor.py --live        # 看 gangtise 行绿灯

# 实时模式 (上线后):
nohup python3 scraper.py --watch --resume --interval 1800 \
    --throttle-base 4 --throttle-jitter 3 --daily-cap 300 \
    > logs/watch.log 2>&1 &
```
