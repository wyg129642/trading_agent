# alphapai-web.rabyte.cn 多分类爬虫

从 [Alpha派 AlphaPai](https://alphapai-web.rabyte.cn) 爬取四大类投研内容（**会议/路演**、**券商点评**、**券商研报**、**社媒/微信公众号**），写入 MongoDB。

数据用于：(1) 大模型训练; (2) 网站讯息实时更新。

---

## 1. 工作流

```
① 浏览器登录 https://alphapai-web.rabyte.cn
② F12 → Application → Local Storage → 复制 USER_AUTH_TOKEN (JWT)
③ 粘贴到 scraper.py 顶部 USER_AUTH_TOKEN, 或通过 --auth / 环境变量 JM_AUTH 传入
④ 启动 MongoDB (默认 mongodb://localhost:27017)
⑤ python3 scraper.py --max 100              # 各分类抓 100 条
⑥ python3 scraper.py --watch --interval 600 # 实时模式
```

`Authorization` 头直接传 JWT 字符串（**不是** `Bearer xxx`）。`uid` 自动从 JWT payload 解析出来。

---

## 2. 已接入的接口

### 2.1 列表接口（POST，分页 `pageNum`/`pageSize`）

| 分类 | 接口 path | 列表 ID 字段 | 是否稳定 |
|---|---|---|---|
| roadshow（会议） | `reading/roadshow/summary/list` | `id`, `roadshowId` | **❌ 每次请求重新加密** |
| comment（点评） | `reading/comment/list` | `id` (`HCMT...`) | ✓ 稳定 |
| report（研报） | `reading/report/list` *(注：不是 v2)* | `id` | **❌ 每次请求重新加密** |
| wechat（社媒） | `reading/social/media/wechat/article/list` | `id` (`RAR...`), `supplierId` | ✓ 稳定 |

注意：`list_item.content` 是 **180 字截断** 的列表摘要；完整核心观点 ("展开" 后的文本) 在
详情接口的 `data.content`，脚本自动用详情版覆盖顶层 `content` 字段。

返回 shape 统一：
```json
{
  "code": 200000,
  "message": "SUCCESS",
  "data": { "total": <int>, "list": [...], "tip": null }
}
```
`code=200000` 表示成功；`page` / `size`（无 Num/Size 后缀）会被服务端忽略，必须用 `pageNum` / `pageSize`。
列表按发布时间倒序，无 `hasMore` 字段，**判停条件 = `len(list) < pageSize`** 或命中已知 dedup id。

### 2.2 详情接口（GET）

| 分类 | 接口 path | 参数 |
|---|---|---|
| roadshow | `reading/summary/detail` | `id` |
| comment | `reading/comment/detail` | `commentId`（**注意：不是 `id`**） |
| report | `reading/report/detail` | `id` |
| report PDF | `reading/report/detail/pdf` | `id` + `version` → 返回**相对路径字符串** |
| wechat | `reading/social/media/wechat/article/detail` | `id` + `supplierId` |

研报 PDF 下载：
- `/reading/report/detail/pdf?id=<id>&version=<version>` 返回 `reading-server/YYYY-MM-DD/report/<file>.pdf` 形式的相对路径
- 完整下载 URL = `https://alphapai-storage.rabyte.cn/report/<urlencoded-path>?authorization=<JWT>&platform=web`
- 请求必须带 `Referer: https://alphapai-web.rabyte.cn/` 和 `Origin: https://alphapai-web.rabyte.cn`，否则 403
- 脚本内 `download_report_pdf()` 负责落盘到 `--pdf-dir/<YYYY-MM>/<basename>.pdf`，写入魔数校验 (`%PDF`) 成功后原子重命名

### 2.3 发现的坑

- **`report/list/v2` 始终返回 500000**，必须用旧版 `report/list`。
- **roadshow 和 report 的 `id` 是会话级加密 token**：每次拉列表都是新值，但之前拉到的旧 id 仍可用作 detail 请求。⇒ dedup 不能靠 `id`，必须用 `SHA1(category|title|publish_time)`。
- **comment 和 wechat 的 `id` 是稳定 DB ID**（`HCMT...`、`RAR...`），可直接当 `_id`。
- 翻太深时单页返回数 < pageSize（实测 roadshow page 2 返回 33 条）—— 说明服务器对快速翻页有截断，请配合 `--watch` 持续低频拉取。
- `wechat/article/count` 和 `comment/count/today` 接口当前返回 500000 / 404000 —— 不可用。
- `Cookie` 不是必需的，只发 `Authorization` 头即可。

### 2.4 账户/元数据接口（首次抓一次，存到 `account` collection）

| 接口 | 用途 |
|---|---|
| `reading/report/type/list` | 研报 13 种类型代码 |
| `reading/report/list/selector` | 研报筛选项（type / 国家 / 语言） |
| `reading/wechat/home/wechat/info` | 微信主页信息 |

---

## 3. MongoDB 数据模型

数据库默认 `alphapai`，集合：

| collection | 内容 | `_id` 策略 |
|---|---|---|
| `roadshows` | 会议/路演 | `SHA1(roadshow|title|publish_time)` |
| `comments` | 券商点评 | item.id（`HCMT...`） |
| `reports` | 券商研报 | `SHA1(report|title|publish_time)` |
| `wechat_articles` | 社媒文章 | item.id（`RAR...`） |
| `account` | 账户元数据 | 接口名 |
| `_state` | checkpoint + 当日统计 | `crawler_<cat>` / `daily_<date>` |

每条业务文档结构：
```js
{
  _id: <stable id>,
  category: "roadshow" | "comment" | "report" | "wechat",
  title: String,
  publish_time: "YYYY-MM-DD HH:MM",
  raw_id: <服务端原始 id, 可能为加密 token>,
  list_item: {...},      // 列表接口原始返回 (其中 content 是 180 字截断)
  detail: {...},         // 详情接口原始返回 (含展开后的 content 全文)
  web_url: "https://alphapai-web.rabyte.cn/...",  // 人类阅读 URL
  crawled_at: ISODate,
  // 常用提取字段(便于 mongo 查询)
  supplierId, publishInstitution, institution, stock, industry,
  analyst, analysts, url, content, accountName, accountId,

  // 研报独有
  pdf_flag: Bool,                     // 是否有 PDF
  pdf_rel_path: "reading-server/YYYY-MM-DD/report/xxx.pdf",
  pdf_local_path: "/abs/path/pdfs/YYYY-MM/xxx.pdf",
  pdf_size: Int,                      // 下载字节数
  pdf_error: "http_403 | not_pdf ...", // 失败时才有
  htmlContent, summaryCnHtml, summaryEnHtml, pageNum, reportType, hasPermission
}
```

研报 `content` 会**自动取 detail.content** (展开后完整核心观点, 通常 1-2k 字)，覆盖列表里的 180 字截断版。

索引：`title`, `publish_time`, `category`, `crawled_at`。

---

## 4. 命令行参数

```bash
python3 scraper.py --help
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--category {all,roadshow,comment,report,wechat}` | `all` | 限定单分类 |
| `--max N` | 无限 | 最多爬 N 条（**单分类**），默认翻页直到 `len(list) < page-size` |
| `--page-size N` | 40 | 每页大小 |
| `--force` | off | 强制重爬已入库的内容（默认跳过） |
| `--watch` | off | 实时模式，定时轮询 |
| `--interval N` | 600 | 实时模式间隔秒数 |
| `--auth VALUE` | 脚本内硬编码 | 覆盖 `USER_AUTH_TOKEN`（或 env `JM_AUTH`） |
| `--mongo-uri URI` | `mongodb://localhost:27017` | MongoDB 地址（或 env `MONGO_URI`） |
| `--mongo-db NAME` | `alphapai` | 数据库名（或 env `MONGO_DB`） |
| `--resume` | off | **增量模式**：遇到上次已爬过的 top dedup_id 即停止分页 |
| `--show-state` | off | 打印 checkpoint 和各 collection 总数后退出 |
| `--reset-state` | off | 清除 `crawler_*` checkpoint（保留 `daily_*` 统计） |
| `--today` | off | 统计今日各分类平台条数对比本地库，结果存 `_state` |
| `--date YYYY-MM-DD` | 今天 | 配合 `--today` 指定日期 |
| `--pdf-dir PATH` | `./pdfs` | 研报 PDF 下载目录 (env `ALPHAPAI_PDF_DIR`) |
| `--skip-pdf` | off | 只记录 `pdf_rel_path`, 不下载 PDF 文件 |
| `--clean-reports` | off | 删除 `reports` 集合 + `crawler_report` checkpoint 后退出 (本地 PDF 文件保留，需手动 `rm -r pdfs`) |

### 示例

```bash
# 首次入库各分类前 100 条
python3 scraper.py --max 100

# 单分类（研报）抓全量
python3 scraper.py --category report

# 增量监听，每 10 分钟拉一次新内容
python3 scraper.py --watch --interval 600 --resume

# 重爬已有数据（如修复字段后）
python3 scraper.py --max 50 --force

# 当日统计
python3 scraper.py --today
python3 scraper.py --today --date 2026-04-15

# 远程 MongoDB
python3 scraper.py --mongo-uri mongodb://user:pass@host:27017 --mongo-db mydb

# 清掉旧研报 → 重新抓取 + 下载 PDF
python3 scraper.py --clean-reports
python3 scraper.py --category report --max 200

# 只抓研报元数据不下载 PDF (快速入库)
python3 scraper.py --category report --max 500 --skip-pdf
```

### 4.1 Checkpoint 行为

`_state` collection 的 `_id: "crawler_<category>"` 文档：

```js
{
  top_dedup_id:        // 上次抓到的最新条目 dedup_id
  last_dedup_id:       // 最后处理的 dedup_id
  last_processed_at:
  last_run_end_at:
  last_run_stats: { added, skipped, failed },
  in_progress: Bool,   // 中断后 True 表示上次未跑完
  updated_at:
}
```

恢复场景：

| 场景 | 命令 | 行为 |
|---|---|---|
| 中断后重跑 | `python3 scraper.py --max 200` | 靠 `_id` upsert 去重 |
| 增量爬新内容 | `python3 scraper.py --resume` | 翻页遇到 `top_dedup_id` 即停 |
| 重置锚点 | `python3 scraper.py --reset-state` | 清 checkpoint，下次全量 |
| 查看状态 | `python3 scraper.py --show-state` | 打印 checkpoint 和总数 |

### 4.2 当日统计 `--today`

针对每个分类，按发布时间扫描列表直到时间早于目标日期 0 点，统计平台当日总数 vs 本地已入库。结果持久化到 `_state` 的 `_id = "daily_YYYY-MM-DD"`。

---

## 5. 依赖

```bash
pip install requests pymongo tqdm
```

Python 3.8+。

---

## 6. 已知不稳定问题

### 6.1 认证过期
- `USER_AUTH_TOKEN` 是 JWT，30 天有效期（`exp` 字段）。脚本启动时会打印过期时间。
- 过期表现：所有接口返回 `code=401xxx` 或 401。
- **修复**：浏览器重新登录 → F12 → `localStorage.USER_AUTH_TOKEN` → 替换。

### 6.2 翻页深度限制
- 实测 roadshow `pageNum=2` 仅返回 33 条（不足 40）。服务端可能对快速翻深页做了截断。
- **建议**：`--max` 配合 `--watch --resume` 持续低频拉取，比一次性翻几百页更稳。

### 6.3 部分接口返回 500000 / 404000
- `report/list/v2`、`flow/information/list/report/recommend`、`comment/count/today`、`comment/calendar`、`social/media/wechat/article/count`、`stock/follow/group/list`、`share/permissions/query` 等当前不可用，账号权限不足或接口未上线。
- 已在 `account` collection 标记 `code=xxx`，不影响主流程。

### 6.4 `id` 不稳定（roadshow / report）
- 每次请求列表，`id` 都是新加密 token；旧 id 仍可用作 detail 调用。
- 因此用 `SHA1(category|title|publish_time)` 做 dedup `_id`，**注意若标题被改写会被当成新内容**。

### 6.5 限流 / 风控
- 当前固定 `time.sleep(0.25)` 节流，未识别 429。
- 一次抓几千条建议加大间隔或拆批。

### 6.6 MongoDB 安全
- 默认 `mongodb://localhost:27017` 无认证。
- 公网环境必须用防火墙 / SSH 隧道。

---

## 7. 目录结构

```
alphapai_crawl/
├── README.md           # 本文档
└── scraper.py          # 主脚本
```
