# brm.comein.cn 纪要 + 研报爬虫系统

从 brm.comein.cn 爬取:

- **AI 纪要** (速览 / 章节概要 / 指标 / 对话) → MongoDB `meetings`
- **研报** (券商报告 + 核心观点 + PDF) → MongoDB `reports` + 本地 `pdfs/YYYY-MM/<reportId>.pdf`

---

## 1. 工作流

### 1.1 业务流程 (日常使用)

```
① 浏览器登录 brm.comein.cn
② F12 → Application → Local Storage → 复制 JM_AUTH_INFO (base64)
③ 粘贴到 scraper.py 顶部, 或通过 --auth / 环境变量 JM_AUTH 传入
④ 启动 Docker MongoDB (首次需要)
⑤ python3 scraper.py --max 200
⑥ 浏览器 http://localhost:8081 查看数据
```

### 1.2 技术流程 (单条会议抓取)

```
parse_auth(JM_AUTH_INFO)              # base64 JSON → uid + webtoken
  ↓
create_session(auth)                  # 带 uid/token 等 HTTP 头
  ↓
fetch_items_paginated(max, page_size) # 分页拉列表 (roadshow-list_summary)
  ↓
for each item:
  summary-info         → aiSummaryId, summaryId
  detail-page-auth     → 权限
  query-summary-points → 速览 (markdown, "可复制"面板)
  list-ai-chapter-summary → 章节概要
  query-summary-index-list → 指标
  summary-content-list → 对话 (精炼版)
  ↓
decrypt_response(r)  # 响应头含 k 字段时 → AES-CBC 解密
  ↓
db.meetings.replace_one(_id=roadshowId, doc, upsert=True)
```

---

## 2. 系统架构

```
┌─────────────────────────┐
│  brm.comein.cn (API)    │
└──────────────┬──────────┘
               │ HTTPS + AES 加密响应
               ▼
┌─────────────────────────┐
│  scraper.py             │  requests + pycryptodome
│  (本机, Python)          │  pymongo
└──────────────┬──────────┘
               │ mongodb://localhost:27017
               ▼
┌─────────────────────────┐
│  crawl_data (Docker)  │  volume: crawl_data
│  mongo:7.0.31           │
└──────────────┬──────────┘
               │
       ┌───────┴───────────────────────────┐
       ▼                                   ▼
┌─────────────────┐                ┌──────────────────┐
│ mongo-express   │                │  mongosh / Compass │
│ localhost:8081  │                │                    │
└─────────────────┘                └──────────────────┘
```

---

## 3. 已接入的 API

### 3.1 会议级接口

| 接口 | 参数 | 用途 | 状态 |
|---|---|---|---|
| `json_roadshow-list_summary` | page, size, type=13 | 分页列表 | ✓ |
| `json_summary_summary-info` | roadshowId | 详情元信息 (含 aiSummaryId) | ✓ |
| `json_summary_detail-page-auth` | roadshowId | 详情页权限 | ✓ |
| `json_summary_query-summary-points` | aiSummaryId | **速览** (markdown 分点) | ✓ |
| `json_summary_list-ai-chapter-summary` | aiSummaryId | 章节概要 | ✓ |
| `json_summary_query-summary-index-list` | aiSummaryId | 指标列表 | ✓ |
| `json_summary_summary-content-list` | summaryId | **对话** (精炼版) | ✓ |

### 3.1a 研报级接口 (新)

对应页面 `https://brm.comein.cn/reportManage/index`。

| 接口 | 参数 | 用途 | 状态 |
|---|---|---|---|
| `json_research_search` | page, size | 研报列表 (按 releaseTime desc) | ✓ |
| `json_research_detail` | id | 研报详情 (完整 summary + originalUrl PDF) | ✓ |
| `database.comein.cn/original-data/pdf/...` | GET | 直接下载 PDF (无需签名) | ✓ |

list item 关键字段:

| 字段 | 用途 |
|---|---|
| `id` | 研报 ID, 主键 (= URL `reportManage/domesticCapitalReportDetail/<id>`) |
| `reportId` | 外部编号 (如 `mndj_27065700`), 用作 PDF 文件名 |
| `releaseTime` | 发布时间戳 (ms) |
| `organizationName` / `organizationList` | 发布机构 |
| `contentTagNames` / `typeName` | 分类标签 (公司研究/行业研究/其它报告...) |
| `summaryPoint` | 核心观点 (列表版, 预览, 会被截断) |
| `ossUrl` | PDF 直链 |
| `pdfNum` | PDF 页数 |
| `isVipResearch` | 是否 VIP 专享 |

detail 额外字段:

| 字段 | 用途 |
|---|---|
| `summary` | 完整核心观点 (比 summaryPoint 长, 对应前端"展开") |
| `originalUrl` | PDF 直链 (同 ossUrl) |
| `industryTagNames` | 行业标签 |
| `companyDtoList` | 关联公司 |
| `originSource` | 来源枚举 |

### 3.2 账户级接口 (一次性)

| 接口 | 用途 | 状态 |
|---|---|---|
| `json_ai-chat_agent-group-list` | AI 智能体分组 | ✓ |
| `json_ai-chat-thought_list` | 思维模板 | ✓ |
| `json_ai-chat-thought_global-switch-status` | 全局开关 | ✓ |
| `json_wallet_mywallet` | 钱包 | ✓ |
| `json_membership_identity-cards` | 会员卡 | ✓ |
| `json_common_get-user-browse` | 浏览历史 | ✓ |

### 3.3 已探测但未拿到数据的接口

| 接口 | 情况 | 可能原因 |
|---|---|---|
| `json_summary_query-summary-ppt-images` | code=1002 | 账号权限不足 / 该会议无 PPT |
| `json_summary_query-summary-word-list` | code=0 data=[] | 账号无逐字稿权限, 或接口名不对 |
| `json_roadshow_noticepagedocument` | data=[] | 无通知文档 |
| `json_roadshow_attachmentlist` | data=[] | 无附件 |
| **原版对话接口** | **未知** | 多次猜测 (`json_ai_streaming_query-ori-text` 等) 均 404. 需要浏览器点击"原版"按钮时抓真实 URL |

### 3.4 解密方案

- 响应头含 `k` 字段时需解密:
  - `k_decoded = base64decode(k).decode()` = `"<32hex>:<13digits>"`
  - `key = MD5(k_decoded + ":" + SALT).hexdigest().upper().encode()` (AES-256 key 32 字节)
  - `SALT = "039ed7d839d8915bf01e4f49825fcc6b"` (从前端 JS 反推)
  - 响应体 = `base64decode(body)`，前 16 字节是 IV，其余为密文
  - AES-CBC 解密后 PKCS7 去填充 → UTF-8 → JSON

---

## 4. MongoDB 数据模型

数据库: `jinmen`

### 4.1 `meetings` collection

```js
{
  _id: <Long> roadshowId,
  roadshowId, summaryId, aiSummaryId, eid, rid,

  // 元信息
  title: String,
  release_time: "YYYY-MM-DD HH:MM",
  organization: String,
  industry: [String],
  stocks: [{name, code, fullCode, market}],
  themes: [String],
  creators: [String],              // 发言人
  guests: [String],                // 嘉宾
  auth_tag, content_types, featured_tag, speaker_tag,
  present_url,

  // 可读文本 (markdown)
  points_md:            "### 1. 业绩回顾...\n- ...",   // 速览
  chapter_summary_md:   "【章节1】(01:30-05:00)\n...",
  indicators_md:        "[指标名] 主体 值 (日期) 原文",
  transcript_md:        "[00:10] 周总: ...\n[00:53] 盛开: ...",

  // 原始结构化数据
  chapters: [...],              // list-ai-chapter-summary 原始返回
  indicators: [...],            // query-summary-index-list
  content_items: [...],         // summary-content-list

  // 原始 API 响应
  list_item: {...},             // 列表接口里该条目的完整数据
  summary_info: {aiSummary, meetSummary, ...},
  detail_auth: {...},           // 权限

  // 统计 / 元
  stats: {"速览字数", "章节", "指标", "对话条数"},
  crawled_at: ISODate
}
```

索引: `title`, `release_time`, `organization`, `crawled_at`

### 4.1a `reports` collection (研报)

```js
{
  _id: <Long> id,                    // research id (同 URL 里的数字)
  id, report_id,                     // report_id = "mndj_27065700" 之类
  title,
  release_time: "YYYY-MM-DD HH:MM",
  release_time_ms: <long>,
  organization_name, organizations: [...],
  type_name,
  content_tags: [...],               // ["公司研究" / "行业研究" / "其它报告" ...]
  industry_tags: [...],
  companies: [...],
  is_vip: Bool,
  pdf_num: Int,                      // PDF 页数
  has_image: Bool,
  origin_source: Int,

  // 可读文本
  summary_point_md,                  // 列表版 (短, 预览)
  summary_md,                        // 详情版 (长, 展开后完整)

  // PDF
  original_url,                      // OSS 直链
  pdf_local_path,                    // pdfs/YYYY-MM/<reportId>.pdf 绝对路径
  pdf_size_bytes,
  pdf_download_error,                // 下载失败原因 (HTTP 码 / magic check 失败等)

  // 原始
  list_item: {...},
  detail_result: {...},

  stats: {摘要字数, 页数, 机构数, 公司数, pdf_大小},
  crawled_at: ISODate
}
```

索引: `title`, `release_time`, `organization_name`, `crawled_at`。

PDF 文件落盘路径: `pdfs/YYYY-MM/<reportId>.pdf` (按发布月份分目录, reportId 为外部唯一编号,
无 reportId 则 fallback 用 `report_<id>` / title 净化后作为文件名)。下载用 `.part` 临时文件 +
`%PDF` 魔数校验 + 原子 `rename`。

### 4.2 `account` collection

```js
{
  _id: "agent-group-list" | "thought-list" | "wallet" | ...,
  endpoint: "json_<mod>_<act>",
  response: {code, data, msg, ...},
  updated_at: ISODate
}
```

---

## 5. 命令行参数

```bash
python3 scraper.py --help
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `--max N` | 无限 | 最多爬 N 条, 默认翻页直到 hasMore=false |
| `--page-size N` | 40 | 每页大小 |
| `--force` | off | 强制重爬已入库的会议 (默认跳过) |
| `--watch` | off | 实时模式, 定时轮询 |
| `--interval N` | 600 | 实时模式间隔秒数 |
| `--auth VALUE` | 脚本内硬编码 | 覆盖 JM_AUTH_INFO (或 env JM_AUTH) |
| `--mongo-uri URI` | `mongodb://localhost:27017` | MongoDB 地址 (或 env MONGO_URI) |
| `--mongo-db NAME` | `jinmen` | 数据库名 (或 env MONGO_DB) |
| `--resume` | off | **增量模式**: 遇到上次已爬过的 top 即停止分页 |
| `--show-state` | off | 打印 checkpoint 后退出 |
| `--reset-state` | off | 清除 checkpoint 后退出 |
| `--today` | off | **统计今天平台内容条数**, 对比本地库, 结果存 `_state` |
| `--date YYYY-MM-DD` | 今天 | 配合 `--today` 指定日期 |
| `--reports` | off | **研报模式** (抓 `reportManage/index` 并下 PDF), 其他所有参数同义复用 |
| `--skip-pdf` | off | 研报模式下跳过 PDF 下载, 只入库元数据 |
| `--pdf-dir PATH` | `./pdfs` | 研报 PDF 存放目录 (或 env `JINMEN_PDF_DIR`) |
| `--clean-reports` | off | 清空 `reports` collection + 研报 checkpoint (不删本地 PDF), 退出 |

示例:

```bash
# ===== 纪要 =====
# 首次入库 200 条
python3 scraper.py --max 200

# 增量抓取 (已有的跳过)
python3 scraper.py --max 500

# 实时监听, 每 5 分钟拉一次新纪要
python3 scraper.py --watch --interval 300

# 重爬已有数据 (token 变了/字段改动后)
python3 scraper.py --max 200 --force

# ===== 研报 =====
# 首次入库 200 篇研报 (含 PDF 下载)
python3 scraper.py --reports --max 200

# 只入库元数据, 不下 PDF
python3 scraper.py --reports --max 200 --skip-pdf

# 增量抓取研报 (按 id 锚点)
python3 scraper.py --reports --resume --watch --interval 600

# 仅抓今日研报 (时间窗 24h)
python3 scraper.py --reports --since-hours 24

# 清空研报库重来 (不删本地 PDF)
python3 scraper.py --clean-reports
python3 scraper.py --reports --max 500

# 统计今日研报
python3 scraper.py --reports --today

# 自定义 PDF 存放目录
python3 scraper.py --reports --max 100 --pdf-dir /data/jinmen_pdfs

# 用远程 MongoDB
python3 scraper.py --mongo-uri mongodb://user:pass@host:27017 --mongo-db mydb
```

### 5.1 进度条

运行时 tqdm 实时显示百分比/速率/ETA/后缀统计:

```
抓取:  60%|██████    | 120/200 [02:30<01:40, 1.25s/条] 新增=85 跳过=32 失败=3
  ✓ [1781448423634172] 申万宏源｜...  速览2094字 章节10 指标60 对话31条
  · [1748120051377404] 光大海外 | ...  已存在, 跳过
  ✗ [...] ...  ERR: connection timeout
```

### 5.2 断点 / 增量恢复

`COL_STATE` 里的 `_id: "crawler"` document 记录了:

```js
{
  top_roadshow_id:            // 上次爬取时列表的第 1 条 id (增量锚点)
  last_processed_roadshow_id: // 最后处理的会议 id (每条更新)
  last_processed_at:
  last_run_end_at:
  last_run_stats: {added, skipped, failed},
  in_progress:                // 中断后 True 表示上次未跑完
}
```

三种恢复场景:

| 场景 | 命令 | 行为 |
|---|---|---|
| 中断后重跑 (同一批) | `python3 scraper.py --max 500` | 靠 `_id` upsert 去重, 已入库的直接跳过 |
| 增量爬新纪要 | `python3 scraper.py --resume` | 列表翻页遇到 top_roadshow_id 就停, 只抓新的 |
| 重置锚点 | `python3 scraper.py --reset-state` | 清 checkpoint, 下次全量 |
| 查看当前状态 | `python3 scraper.py --show-state` | 打印 checkpoint 和统计 |

### 5.3 当日内容统计 `--today`

统计平台**今天**发布了多少纪要, 和本地库对比缺口, 结果持久化到 `_state` collection:

```bash
python3 scraper.py --today                # 今天
python3 scraper.py --today --date 2026-04-15   # 指定日期
```

输出:
- 平台当日总数 / 本地已入库 / 待入库
- 按机构 Top 10
- 按行业 Top 10
- 按标签分布

历史统计存在 `db._state` 的 `_id = "daily_YYYY-MM-DD"` 下, 可以:

```js
db._state.find({_id: /^daily_/}).sort({date: -1})  // 所有日期统计
db._state.aggregate([                               // 近 7 日趋势
  {$match: {_id: /^daily_/}},
  {$sort: {date: -1}}, {$limit: 7},
  {$project: {date:1, total_on_platform:1, in_db:1}}
])
```

---

## 6. 查看数据

| 方式 | 地址 / 命令 |
|---|---|
| **Web UI (推荐)** | http://localhost:8081  (账号 `admin` / 密码 `admin`) |
| mongosh | `docker exec -it crawl_data mongosh jinmen` |
| MongoDB Compass | 连接串 `mongodb://localhost:27017`, 选 `jinmen` |

SSH 远程场景: `ssh -L 8081:localhost:8081 -L 27017:localhost:27017 your-server`

---

## 7. 已知不稳定问题

### 7.1 认证过期 (最常见)
- `JM_AUTH_INFO` 是浏览器 localStorage 里的 token, **有效期通常几小时到几天**
- 过期表现: 接口返回 `code=401/403` 或解密失败
- **修复**: 重新浏览器登录 → F12 复制 JM_AUTH_INFO → 替换进脚本
- **改进方向**: 实现自动刷新 (需要找到 refresh-token 接口)

### 7.2 响应加密方案漂移
- 解密 salt `039ed7d839d8915bf01e4f49825fcc6b` 硬编码, 来自前端 JS 反推
- 如果网站更新加密 (换 salt / 换算法), 整个爬虫会全面失效
- **症状**: `decrypt_response()` 抛异常, 响应看起来像二进制乱码
- **修复**: 重新抓前端 JS 分析

### 7.3 "原版"对话接口未解决
- 截图里左侧有"**原版**"和"**精炼版**"切换按钮, 但切换时的真实 API 名字没找到
- 现在爬到的是"**精炼版**"（书面化改写过, 非 ASR 原话）
- 已试过的候选 (全部 404 / 空):
  `json_ai_streaming_query-ori-text`, `json_summary_query-summary-word-list`,
  `summary-content-list` 加 `type/isOpt/oriLanguage/refinedLanguage/version` 等参数
- **修复路径**: 浏览器点"原版"按钮 → F12 Network 找新请求 URL

### 7.4 限流 / 风控
- 当前节流是固定 `time.sleep(0.3)`
- 没有对 429 / 403 的识别和指数退避
- 一次爬几千条可能触发账号风控 (IP 封禁或强制登出)
- **改进**: 加 retry + exponential backoff, 识别 429 后暂停

### 7.5 `hasAISummary` 标志不可信
- 列表返回里的 `hasAISummary=0` 的条目, 实际 `summary-info` 仍返回有效 `aiSummaryId`
- 当前已绕过 (无论标志值都去请求详情)
- 但其他类似标志 (`hasAISummaryIndex`, `docExplanationTag`) 可能也不可信, 未验证

### 7.6 接口名漂移
- 前端 endpoint 命名混乱, 下划线/连字符混用:
  - `json_summary_summary-content-list` ✓
  - `json_ai-chat-thought_global-switch-status` ✓
  - `json_ai-chat_agent-group-list` ✓ (注意 `ai-chat` vs `ai-chat-thought`)
- `headers_for()` 用 `split("_", 2)` 解析 app/mod/act, 对部分接口可能错误但侥幸工作
- 如果服务端重命名任何接口, 对应功能直接失效

### 7.7 权限相关返回空
- PPT 图片 / word-list / 附件 / 通知文档都返回空或 1002
- 当前账号可能不是钻石会员, 部分数据无权限
- 换更高级别账号可能拿到

### 7.8 串行速度慢
- 每条会议 ~3 秒 (6 个请求 + 0.3s 节流), 1000 条约 1 小时
- 没有并发
- **改进**: `concurrent.futures.ThreadPoolExecutor(max_workers=3)` (注意风控)

### 7.9 无断点续传 / 失败重试
- 某条会议抓失败只打印 [ERR] 继续, 不会自动重试
- 中途 Ctrl+C 后重跑: 已入库的跳过 (靠 mongo `_id`), 失败的会重试
- 但如果失败原因是 token 过期, 后续全部失败到结束

### 7.10 MongoDB 安全
- 本地 Docker 无认证 (`mongodb://localhost:27017` 任何人可读写)
- 端口 `27017` 和 `8081` 绑在 `0.0.0.0` (所有接口)
- **如果机器有公网 IP 必须用防火墙/SSH 隧道保护, 否则数据被外部拿走**

### 7.11 数据持久化
- 数据在 Docker volume `crawl_data`
- `docker rm crawl_data` 不会删 volume, 数据保留
- 但 `docker volume rm crawl_data` 或整机重装就丢失
- **建议**: 定期 `mongodump`:
  ```bash
  docker exec crawl_data mongodump --db jinmen --out /dump
  docker cp crawl_data:/dump ./backup_$(date +%Y%m%d)
  ```

### 7.12 列表 `hasMore=True` 无上限
- 测试到 page=20 仍 hasMore=True, 可能有几千条历史
- 全量爬取可能数小时, 且占用账号持续调用
- **建议**: 用 `--max` 分批次进行

---

## 8. 目录结构

```
jinmen/
├── README.md              # 本文档
├── scraper.py             # 主脚本
└── images/                # 调研截图 (分析接口时参考)
```

## 9. 依赖

```bash
pip install requests pycryptodome pymongo openpyxl
```

## 10. 恢复 / 重建环境

```bash
# MongoDB 容器
docker run -d --name crawl_data -p 27017:27017 \
  -v crawl_data:/data/db docker.1ms.run/library/mongo:7

# Web UI
docker network create jinmen-net
docker network connect jinmen-net crawl_data
docker run -d --name crawl_data-ui --network jinmen-net -p 8081:8081 \
  -e ME_CONFIG_MONGODB_URL=mongodb://crawl_data:27017 \
  -e ME_CONFIG_BASICAUTH_USERNAME=admin \
  -e ME_CONFIG_BASICAUTH_PASSWORD=admin \
  docker.1ms.run/library/mongo-express
```
