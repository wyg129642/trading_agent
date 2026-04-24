# Meritco Research Forum 爬虫

从 [`https://research.meritco-group.com/forum`](https://research.meritco-group.com/forum) (久谦中台) 抓取论坛内容 (纪要 / 专家访谈 / 业绩交流 / 券商路演 / 研报 / 活动 / 久谦自研) 并存入 MongoDB。

`scraper.py` 跟 `crawl/jinmen/scraper.py` 与 `crawl/alphapai_crawl/scraper.py` 同风格: 同一套 CLI 参数、checkpoint、`--watch`、`--today` 等。

---

## 1. 工作流

```
① 浏览器登录 https://research.meritco-group.com/forum
② F12 → Network → 任一 /matrix-search/forum/... XHR
③ Request Headers 复制 token (32 位十六进制)
④ 写入 credentials.json  {"token": "<32hex>"}
⑤ python3 scraper.py --max 200          # 抓前 200 条 type=2
⑥ python3 scraper.py --watch --interval 300   # 5 分钟轮询
```

token 也可通过 `--auth` 或环境变量 `MERITCO_AUTH` 传入。

---

## 2. 接口说明

### 2.1 列表 (POST `/matrix-search/forum/select/list`)

body 字段必须跟前端一模一样, 少一个就 `code=500 "参数错误"`。重要字段:

| 字段 | 值 |
|---|---|
| `forumId` | `null` |
| `page` / `pageSize` | 整数 |
| `module` | `"CLASSIC_ALL_SEARCH"` |
| `sortColumn` | `"articleDate"` |
| `sort` | `2` |
| `type` | `1/2/3` (见下) |
| `platformArr` | `type=2` 时传 7 个专业内容标签, 其他 type 传 `[]` |
| `totalPage` / `companyUniqueKeyForInfoCenter` | `""` (容易漏) |
| `platform` | `"RESEARCH_PC"` |

`type` 含义:
- `1` = 活动 / 活动预告
- `2` = 专业内容 (纪要+研报+其他报告, 默认)
- `3` = 久谦自研

返回 shape: `{code: 200, result: {forumList: [...], total, highlight}}`。

### 2.2 详情 (POST `/matrix-search/forum/select/id?forumId=<id>`)

body 只需要 `{"platform": "RESEARCH_PC"}`。返回含 `insight/content/keywordArr/forumExpertDetailList` 等。

### 2.2.1 PDF 附件下载 (POST `/matrix-search/forum/pdfDownloadWatermark`)

从 `forum/select/id` 返回的 `pdfUrl` 字段是一个 JSON 字符串 (形如
`[{"uid":...,"name":"...pdf","size":...,"url":"<enc>"}]`), 其中 `url` 是 base64
编码的加密 OSS 地址. 前端点击附件时会打开 `/forumPDF?query=...` 页面, 该页面
用此加密 url POST 到:

```
POST /matrix-search/forum/pdfDownloadWatermark
body: {"pdfOSSUrlEncoded": "<enc>"}
```

返回 `application/pdf` 流 (带水印, 防止二次分发). 此端点**无 5 天时间窗口限制** —
前端 UI 对 5 天内的文档只显示"预览"按钮, 但后端 API 对任意时间的文档都直接返
PDF 流. `scraper.py` 的 `fetch_pdf_bytes()` 直接按此规则下载, 无需走前端路由.

**注意**: 该端点不在 X-My-Header RSA 签名名单里, 只要 `token` header 有效即可
调用.

### 2.3 账户级 (只拉一次)

| 名字 | path | 方法 |
|---|---|---|
| `user-info` | `/meritco-user/research/info/get` | **POST** |
| `follow-list` | `/meritco-chatgpt/research/user/follow/list` | POST |
| `industries` | `/matrix-search/forum/company/industries` | **GET** |
| `calendar` | `/matrix-search/forum/calendar` | POST |

### 2.4 X-My-Header 签名 (2026-04 起必须)

前端 `be3b` 模块的请求拦截器对特定接口的 `X-My-Header` 做 **RSA PKCS#1 v1.5** 签名后 base64。2026-04 后名单从 `research/article/*` 扩展到 `forum/select/list` 和 `forum/select/id`。**不带 X-My-Header, 后端统一返回 `code=500 message="参数错误"`, 极易误判为 body 字段缺失。**

签名规则:
- list: `X-My-Header = base64(RSA(pubKey, token + keyword + page))`
- detail: `X-My-Header = base64(RSA(pubKey, token + forumId))`

公钥硬编码在前端 JS 里, `scraper.py` 也同样硬编码。如果将来前端换公钥, 重新从 JS 里抠 `h.setPublicKey(v)` 的 `v` 即可。

### 2.5 "参数错误" 故障排查清单

按以下顺序排查:

1. **X-My-Header 缺失或错了** — 最常见原因。`scraper.py` 已内建签名, 如果你自己发请求记得也要加。
2. **公钥变了** — 前端换了 RSA 公钥。从新版 JS 的 `be3b` 模块反推。
3. **body 字段缺失** — 尤其 `totalPage` 和 `companyUniqueKeyForInfoCenter` 两个后加的字段。
4. **token 真的过期** — 一般会返回 HTTP 401 / `code=401xxx`, 但偶尔表现为 "参数错误"。调 `user-info` (`--show-state` 自动做) 确认。

---

## 3. MongoDB 数据模型

数据库默认 `meritco`, 三个 collection:

### 3.1 `forum` — 每条论坛条目一个 document

```js
{
  _id: <forumId>,
  id: <forumId>,
  forum_type: 1 | 2 | 3,
  title,
  release_time: "YYYY-MM-DD HH:MM",
  release_time_ms: <long>,
  meeting_time, create_time, operation_time,
  industry, type, language, author, operator,
  expert_information, expert_type_name, report_type_name,
  related_targets: [...], authors: [...], experts: [...],
  keyword_arr: [...], keyword_black_arr: [...],
  hot_flag: Bool, is_top: Int,
  hc_conf_id, hc_conf_number, meeting_link, pdf_url,

  // PDF 附件 (若有)
  pdf_attachments: [
    {uid, name, size, type, url,
     pdf_rel_path, pdf_local_path, pdf_size_bytes,
     pdf_downloaded_at, pdf_download_error?}
  ],
  // 跨平台一致的单 PDF 字段 (取第一个附件, 99%+ meritco doc 只有 1 个附件)
  pdf_rel_path,        // 相对 pdf_dir, 如 "2026-04/3104_xxx.pdf"
  pdf_local_path,      // 本机绝对路径
  pdf_size_bytes,      // 实际落盘字节
  pdf_download_error,  // 失败原因; 成功则为 ""

  // 可读文本 (HTML → 纯文本)
  summary_md, topic_md, background_md, expert_content_md,
  insight_md,      // 速览 (html 扁平化)
  content_md,      // 正文 (html 扁平化)

  // 原始数据
  list_item: {...},
  detail_result: {...},

  stats: {正文字数, 速览字数, 摘要字数, 专家数, 关联标的},
  crawled_at: ISODate
}
```

索引: `title`, `release_time`, `type`, `industry`, `crawled_at`。

### 3.2 `account`

一次性元数据 (`user-info`, `follow-list`, `industries`, `calendar`)。`--force` 可刷新。

### 3.3 `_state`

- `_id = crawler_type{N}` — 每个 forumType 的 checkpoint (`top_id`, `last_processed_id`, `last_run_stats`, 时间戳)
- `_id = daily_type{N}_{YYYY-MM-DD}` — `--today` 统计快照

---

## 4. 命令行参数

| 参数 | 默认 | 说明 |
|---|---|---|
| `--type N` | `2` | forumType (1=活动 / 2=专业内容 / 3=久谦自研) |
| `--max N` | 无限 | 最多爬 N 条 |
| `--page-size N` | `40` | 每页大小 |
| `--force` | off | 强制重爬已入库 + 刷新 account |
| `--resume` | off | 增量模式, 遇到已知 top_id 停 |
| `--watch` | off | 定时轮询 (Ctrl+C 退出) |
| `--interval N` | `600` | `--watch` 间隔秒数 |
| `--show-state` | off | 打印 checkpoint + token 健康检查, 退出 |
| `--reset-state` | off | 清空所有 checkpoint / daily 统计, 退出 |
| `--today` | off | 统计今日平台内容 vs 本地库, 存 `_state` |
| `--date YYYY-MM-DD` | 今天 | 配合 `--today` |
| `--delay N` | `1.5` | 请求间延迟 (带抖动) |
| `--max-retries N` | `5` | 429/5xx/超时重试次数 |
| `--auth TOKEN` | `credentials.json` / env `MERITCO_AUTH` | 覆盖 token |
| `--mongo-uri URI` | `mongodb://localhost:27017` | 或 env `MONGO_URI` |
| `--mongo-db NAME` | `meritco` | 或 env `MONGO_DB` |
| `--pdf-dir PATH` | `/home/ygwang/crawl_data/meritco_pdfs` | PDF 下载目录, env `MERITCO_PDF_DIR` 可覆盖; 设为 `""` 禁用 |
| `--skip-pdf` | off | 只记录附件元数据, 不下载 PDF |
| `--force-pdf` | off | 强制重下本地已存在的 PDF |
| `--pdf-only` | off | Backfill 模式: 不抓列表, 只扫已入库文档补齐缺失 PDF |

### 示例

```bash
# 首次入库前 200 条专业内容
python3 scraper.py --max 200

# 活动
python3 scraper.py --type 1 --max 100

# 增量监听, 每 5 分钟
python3 scraper.py --watch --interval 300 --resume

# 今日统计
python3 scraper.py --today

# token 健康检查 + 入库总数
python3 scraper.py --show-state

# 远程 MongoDB
python3 scraper.py --mongo-uri mongodb://user:pass@host:27017 --mongo-db mydb

# Backfill 所有历史文档的 PDF 附件 (单跑, 不抓新内容)
python3 scraper.py --pdf-only --throttle-base 1.5 --daily-cap 200

# 禁用 PDF 下载, 仅抓文本
python3 scraper.py --max 200 --skip-pdf

# 自定义 PDF 目录
MERITCO_PDF_DIR=/mnt/data/meritco_pdfs python3 scraper.py --max 200
```

---

## 5. 依赖

```bash
pip install httpx pymongo tqdm pycryptodome
```

Python 3.9+。`pycryptodome` 用于 X-My-Header RSA PKCS1v1.5 签名。

---

## 6. 目录结构

```
meritco_crawl/
├── scraper.py          # 主脚本
├── credentials.json    # token (gitignored)
├── README.md
├── promts.md           # 平台接口参考
└── .gitignore
```

---

## 7. 安全

- `credentials.json` 已 gitignore, 不要提交到仓库
- 默认 `mongodb://localhost:27017` 无认证; 公网部署必须用防火墙 / SSH 隧道
- 一个账号只跑一个 scraper 进程, 避免触发风控
