# 进门外资研报 (oversea) 全量回灌

> **Status (2026-04-22):** Phase 1 metadata 扫描进行中 · PID **2901530** · ETA ~10h @ 44 id/s
> **Script:** `crawl/jinmen/download_oversea_pdfs.py`
> **Writes:** `jinmen.oversea_reports` MongoDB + `/home/ygwang/crawl_data/jinmen_pdfs/`

---

## 1. 背景

- `json_oversea-research_search` 列表 API 返回 top 10,000 条。我们的实时 watcher 早已 saturate 这个窗口,再往深处就翻不到。
- 平台实际存量远不止 10,000 —— 抽样 `researchId` 1 → 1,669,000 几乎全是真实报告,估 60% 命中率,对应**~1M 可下载外资研报**。

---

## 2. 关键发现 (漏洞)

### 2.1 `json_oversea-research_preview` 无视付费墙

端点 `POST https://server.comein.cn/comein/json_oversea-research_preview`,
body `{"researchId": N}`:

- 对任意 `N ∈ [1, 1_669_000]` 都返 `{code:0, data:{...}}`,包含完整 metadata
- 即使 `data.isUnlock == 0` (付费墙),**响应照样带 `homeOssPdfUrl`**
- 无 `dailyLimit / dailyQuota` 强制限额(字段都是 0)

### 2.2 OSS PDF 公开可读 (与 domestic 相同)

`homeOssPdfUrl` 形如 `https://database.comein.cn/original-data/pdf/mndj_report/<md5>.pdf`。

实测 **完全不需要任何 auth**:
```bash
curl https://database.comein.cn/original-data/pdf/mndj_report/a644fb1f642370578ccb1d3c33dbce2b.pdf
# → HTTP 200, application/pdf, 721 KB
```

同 bucket(`comein-crawler.oss-accelerate.aliyuncs.com`)也承载 domestic
`mndj_<int>.pdf`,全部 public-read。

### 2.3 与 domestic mndj 的关键差别

|  | domestic `mndj_<id>.pdf` | oversea `<md5>.pdf` |
|---|---|---|
| 文件名生成 | 顺序 `int` | 随机 MD5 (256-bit) |
| 可枚举? | ✓ 直接扫 ID 就行 | ✗ 256-bit 空间 |
| 发现 URL 途径 | **公开 OSS** | 走 `json_oversea-research_preview` |
| 发现 URL 要 auth? | 不要 | **要** (无 token → `code:201` 拒绝) |
| 下载 PDF 要 auth? | 不要 | **不要** |

**所以 oversea 无法像 domestic 那样"完全无账号"裸跑**,只能 hybrid:
Phase 1 用账号拿 URL,Phase 2 无账号下 PDF。

---

## 3. 架构

```
┌────────────────────────────────────────┐
│  Phase 1: metadata scan (16 → 8h)      │  ← 要账号
│                                         │
│  for rid in [1, 1_700_000]:            │
│    POST json_oversea-research_preview   │
│      with JM_AUTH_INFO                  │
│    upsert jinmen.oversea_reports        │
│      (_id = rid)                        │
│                                         │
│  Concurrency: 10 threads                │
│  Observed: 58 id/s (5× smoke test)      │
│  Valid hits: ~60% (rest "已失效" gaps)  │
└─────────────┬──────────────────────────┘
              │
              ▼  每条 doc 带 homeOssPdfUrl
┌────────────────────────────────────────┐
│  Phase 2: PDF 下载 (可并行,无账号)       │  ← auth-free
│                                         │
│  for doc in oversea_reports:            │
│    GET doc.original_url                 │
│      (OSS direct, no cookies)           │
│    write pdf_local_path                 │
│                                         │
│  Concurrency: 50+ (OSS bandwidth cap)   │
│  Can shard across ECS IPs               │
└────────────────────────────────────────┘
```

**为什么拆两阶段?**

- Phase 1 的 1.67M API 调用**全部经过我们的账号**,是唯一的风控风险面
- Phase 1 跑完后,**所有 URL 已入库**。Phase 2 可以从 MongoDB 直接取
  `original_url`,任意 IP 任意并发下 PDF,账号零风险
- 任一阶段被打断都可以 `--resume` 续跑(progress JSON 30s 持久化)

---

## 4. 数据模型

Phase 1 写入的 doc 结构与 scraper 的 `dump_oversea_report` 完全一致,
下游 `enrich_tickers` / backend `/api/jinmen-db/oversea-reports` / 前端 列表页
全部自动兼容。

```js
{
  _id: <researchId>,                    // 1 ... ~1_669_000
  id: <researchId>,
  report_id: "mndj_rtime_961",          // 平台内部 ID
  title: "高盛 - 戴文能源(us.DVN) - ...", // 中文(有则)或英文
  title_cn: "高盛 - 戴文能源 ...",
  title_en: "Goldman Sachs - DVN ...",
  release_time: "2024-07-08 00:00",
  release_time_ms: 1720368000000,
  organization_name: "高盛",            // 中文
  organization_name_en: "Goldman Sachs",
  report_type: "公司研究",
  language_list: ["英语"],
  country_list: ["美国"],
  stocks: [{market, code, fullCode, name, ...}],   // 同 scraper schema
  industries: [...],
  authors: [...],
  summary_md: "### 交易详情\n**增强在威利斯顿盆地的规模。** ...",
  original_url: "https://database.comein.cn/original-data/pdf/mndj_report/585c2c….pdf",
  link_url: "https://mobile.comein.cn/mobile/oversea_research_report?id=1",
  is_realtime: false,
  pdf_num: 7,
  pdf_local_path: "",                   // Phase 2 才填
  pdf_size_bytes: 0,                    //   同上
  preview_result: {...},                // 完整 preview API 响应
  crawled_at: ISODate,
  _canonical_extract_source: "jinmen_oversea_bulk",
}
```

---

## 5. 运行 / 监控 / 中止

### 5.1 启动 Phase 1

```bash
cd /home/ygwang/trading_agent/crawl/jinmen

nohup setsid env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY \
                 -u all_proxy -u ALL_PROXY \
  python3 -u download_oversea_pdfs.py \
    --start 1 --end 1700000 --concurrency 10 --skip-pdf \
  > /home/ygwang/trading_agent/logs/backfill/jinmen_oversea_metadata.log 2>&1 &
```

### 5.2 监控

```bash
# 实时日志 (tqdm 进度条)
tail -F /home/ygwang/trading_agent/logs/backfill/jinmen_oversea_metadata.log | tr '\r' '\n'

# 进度 checkpoint
cat crawl/jinmen/_progress_oversea.json | jq

# DB 累计数
docker exec crawl_data mongosh --quiet --eval \
  'print(db.getSiblingDB("jinmen").oversea_reports.countDocuments({}))'
```

### 5.3 中止 / 续跑

```bash
kill <PID>                      # 安全停 (progress JSON 已落盘 ≤ 30s)
# 重启时加 --resume 从 last_scanned_id+1 续
python3 download_oversea_pdfs.py --start 1 --end 1700000 \
        --concurrency 10 --skip-pdf --resume &
```

### 5.4 Phase 2 (PDF 下载, 元数据扫完后)

```bash
# 方式 A: 同脚本去掉 --skip-pdf,从头扫一遍,已有 meta 的只补 PDF
python3 download_oversea_pdfs.py --start 1 --end 1700000 --concurrency 30

# 方式 B: 自己写个小脚本从 MongoDB 拉 original_url,curl 并发下,
#         不走平台 API,完全 auth-free 可换 IP
```

---

## 6. CLI flags 速查

| flag | 语义 | 默认 |
|---|---|---|
| `--start N` | 起始 `researchId` | 1 |
| `--end N` | 结束 `researchId` | 1,700,000 |
| `--concurrency N` | 线程数 | 10 |
| `--skip-pdf` | 只写 metadata,不下 PDF | off(下 PDF) |
| `--force` | 强制重抓已入库条目 | off(跳过) |
| `--resume` | 从 progress JSON 续跑 | off |
| `--pdf-dir` | PDF 存放目录 | `/home/ygwang/crawl_data/jinmen_pdfs` |
| `--mongo-uri` / `--mongo-db` | Mongo 连接 | `localhost:27017` / `jinmen` |

---

## 7. 状态分类 (脚本输出里的 `stats`)

| tag | 含义 |
|---|---|
| `downloaded` | 新增 doc + PDF 成功(只在非 `--skip-pdf` 档出现) |
| `meta_only` | `--skip-pdf` 档下的成功入库 |
| `skipped_existing` | 已入库且已有 PDF,跳过 |
| `skipped_meta` | `--skip-pdf` 下已入库,跳过 |
| `invalid` | preview 返 `code:500 "外资研报已失效"`(平台内部 gap) |
| `error` | HTTP / 解密 / 超时,可重试 |
| `pdf_fail` | metadata 成功但 PDF 下载失败(`not a PDF` / HTTP 非 200) |

---

## 8. 已知边界

1. **有效 ID 范围约为 1 → 1,669,000**。`1_700_000` 以上全返 `code:500 "外资研报已失效"`,
   脚本会记 `invalid` 然后继续(不重试)。每隔几天重跑时可以把 `--end` 往上顶
   以吃到新加的数据(每天约 +100 新 ID)。
2. **~40% IDs 是 `invalid`**(平台历史 gap / 撤销)—— 不是我们的 bug。实际
   有效命中率抽样 60%。
3. **Phase 1 不下 PDF 的原因**:OSS 下载是带宽大头(~1M × 500KB ≈ **500 GB**)。
   先把 metadata 压到 MongoDB,方便后续挑选(按 orgName / country / dateRange / ticker)
   再决定下哪一批。全量下也可以,但建议用 Phase 2 专脚本并发拉高。
4. **账号风控** 是 Phase 1 唯一风险点。我们的 JM_AUTH_INFO (TOKEN_1deadd…) 已在
   watcher 里持续使用 18h+ 未被风控,concurrency 10 应当也安全。被风控时脚本
   会把后续调用写到 `error` 但不崩 —— 观察 log 里 `err=` 计数,>10% 就降并发
   或停下来等 2h。

---

## 9. 改动清单 (2026-04-22)

- **新增** `crawl/jinmen/download_oversea_pdfs.py` —— bulk backfiller
- **新增** `crawl/jinmen/_progress_oversea.json` —— runtime checkpoint (gitignore)
- **新增** `crawl/jinmen/OVERSEA_BULK_DOWNLOAD.md` —— 本文件
- `scraper.py` 未改(这个脚本独立运行,不侵入 realtime watcher / `--oversea-reports`
  主流程)

---

## 10. 后续打算 (Phase 2)

Metadata 扫完后,跑 Phase 2。可选方案:

**A. 复用同脚本 (最简单)**
```bash
python3 download_oversea_pdfs.py --start 1 --end 1700000 --concurrency 30
# 会跳过 meta=有 + pdf=有 的, 只下没下过的 PDF
```
但仍然逐 ID 调 preview 再下 PDF,会重复发 preview 调用。

**B. 专门的 PDF-only 小脚本 (不经过账号,可分 IP 并发)**
```python
for doc in db.oversea_reports.find({'pdf_local_path': {'$in': [None, '']}}, {'_id':1,'original_url':1,'release_time_ms':1,'report_id':1}):
    if doc['original_url']:
        r = requests.get(doc['original_url'], timeout=60)
        # write to jinmen_pdfs/YYYY-MM/mndj_rtime_<N>.pdf
```
无账号调用,可拉到 50+ 并发,甚至 rsync 到阿里云 cn-hangzhou ECS 跑分流。

建议走 **B**,和 domestic `download_mndj_pdfs.py` 同架构。Phase 1 完后单独写。
