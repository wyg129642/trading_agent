# AceCamp Scraper (`crawl/AceCamp/`)

抓取 [acecamptech.com](https://www.acecamptech.com) 的两类内容到 MongoDB
`acecamp` 数据库,跨 2 个 watcher 进程并行 (`articles` / `events`)。

| Collection | 内容 | _id 前缀 | 当前规模 |
|---|---|---|---|
| `articles` | 观点 / 纪要 (含正文 + transcribe) | `a<int>` | 765 (平台 ~24k+) |
| `events` | 调研 / 专家会 (会议元数据) | `e<int>` | 111 (平台 ~5k+) |

## 鉴权 — Cookie 三件套

平台采用 `user_token` (JWT 3 个月有效) + `_ace_camp_tech_production_session`
(Rails) + `aceid` 三段式 cookie,**任何一段缺失都返 401**。

1. 浏览器登录 [www.acecamptech.com](https://www.acecamptech.com)
2. F12 → Network → 任一 `api.acecamptech.com/api/v1/...` 请求 → Request
   Headers → 完整复制 `Cookie:` 整行
3. 写到 `credentials.json`:
   ```json
   { "cookie": "user_token=...; _ace_camp_tech_production_session=...; aceid=..." }
   ```
   或通过 env `ACECAMP_AUTH=<cookie>` / CLI `--auth <cookie>`。

> 凭证管理 UI (`/data-sources` 页面) 也可走 Playwright 自动登录,
> 无须手动复制。

## CLI

照 `crawl/README.md §4` 的统一约定;额外的:

| 参数 | 说明 |
|---|---|
| `--type articles` / `--type events` | 仅抓某类 (生产推荐分进程) |
| `--type all` (默认) | 串行 articles → events |

```bash
python3 scraper.py --show-state              # cookie 健康 + checkpoint
python3 scraper.py --type articles --max 10  # 试跑 10 条
python3 scraper.py --watch --resume \
    --since-hours 24 --interval 60 \
    --throttle-base 1.5 --throttle-jitter 1.0 \
    --type articles --burst-size 0 --daily-cap 0
```

## 数据 schema

参照 `crawl/CRAWLERS.md §5`,所有 collection 共享:
`_id / title / release_time / release_time_ms / content_md / list_item /
detail_result / stats / crawled_at` + 富化字段
`_canonical_tickers / _unmatched_raw / _canonical_tickers_at`。

## 端点速查

| 用途 | Method | Path |
|---|---|---|
| 观点列表 | GET | `/articles/article_list?page=N&per_page=M` |
| 观点详情 | GET | `/articles/article_info?id=X` |
| 调研列表 | GET | `/events/event_list?page=N&per_page=M` |
| 调研详情 | GET | `/events/event_info?event_id=X` |
| 用户信息 | GET | `/account/...` (`--show-state` 健康探测) |

## 已知坑

- **Cookie TTL 不齐**:`user_token` 是 90 天 JWT,`_ace_camp_tech_production_session`
  常规约 7 天就刷,后者过期会直接 401。看到 `--show-state` 报 `auth dead`
  就重新走 §鉴权 流程。
- **AceCamp 节流偏紧**:default `throttle-base=2.5s` (其它平台 3s),
  实际生产 watcher 用 `--throttle-base 1.5 --burst-size 0 --daily-cap 0`
  才能赶上当日产出。
- **detail 异常会跳过**:遇到 `code=500` 不会重试整个 list,只标 `failed`
  然后下一条;失败计数会写到 `_state.last_run_stats`。
