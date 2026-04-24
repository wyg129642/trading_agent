# The Information — 探针结果 (2026-04-24 公开探针)

## 站点技术栈
- **Rails 13.4.0 Pro 3.3.1** + Webpacker (`ti-assets.theinformation.com/packs/js/*`)
- **Cloudflare** 全站前置 (站点 key `0x4AAAAAABImzmbslpav3Ywl`, Turnstile)
- **Ahoy** gem 做分析 (`/ahoy/visits`, `/ahoy/events`)
- **BlueConic** CDP 做个性化 (`d712.theinformation.com`) — 爬虫可全部无视
- 内容 **SSR 在 HTML 里**, 没有客户端 `/api/` 或 `/graphql`

## 重要 endpoint

| 路径 | 状态 | 用途 |
|---|---|---|
| `/` | ✅ 200 | 首页,最新 9 条卡片,全 SSR HTML |
| `/articles?page=N` | ✅ 200 | 主列表,**每页 9 条**, 最大 `?page=678` → **约 6100 条历史归档** |
| `/articles/<slug>` | ✅ 200 | 详情页, slug-based URL (无数字 ID) |
| `/newsletters/the-briefing` | ✅ 200 | 每日简报 (The Briefing by Martin Peers) |
| `/sign-in` | ❌ 403 (CF) | **Cloudflare 直接 block** — 需真浏览器交互 |
| `/tech` | ❌ 403 (CF) | 同上,应该用 `/features/tech` 或 `/topics/tech` |

## 分页
- 简单 `?page=N`, 整数递增
- `<div class="pagination">` 里直接列出 `page=1..5` + `page=678`
- **无 cursor / since_id**, 传统整数翻页

## 数据位置 (在列表页 HTML 里)
- 文章卡: `<a href="/articles/<slug>">`
- CSRF token: `<meta name="csrf-token" content="...">` — 登录 POST 需要
- `<script type="application/json">` 内嵌 Rails props:
  - `env.CLOUDFLARE_SITE_KEY` — Turnstile site key (登录时需要)
  - `env.GOOGLE_CLIENT_ID` — OAuth (备选登录方式)
  - `currentUser.isLoggedIn`, `currentUser.canViewForYou` — 登录状态
  - `isContentPaywalled: false/true` — 付费墙状态
  - `railsEnv: production`, `rorVersion: 13.4.0`

## 付费墙 (匿名状态下推断)
- 首页 + 列表页的 **标题 + 作者 + 图片** 都可见
- 详情页的 body 大概率被截断到前几段 + "Subscribe to read more" 按钮
- `isContentPaywalled` 字段可直接用作入库标志位
- 已登录订阅用户会拿到全文

## 认证流
- `/sign-in` 403 → 必须走 **Playwright 真浏览器**(有 Turnstile captcha)
- 登录成功后 cookie 包含 Rails session(`_session_id` 或类似) + `cf_clearance`(Cloudflare 清关)
- cf_clearance TTL 约 30 min~12 h, 需定期 Playwright 轻访问刷新

## 爬虫架构结论

**两阶段策略**:

1. **登录阶段**(Playwright): `auto_login.py` + email + password + Turnstile
   - 输出: `credentials.json` 带完整 cookie jar (含 `cf_clearance` + session)
2. **抓取阶段**(httpx + 已有 cookie): 速度 5-10× Playwright
   - 列表: GET `/articles?page=N` HTML → BeautifulSoup parse `a.href=/articles/*`
   - 详情: GET `/articles/<slug>` HTML → parse `<article>` body
   - 定期 refresh: 每 30 min 跑一次 Playwright 无头轻访问刷 cf_clearance

## 反爬注意事项
- Cloudflare 对 `/sign-in` 严格 → 必须 Playwright + stealth
- 首页 / 列表页 可直接 HTTP 请求(带 cf_clearance)
- 4 个 URL 连续打 40s 内就有一个触发 403 → **节奏要慢**, 建议 base 5s / jitter 3s
- Turnstile = 真人交互, 可以先试 Playwright headful 模式手工过一次

## 未知 (待认证后验证)
- 付费墙具体表现(truncate 长度?)
- 有无类似 `/api/articles.json` 的内部 JSON endpoint
- Briefing / TITV / Weekend / Org Charts 各分区的 URL 结构
