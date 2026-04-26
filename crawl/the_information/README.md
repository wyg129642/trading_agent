# The Information (theinformation.com) 爬虫

> **状态**: 正在接入,当前阶段 = CDP 探针摸 API. 9th platform (紧随 sentimentrader).
>
> **DB**: `foreign-website.theinformation_posts` (与 SemiAnalysis 同库,本机 ta-mongo-crawl :27018).
> 注:DB-name 决策在 2026-04 远端 Mongo 时期,因 u_spider 无 createDatabase 权限只能 co-host;
> 2026-04-26 迁回本机后保持原布局以避免改 schema。

## 为什么 CDP 先行

The Information 是英文订阅新闻站(~$399/yr),付费墙严,API 是 Next.js 内部 endpoint,
不发公开 docs. 上来就写 scraper 盲猜端点 = 浪费时间. 所以先:

```bash
# 1. 无 auth 探针 — 摸首页 + 付费墙标记
python3 cdp_probe.py --url https://www.theinformation.com/ \
    --label homepage_anon --dwell 20

# 2. 公开页面批量扫 (列表页 / briefing / about 之类)
python3 cdp_probe.py --urls https://www.theinformation.com/,\
https://www.theinformation.com/articles,\
https://www.theinformation.com/briefing,\
https://www.theinformation.com/events \
    --label public_tour --dwell 12

# 3. 拿到凭证后带 cookie 再跑 (绕付费墙 — 真实 API surface 才暴露)
#    填 credentials.json 的 "cookie" 字段后加 --creds
python3 cdp_probe.py --url https://www.theinformation.com/articles \
    --label auth_articles --creds --dwell 25

# 读结果
ls debug_screenshots/           # 时间序列 JPEG
cat debug_network.*.jsonl | jq  # 所有 XHR/fetch
cat debug_html/*_final.html     # 最终 DOM (CSS selector 初选)
```

## 输出文件

| 文件 | 来源 | 用途 |
|---|---|---|
| `debug_screenshots/<label>_NN.jpg` | Playwright 每 3s 截图 | 看 SPA 渲染 / 付费墙 UI |
| `debug_network.<label>.jsonl` | `page.on("response")` | grep 找 API endpoint |
| `debug_html/<label>_final.html` | `page.content()` | CSS selector 起点 |

## 凭证文件

```json
{
  "cookie": "session=...; other=...",
  "user_agent": "Mozilla/5.0 ..."
}
```

`credentials.json` 已在 .gitignore. 填写方法:
1. 浏览器登录 https://www.theinformation.com/
2. F12 → Application → Cookies → 全选复制成 `key=value; key2=value2` 格式粘进 `cookie` 字段
3. `navigator.userAgent` 抄出来粘 `user_agent`

登录后台 = `/data-sources` 凭证管理页 (auto_login.py 接入后)

## 待办

- [ ] 公开 probe 跑完后总结 API surface
- [ ] 拿到订阅凭证
- [ ] 实现 auto_login.py(email + password via Playwright)
- [ ] 实现 scraper.py(articles 主分类,briefing 次优先)
- [ ] 接入 antibot.py / crawler_monitor.py / crawler_manager.py
- [ ] backend/app/api/the_information_db.py
- [ ] 前端页面 + 侧边栏 + i18n
