# SentimenTrader Scraper

Scrapes three paid chart pages the user has a subscription to, stores the
latest snapshot + short history in MongoDB, and exposes it to the frontend
via `GET /api/sentimentrader/indicators`. The frontend renders a 3-card
strip at the top of the Portfolio page.

- **Smart Money / Dumb Money Confidence Spread** (differential) —
  https://users.sentimentrader.com/users/charts/model_smart_dumb_spread
- **Smart Money / Dumb Money Confidence** (raw, dual-line) —
  https://users.sentimentrader.com/users/charts/smart_dumb
- **CNN Fear & Greed Model** —
  https://users.sentimentrader.com/users/charts/model_cnn_fear_greed
- **QQQ Optix** —
  https://users.sentimentrader.com/users/charts/etf_qqq

Only the latest ~750 daily points per series are kept in MongoDB. The API
serves the most recent 90 for the sparkline. A Playwright-captured PNG of
each official Highcharts rendering is also saved to
`/home/ygwang/crawl_data/sentimentrader_images/{slug}.png` so the frontend
can show the real chart (including SPX overlay, threshold lines, navigator).

## Credentials

`credentials.json` (gitignored) next to `scraper.py`:

```json
{ "email": "your@email.com", "password": "yourpassword" }
```

Or use env vars `SENTIMENTRADER_EMAIL` / `SENTIMENTRADER_PASSWORD`.

## Running

```bash
# One-shot refresh (manual)
python3 scraper.py

# Inspect stored snapshot
python3 scraper.py --show-state

# Long-running daily watcher (recommended)
./run_watcher.sh          # start (or restart)
./run_watcher.sh status
./run_watcher.sh stop

# Debug a login issue — see the browser
python3 scraper.py --headful --force-login
```

The scraper logs into `sentimentrader.com/login` with Playwright, persists
the session to `playwright_data/storage_state.json`, and only re-logs in
when the session expires. Each chart page is opened in-context, and
`window.Highcharts.charts[0].series` is read directly — no XHR reverse
engineering needed.

## MongoDB shape

Collection: `sentimentrader.indicators`, one document per indicator,
keyed by slug (`_id == slug`). Fields:

- `slug`, `name`, `source_url`
- `chart_title`, `indicator_name`, `benchmark_name`
- `latest_value`, `latest_ts_ms`, `latest_benchmark_value`
- `history_trimmed` — last 750 `[ts_ms, value]` pairs for the indicator series
- `benchmark_trimmed` — last 750 `[ts_ms, value]` pairs for the price series
- `full_point_count`, `updated_at`

## Scheduling

Data on sentimentrader.com updates once per day after US market close
(~16:00 ET), typically reaching the site 1–3 hours later. For Beijing users
that's overnight into early morning CST.

**Currently installed:** cron entry at 06:00 CST (`crontab -l` to verify):

```cron
0 6 * * * cd /home/ygwang/trading_agent/crawl/sentimentrader && /home/ygwang/miniconda3/envs/agent/bin/python3 scraper.py --retry-until-fresh --max-retries 8 --retry-interval 3600 >> /home/ygwang/trading_agent/logs/sentimentrader_cron.log 2>&1
```

`--retry-until-fresh` makes the scraper compare post-scrape timestamps
against what was already in MongoDB. If the site hasn't published the
new EOD data yet, it sleeps `--retry-interval` (1 h) and tries again,
up to `--max-retries` (8) times — so the window covers 06:00–14:00 CST.
Exits 0 on fresh, 3 if gave up stale (normal on weekends / US holidays),
1 on hard failure. Cron ignores the non-zero exits gracefully.

The old `run_watcher.sh --watch --interval 86400` loop is superseded and
should stay stopped. Kept around only for manual debug runs.
