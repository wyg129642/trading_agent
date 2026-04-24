# Meritco Research Forum Crawler

Bulk downloader for `https://research.meritco-group.com/forum` — the 久谦中台 professional research platform.

Uses the site's own `/matrix-search/forum/` API (POST JSON) with a session token.

## Files

```
meritco_crawl/
├── crawler.py          Main crawler script (httpx-based)
├── credentials.json    Your token (gitignored — never commit)
├── status.sh           Quick progress check
├── .gitignore          Excludes credentials + data
└── data/               Output (gitignored)
    ├── lists/          Paginated list responses (type{N}_page_XXXX.json)
    ├── details/        Per-item detail JSON ({forumId}.json)
    ├── progress.json   Checkpoint (resumable)
    ├── crawl.log       Log file
    └── crawler.pid     PID of running crawler (if any)
```

## Usage

### First run
```bash
python3 crawler.py             # crawl forumType=2 (default: all professional content)
```

### Common options
```bash
python3 crawler.py --type 2                # forumType (2 = 专业内容 纪要+研报+其他)
python3 crawler.py --delay 3               # slower (safer under load)
python3 crawler.py --page-size 50          # items per list request
python3 crawler.py --max-list-pages 5      # cap how many list pages to pull
python3 crawler.py --lists-only            # only paginate lists, skip details
python3 crawler.py --details-only          # skip list, re-fetch missing details from saved lists
python3 crawler.py --reset-progress        # ignore checkpoint, start fresh
```

### Background run (long jobs)
```bash
nohup python3 crawler.py > data/crawl.stdout.log 2>&1 &
echo $! > data/crawler.pid
./status.sh                                # check progress anytime
```

### Check progress
```bash
./status.sh
tail -f data/crawl.log
```

### Stop cleanly
```bash
kill -INT $(cat data/crawler.pid)          # graceful (saves checkpoint)
```

### Resume after interrupt
```bash
python3 crawler.py                         # auto-resumes from progress.json
```

## Token refresh (when it expires)

The `token` header expires periodically (hours–days). When the crawler logs
`!!! TOKEN EXPIRED`, re-capture it:

1. Open `https://research.meritco-group.com/forum` in Chrome (must be logged in).
2. Open DevTools → Network → filter XHR → click any request (e.g. `select/list`).
3. In **Request Headers**, find `token: <32-hex-chars>` → copy that value.
4. Paste it into `credentials.json` → `token` field.
5. Re-run `python3 crawler.py` — it picks up from the checkpoint.

## Scope note (forumType)

Currently hardcoded to the **professional content** categories via `platformArr`:
纪要 (国内/海外 × 专家访谈/业绩交流/券商路演), 研报 (国内/海外), 其他报告.

To crawl a different section (e.g. `forumType=1` or `3`), run with `--type N`.
You may also want to inspect a fresh HAR from that section to confirm the
`platformArr` values that apply there.

## Safety + polite-crawling notes

- **Single session only**. Don't run this twice in parallel against the same
  account — the server may flag/throttle or revoke your session.
- Default delay is 2s + jitter. Lower it only if you've verified there's no
  rate limit being hit.
- If you see repeated `429` or `5xx`, increase `--delay` and `--max-retries`.
- Token + content likely watermarked to your user ID — treat the dump as private.

## Re-running against a different category

If you want to crawl another section (e.g. `/forum?forumType=1`):

1. Load that page in the browser and export HAR (or just confirm the URL works).
2. Update `DEFAULT_PLATFORM_ARR` in `crawler.py` if the new section uses different
   platform tags (inspect a `/select/list` request's body to see).
3. Run `python3 crawler.py --type N`.

Data for each forumType is kept under its own list-page prefix
(`type{N}_page_XXXX.json`) and details are shared (IDs are global).
