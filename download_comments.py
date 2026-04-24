#!/usr/bin/env python3
"""Download 点评 (comment) data from 进门财经 MCP server, organized by month.

Uses background SSE process to keep session alive while making POST calls.
"""

import json, time, re, os, sys, subprocess, signal, threading
from datetime import datetime, timedelta
import calendar

MCP_URL = "https://mcp-server-global.comein.cn"
MCP_KEY = "cm_cf17e0751ce6457e9d80ee8cfa84d9a4"
OUTPUT_DIR = "/home/ygwang/trading_agent/comment-jinmen"
LOG_FILE = os.path.join(OUTPUT_DIR, "download.log")

# Clean env: strip all proxy vars
CLEAN_ENV = {k: v for k, v in os.environ.items() if 'proxy' not in k.lower()}


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


class MCPSession:
    """Manages an MCP session with a background SSE process to keep it alive."""

    def __init__(self):
        self.endpoint = None
        self.sse_proc = None

    def connect(self):
        """Start SSE in background, init session."""
        self.close()
        # Start SSE process (keeps running to hold session open)
        self.sse_proc = subprocess.Popen(
            ["curl", "-s", "-N",
             f"{MCP_URL}/mcp-servers/mcp-server-brm/sse",
             "-H", f"x-mcp-key: {MCP_KEY}"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env=CLEAN_ENV
        )
        # Read first event to get endpoint
        endpoint = None
        for _ in range(20):  # read up to 20 lines
            line = self.sse_proc.stdout.readline().decode().strip()
            if line.startswith("data:"):
                endpoint = line[5:].strip()
                break
        if not endpoint:
            self.close()
            return False

        self.endpoint = endpoint

        # Initialize
        self._post(json.dumps({
            "jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05", "capabilities": {},
                "clientInfo": {"name": "dl", "version": "1.0.0"}
            }
        }))
        time.sleep(0.2)
        self._post(json.dumps({
            "jsonrpc": "2.0", "method": "notifications/initialized"
        }))
        time.sleep(0.2)
        return True

    def _post(self, body):
        """POST to MCP endpoint via curl."""
        url = f"{MCP_URL}{self.endpoint}"
        try:
            result = subprocess.run(
                ["curl", "-s", "--max-time", "30", "-X", "POST", url,
                 "-H", "Content-Type: application/json",
                 "-H", f"x-mcp-key: {MCP_KEY}",
                 "-d", body],
                capture_output=True, text=True, env=CLEAN_ENV, timeout=35
            )
            return result.stdout
        except:
            return ""

    def query(self, start, end, page, req_id):
        """Query one page of comments. Returns raw text."""
        body = json.dumps({
            "jsonrpc": "2.0", "id": req_id, "method": "tools/call",
            "params": {
                "name": "research_query",
                "arguments": {
                    "startTime": start, "endTime": end,
                    "type": "comment", "page": page, "pageSize": 100
                }
            }
        })
        raw = self._post(body)
        if not raw:
            return ""
        try:
            resp = json.loads(raw)
            return resp['result']['content'][0]['text']
        except:
            return ""

    def close(self):
        if self.sse_proc:
            try:
                self.sse_proc.kill()
                self.sse_proc.wait(timeout=3)
            except:
                pass
            self.sse_proc = None
        self.endpoint = None

    def reconnect(self):
        """Reconnect with retries."""
        for attempt in range(3):
            if self.connect():
                return True
            time.sleep(2)
        return False


def parse_comments(text):
    """Parse markdown response into structured records."""
    records = []
    items = re.split(r'###\s+\d+\.\s+', text)
    for item in items[1:]:
        rec = {}
        lines = item.strip().split('\n')
        rec['title'] = lines[0].strip()
        content_lines = []
        in_content = False
        for line in lines[1:]:
            s = line.strip()
            if s.startswith('- 时间：'):
                rec['time'] = s[len('- 时间：'):]
                in_content = False
            elif s.startswith('- 内容：'):
                content_lines.append(s[len('- 内容：'):])
                in_content = True
            elif s.startswith('- 附加：'):
                rec['extra'] = s[len('- 附加：'):]
                in_content = False
            elif in_content and s:
                content_lines.append(s)
        if content_lines:
            rec['content'] = '\n'.join(content_lines)
        if rec.get('title') and rec.get('time'):
            records.append(rec)
    return records


def download_day(session, date_str, id_base):
    """Download all comments for one day. Returns list of records."""
    start = f"{date_str} 00:00:00"
    end = f"{date_str} 23:59:59"
    all_recs = []
    page = 1

    while True:
        text = session.query(start, end, page, id_base + page)
        recs = parse_comments(text)
        if not recs:
            break
        all_recs.extend(recs)
        if len(recs) < 100:
            break
        page += 1
        time.sleep(0.3)
    return all_recs


def get_months(start_date, end_date):
    months = []
    d = start_date.replace(day=1)
    while d <= end_date:
        months.append((d.year, d.month))
        if d.month == 12:
            d = d.replace(year=d.year + 1, month=1)
        else:
            d = d.replace(month=d.month + 1)
    return months


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Clean stale log
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)

    log("=" * 50)
    log("3-year comment download from 进门财经")
    log("=" * 50)

    end_date = datetime(2026, 4, 2)
    start_date = datetime(2023, 4, 3)
    months = get_months(start_date, end_date)
    log(f"Range: {start_date.date()} ~ {end_date.date()} | {len(months)} months")

    session = MCPSession()
    grand_total = 0

    for mi, (year, month) in enumerate(months):
        month_str = f"{year}-{month:02d}"
        out_file = os.path.join(OUTPUT_DIR, f"{month_str}.jsonl")

        # Skip already downloaded
        if os.path.exists(out_file):
            n = sum(1 for _ in open(out_file, encoding='utf-8'))
            if n > 0:
                log(f"[{mi+1}/{len(months)}] {month_str}: skip ({n} exist)")
                grand_total += n
                continue

        # Connect fresh session for each month
        if not session.reconnect():
            log(f"FATAL: cannot connect")
            session.close()
            sys.exit(1)

        month_start = max(datetime(year, month, 1), start_date)
        last_day = calendar.monthrange(year, month)[1]
        month_end = min(datetime(year, month, last_day), end_date)

        month_records = []
        d = month_start
        day_count = 0

        while d <= month_end:
            date_str = d.strftime("%Y-%m-%d")
            recs = download_day(session, date_str, mi * 10000 + d.day * 100)
            month_records.extend(recs)
            d += timedelta(days=1)
            day_count += 1
            time.sleep(0.15)

            # Reconnect every 5 days to keep session fresh
            if day_count % 5 == 0:
                session.reconnect()

        # Save
        with open(out_file, 'w', encoding='utf-8') as f:
            for rec in month_records:
                f.write(json.dumps(rec, ensure_ascii=False) + '\n')

        grand_total += len(month_records)
        log(f"[{mi+1}/{len(months)}] {month_str}: {len(month_records)} records saved")

    session.close()
    log("=" * 50)
    log(f"DONE | total: {grand_total} records")
    log("=" * 50)


if __name__ == "__main__":
    main()
