#!/usr/bin/env python3
"""Fetch A-share, US, and HK stock lists via AKShare (sina source) and save as CSV.

The generated CSVs are used by StockVerifier for ticker validation.

Usage:
    python scripts/fetch_stock_lists.py            # Fetch all markets
    python scripts/fetch_stock_lists.py --us       # US stocks only
    python scripts/fetch_stock_lists.py --hk       # HK stocks only
    python scripts/fetch_stock_lists.py --a        # A-shares only

Output:
    data/a_stock_list.csv   — columns: code, name   (A-shares, ~5500 rows)
    data/us_stock_list.csv  — columns: code, name   (US stocks, ~17000 rows)
    data/hk_stock_list.csv  — columns: code, name   (HK stocks, ~2700 rows)

Note: US stock fetch takes ~8 minutes (sina paginates 852 pages).
"""
import argparse
import csv
import os
import sys
import time

# Disable proxy before importing akshare
os.environ.pop("http_proxy", None)
os.environ.pop("https_proxy", None)
os.environ.pop("HTTP_PROXY", None)
os.environ.pop("HTTPS_PROXY", None)

import akshare as ak

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(DATA_DIR, exist_ok=True)


def fetch_a_shares():
    """Fetch all A-share stocks using ak.stock_info_a_code_name() (~3s)."""
    print("Fetching A-shares...")
    t0 = time.time()
    df = ak.stock_info_a_code_name()
    out_path = os.path.join(DATA_DIR, "a_stock_list.csv")

    count = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["代码", "名称"])
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            name = str(row.get("name", "")).strip()
            if not code or not name:
                continue
            writer.writerow([code, name])
            count += 1

    print(f"  Saved {count} A-shares to {out_path} ({time.time() - t0:.1f}s)")


def fetch_us_stocks():
    """Fetch all US stocks using ak.stock_us_spot() (~8 min, sina source)."""
    print("Fetching US stocks (this takes ~8 minutes)...")
    t0 = time.time()
    df = ak.stock_us_spot()
    out_path = os.path.join(DATA_DIR, "us_stock_list.csv")

    count = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name"])
        for _, row in df.iterrows():
            symbol = str(row.get("symbol", "")).strip()
            cname = str(row.get("cname", "")).strip()
            if not symbol or not cname:
                continue
            writer.writerow([symbol, cname])
            count += 1

    print(f"  Saved {count} US stocks to {out_path} ({time.time() - t0:.1f}s)")


def fetch_hk_stocks():
    """Fetch all HK stocks from HKEX official xlsx (~5s).

    Falls back to ak.stock_hk_spot() (sina) if HKEX download fails.
    """
    print("Fetching HK stocks...")
    t0 = time.time()
    out_path = os.path.join(DATA_DIR, "hk_stock_list.csv")

    # Primary: HKEX official securities list (most complete)
    try:
        import requests
        import openpyxl
        from io import BytesIO

        url = "https://www.hkex.com.hk/chi/services/trading/securities/securitieslists/ListOfSecurities_c.xlsx"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        wb = openpyxl.load_workbook(BytesIO(resp.content), read_only=False)
        ws = wb.active
        rows = list(ws.values)

        stocks = []
        for row in rows[3:]:  # Skip header rows 0-2
            code = str(row[0]).strip() if row[0] else ""
            name = str(row[1]).strip() if row[1] else ""
            category = str(row[2]).strip() if row[2] else ""
            if not code or not name:
                continue
            # Only include equity stocks (股本), skip warrants/debt/etc
            if "股本" in category:
                stocks.append((code.zfill(5), name))

        count = 0
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["code", "name"])
            for code, name in stocks:
                writer.writerow([code, name])
                count += 1

        print(f"  Saved {count} HK stocks to {out_path} ({time.time() - t0:.1f}s) [HKEX source]")
        return
    except Exception as e:
        print(f"  HKEX download failed: {e}, trying akshare fallback...")

    # Fallback: akshare sina source
    df = ak.stock_hk_spot()
    count = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code", "name"])
        for _, row in df.iterrows():
            code = str(row.get("代码", "")).strip()
            name = str(row.get("中文名称", "")).strip()
            if not code or not name:
                continue
            writer.writerow([code, name])
            count += 1

    print(f"  Saved {count} HK stocks to {out_path} ({time.time() - t0:.1f}s) [sina source]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch stock lists via AKShare")
    parser.add_argument("--a", action="store_true", help="Fetch A-shares only")
    parser.add_argument("--us", action="store_true", help="Fetch US stocks only")
    parser.add_argument("--hk", action="store_true", help="Fetch HK stocks only")
    args = parser.parse_args()

    fetch_all = not (args.a or args.us or args.hk)

    if fetch_all or args.a:
        try:
            fetch_a_shares()
        except Exception as e:
            print(f"  A-shares failed: {e}", file=sys.stderr)

    if fetch_all or args.hk:
        try:
            fetch_hk_stocks()
        except Exception as e:
            print(f"  HK stocks failed: {e}", file=sys.stderr)

    if fetch_all or args.us:
        try:
            fetch_us_stocks()
        except Exception as e:
            print(f"  US stocks failed: {e}", file=sys.stderr)

    print("Done.")
