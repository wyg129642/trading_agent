#!/usr/bin/env python3
"""岗底斯 Datapipe 数据导入器（XML → MongoDB）。

Datapipe 客户端以 mode=down 跑在 /home/ygwang/crawl_data/gangtise_datapipe/，
把订阅产品按时间戳增量推送到 work/download/<product>/<product>_yyyymmddhhmmss.xml。
本脚本读这些 XML，把每行数据按产品名导入 Mongo `gangtise-full.dp_<product>`。

设计要点：
- 幂等：以文件相对路径作为 _id 写到 `dp_state`，再跑跳过
- schema 驱动：从 <fields> 块拿 primary_key 当 Mongo _id，按声明类型做转换
- bulk upsert：每文件一次 bulk_write
- 失败不堵：单文件解析/插入异常 → 记到 dp_state.error 字段，继续下一个

用法：
  python3 datapipe_importer.py --once       # 扫一遍所有 product，已处理的跳过
  python3 datapipe_importer.py --watch 60   # 守护：每 60 秒扫一次
  python3 datapipe_importer.py --product news_financialflash  # 只跑这一个产品
  python3 datapipe_importer.py --reset news_financialflash    # 清掉这个产品的导入历史 (谨慎)
  python3 datapipe_importer.py --stats      # 看每个 collection 行数 + 最新文件
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

DATAPIPE_DIR = Path(os.environ.get(
    "DATAPIPE_DIR",
    "/home/ygwang/crawl_data/gangtise_datapipe/work/download",
))
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
MONGO_DB = os.environ.get("MONGO_DB", "gangtise-full")
COL_STATE = "dp_state"


def coerce(value: str | None, dtype: str) -> Any:
    """XML 里所有值都是 str；按声明类型转 Python 原生类型。"""
    if value is None or value == "":
        return None
    t = dtype.upper()
    try:
        if t in ("BIGINT", "INT", "TINYINT", "SMALLINT", "INTEGER"):
            return int(value)
        if t in ("DECIMAL", "DOUBLE", "FLOAT", "NUMERIC"):
            return float(value)
    except ValueError:
        return value
    return value  # VARCHAR / TEXT / MEDIUMTEXT / LONGTEXT / DATETIME → keep as str


def parse_xml(path: Path) -> tuple[list[dict[str, str]], list[dict[str, Any]], str | None]:
    """返回 (fields, rows, primary_key_name)."""
    tree = ET.parse(path)
    root = tree.getroot()
    fields_el = root.find("fields")
    if fields_el is None:
        return [], [], None

    fields: list[dict[str, str]] = []
    primary_key = None
    for f in fields_el.findall("field"):
        name = f.get("name", "")
        dtype = f.get("type", "VARCHAR")
        is_pk = (f.get("primary_key", "").lower() == "true")
        fields.append({"name": name, "type": dtype, "pk": is_pk})
        if is_pk and primary_key is None:
            primary_key = name

    rows_el = root.find("rows")
    rows: list[dict[str, Any]] = []
    if rows_el is None:
        return fields, rows, primary_key

    for row in rows_el.findall("row"):
        doc: dict[str, Any] = {}
        for child in row:
            ftype = next((f["type"] for f in fields if f["name"] == child.tag), "VARCHAR")
            doc[child.tag] = coerce(child.text, ftype)
        rows.append(doc)

    return fields, rows, primary_key


def import_file(db, product: str, xml_path: Path, rel_path: str) -> tuple[int, int, int]:
    """导入一个 XML 文件。返回 (rows_total, upserts, deletes)."""
    fields, rows, pk = parse_xml(xml_path)
    if not rows:
        return 0, 0, 0

    coll = db[f"dp_{product}"]
    ops: list = []
    deletes = 0
    upserts = 0

    for r in rows:
        op_mode = r.get("op_mode", 0)
        # op_mode: 0=insert/update, 1=update, 2=delete (推断)
        # 缺 pk 时退化用 hash
        key_val = r.get(pk) if pk else None
        if key_val is None:
            # 没主键时用全字段 hash 当 _id (退化)
            key_val = hash(tuple(sorted((k, str(v)) for k, v in r.items())))

        doc = dict(r)
        doc["_id"] = key_val
        doc["_dp_imported_at"] = datetime.now(timezone.utc)
        doc["_dp_source_file"] = rel_path

        if op_mode == 2:
            ops.append(UpdateOne({"_id": key_val}, {"$set": {"_dp_deleted": True, "_dp_deleted_at": datetime.now(timezone.utc)}}, upsert=False))
            deletes += 1
        else:
            ops.append(UpdateOne({"_id": key_val}, {"$set": doc}, upsert=True))
            upserts += 1

    if ops:
        coll.bulk_write(ops, ordered=False)

    return len(rows), upserts, deletes


def file_key(product: str, fname: str) -> str:
    return f"{product}/{fname}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", default=True, help="扫一遍后退出（默认）")
    ap.add_argument("--watch", type=int, default=0, help="守护模式，每 N 秒扫一次")
    ap.add_argument("--product", help="只处理这一个产品名")
    ap.add_argument("--reset", help="清掉指定产品的 dp_state 历史并退出（不删 dp_<product> collection）")
    ap.add_argument("--stats", action="store_true", help="只打印每个 dp_<product> collection 的行数 + 最新文件")
    ap.add_argument("--datapipe-dir", default=str(DATAPIPE_DIR))
    ap.add_argument("--mongo-uri", default=MONGO_URI)
    ap.add_argument("--mongo-db", default=MONGO_DB)
    args = ap.parse_args()

    base = Path(args.datapipe_dir).resolve()
    if not base.is_dir():
        print(f"[err] DATAPIPE_DIR 不存在: {base}", file=sys.stderr)
        return 2

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[args.mongo_db]
    state = db[COL_STATE]

    if args.reset:
        n = state.delete_many({"product": args.reset}).deleted_count
        print(f"[reset] cleared {n} state entries for product={args.reset}")
        return 0

    if args.stats:
        print(f"{'product':<28} {'rows':>10} {'newest_file'}")
        for cn in sorted(db.list_collection_names()):
            if not cn.startswith("dp_") or cn == COL_STATE:
                continue
            cnt = db[cn].estimated_document_count()
            newest = db[cn].find_one(sort=[("_dp_imported_at", -1)]) or {}
            src = newest.get("_dp_source_file", "")
            print(f"{cn:<28} {cnt:>10} {src}")
        return 0

    def scan_once() -> dict[str, int]:
        agg = {"files_processed": 0, "files_skipped": 0, "rows_total": 0, "errors": 0}
        for product_dir in sorted(base.iterdir()):
            if not product_dir.is_dir():
                continue
            product = product_dir.name
            if args.product and product != args.product:
                continue
            for xml in sorted(product_dir.iterdir()):
                if xml.suffix != ".xml":
                    continue
                key = file_key(product, xml.name)
                if state.find_one({"_id": key, "ok": True}):
                    agg["files_skipped"] += 1
                    continue
                rel = key
                try:
                    rows, ups, dels = import_file(db, product, xml, rel)
                    state.update_one(
                        {"_id": key},
                        {"$set": {
                            "product": product,
                            "ok": True,
                            "rows": rows, "upserts": ups, "deletes": dels,
                            "imported_at": datetime.now(timezone.utc),
                        }},
                        upsert=True,
                    )
                    agg["files_processed"] += 1
                    agg["rows_total"] += rows
                    print(f"  [+] {key:<60} rows={rows} ups={ups} dels={dels}")
                except (ET.ParseError, PyMongoError, OSError) as e:
                    agg["errors"] += 1
                    state.update_one(
                        {"_id": key},
                        {"$set": {
                            "product": product,
                            "ok": False,
                            "error": f"{type(e).__name__}: {e}",
                            "tried_at": datetime.now(timezone.utc),
                        }},
                        upsert=True,
                    )
                    print(f"  [!] {key}: {type(e).__name__}: {e}", file=sys.stderr)
                except Exception:
                    agg["errors"] += 1
                    print(f"  [!!] {key}: unexpected", file=sys.stderr)
                    traceback.print_exc()
        return agg

    if args.watch > 0:
        print(f"[watch] every {args.watch}s, ctrl-c to stop")
        while True:
            t0 = time.time()
            r = scan_once()
            print(f"[scan] processed={r['files_processed']} skipped={r['files_skipped']} rows={r['rows_total']} errors={r['errors']} ({time.time()-t0:.1f}s)")
            time.sleep(max(0, args.watch - (time.time() - t0)))
    else:
        r = scan_once()
        print(f"[done] processed={r['files_processed']} skipped={r['files_skipped']} rows={r['rows_total']} errors={r['errors']}")
        return 0 if r["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
