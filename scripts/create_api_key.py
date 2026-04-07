#!/usr/bin/env python3
"""Generate an Open API key and insert it into the database.

Usage:
    python scripts/create_api_key.py --name "Alice's OpenClaw agent"
    python scripts/create_api_key.py --name "Bob's agent" --rate-limit 120
"""
from __future__ import annotations

import argparse
import hashlib
import secrets
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import create_engine, text
from backend.app.config import get_settings


def main():
    parser = argparse.ArgumentParser(description="Create an Open API key")
    parser.add_argument("--name", required=True, help="Human-readable name for this key")
    parser.add_argument("--description", default="", help="Optional description")
    parser.add_argument("--rate-limit", type=int, default=60, help="Requests per minute (default: 60)")
    args = parser.parse_args()

    # Generate a secure random key: ta_<48 hex chars>
    raw_key = f"ta_{secrets.token_hex(24)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    settings = get_settings()
    engine = create_engine(settings.database_url_sync)

    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO api_keys (key_hash, name, description, rate_limit)
                VALUES (:key_hash, :name, :description, :rate_limit)
            """),
            {
                "key_hash": key_hash,
                "name": args.name,
                "description": args.description,
                "rate_limit": args.rate_limit,
            },
        )
        conn.commit()

    print(f"API key created successfully!")
    print(f"  Name:       {args.name}")
    print(f"  Rate limit: {args.rate_limit} req/min")
    print(f"  Key:        {raw_key}")
    print()
    print("Store this key securely — it cannot be retrieved later.")


if __name__ == "__main__":
    main()
