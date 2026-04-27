"""One-off audit script: re-run llm_tag_tickers logic on a fixed list of doc IDs
with the *current* prompt + post-filter, and compare against the OLD result
already stored in `_llm_canonical_tickers`. No DB writes."""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

os.environ["NO_PROXY"] = "127.0.0.1,localhost,dashscope.aliyuncs.com,aliyuncs.com"

from motor.motor_asyncio import AsyncIOMotorClient  # noqa: E402
from openai import AsyncOpenAI  # noqa: E402

from backend.app.config import get_settings  # noqa: E402
from backend.app.services.ticker_normalizer import normalize_with_unmatched  # noqa: E402
from scripts.llm_tag_tickers import (  # noqa: E402
    MODELS,
    SOURCES,
    _build_user_prompt,
    _call_llm,
    _load_alias_index,
    _validate_by_mention,
)

# 20 audit IDs from prior random sample (alphapai.roadshows)
AUDIT_IDS = [
    "1d48b60ae07d2d294c9837ddc861c6d8da8096d5",
    "397904e9e0cb7d9de12c5ec438f1c09a9cc53646",
    "ebab2eaa96a9d60b48761d97e67f2742e7e28e5c",
    "220701e919f0e37ec3556b3817460342bb079314",
    "b973af74007a96c36a3ae5b609f87242ab81d41a",
    "e2cc8e2f9ba0a2a589ced4d12cb1eb088d2edd6f",
    "2254ac16b22172e34ce2e646a57406bcb12f7801",
    "9c66dd909dd5b722783a5120d4c90547cbab6365",
    "a0f106e8460b6c908beffc03961a7a80b5187442",
    "a48c9ef7743a0fc7a689a076bb7d62a0e8374ea1",
    "12abaa0e6a6cdc3d6088ab9bad9f59b8f57d3329",
    "855de17f96f3f123bb94d1f4f9778d03d398d911",
    "5ac64023f1f77e4d48907b03bf62287933a38e2d",
    "d1a4d377b18c130629808ac48cb07f9372080d89",
    "fba8fbbeeac3b2d007aa77d6cd60857eb030006b",
    "50343549e0e4ab1fc838d6fb1229fa4f3b82b97a",
    "1e054f9441c51bbeff6f0c8f4d33b0bd5f85ba4f",
    "c70846770c6dda3fcc7371dfed8ddb5795840205",
    "071eff05bc1f887b10057e4948248c1eac51ce99",
    "694f5b23dcfaed30c2e76a6b960b59ea7cdc0c30",
]


async def main() -> None:
    s = get_settings()
    spec = MODELS["qwen-plus"]
    client = AsyncOpenAI(
        api_key=s.llm_enrichment_api_key,
        base_url=s.llm_enrichment_base_url,
    )
    alias_index = _load_alias_index()
    coll_spec = SOURCES["alphapai"]["roadshows"]

    mongo = AsyncIOMotorClient("mongodb://127.0.0.1:27018/", tz_aware=True)
    coll = mongo["alphapai-full"]["roadshows"]

    results: list[dict] = []
    for doc_id in AUDIT_IDS:
        doc = await coll.find_one({"_id": doc_id})
        if not doc:
            results.append({"id": doc_id, "missing": True})
            continue

        old_tags = doc.get("_llm_canonical_tickers") or []
        prompt = _build_user_prompt(doc, coll_spec)

        try:
            raw, in_tok, out_tok = await _call_llm(client, spec, prompt)
        except Exception as e:  # noqa: BLE001
            results.append(
                {"id": doc_id, "title": doc.get("title", ""), "old": old_tags, "err": str(e)}
            )
            continue

        normalized, normalizer_unmatched = normalize_with_unmatched(raw)
        kept, dropped_by_mention = _validate_by_mention(normalized, prompt, alias_index)

        results.append(
            {
                "id": doc_id,
                "title": doc.get("title") or doc.get("title_cn") or doc.get("title_en") or "",
                "old": old_tags,
                "raw": raw,
                "normalized": normalized,
                "kept": kept,
                "dropped_mention": dropped_by_mention,
                "unmatched_normalizer": list(normalizer_unmatched or []),
                "in_tok": in_tok,
                "out_tok": out_tok,
            }
        )

    # Pretty print
    print("\n=== OLD vs NEW comparison (alphapai.roadshows, qwen-plus) ===\n")
    total_in = 0
    total_out = 0
    for i, r in enumerate(results, 1):
        if r.get("missing"):
            print(f"{i:>2}. [{r['id'][:8]}] MISSING")
            continue
        if r.get("err"):
            print(f"{i:>2}. [{r['id'][:8]}] ERROR: {r['err']}")
            continue
        title = r["title"][:80]
        old = r["old"]
        new = r["kept"]
        same = sorted(old) == sorted(new)
        marker = "  " if same else "Δ "
        print(f"{i:>2}. {marker}{title}")
        print(f"      OLD: {old}")
        if r["raw"] != new or not same:
            print(f"      LLM-raw: {r['raw']}")
        if r["dropped_mention"]:
            print(f"      mention-dropped: {r['dropped_mention']}")
        if r["unmatched_normalizer"]:
            print(f"      normalizer-unmatched: {r['unmatched_normalizer']}")
        print(f"      NEW: {new}")
        print()
        total_in += r["in_tok"]
        total_out += r["out_tok"]

    print(
        f"\nTokens: in={total_in:,} out={total_out:,} "
        f"cost=${total_in / 1e6 * spec.in_usd_per_mtok + total_out / 1e6 * spec.out_usd_per_mtok:.4f}"
    )

    mongo.close()
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
