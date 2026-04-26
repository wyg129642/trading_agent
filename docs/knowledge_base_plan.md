# Proprietary Knowledge Base for AI Research Assistant — Design Plan

**Author:** Claude (design) / yugang (approver)
**Date:** 2026-04-20 (v1 full-RAG draft) / 2026-04-20 (v2 addendum below)
**Status:** Historical planning doc — current state diverges from the body
**Scope:** Build a production knowledge-base layer that the chat assistant uses through its existing multi-round tool loop, so subjective researchers can ask deep questions and the LLM iteratively pulls evidence from proprietary crawler data.

> **Note (2026-04-26):** Sections referring to "remote Mongo" or
> `192.168.31.176:35002` are historical. The crawler corpus + personal KB
> were migrated to the remote ops cluster on 2026-04-23 and migrated back
> to the local `ta-mongo-crawl` container (`mongodb://127.0.0.1:27018/`)
> on 2026-04-26. Schema and DB names (`-full` suffixes, `ti-user-knowledge-base`)
> carried over both moves. Read CLAUDE.md "Database Architecture" for the
> current truth.

---

## Addendum (v2) — Do we actually need RAG, and how does DataFlow fit in?

The original v1 plan below proposed a full production RAG stack (Qdrant + BGE-M3 + reranker + contextual chunking + eval harness). After re-reading the plan against the actual corpus shape, against the existing vendor-hosted retrieval we already have, and against the `/home/ygwang/DataFlow` repository, I'm revising the recommendation **down**.

### What DataFlow actually is

DataFlow (OpenDCAI) is a **data-preparation framework for LLM training data generation** — "turn raw data into high-quality LLM training datasets". Its two relevant pipelines (Knowledge Base Clean, Agentic RAG) both produce **training datasets**, not runtime retrieval. Its `serving/flash_rag_serving.py` and `serving/light_rag_serving.py` are thin wrappers around research libraries (FlashRAG, LightRAG) used as building blocks inside data-generation pipelines.

**DataFlow is not a production KB serving stack and should not replace the plan.** But three of its components are genuinely useful and worth borrowing:

1. **MinerU** (wrapped in `operators/knowledge_cleaning/generate/mineru_operators.py`) — a CN-team PDF→markdown extractor, widely considered SOTA for mixed Chinese/English PDFs. This is a better pick than the Marker / LlamaParse I originally suggested for the ~3 K research PDFs with weak `summary_md`.
2. **chonkie** (wrapped in `kbc_chunk_generator.py`) — a mature chunking library with token / sentence / semantic / recursive chunkers. Use directly; don't roll our own chunker.
3. **multihop QA generator** (`kbc_multihop_qa_generator_batch.py`) + `qa_extract.py` — auto-seeds evaluation questions from our corpus. Saves a week of manual work on the 300-question golden set.

### Do we need RAG at all? — not for phase 1, probably yes for phase 2

Arguments against building in-house RAG right now:

- **Two of seven platforms already have vendor-hosted vector search we can't improve on** — `alphapai_recall` and `jinmen_*` delegate to AlphaPai's and Jinmen's own vector backends. That infra is not ours to own; duplicating it locally is busywork.
- **The corpus is small.** 56 K docs / 2.4 GB. After a ticker filter or doc-type filter a typical analyst query narrows to ~20–100 documents. That is **25 K–250 K tokens** — well inside Claude Opus 4.7's 1 M context and Gemini 2.x's 2 M context.
- **Analyst queries are already well-anchored** — 80%+ carry a ticker and a time window. Filter-first lookups (Mongo index + BM25 within the filtered set) handle this class cleanly, no embeddings required.
- **The multi-round tool loop compensates for imperfect recall** — if the LLM gets a weak first hit, it refines the query and retries. Chunk-perfect retrieval is less critical with agentic retrieval than with one-shot RAG.
- **Operational cost is real** — GPU host, Qdrant, embedding pipeline, reranker, eval harness. Several weeks of build + ongoing ops. Worth it only if naive search actually fails measurably.

Arguments for RAG (will come due eventually):

- Fuzzy-concept queries without a ticker anchor — "全球半导体供应链近期观点", "AI 应用层投资机会" — where filter-first returns hundreds of docs and BM25 can't rank them semantically.
- Cross-language retrieval — Chinese question against an English document (Funda, Third Bridge translations).
- Cross-platform deduplication — surfacing the same report 6 times from 6 platforms is noise.
- Chunk-level precision when the answer is 2 sentences inside a 40 K-char transcript.

**My revised recommendation: build it in two phases and let data decide phase 2.**

### Phase A — "Filter + BM25 + long-context" (1–2 weeks, no RAG)

What we ship:

1. **One unified KB tool** on the existing multi-round loop:
   - `kb_search(query, tickers, doc_types, sources, date_range, top_k)` — runs against local Mongo across all 5 non-vendor platforms (meritco, thirdbridge, funda, gangtise, acecamp) + optionally AlphaPai/Jinmen raw Mongo for breadth. Metadata filter first, then MongoDB `$text` index (or Postgres FTS if we mirror text to Postgres — probably cleaner) to BM25-rank within the filtered set. Returns top-N document summaries with full metadata.
   - `kb_fetch_document(doc_id, max_chars)` — pulls the full `content_md` / `transcript_md`. Lets the LLM read up to 30 K chars of a single hit.
2. **Keep the existing tools** — `alphapai_recall`, `jinmen_*`, `web_search` — unchanged. The LLM picks the right instrument per question.
3. **Canonical ticker filter** via the existing `_canonical_tickers` array (already populated by `scripts/enrich_tickers.py`). No new indexing required.
4. **MongoDB text indexes** on each collection's primary text field — `db.alphapai.wechat_articles.createIndex({content_md: "text", title: "text"})` and similar. Fifteen-minute one-time job.
5. **Extend `ChatTrace`** with `KB_*` events mirroring existing tool observability.
6. **Minimal UI change** — new `source_type: "kb"` badge in `CitationRenderer.tsx`, side-panel "show source" that fetches the full doc via `kb_fetch_document`.

No GPU. No vector store. No reranker. No embedding pipeline. Two engineers × one week.

What this buys us: researchers can ask ticker-anchored questions across all 7 platforms uniformly, the LLM iterates on weak hits, full docs fit in context for synthesis. My bet is this covers **70–80% of real-world queries** at near-zero ops cost.

### Phase B — Add targeted RAG only where Phase A measurably fails (2–4 weeks, conditional)

After 2–4 weeks of real usage, we grep the `chat_debug.log` traces and look for:

- `kb_search` calls that returned > 50 results but the LLM couldn't pick (over-broad queries)
- Sessions where the LLM kept retrying and giving up (recall failure)
- Explicit user thumbs-down
- Known-missed questions from the golden set (seeded via DataFlow's multihop QA generator)

If the failure rate is **> 15%** and **concentrated in fuzzy-concept or cross-language queries**, we add:

- Vector embeddings (BGE-M3) **only on the 5 platforms without vendor RAG** — keep using AlphaPai/Jinmen vendor search for those two.
- Qdrant single-node, hybrid (dense + sparse).
- Rerank (BGE-reranker-v2-m3).
- `kb_search` gets a `mode: "semantic"` parameter; the existing BM25 path becomes `mode: "lexical"`. The LLM picks — or we route automatically based on query features (presence/absence of ticker, entity density, etc.).

If Phase A is sufficient, Phase B never ships. This is the correct way to build this.

### What changes in sections 4–16 below

- **Section 5 (ingestion)**: Phase A only needs MongoDB text indexes + a canonical-ticker view. The change-stream / chunker / embedder pipeline is Phase B.
- **Section 6 (retrieval)**: Phase A is metadata-filter → BM25 → top-N docs. Phase B adds hybrid + rerank.
- **Section 7 (LLM integration)**: unchanged — the three tools are the right abstraction either way; their internals swap from Phase A to Phase B transparently to the LLM.
- **Section 10 (eval)**: build the golden set in Phase A (use DataFlow's multihop QA generator to seed it). Even "naive" search needs eval.
- **Section 12 (timeline)**: collapses. Phase A is ~2 weeks end-to-end. Phase B is conditional.
- **Section 13 (cost)**: Phase A is essentially free. Phase B same as v1.

The **v1 full-RAG plan below is kept in this doc unchanged** as the Phase B reference design. When / if we escalate, the architecture is ready.

### What to do this week

1. **Infra owner:** unblock the remote Mongo credential (still needed for Phase A — we want the full corpus, not just local).
2. **Me:** spike Phase A on local Mongo — one platform (AlphaPai `wechat_articles` + `comments`), MongoDB text index, one tool, wire into chat. 3 days.
3. **You + 2 researchers:** dogfood for a week. Decide whether to expand Phase A to the other 6 platforms, or whether we already see failure modes that justify going straight to Phase B on day one.

---

## 0. TL;DR (v1 — kept for Phase B reference)

| Decision | Recommendation | Why |
|---|---|---|
| Vector DB | **Qdrant** (single node, dockerised) | Hybrid search native, payload filtering, gRPC client, easy to operate. Our corpus is ~275 K chunks — no Milvus cluster needed. |
| Embedding model | **BGE-M3** (hosted locally on a GPU) | Best-in-class for Chinese + English in one model; emits dense + sparse + multi-vector in a single forward pass. Free, open, reproducible. |
| Sparse / lexical | **Qdrant sparse vectors from BGE-M3** + an **Elasticsearch BM25** fallback for long-tail ticker queries | Get hybrid retrieval out of one model instead of bolting on two indexers. |
| Reranker | **BGE-reranker-v2-m3** (GPU) for top-100 → top-10 | Cheap, CN/EN aware, state of the art on MTEB CN. Fall back to Cohere Rerank 3 if latency spikes. |
| Chunking | **Type-specific, section-aware, with contextual prefix** (Anthropic-style) | Roadshow transcripts, broker reports, forum posts, earnings calls have very different structure — one-size-fits-all chunking is the #1 failure mode of naive RAG. |
| Retrieval | **Hybrid (dense+sparse) → rerank → parent-expansion** | Small-chunk recall, large-chunk context into the LLM. |
| LLM integration | **3 orthogonal tools** on the existing multi-round loop: `kb_search` (hybrid retrieval), `kb_fetch_document` (read full doc), `kb_list_facets` (discover what's available) — plus an agentic query-rewriter that runs *before* the loop on the first turn | Lets Claude / Gemini / GPT decide when to broaden, narrow, or cross-reference — no rigid pipeline. |
| Ingestion | **Change-stream driven** from Mongo → Kafka → consumers that chunk + embed + index | Crawlers keep running; new docs show up in KB within minutes. |
| Metadata / grounding | **Canonical ticker-first filter stack** (`_canonical_tickers`, `release_time`, `source`, `doc_type`, `language`) plus cross-doc dedupe by `(title + release_time + institution)` SimHash | Analysts always filter by ticker + time; dedup prevents the same broker report appearing 6 times across platforms. |
| Eval | **Golden-question set + Ragas + nightly smoke tests** (per source, per question type) | Without eval, retrieval quality silently rots as the corpus grows. |
| Observability | **Extend `ChatTrace` with `KB_*` events + Qdrant dashboard + Grafana** | Same trace-id threads through chat → retrieval → reranker → LLM call. |

**Timeline:** M0 spike (1 week) → M1 MVP indexing one platform (2 weeks) → M2 multi-platform hybrid + rerank (2 weeks) → M3 agentic tool loop + citations (1 week) → M4 eval harness + prod rollout (2 weeks). **~8 weeks** to production.

---

## 1. Prerequisite — unblock the remote MongoDB

Probe result for `mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002`:

- TCP reachable (no firewall / routing issue).
- Auth rejected against every `authSource` we tried (`admin`, `alphapai`, `jinmen`, `meritco`, `thirdbridge`, `funda`, `gangtise`, `crawl`, `spider`, default) and every SCRAM mechanism (`SCRAM-SHA-1`, `SCRAM-SHA-256`, default).
- Server error: `code 18, Authentication failed.`

**Action item for the infra owner** (blocking):

1. Confirm the password string — `prod_X5BKVbAc` is 13 chars, no word separator; a paste may have truncated it.
2. Provide the correct `authSource` (likely a non-obvious ops-scoped DB like `spider_ops` or similar — none of the per-platform DBs accept the credential).
3. Confirm the user still exists: `use admin; db.system.users.find({},{user:1,db:1,_id:0})` on the mongod host.
4. Confirm auth mech isn't `MONGODB-AWS` / `PLAIN` / `x509`.

Until then: **the design below is portable** — it uses the local Mongo (same 7 DBs, same schemas) for spike and MVP, so work is not blocked. The only change once remote auth lands is the connection URI and the change-stream endpoint in the ingest config.

---

## 2. What we're indexing (ground truth from local Mongo)

| DB | Collection | Docs | Data GB | Primary text field(s) | Typical length |
|---|---|---:|---:|---|---|
| alphapai | wechat_articles | 15,335 | 0.29 | `content_md` | 1–10 K chars |
| alphapai | comments | 10,495 | 0.03 | `content_md`, `contextInfo` | 200–2 K chars |
| alphapai | reports | 2,558 | 0.01 | `summary_md` (PDF attached) | 500–5 K chars + PDF |
| alphapai | roadshows | 321 | 0.06 | `transcript_md` | 5–50 K chars |
| jinmen | meetings | 9,773 | 1.48 | `insight_md`, `transcript_md` | 5–40 K chars |
| jinmen | reports | 340 | 0.01 | `summary_md` (PDF attached) | 500–5 K chars + PDF |
| meritco | forum | 2,273 | 0.11 | `content_md` (expert-call notes) | 3–20 K chars |
| thirdbridge | interviews | 148 | <0.01 | `transcript_md` | 10–60 K chars |
| funda | earnings_transcripts | 2,480 | 0.22 | `transcript_md` | 10–80 K chars |
| funda | earnings_reports | 886 | 0.11 | `summary_md` | 500–5 K chars |
| funda | posts | 348 | 0.01 | `content_md` | 200–3 K chars |
| funda | sentiments | 10,307 | 0.01 | sentiment tags (structured) | N/A — store as metadata |
| gangtise | chief_opinions | 150 | <0.01 | `content_md` | 500–5 K chars |
| gangtise | researches | 58 | <0.01 | `summary_md` (PDF attached) | 500–5 K chars + PDF |
| gangtise | summaries | 19 | <0.01 | `summary_md` | 500–5 K chars |
| acecamp | articles | 46 | 0.03 | `content_md` | — |
| **TOTAL** | | **~56 K** | **~2.4 GB** | | |

Plus **~1.4 TB of PDFs** under `/home/ygwang/crawl_data/` — most already have extracted markdown in the Mongo doc, but a second pass with a better OCR / layout extractor (e.g. Marker, Nougat, or LlamaParse) is justified for the ~3 K research reports whose `summary_md` is short because auto-extraction was lossy.

**Chunk math at the recommended chunking policy** (section 5): **~270 K–320 K chunks** total. At 1024-dim float32 dense vectors + sparse, that's **~1.3 GB dense + ~600 MB sparse index + ~2 GB payload** = a **~4 GB Qdrant collection**. Trivial for a single node.

Shared metadata already present across all platforms (big win — let us build a uniform filter stack):

- `_canonical_tickers: ["NVDA.US", "0700.HK", "600519.SH"]` (populated by `scripts/enrich_tickers.py` — see CLAUDE.md)
- `release_time`, `release_time_ms`
- `title`, `institution`, `author`
- platform-specific doc type (report / comment / roadshow / forum / interview / transcript / sentiment / article)

---

## 3. Why "naive RAG" fails on this corpus

For the record — so reviewers know what we are *not* doing:

1. **Mixed-language** — BGE-small-en or ada-002 under-retrieve Chinese. You *must* use a CN-aware embedder.
2. **Wildly mixed doc sizes** — a 200-char comment and a 60 K-char transcript can't share one chunking policy without destroying either small-doc recall or large-doc context.
3. **Ticker queries** — "最近关于 NVDA 的机构观点" is a filter-then-rank problem, not a similarity-first problem. Dense-only RAG will happily return English NVIDIA reports when the analyst wanted Chinese institutional comments. Metadata filtering is not optional.
4. **Time sensitivity** — a broker report from 2021 is almost never the right answer for a 2026 question. Time decay must be built into ranking.
5. **Dedup** — the *same* Morgan Stanley China Internet report can appear in AlphaPai, Jinmen, and Gangtise. Surfacing all three is noise.
6. **Citation integrity** — our users are professional researchers; they will catch hallucinations. Every sentence the LLM emits with a `[N]` must be traceable back to a chunk that actually contains supporting text.
7. **Multi-hop questions** — "how has Tencent's management tone on gaming regulation changed across the last 4 earnings calls" requires the LLM to issue multiple narrow searches, not one vague one. This is why we build a **tool**, not a pipeline.

---

## 4. Architecture overview

```
┌───────────────────────────── Research Assistant (existing) ─────────────────────────────┐
│                                                                                          │
│   User ⇄ chat.py (SSE) ⇄ chat_llm.py (multi-round tool loop)                             │
│                                 │                                                         │
│                                 ├─→ web_search   (existing)                               │
│                                 ├─→ alphapai_*   (existing)                               │
│                                 ├─→ jinmen_*     (existing)                               │
│                                 └─→ kb_* NEW TOOLS ───────┐                              │
│                                                            │                              │
└────────────────────────────────────────────────────────────┼──────────────────────────────┘
                                                             ▼
                  ┌────────────────────── KB Service (new) ────────────────────┐
                  │  kb_search(query, filters, top_k, mode)                      │
                  │  kb_fetch_document(doc_id, window?)                          │
                  │  kb_list_facets(dimension, filters)                          │
                  │                                                               │
                  │  1. Query rewriter (cheap LLM) → {cn_q, en_q, entities, dates}│
                  │  2. Hybrid retrieval: dense + sparse + filters                │
                  │  3. Cross-encoder rerank (BGE-reranker-v2-m3)                 │
                  │  4. Parent-chunk expansion + MMR de-dup                       │
                  │  5. Format with citation indices → ChatTrace                  │
                  └──────────────────────────────┬───────────────────────────────┘
                                                 │
                                                 ▼
             ┌─────────────────────────── Index Layer ───────────────────────────┐
             │  Qdrant ─── dense (1024) + sparse (BGE-M3) + payload              │
             │  Postgres ─── chunks table (chunk_id, doc_id, text, offsets,      │
             │              tickers[], release_time, source, doc_type, lang)     │
             │                                                                    │
             │  Optional: Elasticsearch BM25 index for ticker long-tail         │
             └──────────────────────────────┬───────────────────────────────────┘
                                            ▲
                                            │ upsert
             ┌──────────────────────────────┴───────────────────────────────────┐
             │                    Ingestion pipeline (new)                       │
             │                                                                    │
             │  Mongo change stream ─→ Kafka ─→ worker pool                      │
             │    ├─ Normalizer (ticker resolver, date parse, dedup SimHash)    │
             │    ├─ Chunker (type-specific, section-aware, contextual prefix)  │
             │    ├─ Embedder (BGE-M3 on GPU, batched)                          │
             │    └─ Indexer (Qdrant upsert + Postgres insert transactionally)  │
             └──────────────────────────────┬───────────────────────────────────┘
                                            ▲
                                            │ existing crawlers (unchanged)
                                            │
                                   Local Mongo @ 27017 (7 DBs)
                                   Remote Mongo @ 192.168.31.176:35002 (TBD)
```

---

## 5. Ingestion pipeline

### 5.1 Source of truth & change capture

- **Local Mongo stays authoritative** for the raw crawler output (already in place).
- For new data, we subscribe to **Mongo change streams** on each collection (`insert`, `update`, `delete`) and fan events into a durable queue so indexer restarts don't drop updates.
- For **backfill**, a one-shot walker iterates `_id` in chunks of 500, sending the same events into the queue. Same code path as the live stream — this is important: one chunker, one embedder, one indexer.

**Queue:** start with a single-node **Redis Streams** (already running), move to **Kafka** only if throughput demands it. For 55 K docs + a few hundred/day steady state, Redis Streams is fine.

### 5.2 Normalisation

Each raw Mongo doc becomes a canonical `KBDocument`:

```python
@dataclass
class KBDocument:
    doc_id: str                   # stable hash of (source, source_collection, source_id)
    source: str                   # "alphapai" | "jinmen" | "meritco" | ...
    source_collection: str        # original mongo collection
    source_id: str                # original mongo _id
    doc_type: str                 # "report" | "roadshow" | "comment" | "earnings_transcript" | ...
    title: str
    body_md: str                  # primary text, markdown
    language: str                 # "zh" | "en" | "mixed"
    institution: str | None
    author: str | None
    tickers: list[str]            # canonical "NVDA.US" form from _canonical_tickers
    release_time: datetime
    release_time_ms: int
    pdf_path: str | None
    url: str | None               # platform URL if any
    dedupe_fingerprint: str       # SimHash-64 of title + first 2K of body
    raw_ref: dict                 # {db, collection, _id} for round-trip to Mongo
```

### 5.3 Deduplication

Three layers:

1. **Exact URL / same (source, source_id)** — idempotent upsert by `doc_id`. Re-crawls never double-index.
2. **Near-duplicate across platforms** — 64-bit SimHash of title + first 2 K chars of body; Hamming distance ≤ 3 → mark as `duplicate_of=canonical_doc_id`. Canonical = earliest `release_time`, ties broken by source rank (jinmen > alphapai > gangtise > meritco > thirdbridge > funda > acecamp — tune from data). Duplicates are **still chunked and indexed** but the retriever filters them at query time unless the user explicitly asks for all variants (so we can answer "how many places is this reported").
3. **Contradiction detection** (later, M4) — docs with same ticker + release day but opposite sentiment → flag for the reranker so the LLM sees both sides.

### 5.4 Chunking — per doc type

This is where naive RAG dies. Concrete policy:

| doc_type | Strategy | Target chunk size | Overlap | Parent unit |
|---|---|---|---|---|
| short comments (`alphapai.comments`, `gangtise.chief_opinions`) | **whole doc** = 1 chunk | n/a | n/a | doc |
| medium articles (`alphapai.wechat_articles`, `funda.posts`, `meritco.forum`) | **paragraph-aware sliding** | 512 tokens | 64 tokens | doc |
| research reports (summary_md) | **section-aware** (headers + lists) | 512 tokens | 64 tokens | section |
| roadshow / meeting / interview transcripts | **speaker-turn aware** — never split a Q&A pair | 800 tokens target, 1200 max | 80 tokens | Q&A pair |
| earnings transcripts | **prepared-remarks vs. Q&A split**, then speaker-turn aware | 800 tokens | 80 tokens | Q&A pair |
| structured (`funda.sentiments`) | not chunked, **stored only as metadata filter** (doc-level record with sentiment tags) | — | — | — |
| PDF-backed reports where `summary_md` < 500 chars | **rerun extraction via Marker or LlamaParse on the PDF**, then apply section-aware policy | 512 tokens | 64 tokens | section |

### 5.5 Contextual chunking (Anthropic's recipe)

Every chunk is prefixed, at index time, with a 1-sentence context that a cheap LLM (Haiku / Gemini Flash) generates from the doc-level `(title, institution, release_time, summary)`. This boosts recall ~30% (Anthropic's reported numbers) for pronoun/mention queries. Cost: one cheap-LLM call per **chunk** at index time — amortised, never at query time. For 300 K chunks × ~200 output tokens each at Haiku prices this is a **one-time ~\$20** bill.

The context prefix lives only in the indexed representation. When we return text to the reader LLM we return the **original** chunk text (otherwise the prefix leaks into synthesised answers).

### 5.6 Embeddings

- Dense: **BGE-M3** (dense, 1024-dim). Run locally on one consumer GPU (3090 / 4090 / A10 / L4 all fine). Batched, FP16, ~1 K chunks/sec.
- Sparse: BGE-M3's native sparse output (same forward pass, no separate call).
- Keep the multi-vector / ColBERT output off by default — it triples index size and we don't need it until precision@1 becomes a problem.

Embedding cache keyed by `sha256(text)` → vector. Re-runs of the indexer on unchanged text never re-embed.

### 5.7 Indexing

Qdrant collection `kb_chunks`, one record per chunk:

```python
{
    "id": "<chunk_uuid>",
    "vector": {"dense": [...], "sparse": {"indices":[...], "values":[...]}},
    "payload": {
        "doc_id": "...",
        "source": "alphapai",
        "doc_type": "roadshow",
        "title": "...",
        "institution": "...",
        "author": "...",
        "tickers": ["NVDA.US"],
        "release_time_ms": 1713600000000,
        "language": "zh",
        "chunk_index": 4,
        "parent_unit": "section:2",
        "text_len": 512,
        "is_duplicate_of": null,
        "pdf_path": null,
        "url": "https://..."
    }
}
```

Plus a Postgres `kb_chunks` table with the same keys (no vector) as the source of truth for text + offsets — Qdrant can always be rebuilt from it. This inversion (Postgres authoritative, Qdrant derived) is deliberate: vector indexes drift, schemas change, models get upgraded; we never want to be in a position where the only copy of our chunked text lives in Qdrant.

Transactional ordering: **Postgres insert succeeds → then Qdrant upsert → then mark doc as "indexed" in a `kb_doc_state` table**. If Qdrant fails, the doc is retried from the state table on the next consumer tick. Never "upsert Qdrant first" — that produces ghost rows.

### 5.8 Incremental sync

Every `kb_doc_state` row tracks `last_indexed_at` + `source_updated_at`. A nightly job catches up on anything the change-stream missed (cheap — it's just a diff by `(doc_id, source_updated_at)`).

### 5.9 PDF re-extraction

For reports where `summary_md` is short (< 500 chars) but a PDF exists, run a second extraction pass with **Marker** (open source, layout-aware) or **LlamaParse** (commercial, higher quality, ~\$0.003/page) and replace `body_md` with the better extraction. Gate on confidence (e.g. extracted length ≥ 3 × existing length before swapping). This is a one-time backfill of ~3 K docs, ~\$100 at commercial rates if you don't self-host.

---

## 6. Retrieval layer

The retrieval layer is itself a micro-service (`KBService`) inside the FastAPI process — not an HTTP hop. One module, `backend/app/services/kb_service.py`.

### 6.1 Query understanding (step 1)

Before the hybrid search runs, we normalise the query with a **fast** LLM call (Haiku / Flash, ~200 ms, 300 tokens):

Input: the raw user query and the conversation history.
Output: a JSON envelope —

```python
{
    "cn_q": "腾讯 2025 Q4 业绩电话会 游戏业务 管理层观点",
    "en_q": "Tencent Q4 2025 earnings call gaming business management commentary",
    "tickers": ["0700.HK"],
    "date_range": {"gte": "2025-10-01", "lte": "2026-03-31"},
    "doc_types": ["earnings_transcript", "roadshow", "report"],
    "rewrite_reason": "user asked in mixed Chinese/English; expand to both for retrieval",
    "is_followup": false
}
```

Why this step matters: the reader LLM (Claude, GPT, Gemini) already does query formation when it calls `kb_search`, but it writes the query to *communicate with a human retriever* — not to maximise embedding recall. The rewriter turns analyst-speak into retriever-speak. In an ablation, expect ~15% nDCG@10 uplift.

This can be **cached** aggressively on `sha256(query + last_N_turns)` — hit rate is high in multi-round conversations because the user often rephrases.

### 6.2 Hybrid retrieval (step 2)

Qdrant native hybrid via **Reciprocal Rank Fusion** on (dense_score, sparse_score) with RRF constant k=60. Payload filters applied pre-search (not post) for speed:

```python
filter = {
    "must": [
        {"key": "is_duplicate_of", "is_null": True},
        {"key": "language", "match": {"any": ["zh", "en", "mixed"]}},
    ],
    "should": [
        {"key": "tickers", "match": {"any": ["0700.HK"]}},
    ],
    "must_not": [],
    "range": {
        "release_time_ms": {"gte": 1727740800000, "lte": 1743465600000}
    }
}
```

Retrieve top **100** (overfetch) → pass to reranker.

Why overfetch 100: (a) hybrid scoring has high variance in the 5–50 rank band; (b) the reranker collapses that variance; (c) dedup + MMR will drop some.

**Time-decay** applied as a soft post-score (not a hard filter): `score *= exp(-age_days / tau)` with `tau=180` for general searches, `tau=30` if `doc_type ∈ {earnings_transcript, roadshow}`. Configurable.

### 6.3 Reranking (step 3)

BGE-reranker-v2-m3 on the (query, chunk_text) pairs for the top 100 → rescore → keep top 20. GPU batch of 100 pairs at 512 tokens runs in ~150 ms on L4.

If latency budget is tight: **Cohere Rerank 3** via API (~40 ms for 100 docs, ~\$0.002/call). Keep BGE as the default (zero marginal cost) and Cohere as a knob.

### 6.4 Parent-chunk expansion (step 4)

For each of the top-20 chunks, expand to its **parent unit** (for transcripts: the full Q&A pair; for reports: the full section). This means the reader LLM sees contiguous context, not a torn fragment. Parent text pulled from Postgres (no vector cost). Deduplicate parents — if two chunks in the top 20 point to the same parent, keep one.

### 6.5 MMR on parents (step 5)

Maximum Marginal Relevance (λ=0.6) on parent embeddings to enforce diversity — otherwise a single popular report can fill 5 of the 10 slots. Keep top **N** where N is the `top_k` the tool was called with (default 8, max 20).

### 6.6 Response format

The tool returns a dict with both a **model-facing string** (citation-tagged markdown for the LLM to read) and a **structured sources array** (for the UI's `CitationRenderer.tsx`, matching the existing contract in `backend/app/services/web_search_tool.py`):

```python
{
    "formatted_text": "[1] (roadshow · 中金 · 2026-02-14) 管理层在 Q&A 中强调...\n\n[2] (earnings_transcript · Tencent · 2025-11-13) ...",
    "sources": [
        {
            "index": 1,
            "title": "中金 · 腾讯电话会纪要",
            "url": "",
            "website": "CICC",
            "date": "2026-02-14",
            "source_type": "kb",
            "doc_type": "roadshow",
            "doc_id": "...",
            "chunk_ids": ["..."]  # for drill-down
        }
    ]
}
```

This matches the existing `Source` TypeScript interface in `CitationRenderer.tsx` — adding a new `source_type: "kb"` badge colour is a 5-line change.

---

## 7. LLM integration — the multi-round tool loop

The chat system already runs a **5-round tool loop** per model call with 240 s / 120 s timeouts (`backend/app/services/chat_llm.py`). We slot in as peers to `web_search`, `alphapai_recall`, `jinmen_*`.

### 7.1 Three tools, not one

Research sessions are not one-shot retrievals. The LLM needs to *explore*. We expose three tools so it can:

**`kb_search`** — primary retrieval.

```json
{
  "name": "kb_search",
  "description": "Search the proprietary research knowledge base (roadshows, broker reports, expert calls, earnings transcripts, WeChat articles, sentiment) with hybrid semantic + lexical retrieval. Returns citation-indexed chunks. Prefer this over web_search for any question that might be answerable from internal research.",
  "parameters": {
    "type": "object",
    "properties": {
      "query": {"type": "string", "description": "Natural-language query. Chinese or English or mixed — both will be expanded."},
      "tickers": {"type": "array", "items": {"type": "string"}, "description": "Canonical tickers like NVDA.US, 0700.HK, 600519.SH. Pass exactly what the user asked about — don't guess."},
      "doc_types": {"type": "array", "items": {"type": "string", "enum": ["report","roadshow","comment","earnings_transcript","earnings_report","forum","interview","wechat_article","sentiment","article"]}},
      "sources": {"type": "array", "items": {"type": "string", "enum": ["alphapai","jinmen","meritco","thirdbridge","funda","gangtise","acecamp"]}},
      "date_range": {"type": "object", "properties": {"gte": {"type": "string"}, "lte": {"type": "string"}}},
      "top_k": {"type": "integer", "default": 8, "maximum": 20},
      "mode": {"type": "string", "enum": ["narrow","broad"], "default": "narrow", "description": "narrow = default production retrieval. broad = no time decay, no source dedup — use when user asks 'show me all sources that said X'."}
    },
    "required": ["query"]
  }
}
```

**`kb_fetch_document`** — read more of a hit.

```json
{
  "name": "kb_fetch_document",
  "description": "Fetch the full text (or a larger window) of a document already surfaced via kb_search. Use when one of the search hits looks highly relevant and you need more context than the returned chunk — e.g. to quote a full paragraph or check surrounding content.",
  "parameters": {
    "type": "object",
    "properties": {
      "doc_id": {"type": "string"},
      "max_chars": {"type": "integer", "default": 8000, "maximum": 30000}
    },
    "required": ["doc_id"]
  }
}
```

**`kb_list_facets`** — discovery.

```json
{
  "name": "kb_list_facets",
  "description": "List what's available in the KB along one dimension, optionally filtered. Use when you need to scope a search — e.g. 'what broker reports on NVDA do we have from the last 3 months' before kb_search.",
  "parameters": {
    "type": "object",
    "properties": {
      "dimension": {"type": "string", "enum": ["tickers","doc_types","sources","institutions","date_histogram"]},
      "filters": {"type": "object"},
      "top": {"type": "integer", "default": 20}
    },
    "required": ["dimension"]
  }
}
```

### 7.2 System prompt additions

In the chat system prompt we add a short routing hint so the LLM knows when to reach for the KB over web search:

> You have access to a proprietary research knowledge base (`kb_search`, `kb_fetch_document`, `kb_list_facets`) containing broker reports, roadshow transcripts, earnings calls, expert interviews, and WeChat articles covering A-shares, HK, and US equities. **Always try `kb_search` first** for questions that require specific numbers, quotes, or analyst views — it is higher quality than public web sources. Fall back to `web_search` only when the KB returns nothing relevant, or for very recent news (last 24 h) that crawlers may not have ingested yet. You can iterate — if the first `kb_search` returns weak hits, refine the query, narrow/widen the date range, or use `kb_list_facets` to see what's there.

### 7.3 Tool loop behaviour — worked example

User: *"把腾讯过去4个季度电话会里管理层对游戏监管的口风梳理一下"*

**Round 1** — LLM calls `kb_list_facets(dimension="date_histogram", filters={"tickers":["0700.HK"], "doc_types":["earnings_transcript"]})` → sees 4 earnings calls in the window.

**Round 2** — LLM calls `kb_search(query="游戏监管 管理层口风", tickers=["0700.HK"], doc_types=["earnings_transcript"], date_range={"gte":"2025-01-01"})` → top 8 chunks span all 4 calls.

**Round 3** — 2 of the chunks are very relevant but truncated. LLM calls `kb_fetch_document(doc_id=..., max_chars=15000)` twice to pull the full Q&A sections.

**Round 4** — LLM synthesises. Every sentence carries an inline `[N]` referencing a real chunk. The UI shows 4 blue-tagged KB citations plus hover cards.

This is exactly how a junior analyst would work. The multi-round tool loop is the point — not an optimisation.

### 7.4 Per-round streaming UX

Reuse the existing SSE event types:
- `{"type": "tool_status", "tool": "kb_search", "status": "searching", "query": "..."}`
- `{"type": "kb_result_preview", "hits": [{"title":..., "source":..., "date":...}]}` — new event, lets the UI show "found 8 hits" before the LLM responds
- `{"type": "sources", "sources": [...]}` on completion

All events flow through the existing `ChatTrace` so observability is free.

### 7.5 Timeouts and budgets

Inside the 120 s per-tool budget:
- Query rewriter: hard 2 s
- Qdrant hybrid search: hard 500 ms (overfetch 100)
- Reranker: hard 500 ms on GPU, 2 s on Cohere fallback
- Parent expansion + MMR + format: hard 300 ms (all Postgres + CPU)
- **p50 budget: ~1 s. p99: ~4 s.** Well inside the loop budget.

---

## 8. Grounding & citation integrity

Researchers are the hardest user class to serve because they will catch every hallucination. Three mechanisms:

1. **Inline citations required** — system prompt mandates that every factual sentence has a `[N]`. Claude and GPT comply well; Gemini needs a stricter sample prompt.
2. **Post-hoc citation check** (cheap, async) — a small validator LLM (Haiku) runs over `(sentence, cited_chunk_text)` pairs at the end and flags any citation that doesn't support its sentence. Failed citations turn red in the UI with a hover tooltip "weak match". This is decoupled from the streaming path — it annotates the saved `ChatModelResponse` after the fact.
3. **"Show the source" one-click** — inline `[N]` opens a side panel rendering the full parent chunk with the cited span highlighted. Same Postgres lookup as `kb_fetch_document`.

Keep the existing `chat_model_responses.sources` JSONB contract; extend the `Source` type with `source_type: "kb"` and a new `doc_id` field so the side panel can fetch the parent.

---

## 9. Access control & multi-tenancy (later but plan for it now)

Current users are subjective researchers at one firm — single tenant. The data licences are presumably per-seat / per-firm. Plan for two future needs:

- **Per-source access flags** (some users might not be licensed for Third Bridge). Add a `source_access: list[str]` to the `User` model and inject it into every `kb_search` filter.
- **Row-level access log** — every chunk retrieval is logged (user, chunk_id, doc_id, at). Needed for audit trails with data vendors.

Both are cheap to add now, expensive to retrofit later.

---

## 10. Observability

Extend `backend/app/services/chat_debug.py`'s `ChatTrace` with:

- `KB_REQUEST` — query, filters, mode, rewrite output
- `KB_HYBRID_RESULT` — top-100 IDs + scores (dense, sparse, rrf)
- `KB_RERANK_RESULT` — top-20 IDs + scores
- `KB_PARENT_EXPAND` — parent doc_ids
- `KB_TOOL_RESPONSE` — final top-N, elapsed_ms per stage

Each event carries the same `trace_id` as the rest of the request — one `grep trace=xxxxxxxx logs/chat_debug.log` follows the entire chain end-to-end, same as today.

Infra metrics via Prometheus / Grafana:
- Qdrant: p50/p95/p99 search latency, collection size, segment count
- Embedder GPU: queue depth, batch size, tok/sec, OOM events
- Reranker GPU: queue depth, p95
- KB tool: calls/sec, calls/chat (distribution), hit-rate (top-1 above threshold)

Alert when: Qdrant p95 > 1 s, reranker queue > 20, embedder queue > 500, hit rate < 30%.

---

## 11. Evaluation harness — this is the step most RAG projects skip

Without eval, retrieval silently rots as you add documents and refactor chunkers. Budget one engineer-week to build this properly before launch.

### 11.1 Golden set

~300 hand-curated Q&A pairs, spanning:
- Ticker-scoped questions (A / HK / US)
- Sentiment questions ("how does the street feel about X")
- Quantitative questions ("what's management's guidance for Q2 margin")
- Multi-hop ("how has X's stance on Y changed over Z")
- Time-bounded ("in the last 3 months…")
- Multi-source ("compare broker views on X")

Each item: `{question, must_cite_doc_ids[], must_not_cite_doc_ids[], expected_answer_snippet, notes}`. Build with help from the researchers themselves — they know the corpus.

### 11.2 Metrics

At retrieval layer: **Recall@20, nDCG@10, MRR** on `must_cite_doc_ids`.
At end-to-end: **answer faithfulness** (Ragas), **citation precision** (every `[N]` in the answer maps to a real supporting chunk), **answer relevance** (LLM-judge graded).
Operational: **p50/p99 end-to-end latency**.

### 11.3 Nightly CI

Run the 300-pair set nightly, alert on regression > 3% on any metric vs. rolling 7-day baseline. Block any schema / chunker / embedder change from deploy without a green eval run.

### 11.4 A/B framework

Version every component (`chunker@v2`, `embedder@bge-m3@v1`, `reranker@bge-v2m3@v1`, `query_rewriter@prompt-v4`). Re-index and re-evaluate offline before rolling any change. Sticky routing so we can compare v2 and v3 on live traffic by user cohort.

---

## 12. Phased delivery plan

### M0 — Spike (1 week, 1 eng)

Goal: prove the stack works end-to-end on one platform.

- Stand up Qdrant (docker-compose, single node).
- Load BGE-M3 locally (GPU host), wrap it in an internal HTTP service.
- Write a one-shot ingester for `alphapai.wechat_articles` only: naive paragraph chunking, dense only, no rerank.
- Write `kb_search` as a new tool, wire into `chat_llm.py` dispatch.
- Manually test: chat the assistant, confirm inline citations resolve to real articles.

**Exit criteria:** one happy path, no claims about quality.

### M1 — MVP, single platform (2 weeks, 1–2 eng)

- Ingestion pipeline: Mongo change stream → Redis Streams → chunker → embedder → Qdrant + Postgres. Idempotent, resumable, tested.
- Type-specific chunking policy (section 5.4) for `wechat_articles`, `comments`, `roadshows`, `reports` within AlphaPai only.
- Hybrid (dense + sparse) retrieval with filters.
- Parent-chunk expansion.
- `kb_search` + `kb_fetch_document` tools wired and tested.
- `ChatTrace` events for all KB stages.
- 30 golden questions, hit Recall@20 ≥ 70% before advancing.

**Exit criteria:** subjective researchers dogfood AlphaPai-only KB and prefer it to `alphapai_recall` on ≥ 80% of questions.

### M2 — All platforms + reranker (2 weeks, 1–2 eng)

- Expand ingestion to all 7 platforms.
- Cross-platform dedup (SimHash).
- Canonical-ticker filter stack.
- BGE-reranker-v2-m3 on GPU in the query path.
- `kb_list_facets` tool.
- Contextual-chunk prefix at index time.
- Expand golden set to 150 questions; hit Recall@20 ≥ 80%, nDCG@10 ≥ 0.6.

**Exit criteria:** KB covers every platform; reranker shows measurable quality lift.

### M3 — Agentic loop polish + citations (1 week)

- Query rewriter (cheap-LLM pre-step).
- System prompt updates (KB-first routing).
- Citation validator pass on saved responses.
- Side-panel "show source" UI in `CitationRenderer.tsx`.

**Exit criteria:** 95% of generated citations resolve to a supporting chunk (post-hoc validator).

### M4 — Eval, ops, prod (2 weeks)

- Golden set expanded to 300.
- Nightly CI eval.
- Prometheus + Grafana dashboards.
- Canary deploy (10% of chat traffic) for 1 week.
- Full rollout + kill switch for the 3 tools.

**Exit criteria:** 2 weeks canary at zero regression on existing chat metrics (p95 latency, error rate); eval green; researchers endorse.

### Beyond M4 — items to consider, not commit

- Contradiction detection + present-both-sides in the reranker.
- Time-series aware retrieval (give the LLM a "show the timeline for ticker X" primitive).
- Graph overlay — ticker ↔ sector ↔ event knowledge graph, queryable as a 4th tool.
- Second-pass PDF extraction with LlamaParse for the ~3 K low-quality reports.
- Fine-tune the reranker on our own click logs once we have ≥ 50 K labelled (chat_id, chunk_id, accepted) triples.
- Feedback-loop — thumbs up/down per response already captured in `ChatModelResponse`; feed into hard-negative mining.

---

## 13. Cost and resource estimate

Assumes self-hosted open-source where possible.

| Component | Resource | Notes |
|---|---|---|
| Qdrant | 1 VM, 16 vCPU / 32 GB RAM / 100 GB SSD | Comfortable for 2–5 M chunks. |
| Embedder (BGE-M3) | 1 GPU (L4 / A10 / 3090) | Batched inference service. Can share host with reranker. |
| Reranker (BGE-reranker-v2-m3) | same GPU | ~6 GB VRAM. |
| Postgres | existing cluster | `kb_chunks`, `kb_docs`, `kb_doc_state` tables — ~5–10 GB with indexes. |
| Redis Streams | existing | minimal. |
| Contextual-chunk LLM | Claude Haiku via OpenRouter | **one-time** for 300 K chunks × ~250 out tokens ≈ **$15–30** total. |
| Query rewriter LLM | Claude Haiku / Gemini Flash | ~\$0.0004/call, cached — **~\$20/month** at 50 K chats/month. |
| Citation validator LLM | Haiku | async, ~\$10/month. |
| Ragas eval runs | nightly, 300 q × 5 judges | **~\$3/night**, **~\$90/month**. |

**Total OSS stack capex:** one GPU host + one CPU host. **Total LLM opex:** <\$200/month for 50 K chats. A Cohere Rerank fallback at \$2/1K calls would add \~\$100/month at the same scale if ever enabled — defer.

---

## 14. Risks & mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Remote Mongo `u_spider` credential still broken at M1 start | HIGH | Build on local Mongo (same schema). Only the connection URI changes. Block only on remote-only collections, not on the full MVP. |
| PDF extraction quality uneven → weak retrieval on reports | MED | Gate the re-extraction job on length delta; keep original if new is shorter. Track "short `summary_md`" docs as a known class. |
| Near-duplicates across platforms flood answers | MED-HIGH | SimHash dedup + reranker MMR + `is_duplicate_of` filter. Explicit `mode=broad` escape hatch. |
| Ticker alias drift (e.g. 港股 00700 vs 0700.HK) | MED | Already solved by `scripts/enrich_tickers.py` + alias table. Enforce canonical form in indexer. |
| Query rewriter hallucinates filters (adds wrong ticker) | MED | Strict JSON schema + sanity check (tickers must appear in an allow-list); fall back to raw query on schema fail. |
| Cross-encoder rerank GPU becomes bottleneck | LOW at current scale | Batch, autoscale. Cohere fallback if needed. |
| Contextual-prefix leakage into answers | LOW | Always return original-text to LLM; only prefix is indexed. Covered by integration test. |
| Eval harness never gets built → silent quality rot | HIGH if deprioritised | Make M4 a gate on full rollout, not an afterthought. |
| Hallucinated citations | MED | Post-hoc validator + inline `[N]` strictness in prompt + side-panel source view. |
| Vendor / legal — redistribution of Third Bridge / Jinmen content to end users | HIGH if licence audit happens | Per-source access flags from day one; row-level access log from M2. |

---

## 15. Open questions for the reviewer (you)

1. **Remote Mongo auth** — can you unblock `u_spider` / provide the correct `authSource`? If the remote is a *superset* of what local already has, MVP proceeds on local and we swap at M2. If it's a *different corpus* (not a superset), M1 targets should be re-scoped.
2. **Licence constraints** — are there any platforms whose content we cannot surface to the chat UI verbatim (vs summarised)? If yes, add a per-source `surface_mode = verbatim | summary_only` flag now — cheap to add, very expensive to retrofit.
3. **Researcher cohort size** — 5 users or 50? Affects whether we canary-route in M4 or just enable it for all.
4. **GPU budget** — is there an existing GPU host, or do we need to procure? If no GPU, we fall back to: BGE-M3 on CPU (4–6× slower, still works for 300 K chunks nightly), **or** Cohere Embed v3 + Rerank 3 via API (adds ~\$100/month, zero infra).
5. **Chat recommended question feature** — the recent `chat_recommended_questions` migration suggests a "canned question" surface. Do you want the KB to seed these dynamically from hot topics (e.g. "3 new NVDA reports this week — ask about them")? Easy follow-on if yes.
6. **Latency SLO** — today chat p95 end-to-end is where? If the researchers tolerate 30 s for deep queries, we have headroom for more aggressive rerank / multi-hop. If they want < 5 s, we compress rounds.

---

## 16. What I recommend next

1. **This week:** unblock the remote Mongo credential (section 1). In parallel, M0 spike on local Mongo + AlphaPai only — 3–5 days of work, end of which you have a working KB tool wired into the chat and inline citations in the UI.
2. **Next week:** review the spike with 2–3 researchers. Decide on: chunk sizes, should we surface `content_md` or a summarised variant to the UI, which 3 platforms are priority for M1.
3. **Then:** commit to M1–M4 or descope.

I can start M0 whenever you give the go. I'd like to also put together a short design-review doc (1 page) once you've reacted to this — covering whatever you push back on — so the final shape is locked before any infra is provisioned.

---

*Appendix A — file-level integration checklist (so whoever implements doesn't have to re-discover):*

- `backend/app/services/kb_service.py` — **new**, `KB_TOOLS` list + `async execute_tool()`. Mirrors the shape of `alphapai_service.py` / `jinmen_service.py` (the `Explore` agent mapped these precisely — see the service patterns there).
- `backend/app/services/kb_retrieval.py` — **new**, internal: query rewriter, hybrid search, rerank, MMR, formatter.
- `backend/app/services/kb_ingestion.py` — **new**, change-stream worker.
- `backend/app/services/chat_llm.py` — add `elif name.startswith("kb_"):` branch in `dispatch_tool()` (around line 963).
- `backend/app/api/chat.py` — concat `KB_TOOLS` into the tools list passed to `call_model_stream_with_tools()`.
- `backend/app/services/chat_debug.py` — add `log_kb_*` helpers on `ChatTrace`.
- `backend/app/models/kb.py` — **new**, `KBDocument`, `KBChunk`, `KBDocState` SQLAlchemy models.
- `backend/alembic/versions/p8h9i0j1k2l3_add_kb_tables.py` — **new** migration.
- `backend/app/config.py` — new settings: `qdrant_url`, `qdrant_collection`, `embedder_url`, `reranker_url`, `kb_dedupe_distance`, etc.
- `frontend/src/components/CitationRenderer.tsx` — add `source_type: "kb"` badge colour + "show source" panel handler.
- `frontend/src/pages/AIChat.tsx` — optional: KB-result preview strip under the input.
- `docker-compose.yml` — add `qdrant` service. Persistent volume.
- `scripts/kb_backfill.py` — **new**, one-shot walker with resume.
- `scripts/kb_eval.py` — **new**, runs the golden set + Ragas, writes a JSON report, CI-friendly exit code.
