"""Application configuration loaded from environment variables.

Environment layering
--------------------
At startup pydantic-settings stitches together (in precedence order, last wins):

    1. process env vars (including APP_ENV)
    2. .env           — per-deploy working config (gitignored)
    3. .env.secrets   — shared secrets written by installers (gitignored)

For multi-environment deploys (prod vs staging) we ALSO respect
``APP_ENV``. When ``APP_ENV=staging`` the helper methods on ``Settings``
automatically scope state to a ``_staging`` suffix (Postgres DB, Redis
DB index, Milvus collections, ClickHouse DB) or a ``stg_`` prefix
(Mongo collections in shared databases — both prod and staging share
the same physical DBs in ``ta-mongo-crawl`` :27018, isolated at the
collection level via the prefix).

The suffix logic is *idempotent*: if an explicit env var already carries
the ``_staging`` tail the helpers won't double-apply it. This is why
``.env.staging`` can choose between setting
``POSTGRES_DB=trading_agent_staging`` explicitly or leaving it blank
and relying on ``APP_ENV`` to derive it.
"""
import os
from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache

# Find .env: config.py -> app/ -> backend/ -> trading_agent/ (3 parents)
_ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"
if not _ENV_FILE.exists():
    _ENV_FILE = Path.cwd() / ".env"

# Optional secrets file (e.g. TEI_API_KEY written by scripts/deploy_jumpbox_tei.sh).
# Values here OVERRIDE .env. Gitignored.
_ENV_SECRETS_FILE = Path(__file__).resolve().parent.parent.parent / ".env.secrets"
if not _ENV_SECRETS_FILE.exists():
    _ENV_SECRETS_FILE = Path.cwd() / ".env.secrets"

class Settings(BaseSettings):
    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "trading_agent"
    postgres_user: str = "trading_agent"
    postgres_password: str = "changeme"

    @property
    def database_url(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.effective_postgres_db}"

    @property
    def database_url_sync(self) -> str:
        return f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.effective_postgres_db}"

    @property
    def database_url_prod(self) -> str:
        """Async URL pinned to the *raw* postgres_db (no _staging suffix).

        Lets specific staging endpoints opt into reading prod's tables
        (e.g. portfolio scan results, populated only by the prod-only
        proactive scanner). On a prod process this resolves to the same
        URL as `database_url`, so no code path branches on env.
        """
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""

    @property
    def redis_url(self) -> str:
        db = self.redis_db_index
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{db}"
        return f"redis://{self.redis_host}:{self.redis_port}/{db}"

    # JWT
    jwt_secret_key: str = "changeme_jwt_secret_key_min_32_chars"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expire_minutes: int = 60
    jwt_refresh_token_expire_days: int = 7

    # LLM
    llm_provider: str = "minimax"
    llm_api_key: str = ""
    llm_prompt_language: str = "zh"

    # Feishu
    feishu_webhook_url: str = ""

    # Uqer
    uqer_token: str = ""

    # Baidu
    baidu_api_key: str = ""

    # Tavily Search
    tavily_api_key: str = ""

    # Jina Search + Reader
    jina_api_key: str = ""

    # AlphaPai
    alphapai_base_url: str = "https://api-test.rabyte.cn"
    alphapai_app_agent: str = ""
    alphapai_sync_enabled: bool = False
    alphapai_sync_interval_seconds: int = 3600
    alphapai_batch_size: int = 500

    # ===== 2026-04-26 从远端 192.168.31.176:35002 复制回本机 MongoDB =====
    # 所有 10 个 crawler/user-kb DB 迁回本机 ta-mongo-crawl 容器 :27018,
    # 数据目录 /home/ygwang/crawl_data/mongo。远端保留作为只读备份。
    # MONGO_URI 环境变量统一驱动 REMOTE_CRAWL_MONGO_URI，crawler scrapers 也用同
    # 一个 env 变量。env 不写则默认本机 27018。
    # PDF 全部从本地 SSD `/home/ygwang/crawl_data/<plat>_pdfs/` 加载，仅在
    # 本地 disk miss 时回退到 GridFS (fs.files + fs.chunks)。/mnt/share fallback
    # 已于 2026-04-26 移除 — alphapai_pdfs 已 rsync 回本机。
    REMOTE_CRAWL_MONGO_URI: str = os.environ.get(
        "MONGO_URI",
        "mongodb://127.0.0.1:27018/",
    )

    # AlphaPai MongoDB
    alphapai_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    alphapai_mongo_db: str = "alphapai-full"

    # SentimenTrader — 合并到 funda DB 的 sentimentrader_* 集合 (u_spider 无权限 sentimentrader DB)
    sentimentrader_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    sentimentrader_mongo_db: str = "funda"
    sentimentrader_collection: str = "sentimentrader_indicators"

    # SemiAnalysis (Substack) — 2026-04-24 迁到独立 foreign-website DB (之前 co-host 在 funda)
    semianalysis_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    semianalysis_mongo_db: str = "foreign-website"
    semianalysis_collection: str = "semianalysis_posts"
    semianalysis_state_collection: str = "_state_semianalysis"

    # The Information (theinformation.com) — 2026-04-25 落同 foreign-website DB
    the_information_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    the_information_mongo_db: str = "foreign-website"
    the_information_collection: str = "theinformation_posts"
    the_information_state_collection: str = "_state_theinformation"
    # 研报 PDF 本地落盘目录 — 2026-04-26 起统一回到 /home/ygwang/crawl_data/.
    # 之前一段时间 scraper 写入 /mnt/share/ygwang/alphapai_pdfs (SMB 共享盘),
    # 已 rsync 回本机后该路径退役.
    alphapai_pdf_dir: str = "/home/ygwang/crawl_data/alphapai_pdfs"

    # Jinmen MongoDB
    jinmen_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    jinmen_mongo_db: str = "jinmen-full"
    jinmen_pdf_dir: str = "/home/ygwang/crawl_data/jinmen_pdfs"
    # 外资研报 (oversea_reports) 落盘到独立目录 — scraper 历史路径与 jinmen_pdf_dir
    # 不同, 必须在 stream_pdf_or_file 的允许 root 列表里, 否则 _is_under 校验拒读
    # 本地文件、被迫走 GridFS / upstream 慢路径.
    jinmen_oversea_pdf_dir: str = "/home/ygwang/crawl_data/overseas_pdf"

    # Meritco MongoDB (久谦中台 → 远端 jiuqian-full)
    meritco_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    meritco_mongo_db: str = "jiuqian-full"
    meritco_pdf_dir: str = "/home/ygwang/crawl_data/meritco_pdfs"

    # Third Bridge MongoDB (远端 DB 名带连字符 third-bridge)
    thirdbridge_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    thirdbridge_mongo_db: str = "third-bridge"

    # Funda MongoDB
    funda_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    funda_mongo_db: str = "funda"

    # Gangtise MongoDB
    gangtise_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    gangtise_mongo_db: str = "gangtise-full"
    gangtise_pdf_dir: str = "/home/ygwang/crawl_data/gangtise_pdfs"

    # AceCamp MongoDB
    acecamp_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    acecamp_mongo_db: str = "acecamp"

    # AlphaEngine MongoDB
    alphaengine_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    alphaengine_mongo_db: str = "alphaengine"
    alphaengine_pdf_dir: str = "/home/ygwang/crawl_data/alphaengine_pdfs"

    # AceCamp 内容字段直接从 API 拿到 markdown 全文, 绝大多数无独立 PDF —
    # 仅 can_download 的少数文章会写入此目录 (/articles/download_url 返回 S3 URL)
    acecamp_pdf_dir: str = "/home/ygwang/crawl_data/acecamp_pdfs"

    # Personal Knowledge Base (per-user uploaded documents)
    # Users upload markdown / text / PDF / audio / docx / xlsx files; the
    # service parses them into searchable chunks and exposes `user_kb_search`
    # / `user_kb_fetch_document` tools to the AI chat assistant. Each user's
    # uploads are isolated by user_id filtering on every query.
    # 2026-04-26: replicated back to local Mongo (ta-mongo-crawl :27018).
    # Schema is unchanged — shared `documents` + `chunks` + GridFS, scoped
    # by user_id. Per-user collections would blow past Mongo's soft-limit on
    # collection count and fracture GridFS buckets; user_id scoping is the
    # right pattern at scale. Override via USER_KB_MONGO_URI / USER_KB_MONGO_DB
    # (defaults to MONGO_URI env so a single setting drives all workloads).
    user_kb_mongo_uri: str = os.environ.get(
        "USER_KB_MONGO_URI",
        os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/"),
    )
    user_kb_mongo_db: str = "ti-user-knowledge-base"
    # Per-file upload ceiling (bytes) for text/document uploads. 50 MB is
    # comfortable for reports / transcripts without letting a single upload
    # exhaust Mongo memory. Audio uploads use ``user_kb_max_audio_bytes``.
    user_kb_max_file_bytes: int = 50 * 1024 * 1024
    # Separate ceiling for audio (mp3/wav/m4a/...). A 2-hour meeting mp3 at
    # 128 kbps is ~115 MB; 500 MB gives plenty of headroom for longer or
    # high-bitrate recordings while still bounded well below the jumpbox
    # ASR service's hard cap.
    user_kb_max_audio_bytes: int = 500 * 1024 * 1024
    # Chunking policy used during parsing — 1000-char sliding window with
    # 200-char overlap is a reasonable default for BM25-style `$text` search.
    user_kb_chunk_size: int = 1000
    user_kb_chunk_overlap: int = 200
    # Per-user ceiling on total documents. Prevents a single account from
    # consuming unbounded storage.
    user_kb_max_docs_per_user: int = 500
    # How many parse jobs can run concurrently across the whole process. Each
    # PDF parse spawns a JVM process (opendataloader-pdf) that can consume
    # 200-500 MB RSS; 4 parallel is a sensible default on an 8-GB dev box.
    # Tune up on fatter hardware.
    user_kb_parse_concurrency: int = 4
    # Hard timeout for a single parse. A pathological PDF can keep the JVM
    # busy for tens of minutes — after this many seconds we cancel, mark the
    # doc failed, and free the slot for other uploads.
    user_kb_parse_timeout_seconds: int = 300
    # A `parsing` state older than this is assumed dead (worker crashed mid-flight
    # while the process stayed up). Recovery resets to `pending` and re-enqueues.
    user_kb_parse_stale_seconds: int = 900
    # Max length of a user-supplied filename we'll accept. Prevents pathological
    # inputs from exploding Mongo index size or Content-Disposition headers.
    user_kb_max_filename_length: int = 500

    # Dense retrieval (hybrid BM25 + vector) for the personal KB.
    # Default backend is OpenAI `text-embedding-3-small` (1536 dim) — SOTA-tier
    # multilingual, ~$0.02 per million tokens, negligible cost at our scale.
    # The previously-configured TEI jumpbox (Qwen3-Embedding-8B) is not served
    # anywhere right now; we switched to OpenAI for reliability.
    user_kb_embedding_model: str = "text-embedding-3-small"
    user_kb_embedding_dim: int = 1536
    user_kb_embedding_base_url: str = "https://api.openai.com/v1"
    # Per-batch size for bulk embedding calls. OpenAI accepts up to 2048 inputs
    # per request; 128 is a conservative balance between latency and throughput.
    user_kb_embedding_batch_size: int = 128
    # Milvus collection where user-KB vectors live. Kept separate from the
    # crawled-data collection (`kb_chunks`) because schemas and embedding
    # models differ.
    user_kb_milvus_collection: str = "user_kb_chunks"
    # Fusion: weight given to dense vs lexical in Reciprocal Rank Fusion. The
    # formula is score = 1/(rrf_k + rank). Higher rrf_k flattens the curve
    # (scores closer together); 60 is the empirical default from the paper.
    user_kb_rrf_k: int = 60

    # Qwen3-ASR jumpbox service (used to transcribe uploaded audio into text
    # before the normal user-kb chunk/index pipeline runs). The service binds
    # to 127.0.0.1:8760 on the jumpbox; ops/asr_tunnel/asr_tunnel.sh keeps a
    # supervised SSH tunnel open so this host can reach it at the URL below.
    asr_service_url: str = "http://127.0.0.1:8760"
    # Shared-secret bearer token. Generated by ops/asr_jumpbox/install_asr_jumpbox.sh
    # and written to .env.secrets; that file is loaded into the environment
    # at startup so this field is populated via ASR_SERVICE_API_KEY.
    asr_service_api_key: str = ""
    # Per-HTTP-call timeouts. Uploads can be large (1 GB cap on the far side)
    # over a local-loopback SSH tunnel, so we're generous with the upload one.
    asr_service_upload_timeout_seconds: int = 600
    # Interval between /jobs/{id} polls. 2 s is a good balance for the UI
    # progress bar — fast enough to feel live, not so fast that we DDoS
    # ourselves on a long transcription.
    asr_service_poll_interval_seconds: float = 2.0
    # How many consecutive poll failures before we treat the ASR service as
    # unreachable and surface AsrUnavailable. At 2 s/poll and 15 retries the
    # service has ~30 s to recover from a transient tunnel blip.
    asr_service_poll_retries: int = 15
    # Hard wall-clock ceiling for one transcription job (queued + running).
    # A 2-hour meeting at 60 s/segment takes ~120 GPU-seconds on A100, but we
    # pad heavily for queueing and model-load.
    asr_service_job_timeout_seconds: int = 3600

    # LLM used by on-demand admin scripts (e.g. scripts/llm_tag_tickers.py).
    # OpenAI-compatible endpoint — can point at Aliyun Bailian/DashScope,
    # MiniMax, OpenRouter, or anything similar.
    #
    # Realtime ingest-time LLM enrichment is OFF by default — crawlers and
    # sync services land raw scraped data only; no AlphaPai/Jiuqian processor
    # or hot-news filter will run unless `realtime_llm_enrichment_enabled` is
    # set True. Existing `enrichment` rows in Postgres are left untouched.
    llm_enrichment_api_key: str = ""
    llm_enrichment_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    llm_enrichment_model: str = "qwen-plus"
    realtime_llm_enrichment_enabled: bool = False

    # Realtime LLM ticker tagger — fallback NER for docs the rule path
    # leaves with `_canonical_tickers: []`. Polls fresh empty-canonical docs
    # (newer than `lookback_hours`) every `interval_sec`, calls a cheap chat
    # model, writes `_llm_canonical_tickers` etc. Daily budget is enforced via
    # Redis (`llm_tagger:cost:YYYY-MM-DD`). Service implementation lives at
    # `backend/app/services/realtime_llm_tagger.py`. Use the manual script
    # `scripts/llm_tag_tickers.py` for explicit large backfills.
    llm_tag_realtime_enabled: bool = False
    llm_tag_realtime_model: str = "qwen-plus"
    llm_tag_realtime_daily_budget_usd: float = 5.0
    llm_tag_realtime_interval_sec: int = 60
    llm_tag_realtime_lookback_hours: int = 2
    llm_tag_realtime_batch_size: int = 50

    # OpenRouter (for AI Chat multi-model)
    openrouter_api_key: str = ""

    # OpenAI (native API for GPT models)
    openai_api_key: str = ""

    # Google Gemini (native API with grounding)
    gemini_api_key: str = ""
    gemini_http_proxy: str = ""

    # ClickHouse (generic — used by engine for event study etc.)
    clickhouse_enabled: bool = False
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "default"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # Market data ClickHouse (A-share realtime kline + daily)
    # Separate from generic clickhouse_* because it points to a different node.
    market_ch_host: str = "192.168.31.137"
    market_ch_port: int = 38123
    market_ch_db: str = "db_market"
    market_ch_user: str = "researcher"
    market_ch_password: str = "researcher"

    # Wind consensus forecast MySQL (A-share 一致预期 — ASHARECONSENSUS* tables)
    # Used by dashboard 持仓概览 to show forward PE, target price, analyst ratings.
    consensus_mysql_host: str = "192.168.31.176"
    consensus_mysql_port: int = 3306
    consensus_mysql_user: str = "researcher"
    consensus_mysql_password: str = "researcher"
    consensus_mysql_db: str = "wind"
    consensus_enabled: bool = True

    # Alpaca (US equities realtime — IEX feed is free); fallback when Futu is down
    # Get keys at https://app.alpaca.markets/paper/dashboard/overview (paper account works for free data)
    alpaca_api_key: str = ""
    alpaca_api_secret: str = ""
    alpaca_data_url: str = "https://data.alpaca.markets"

    # Futu OpenAPI (primary source for HK / US — and A-share if permission is granted)
    # Requires running FutuOpenD locally. Login uses 牛牛号/moomoo ID + MD5 password.
    # A-share quote permission has to be opened separately in 富途 App; flip the
    # flag below once that's done. Until then A-shares stay on ClickHouse because
    # Futu's batch API is all-or-nothing — one forbidden ticker kills the batch.
    futu_opend_host: str = "127.0.0.1"
    futu_opend_port: int = 11111
    futu_login_account: str = ""
    futu_login_pwd_md5: str = ""
    futu_ashare_enabled: bool = False

    # Open API (for external agents)
    open_api_base_url: str = "http://localhost:8000"

    # ── Vector retrieval (Phase B hybrid RAG upgrade) ──────────────
    # TEI embedding server on jumpbox (Qwen3-Embedding-8B).
    # Base URL is the jumpbox LAN address + port. TEI_API_KEY is written
    # by scripts/deploy_jumpbox_tei.sh into .env.secrets on first deploy.
    tei_base_url: str = "http://116.239.28.36:8080"
    tei_api_key: str = ""
    tei_model_name: str = "qwen3-embed"

    # Milvus 2.4 standalone (docker-compose.vector.yml).
    milvus_host: str = "localhost"
    milvus_port: int = 19530
    milvus_collection: str = "kb_chunks"

    # Vector sync + kb_search kernel switch.
    # VECTOR_SYNC_ENABLED=False disables poller + delete sweep + reaper
    # (use during early Phase 1 when we only want manual ingestion).
    # KB_SEARCH_LEGACY=True routes kb_search to the old in-memory scorer
    # (rollback flag for Phase 1).
    vector_sync_enabled: bool = True
    kb_search_legacy: bool = False

    # Auto-sync cadence for the crawler-corpus MongoDB → Milvus poller
    # (backend/app/services/kb_vector_sync.py). The poller iterates every
    # spec in SPECS_LIST once per cycle, advancing each spec's watermark.
    # 1h cycle keeps event-loop pressure low (sync pymongo cursor inside
    # async ingest can pin the loop) while still landing fresh crawler
    # volume well inside the 2000-doc per-spec limit.
    kb_vector_sync_interval_seconds: int = 3600       # sleep between cycles (1h)
    kb_vector_sync_per_spec_limit: int = 2000         # max docs/cycle/spec
    kb_vector_sync_embed_batch_size: int = 32         # chunks per TEI call
    # Daily delete sweep fires once in this local-time window (24h clock).
    # The sweep diffs Mongo IDs ↔ Milvus doc_ids so crawler-deletes stop
    # returning stale hits. 03:xx is chosen so it runs outside US/CN market
    # hours and after the 03:10 Postgres backup has finished.
    kb_vector_sync_sweep_hour: int = 3
    kb_vector_sync_sweep_minute_start: int = 5
    kb_vector_sync_sweep_minute_end: int = 10

    # Stamp every chunk with this — used for partial re-embed when the
    # embedding model is swapped out. Bump when you change the model.
    embedding_model_version: str = "qwen3-emb-8b-v1"

    # App
    # APP_ENV is the single source of truth for multi-environment isolation.
    # Recognised values: "production" (default, no scoping), "staging"
    # (all properties below auto-scope state). Any other value is treated
    # as production — we don't create a third environment implicitly.
    app_env: str = "production"
    app_debug: bool = False
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    # ── Selective share-with-prod overrides (staging only) ──────────
    # By default APP_ENV=staging scopes every collection away from prod so
    # experiments can't corrupt prod data. That's right for Postgres
    # (per-env schema), but means staging's knowledge-base kernels
    # (`kb_search` + `user_kb_search`) start with empty Milvus/Mongo
    # collections — the AI assistant has nothing to search. These opt-in
    # flags point the KB tools back at prod's data:
    #
    #   kb_share_with_prod=true      → Milvus `kb_chunks` (crawled corpus).
    #                                  Effectively read-only since only
    #                                  prod's ingest pipeline writes it.
    #   user_kb_share_with_prod=true → Mongo `documents` / `chunks` /
    #                                  `fs.*` + Milvus `user_kb_chunks`
    #                                  (personal KB). Staging users see
    #                                  the same upload history as prod.
    #                                  CAUTION: a staging upload/delete
    #                                  HITS PROD'S DATA. Acceptable for
    #                                  internal testing; flip off for
    #                                  anything externally accessible.
    kb_share_with_prod: bool = False
    user_kb_share_with_prod: bool = False
    # Portfolio scan tables (portfolio_scan_results + portfolio_scan_baselines)
    # are populated only by run_proactive.py, which is prod-only via
    # _prod_only_guard. A staging-isolated Postgres DB therefore has zero
    # breaking-news rows and the dashboard renders empty cards. Defaulting
    # this flag to True so staging reads prod's scan tables — staging never
    # writes to them, so cross-DB reads are inherently safe. Flip to False
    # only to confirm the empty-state UI.
    portfolio_scan_share_with_prod: bool = True

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    # ── Environment scoping helpers ─────────────────────────────
    # Prod / staging share the same machine + Postgres/Redis instance +
    # remote Mongo cluster. To keep them safely isolated every piece of
    # named state — DB name, Redis DB index, Milvus collection, Mongo
    # collection, crawler-process control — is routed through these
    # helpers. Add a new piece of persistent state? Use them.

    @property
    def is_staging(self) -> bool:
        """True when this process is the staging deployment."""
        return (self.app_env or "").lower() == "staging"

    @property
    def env_suffix(self) -> str:
        """`_staging` for staging, empty for prod. Append to SQL/Milvus names."""
        return "_staging" if self.is_staging else ""

    @property
    def collection_prefix(self) -> str:
        """`stg_` for staging, empty for prod. Prepend to Mongo collections
        that live in a shared database — both envs share the same physical
        Mongo (ta-mongo-crawl :27018) and isolate at the collection level."""
        return "stg_" if self.is_staging else ""

    @property
    def redis_db_index(self) -> int:
        """Redis logical DB index. 0 for prod, 1 for staging — a single
        Redis server exposes 16 DBs and `SELECT` switches cheaply."""
        return 1 if self.is_staging else 0

    def _suffixed(self, name: str) -> str:
        """Return `name` with env_suffix, idempotently (never double-appends)."""
        if not self.is_staging or not name:
            return name
        if name.endswith(self.env_suffix):
            return name
        return f"{name}{self.env_suffix}"

    def _prefixed(self, name: str) -> str:
        """Return `name` with collection_prefix, idempotently."""
        if not self.is_staging or not name:
            return name
        if name.startswith(self.collection_prefix):
            return name
        return f"{self.collection_prefix}{name}"

    # Effective (= env-scoped) names. Callers should prefer these over the
    # raw fields. Raw fields remain available for migration tools that need
    # to address the *prod* DB explicitly from a staging process.

    @property
    def effective_postgres_db(self) -> str:
        return self._suffixed(self.postgres_db)

    @property
    def effective_clickhouse_db(self) -> str:
        return self._suffixed(self.clickhouse_db)

    @property
    def effective_milvus_collection(self) -> str:
        """Shared-KB Milvus collection (crawled corpus).

        When `kb_share_with_prod=true` (staging testing), returns the raw
        prod collection so the AI assistant has real data to search.
        """
        if self.kb_share_with_prod:
            return self.milvus_collection
        return self._suffixed(self.milvus_collection)

    @property
    def effective_user_kb_milvus_collection(self) -> str:
        """Personal-KB Milvus collection (user uploads).

        When `user_kb_share_with_prod=true` staging reads/writes the prod
        collection directly — see Settings docstring for the CAUTION.
        """
        if self.user_kb_share_with_prod:
            return self.user_kb_milvus_collection
        return self._suffixed(self.user_kb_milvus_collection)

    # Mongo collections in shared databases. Prod+staging coexist in the
    # same physical DB (`ti-user-knowledge-base` on ta-mongo-crawl :27018)
    # and isolate at the collection level via a `stg_` prefix — carried
    # over from the remote-Mongo era when `u_spider` could not create new
    # DBs and kept after the 2026-04-26 back-migration for simplicity.
    # `user_kb_share_with_prod=true` short-circuits these three so staging
    # reads prod's uploads + GridFS directly.

    @property
    def user_kb_docs_collection(self) -> str:
        if self.user_kb_share_with_prod:
            return "documents"
        return self._prefixed("documents")

    @property
    def user_kb_chunks_collection(self) -> str:
        if self.user_kb_share_with_prod:
            return "chunks"
        return self._prefixed("chunks")

    @property
    def user_kb_gridfs_bucket(self) -> str:
        """GridFS bucket name — maps to `{bucket}.files` + `{bucket}.chunks`."""
        if self.user_kb_share_with_prod:
            return "fs"
        return self._prefixed("fs")

    model_config = {
        # Pydantic-settings loads these in order; later files override earlier.
        # .env.secrets (if present) takes precedence over .env.
        "env_file": (str(_ENV_FILE), str(_ENV_SECRETS_FILE)),
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }

@lru_cache
def get_settings() -> Settings:
    return Settings()
