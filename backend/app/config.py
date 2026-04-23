"""Application configuration loaded from environment variables."""
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
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def database_url_sync(self) -> str:
        return f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""

    @property
    def redis_url(self) -> str:
        if self.redis_password:
            return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}/0"
        return f"redis://{self.redis_host}:{self.redis_port}/0"

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

    # ===== 2026-04-23 迁移至远端 MongoDB =====
    # 所有平台 8 个爬虫 DB 迁到 192.168.31.176:35002 (u_spider:prod_X5BKVbAc).
    # 源 DB 名 → 远端 DB 名映射(含连字符 / -full 后缀由远端授权决定):
    #   alphapai       -> alphapai-full
    #   jinmen         -> jinmen-full
    #   meritco        -> jiuqian-full
    #   thirdbridge    -> third-bridge    (注意连字符)
    #   funda          -> funda
    #   gangtise       -> gangtise-full
    #   acecamp        -> acecamp
    #   alphaengine    -> alphaengine
    #   sentimentrader -> funda (合并, indicators 迁到 funda.sentimentrader_indicators)
    # PDF 统一走 GridFS (fs.files + fs.chunks), filename = 原 pdf_*_dir 相对路径.
    # 本地 pdf_*_dir 作为 backend `stream_pdf_or_file` 的 fallback 保留.
    REMOTE_CRAWL_MONGO_URI: str = (
        "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin"
    )

    # AlphaPai MongoDB
    alphapai_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    alphapai_mongo_db: str = "alphapai-full"

    # SentimenTrader — 合并到 funda DB 的 sentimentrader_* 集合 (u_spider 无权限 sentimentrader DB)
    sentimentrader_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    sentimentrader_mongo_db: str = "funda"
    sentimentrader_collection: str = "sentimentrader_indicators"
    # 研报 PDF 本地落盘目录 — 迁移后作为 GridFS fallback 路径保留
    alphapai_pdf_dir: str = "/home/ygwang/crawl_data/alphapai_pdfs"

    # Jinmen MongoDB
    jinmen_mongo_uri: str = REMOTE_CRAWL_MONGO_URI
    jinmen_mongo_db: str = "jinmen-full"
    jinmen_pdf_dir: str = "/home/ygwang/crawl_data/jinmen_pdfs"

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

    # Research interaction log — captures full AI research assistant lifecycle
    # (LLM calls, tool calls with full args, searches, webpage reads, final
    # responses) for the admin-only visualization page. Writes are best-effort
    # fire-and-forget; auth/connection failures degrade to a no-op.
    # Remote target per docs/knowledge_base_plan.md §1:
    #   mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002
    # — credentials currently rejected, so we default to the local Mongo that
    # already hosts the crawler DBs. Override via RESEARCH_LOG_MONGO_URI once
    # the remote auth lands.
    research_log_mongo_uri: str = "mongodb://localhost:27017"
    research_log_mongo_db: str = "research-agent-interaction-process-all-accounts"
    # AceCamp 内容字段直接从 API 拿到 markdown 全文, 绝大多数无独立 PDF —
    # 仅 can_download 的少数文章会写入此目录 (/articles/download_url 返回 S3 URL)
    acecamp_pdf_dir: str = "/home/ygwang/crawl_data/acecamp_pdfs"

    # Personal Knowledge Base (per-user uploaded documents)
    # Users upload markdown / text / PDF / audio / docx / xlsx files; the
    # service parses them into searchable chunks and exposes `user_kb_search`
    # / `user_kb_fetch_document` tools to the AI chat assistant. Each user's
    # uploads are isolated by user_id filtering on every query.
    # 2026-04-23: migrated to remote Mongo `ti-user-knowledge-base` on
    # 192.168.31.176:35002 (shared ops cluster, u_spider has readWrite).
    # Schema is unchanged — shared `documents` + `chunks` + GridFS, scoped
    # by user_id. Per-user collections would blow past Mongo's soft-limit on
    # collection count and fracture GridFS buckets; user_id scoping is the
    # right pattern at scale. Override via USER_KB_MONGO_URI / USER_KB_MONGO_DB.
    user_kb_mongo_uri: str = (
        "mongodb://u_spider:prod_X5BKVbAc@192.168.31.176:35002/?authSource=admin"
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

    # LLM used by the enrichment background services (AlphaPai/Jiuqian
    # processors, hot-news filter). OpenAI-compatible endpoint — can point at
    # Aliyun Bailian/DashScope, MiniMax, OpenRouter, or anything similar.
    llm_enrichment_api_key: str = ""
    llm_enrichment_base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"
    llm_enrichment_model: str = "qwen-plus"

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

    # Stamp every chunk with this — used for partial re-embed when the
    # embedding model is swapped out. Bump when you change the model.
    embedding_model_version: str = "qwen3-emb-8b-v1"

    # App
    app_env: str = "development"
    app_debug: bool = True
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

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
