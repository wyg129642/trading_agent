"""Application configuration loaded from environment variables."""
from pathlib import Path
from pydantic_settings import BaseSettings
from functools import lru_cache

# Find .env: config.py -> app/ -> backend/ -> trading_agent/ (3 parents)
_ENV_FILE = Path(__file__).resolve().parent.parent.parent / ".env"
if not _ENV_FILE.exists():
    _ENV_FILE = Path.cwd() / ".env"

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

    # MiniMax (for AlphaPai enrichment)
    minimax_api_key: str = ""
    minimax_base_url: str = "https://api.minimaxi.com/v1"
    minimax_model: str = "MiniMax-M2"

    # OpenRouter (for AI Chat multi-model)
    openrouter_api_key: str = ""

    # ClickHouse
    clickhouse_enabled: bool = False
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "default"
    clickhouse_user: str = "default"
    clickhouse_password: str = ""

    # Open API (for external agents)
    open_api_base_url: str = "http://localhost:8000"

    # App
    app_env: str = "development"
    app_debug: bool = True
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    cors_origins: str = "http://localhost:5173,http://localhost:3000"

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    model_config = {"env_file": str(_ENV_FILE), "env_file_encoding": "utf-8"}

@lru_cache
def get_settings() -> Settings:
    return Settings()
