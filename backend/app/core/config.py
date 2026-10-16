"""
app/core/config.py
------------------
Application configuration via Pydantic Settings.

All values are sourced exclusively from environment variables (or a .env file).
Hard-coding secrets or environment-specific values here is forbidden.
"""

from functools import lru_cache
from typing import List

from pydantic import AnyUrl, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Passwords that look like they were copy-pasted from documentation.
# Defined at module level (not as a Pydantic field) so it is a plain frozenset
# and not a ModelPrivateAttr.  Rejected in every environment.
_PLACEHOLDER_PASSWORDS: frozenset[str] = frozenset({
    'changeme', 'changeme-dev', 'change-me', 'change-me-before-running',
    'admin123', 'admin', 'password', 'secret', 'letmein',
    'change_me', 'CHANGE_ME', 'CHANGEME',
})


class Settings(BaseSettings):
    """
    Central configuration object for the platform.

    Every field maps directly to an environment variable of the same name
    (upper-cased).  Provide defaults that are safe for local development;
    production deployments must override them via the environment.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Application ────────────────────────────────────────────────────────────
    APP_NAME: str = "Resilient Big Data Pipeline Platform"
    APP_VERSION: str = "0.1.0"
    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"
    AI_ANALYSIS_ENABLED: bool = True
    AI_ANOMALY_CONTAMINATION: float = 0.1

    # ── Database ───────────────────────────────────────────────────────────────
    DATABASE_URL: str = (
        "postgresql+asyncpg://platform_user:platform_pass@postgres:5432/platform_db"
    )

    # ── JWT Auth ───────────────────────────────────────────────────────────────
    JWT_SECRET_KEY: str = "CHANGE_ME_in_production_use_a_long_random_string"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60

    # ── Auth ────────────────────────────────────────────────────────────────────
    # Required in all environments. Set via ADMIN_USERNAME / ADMIN_PASSWORD env
    # vars (see .env.example). The application fails fast at startup if these are
    # absent in non-development modes so a misconfigured deployment is caught
    # immediately rather than silently accepting an insecure default.
    ADMIN_USERNAME: str = ''
    ADMIN_PASSWORD: str = ''

    @field_validator('ADMIN_USERNAME', 'ADMIN_PASSWORD', mode='after')
    @classmethod
    def require_credentials_in_production(cls, v: str, info: ValidationInfo) -> str:
        """Fail fast when credentials are weak or unset in non-dev environments."""
        import os
        env = os.environ.get('APP_ENV', 'development')
        field = info.field_name

        if env != 'development' and not v:
            raise ValueError(
                f"{field} must be set via environment variable in "
                f"APP_ENV={env!r}. See .env.example."
            )

        # Reject placeholder values in every environment so misconfiguration
        # is caught at startup rather than at a security review.
        if field == 'ADMIN_PASSWORD' and v in _PLACEHOLDER_PASSWORDS:
            raise ValueError(
                f"ADMIN_PASSWORD is a known placeholder value ({v!r}). "
                "Set a strong password in your .env file."
            )

        return v

    # ── CORS ───────────────────────────────────────────────────────────────────
    CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000"]

    # ── Spark ──────────────────────────────────────────────────────────────────
    SPARK_MASTER_URL: str = "spark://spark-master:7077"
    SPARK_JOBS_PATH: str = "/app/spark-jobs"
    SPARK_SUBMIT_TIMEOUT_SECONDS: int = 300

    # ── Kafka Messaging ────────────────────────────────────────────────────────
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9093"
    KAFKA_CLIENT_ID: str = "pipeline-platform"
    KAFKA_DEFAULT_PARTITIONS: int = 3
    KAFKA_DEFAULT_REPLICATION_FACTOR: int = 1
    KAFKA_PRODUCER_ACKS: str = "all"
    KAFKA_CONSUMER_GROUP_PREFIX: str = "pipeline-group"

    # ── HDFS Storage (Phase 6) ─────────────────────────────────────────────────
    HDFS_NAMENODE_URL: str = "http://namenode:9870"
    HDFS_USER: str = "root"
    HDFS_DEFAULT_REPLICATION: int = 1  # single-datanode local dev default
    KEEP_STREAMING_CHECKPOINTS: bool = False

    # ── ChaosLab Reliability Engine (Phase 7) ──────────────────────────────────
    compose_project_name: str = "resilient-big-data-platform"
    COMPOSE_PROJECT_NAME: str = "resilient-big-data-platform"
    PROJECT_CONTAINER_PREFIX: str = "resilient-big-data-platform"
    """
    Docker Compose project name / container name prefix.
    Used by SafetyValidator to:
    1. Resolve the expected container name for each target service.
    2. Verify the container's com.docker.compose.project label matches
       this platform's own project — prevents targeting foreign containers.
    Override with the value of COMPOSE_PROJECT_NAME if set.
    """

    @field_validator("compose_project_name", "COMPOSE_PROJECT_NAME", "PROJECT_CONTAINER_PREFIX", mode="before")
    @classmethod
    def set_project_container_prefix(cls, v: object, info: ValidationInfo) -> str:
        import os
        return os.environ.get("COMPOSE_PROJECT_NAME", "resilient-big-data-platform")

    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: object) -> List[str]:
        """Accept a comma-separated string or a JSON list from the environment."""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        return v  # type: ignore[return-value]

    # ── Output Preview & Download (UI-6) ───────────────────────────────────────
    OUTPUT_PREVIEW_MAX_ROWS: int = 500
    OUTPUT_PREVIEW_MAX_FILE_BYTES: int = 256 * 1024 * 1024  # 256 MiB
    OUTPUT_DOWNLOAD_MAX_BYTES: int = 500 * 1024 * 1024      # 500 MiB
    OUTPUT_DOWNLOAD_TOKEN_TTL_SECONDS: int = 60


@lru_cache
def get_settings() -> Settings:
    """
    Return the singleton Settings instance.

    Decorated with lru_cache so the .env file is read only once per process.
    """
    return Settings()
