from dataclasses import dataclass
import os
from functools import lru_cache

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    env: str = os.getenv("STXTOFR_ENV", "local")
    log_level: str = os.getenv("STXTOFR_LOG_LEVEL", "INFO")
    repository_backend: str = os.getenv("STXTOFR_REPOSITORY_BACKEND", "db")
    database_url: str = os.getenv(
        "STXTOFR_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/stxtofr",
    )
    archive_backend: str = os.getenv("STXTOFR_ARCHIVE_BACKEND", "db")
    file_archive_path: str = os.getenv("STXTOFR_FILE_ARCHIVE_PATH", "./var/raw_payloads")
    publisher_backend: str = os.getenv("STXTOFR_PUBLISHER_BACKEND", "noop")
    kafka_bootstrap_servers: str = os.getenv(
        "STXTOFR_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )
    kafka_topic_facilities: str = os.getenv(
        "STXTOFR_KAFKA_TOPIC_FACILITIES", "stxtofr.facilities"
    )
    kafka_topic_gaps: str = os.getenv("STXTOFR_KAFKA_TOPIC_GAPS", "stxtofr.gaps")
    baserow_backend: str = os.getenv("STXTOFR_BASEROW_BACKEND", "noop")
    baserow_url: str = os.getenv("STXTOFR_BASEROW_URL", "http://127.0.0.1:8080")
    baserow_token: str = os.getenv("STXTOFR_BASEROW_TOKEN", "")
    baserow_table_id: str = os.getenv("STXTOFR_BASEROW_TABLE_ID", "")
    baserow_view_url: str = os.getenv("STXTOFR_BASEROW_VIEW_URL", "http://127.0.0.1:8080")
    baserow_admin_email: str = os.getenv("STXTOFR_BASEROW_ADMIN_EMAIL", "")
    baserow_admin_password: str = os.getenv("STXTOFR_BASEROW_ADMIN_PASSWORD", "")
    export_schema_version: str = os.getenv("STXTOFR_EXPORT_SCHEMA_VERSION", "stxtofr.facilities.v1")
    trafikverket_api_url: str = os.getenv(
        "STXTOFR_TRAFIKVERKET_API_URL",
        "https://api.trafikinfo.trafikverket.se/v2/data.json",
    )
    trafikverket_api_key: str = os.getenv("STXTOFR_TRAFIKVERKET_API_KEY", "")
    api_key: str = os.getenv("STXTOFR_API_KEY", "")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
