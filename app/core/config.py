from dataclasses import dataclass
import os
from functools import lru_cache

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    env: str = os.getenv("STXTOFR_ENV", "local")
    log_level: str = os.getenv("STXTOFR_LOG_LEVEL", "INFO")
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
    trafikverket_api_url: str = os.getenv(
        "STXTOFR_TRAFIKVERKET_API_URL",
        "https://api.trafikinfo.trafikverket.se/v2/data.json",
    )
    trafikverket_api_key: str = os.getenv("STXTOFR_TRAFIKVERKET_API_KEY", "")


@lru_cache()
def get_settings() -> Settings:
    return Settings()

