from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    POSTGRES_DSN: str 
    MASTER_LIST_POLL_INTERVAL_S: int = 60
    MASTER_LIST_MAX_BACKOFF_S: int = 300
    POLL_INTERVAL_ACTIVE_S: int = 20
    POLL_INTERVAL_EMPTY_S: int = 180
    POLL_INTERVAL_OFFLINE_S: int = 900
    OFFLINE_FAILURE_THRESHOLD: int = 3
    SERVER_QUERY_TIMEOUT_S: float = 4.0
    WORKER_COUNT: int = 200

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()
