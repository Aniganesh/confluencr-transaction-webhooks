from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Intentionally fixed: the assessment requires a real 30-second delay.
PROCESSING_DELAY_SECONDS = 30


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://webhooks:webhooks@127.0.0.1:55432/webhooks"
    pool_min_size: int = Field(default=2, ge=1, le=100)
    pool_max_size: int = Field(default=10, ge=1, le=100)
    request_timeout_seconds: float = Field(default=0.45, gt=0, le=0.45)
    worker_poll_seconds: float = Field(default=0.1, ge=0.01, le=10)
    worker_batch_size: int = Field(default=100, ge=1, le=1000)
    log_level: str = "INFO"

    @model_validator(mode="after")
    def validate_pool_size(self):
        if self.pool_min_size > self.pool_max_size:
            raise ValueError("POOL_MIN_SIZE must not exceed POOL_MAX_SIZE")
        return self
