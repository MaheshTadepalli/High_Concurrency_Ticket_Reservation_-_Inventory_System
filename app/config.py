from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://ticket:ticket@localhost:5432/tickets"
    reservation_ttl_seconds: int = 120
    db_pool_min: int = 5
    db_pool_max: int = 40
    default_strategy: str = "atomic"
    host: str = "0.0.0.0"
    port: int = 8000


@lru_cache
def get_settings() -> Settings:
    return Settings()
