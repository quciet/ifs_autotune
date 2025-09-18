from pydantic import BaseSettings


class Settings(BaseSettings):
    DB_PATH: str = "bigpopa.db"  # Results DB (created later)
    # Future: IFS_CORE_PATH, RUN_TIMEOUT_SEC, etc.


settings = Settings()
