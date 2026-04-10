from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import field_validator
from pydantic_settings import BaseSettings

# Always resolve .env relative to this file's directory (backend/),
# regardless of the working directory uvicorn was launched from.
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

_DEFAULT_CORS = ["http://localhost:5173", "http://localhost:4173"]


class Settings(BaseSettings):
    supabase_url: str
    supabase_service_key: str
    supabase_jwt_secret: str
    openai_api_key: str = ""
    database_url: str = ""
    # Stored as string in .env, parsed to list by validator.
    # Example: CORS_ORIGINS=["https://app.vercel.app"]
    cors_origins: List[str] = _DEFAULT_CORS

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors(cls, v: object) -> object:
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return _DEFAULT_CORS
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
            return [o.strip() for o in v.split(",") if o.strip()]
        return v

    model_config = {
        "env_file": str(_ENV_FILE),
        "env_file_encoding": "utf-8",
    }


@lru_cache
def get_settings() -> Settings:
    return Settings()
