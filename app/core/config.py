from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # App
    app_name: str = Field(default="weather-api", alias="APP_NAME")
    environment: str = Field(default="local", alias="ENVIRONMENT")  # local|dev|prod
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    # Database
    database_url: str = Field(
        default="postgresql+psycopg://weather:weather@localhost:5432/weather",
        alias="DATABASE_URL",
    )

    # External providers
    aemet_api_key: Optional[str] = Field(default=None, alias="AEMET_API_KEY")
    meteocat_api_key: Optional[str] = Field(default=None, alias="METEOCAT_API_KEY") 


settings = Settings()