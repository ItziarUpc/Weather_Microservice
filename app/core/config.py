from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application configuration settings.

    This class loads configuration values from environment variables
    and optionally from a `.env` file. It uses Pydantic Settings
    to provide type validation and default values.

    Environment variables take precedence over `.env` values.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ---------------------------------------------------------------------
    # Application settings
    # ---------------------------------------------------------------------

    app_name: str = Field(
        default="weather-api",
        alias="APP_NAME",
        description="Application name displayed in logs and API documentation",
    )

    environment: str = Field(
        default="local",
        alias="ENVIRONMENT",
        description="Runtime environment (local, dev, prod)",
    )

    log_level: str = Field(
        default="INFO",
        alias="LOG_LEVEL",
        description="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )

    # ---------------------------------------------------------------------
    # Database settings
    # ---------------------------------------------------------------------

    database_url: str = Field(
        default="postgresql+psycopg://weather:weather@localhost:5432/weather",
        alias="DATABASE_URL",
        description="PostgreSQL connection URL",
    )

    # ---------------------------------------------------------------------
    # External weather providers
    # ---------------------------------------------------------------------

    aemet_api_key: Optional[str] = Field(
        default=None,
        alias="AEMET_API_KEY",
        description="API key used to authenticate requests to the AEMET API",
    )

    meteocat_api_key: Optional[str] = Field(
        default=None,
        alias="METEOCAT_API_KEY",
        description="API key used to authenticate requests to the Meteocat API",
    )


# Singleton settings instance
settings = Settings()