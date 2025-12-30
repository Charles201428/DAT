"""
Treasury Pipeline Configuration

Settings and configuration management for the Treasury Pipeline.
This is a self-contained configuration module that reads from environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TreasurySettings:
    """
    Treasury Pipeline Settings.
    
    Reads configuration from environment variables with sensible defaults.
    This replaces the pydantic-based Settings class from app/config.py
    to make the tasks module self-contained.
    """
    
    # CryptoPanic API
    cryptopanic_base: str = field(
        default_factory=lambda: os.environ.get(
            "CRYPTOPANIC_BASE", "https://cryptopanic.com/api/growth/v2/posts/"
        )
    )
    cryptopanic_token: Optional[str] = field(
        default_factory=lambda: os.environ.get("CRYPTOPANIC_TOKEN")
    )
    cryptopanic_public: bool = field(
        default_factory=lambda: os.environ.get("CRYPTOPANIC_PUBLIC", "true").lower() == "true"
    )
    cryptopanic_filter: Optional[str] = field(
        default_factory=lambda: os.environ.get("CRYPTOPANIC_FILTER")
    )
    cryptopanic_currencies: Optional[str] = field(
        default_factory=lambda: os.environ.get("CRYPTOPANIC_CURRENCIES")
    )
    cryptopanic_kind: str = field(
        default_factory=lambda: os.environ.get("CRYPTOPANIC_KIND", "all")
    )
    cryptopanic_pages: int = field(
        default_factory=lambda: int(os.environ.get("CRYPTOPANIC_PAGES", "1"))
    )
    cryptopanic_require_keyword: Optional[str] = field(
        default_factory=lambda: os.environ.get("CRYPTOPANIC_REQUIRE_KEYWORD")
    )
    
    # OpenAI
    openai_api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("OPENAI_API_KEY")
    )
    openai_classify_workers: int = field(
        default_factory=lambda: int(os.environ.get("OPENAI_CLASSIFY_WORKERS", "10"))
    )
    
    # Alpha Vantage
    alphavantage_api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("ALPHAVANTAGE_API_KEY")
    )
    alphavantage_base: str = field(
        default_factory=lambda: os.environ.get(
            "ALPHAVANTAGE_BASE", "https://www.alphavantage.co/query"
        )
    )
    
    # CoinGecko
    coingecko_api_key: Optional[str] = field(
        default_factory=lambda: os.environ.get("CG_API_KEY")
    )
    
    # HTTP
    user_agent: str = field(
        default_factory=lambda: os.environ.get(
            "USER_AGENT", "CryptoTreasuryBot/0.1"
        )
    )


def get_settings() -> TreasurySettings:
    """
    Get Treasury Pipeline settings.
    
    Returns:
        TreasurySettings instance with values from environment variables.
    """
    return TreasurySettings()

