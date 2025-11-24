"""Configuration loader that supports both YAML config files and environment variables."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load configuration from YAML file and merge with environment variables.
    
    Environment variables take precedence over YAML config.
    Returns a flat dictionary suitable for use with pydantic Settings.
    """
    config: dict[str, Any] = {}
    
    # Try to load YAML config if path is provided
    if config_path:
        if not HAS_YAML:
            print(f"Warning: PyYAML not installed. Install with: pip install pyyaml")
            print(f"Falling back to environment variables only.")
        else:
            config_path = Path(config_path)
            if config_path.exists():
                try:
                    with config_path.open("r", encoding="utf-8") as f:
                        yaml_config = yaml.safe_load(f) or {}
                        # Flatten nested structure
                        config = _flatten_config(yaml_config)
                except Exception as e:
                    print(f"Warning: Failed to load config from {config_path}: {e}")
    
    # Override with environment variables (they take precedence)
    env_mappings = {
        "CRYPTOPANIC_TOKEN": "cryptopanic_token",
        "OPENAI_API_KEY": "openai_api_key",
        "ALPHAVANTAGE_API_KEY": "alphavantage_api_key",
        "CG_API_KEY": "coingecko_api_key",
        "CRYPTOPANIC_BASE": "cryptopanic_base",
        "CRYPTOPANIC_PUBLIC": "cryptopanic_public",
        "CRYPTOPANIC_FILTER": "cryptopanic_filter",
        "CRYPTOPANIC_CURRENCIES": "cryptopanic_currencies",
        "CRYPTOPANIC_KIND": "cryptopanic_kind",
        "CRYPTOPANIC_SIZE": "cryptopanic_size",
        "CRYPTOPANIC_PAGES": "cryptopanic_pages",
        "CRYPTOPANIC_REQUIRE_KEYWORD": "cryptopanic_require_keyword",
        "OPENAI_CLASSIFY_WORKERS": "openai_classify_workers",
        "NEWS_TEXT_DIR": "news_text_dir",
        "POSITIVE_TEXT_DIR": "positive_text_dir",
        "ALPHAVANTAGE_BASE": "alphavantage_base",
        "COINGECKO_BASE": "coingecko_base",
        "POSTGRES_DSN": "postgres_dsn",
        "LOG_LEVEL": "log_level",
        "USER_AGENT": "user_agent",
    }
    
    for env_key, config_key in env_mappings.items():
        env_value = os.getenv(env_key)
        if env_value is not None:
            # Convert string booleans
            if env_value.lower() in ("true", "1", "yes"):
                config[config_key] = True
            elif env_value.lower() in ("false", "0", "no"):
                config[config_key] = False
            else:
                # Try to convert to int if it's a number
                try:
                    config[config_key] = int(env_value)
                except ValueError:
                    config[config_key] = env_value
    
    return config


def _flatten_config(nested: dict[str, Any], parent_key: str = "", sep: str = "_") -> dict[str, Any]:
    """Flatten a nested dictionary."""
    items: list[tuple[str, Any]] = []
    for k, v in nested.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(_flatten_config(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_settings_from_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Get settings dictionary compatible with app.config.Settings."""
    config = load_config(config_path)
    
    # Map YAML keys to environment variable names (for pydantic Settings)
    # This allows scripts to use the same Settings class
    env_dict: dict[str, Any] = {}
    
    mapping = {
        "cryptopanic_token": "CRYPTOPANIC_TOKEN",
        "openai_api_key": "OPENAI_API_KEY",
        "alphavantage_api_key": "ALPHAVANTAGE_API_KEY",
        "coingecko_api_key": "CG_API_KEY",
        "cryptopanic_base": "CRYPTOPANIC_BASE",
        "cryptopanic_public": "CRYPTOPANIC_PUBLIC",
        "cryptopanic_filter": "CRYPTOPANIC_FILTER",
        "cryptopanic_currencies": "CRYPTOPANIC_CURRENCIES",
        "cryptopanic_kind": "CRYPTOPANIC_KIND",
        "cryptopanic_size": "CRYPTOPANIC_SIZE",
        "cryptopanic_pages": "CRYPTOPANIC_PAGES",
        "cryptopanic_require_keyword": "CRYPTOPANIC_REQUIRE_KEYWORD",
        "openai_classify_workers": "OPENAI_CLASSIFY_WORKERS",
        "news_text_dir": "NEWS_TEXT_DIR",
        "positive_text_dir": "POSITIVE_TEXT_DIR",
        "alphavantage_base": "ALPHAVANTAGE_BASE",
        "coingecko_base": "COINGECKO_BASE",
        "postgres_dsn": "POSTGRES_DSN",
        "log_level": "LOG_LEVEL",
        "user_agent": "USER_AGENT",
    }
    
    for yaml_key, env_key in mapping.items():
        if yaml_key in config:
            env_dict[env_key] = config[yaml_key]
    
    return env_dict

