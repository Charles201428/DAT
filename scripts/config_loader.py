"""Configuration loader for standalone scripts."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load configuration from YAML file or environment variables.
    
    If config_path is provided, loads from that file.
    Otherwise, looks for config.yaml in the project root.
    Falls back to environment variables if no config file found.
    """
    if config_path:
        config_file = Path(config_path)
    else:
        # Look for config.yaml in project root (parent of scripts/)
        project_root = Path(__file__).parent.parent
        config_file = project_root / "config.yaml"
    
    config: dict[str, Any] = {}
    
    if config_file.exists():
        try:
            with config_file.open("r", encoding="utf-8") as f:
                config = yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Warning: Failed to load config from {config_file}: {e}")
    
    # Override with environment variables (for API keys, etc.)
    # These are typically set in .env and loaded by pydantic BaseSettings
    return config

