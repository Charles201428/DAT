"""Step 1: Ingest news from CryptoPanic API."""
from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from app.config import get_settings
from app.ingest.cryptopanic import ingest_cryptopanic
from scripts.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest news from CryptoPanic API")
    parser.add_argument("--hours", type=int, default=24, help="Hours to look back (default: 24)")
    parser.add_argument("--config", type=str, help="Path to config YAML file")
    args = parser.parse_args()
    
    # Load config (for future extensibility)
    config = load_config(args.config)
    
    # Get settings (loads from .env)
    settings = get_settings()
    
    # Override with config if provided
    hours = config.get("ingest", {}).get("hours", args.hours)
    
    logger.info(f"Starting ingestion for last {hours} hours...")
    
    try:
        result = await ingest_cryptopanic(hours=hours)
        logger.info(f"Ingestion complete: {result}")
        logger.info(f"Saved directory: {result.get('saved_dir', 'N/A')}")
    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())

