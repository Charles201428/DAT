"""Step 4: Enrich JSON files with stock and token price data."""
from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from app.enrich.alpha import enrich_folder_with_alpha
from app.enrich.coingecko import enrich_folder_with_coingecko
from scripts.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich JSON files with price data")
    parser.add_argument("--input-dir", type=str, required=True, help="Directory containing .json files")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    parser.add_argument("--as-of", type=str, help="ISO datetime string (default: now UTC)")
    parser.add_argument("--stock-only", action="store_true", help="Only enrich stock prices")
    parser.add_argument("--token-only", action="store_true", help="Only enrich token prices")
    parser.add_argument("--config", type=str, help="Path to config YAML file")
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    enrich_config = config.get("enrich", {})
    
    # Determine input directory
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Directory not found: {args.input_dir}")
    
    # Parse as_of datetime
    as_of = None
    if args.as_of:
        try:
            as_of = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
            if as_of.tzinfo is None:
                as_of = as_of.replace(tzinfo=timezone.utc)
        except Exception as e:
            raise ValueError(f"Invalid as_of format: {e}")
    elif enrich_config.get("as_of"):
        try:
            as_of = datetime.fromisoformat(enrich_config["as_of"].replace("Z", "+00:00"))
            if as_of.tzinfo is None:
                as_of = as_of.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    
    # Get limit
    limit_files = args.limit or enrich_config.get("limit_files")
    
    # Determine what to enrich
    enrich_stock = not args.token_only
    enrich_token = not args.stock_only
    
    logger.info(f"Enriching files in {input_dir}...")
    logger.info(f"Limit: {limit_files}, As of: {as_of or 'now'}")
    logger.info(f"Enrich stock: {enrich_stock}, Enrich token: {enrich_token}")
    
    try:
        results = {}
        
        # Enrich stocks
        if enrich_stock:
            logger.info("Enriching stock prices with Alpha Vantage...")
            stock_result = await enrich_folder_with_alpha(
                input_dir,
                as_of=as_of,
                limit_files=limit_files,
            )
            results["stocks"] = stock_result
            logger.info(f"Stock enrichment: {stock_result.get('saved', 0)} saved, {stock_result.get('skipped', 0)} skipped")
        
        # Enrich tokens
        if enrich_token:
            logger.info("Enriching token prices with CoinGecko...")
            token_result = await enrich_folder_with_coingecko(
                input_dir,
                as_of=as_of,
                limit_files=limit_files,
            )
            results["tokens"] = token_result
            logger.info(f"Token enrichment: {token_result.get('saved', 0)} saved, {token_result.get('skipped', 0)} skipped")
        
        logger.info("Enrichment complete!")
        
    except Exception as e:
        logger.error(f"Enrichment failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())

