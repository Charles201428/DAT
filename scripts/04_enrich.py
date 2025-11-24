#!/usr/bin/env python3
"""Step 4: Enrich JSON files with stock and token price data.

Usage:
    python scripts/04_enrich.py --input-dir DIR [--limit LIMIT] [--config CONFIG_FILE]

Example:
    python scripts/04_enrich.py --input-dir positive_DAT/20251120_190823Z
    python scripts/04_enrich.py --input-dir positive_DAT/20251120_190823Z --limit 50
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.enrich.alpha import enrich_folder_with_alpha
from app.enrich.coingecko import enrich_folder_with_coingecko
from scripts.config_loader import get_settings_from_config


def main():
    parser = argparse.ArgumentParser(description="Enrich JSON files with price data")
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing .json files to enrich",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files to process",
    )
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="ISO timestamp for enrichment (default: now UTC)",
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to YAML config file (default: config.yaml or .env)",
    )
    
    args = parser.parse_args()
    
    # Load config if provided
    if args.config:
        config = get_settings_from_config(args.config)
        for key, value in config.items():
            os.environ[key] = str(value)
    
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        print(f"Error: Input directory does not exist: {input_dir}")
        sys.exit(1)
    
    # Parse as_of timestamp if provided
    as_of_dt = None
    if args.as_of:
        try:
            from datetime import datetime, timezone
            as_of_dt = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
            if as_of_dt.tzinfo is None:
                as_of_dt = as_of_dt.replace(tzinfo=timezone.utc)
        except Exception as e:
            print(f"Error: Invalid as-of timestamp: {e}")
            sys.exit(1)
    
    print(f"Enriching files in: {input_dir}")
    
    # Run enrichment (both stock and token)
    async def run_enrichment():
        print("\nEnriching with Alpha Vantage (stocks)...")
        stock_result = await enrich_folder_with_alpha(input_dir, as_of=as_of_dt, limit_files=args.limit)
        print(f"  Stock enrichment complete: {stock_result.get('processed', 0)} files processed")
        
        print("\nEnriching with CoinGecko (tokens)...")
        token_result = await enrich_folder_with_coingecko(input_dir, as_of=as_of_dt, limit_files=args.limit)
        print(f"  Token enrichment complete: {token_result.get('processed', 0)} files processed")
        
        return {"stocks": stock_result, "tokens": token_result}
    
    result = asyncio.run(run_enrichment())
    
    print(f"\nEnrichment complete!")
    print(f"  Stock files processed: {result['stocks'].get('processed', 0)}")
    print(f"  Token files processed: {result['tokens'].get('processed', 0)}")
    
    print(f"\nNext step: Run deduplication on this directory:")
    print(f"  python scripts/05_dedup.py --input-dir {input_dir}")


if __name__ == "__main__":
    main()

