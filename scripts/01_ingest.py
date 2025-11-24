#!/usr/bin/env python3
"""Step 1: Ingest news from CryptoPanic API.

Usage:
    python scripts/01_ingest.py [--hours HOURS] [--config CONFIG_FILE]

Example:
    python scripts/01_ingest.py --hours 24
    python scripts/01_ingest.py --hours 720 --config config.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_settings
from app.ingest.cryptopanic import ingest_cryptopanic
from scripts.config_loader import get_settings_from_config


def main():
    parser = argparse.ArgumentParser(description="Ingest news from CryptoPanic API")
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Number of hours to look back (default: 24, max: 720)",
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
        # Set environment variables so get_settings() picks them up
        for key, value in config.items():
            os.environ[key] = str(value)
    
    # Validate hours
    if args.hours < 1 or args.hours > 720:
        print(f"Error: hours must be between 1 and 720, got {args.hours}")
        sys.exit(1)
    
    # Run ingestion
    print(f"Ingesting news from last {args.hours} hours...")
    result = asyncio.run(ingest_cryptopanic(hours=args.hours))
    
    print(f"\nIngestion complete!")
    print(f"  Inserted: {result.get('inserted', 0)} articles")
    print(f"  Saved to: {result.get('saved_dir', 'N/A')}")
    
    if result.get('saved_dir'):
        print(f"\nNext step: Run classification on this directory:")
        print(f"  python scripts/02_classify.py --input-dir {result['saved_dir']}")


if __name__ == "__main__":
    main()

