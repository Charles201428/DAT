#!/usr/bin/env python3
"""Step 3: Extract structured data from text files using GPT.

Usage:
    python scripts/03_format.py --input-dir DIR [--limit LIMIT] [--config CONFIG_FILE]

Example:
    python scripts/03_format.py --input-dir positive_DAT/20251120_190823Z
    python scripts/03_format.py --input-dir positive_DAT/20251120_190823Z --limit 50
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.analyze.gpt import format_texts_from_dir
from scripts.config_loader import get_settings_from_config


def main():
    parser = argparse.ArgumentParser(description="Extract structured data from text files")
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing .orig.txt or .txt files to format",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files to process",
    )
    parser.add_argument(
        "--orig-only",
        action="store_true",
        default=True,
        help="Only process .orig.txt files (default: True)",
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
    
    print(f"Formatting files in: {input_dir}")
    
    # Run formatting
    result = format_texts_from_dir(
        input_dir,
        limit_files=args.limit,
        orig_only=args.orig_only,
    )
    
    print(f"\nFormatting complete!")
    print(f"  Files processed: {result.get('saved', 0)}")
    print(f"  Errors: {result.get('errors', 0)}")
    
    if result.get('saved', 0) > 0:
        print(f"\nNext step: Run enrichment on this directory:")
        print(f"  python scripts/04_enrich.py --input-dir {input_dir}")


if __name__ == "__main__":
    main()

