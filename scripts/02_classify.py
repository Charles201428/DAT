#!/usr/bin/env python3
"""Step 2: Binary classification of DAT events using GPT.

Usage:
    python scripts/02_classify.py --input-dir DIR [--output-dir DIR] [--limit LIMIT] [--workers WORKERS] [--config CONFIG_FILE]

Example:
    python scripts/02_classify.py --input-dir news_text/20251120_190823Z
    python scripts/02_classify.py --input-dir news_text/20251120_190823Z --limit 100 --workers 10
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.analyze.gpt import classify_texts_from_dir
from app.config import get_settings
from scripts.config_loader import get_settings_from_config


def main():
    parser = argparse.ArgumentParser(description="Classify news articles as DAT events")
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing .txt files to classify",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory to save positive classifications (default: positive_DAT/{input_dir_name})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files to process",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: from config)",
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
    
    # Determine output directory
    settings = get_settings()
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(settings.positive_text_dir) / input_dir.name
    
    print(f"Classifying files in: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    # Run classification
    result = classify_texts_from_dir(
        input_dir,
        save_jsonl=True,
        limit_files=args.limit,
        workers=args.workers,
    )
    
    print(f"\nClassification complete!")
    print(f"  Total processed: {result.get('count', 0)}")
    print(f"  Positives (DAT events): {result.get('positives', 0)}")
    print(f"  Classifications saved to: {result.get('saved_file', 'N/A')}")
    
    # Copy positive files to output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    positives_copied = 0
    for r in result.get("results", []):
        if r.get("is_dat"):
            src = input_dir / r["file"]
            if src.exists():
                shutil.copy2(src, output_dir / src.name)
                positives_copied += 1
    
    print(f"  Positive files copied to: {output_dir}")
    print(f"  Files copied: {positives_copied}")
    
    if result.get("positives", 0) > 0:
        print(f"\nNext step: Run formatting on this directory:")
        print(f"  python scripts/03_format.py --input-dir {output_dir}")


if __name__ == "__main__":
    main()

