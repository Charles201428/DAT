#!/usr/bin/env python3
"""Step 5: Deduplicate JSON files based on stock ticker, token, and date.

Usage:
    python scripts/05_dedup.py --input-dir DIR [--keep STRATEGY] [--remove] [--config CONFIG_FILE]

Example:
    python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z
    python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z --keep largest --remove
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.utils.dedupe import dedupe_folder
from scripts.config_loader import get_settings_from_config


def main():
    parser = argparse.ArgumentParser(description="Deduplicate JSON files")
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing .json files to deduplicate",
    )
    parser.add_argument(
        "--keep",
        type=str,
        choices=["largest", "newest", "most_filled", "first"],
        default="largest",
        help="Strategy for which file to keep (default: largest)",
    )
    parser.add_argument(
        "--remove",
        action="store_true",
        help="Delete duplicates instead of moving to _dedup_trash",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report duplicates without modifying files",
    )
    parser.add_argument(
        "--require-all",
        action="store_true",
        default=True,
        help="Require stock, token, and date to deduplicate (default: True)",
    )
    parser.add_argument(
        "--include-related",
        action="store_true",
        default=True,
        help="Also move/delete sibling files (e.g., .orig.txt) (default: True)",
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
    
    print(f"Deduplicating files in: {input_dir}")
    print(f"  Keep strategy: {args.keep}")
    print(f"  Remove duplicates: {args.remove}")
    print(f"  Dry run: {args.dry_run}")
    
    # Run deduplication
    result = dedupe_folder(
        input_dir,
        keep=args.keep,
        require_all=args.require_all,
        remove_duplicates=args.remove,
        include_related=args.include_related,
        dry_run=args.dry_run,
    )
    
    print(f"\nDeduplication complete!")
    print(f"  Total files: {result.get('total_files', 0)}")
    print(f"  Unique groups: {result.get('unique_groups', 0)}")
    print(f"  Duplicates found: {result.get('duplicates_found', 0)}")
    
    if not args.dry_run:
        print(f"  Files moved/deleted: {result.get('files_moved', 0)}")
    
    print(f"\nNext step: Export to CSV:")
    print(f"  python scripts/06_export_csv.py --input-dir {input_dir}")


if __name__ == "__main__":
    main()

