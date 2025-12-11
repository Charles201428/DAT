"""Step 5: Deduplicate JSON files."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from app.utils.dedupe import dedupe_folder
from scripts.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplicate JSON files")
    parser.add_argument("--input-dir", type=str, required=True, help="Directory containing .json files")
    parser.add_argument("--keep", type=str, choices=["largest", "newest", "most_filled", "first"], 
                       default="largest", help="Strategy for keeping files (default: largest)")
    parser.add_argument("--remove-duplicates", action="store_true", help="Delete duplicates instead of moving to trash")
    parser.add_argument("--no-related", action="store_true", help="Don't move/delete related files (.orig.txt, etc.)")
    parser.add_argument("--dry-run", action="store_true", help="Only report, don't modify files")
    parser.add_argument("--config", type=str, help="Path to config YAML file")
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    dedup_config = config.get("dedup", {})
    
    # Determine input directory
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Directory not found: {args.input_dir}")
    
    # Get parameters
    keep = args.keep or dedup_config.get("keep", "largest")
    remove_duplicates = args.remove_duplicates or dedup_config.get("remove_duplicates", False)
    include_related = not args.no_related and dedup_config.get("include_related", True)
    dry_run = args.dry_run or dedup_config.get("dry_run", False)
    require_all = dedup_config.get("require_all", True)
    
    logger.info(f"Deduplicating files in {input_dir}...")
    logger.info(f"Strategy: {keep}, Remove: {remove_duplicates}, Related: {include_related}, Dry run: {dry_run}")
    
    try:
        result = dedupe_folder(
            input_dir,
            keep=keep,
            require_all=require_all,
            remove_duplicates=remove_duplicates,
            include_related=include_related,
            dry_run=dry_run,
        )
        
        logger.info(f"Deduplication complete:")
        logger.info(f"  Groups considered: {result['groups_considered']}")
        logger.info(f"  Groups deduplicated: {result['groups_deduped']}")
        logger.info(f"  Files kept: {result['kept_count']}")
        logger.info(f"  Duplicates: {result['duplicate_count']}")
        
        if dry_run:
            logger.info("  (Dry run - no files were modified)")
        else:
            logger.info(f"  Actions taken: {len(result['duplicate_actions'])}")
        
    except Exception as e:
        logger.error(f"Deduplication failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

