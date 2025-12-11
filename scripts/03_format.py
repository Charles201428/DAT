"""Step 3: Format text files into structured JSON using GPT."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from app.analyze.gpt import format_texts_from_dir
from scripts.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Format text files into structured JSON")
    parser.add_argument("--input-dir", type=str, required=True, help="Directory containing .orig.txt files")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    parser.add_argument("--allow-txt", action="store_true", help="Also process .txt files (not just .orig.txt)")
    parser.add_argument("--config", type=str, help="Path to config YAML file")
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    format_config = config.get("format", {})
    
    # Determine input directory
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Directory not found: {args.input_dir}")
    
    # Get parameters
    limit_files = args.limit or format_config.get("limit_files")
    orig_only = not args.allow_txt and format_config.get("orig_only", True)
    
    logger.info(f"Formatting files in {input_dir}...")
    logger.info(f"Limit: {limit_files}, Orig only: {orig_only}")
    
    try:
        result = format_texts_from_dir(
            input_dir,
            limit_files=limit_files,
            orig_only=orig_only,
        )
        
        logger.info(f"Formatting complete:")
        logger.info(f"  Saved: {result['saved']}")
        logger.info(f"  Errors: {result['errors']}")
        logger.info(f"  Output files: {len(result['outputs'])}")
        
    except Exception as e:
        logger.error(f"Formatting failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

