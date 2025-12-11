"""Step 2: Classify text files as DAT events using GPT."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from app.analyze.gpt import classify_texts_from_dir
from app.config import get_settings
from scripts.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify text files as DAT events")
    parser.add_argument("--input-dir", type=str, required=True, help="Directory containing .txt files")
    parser.add_argument("--limit", type=int, help="Limit number of files to process")
    parser.add_argument("--workers", type=int, help="Number of parallel workers")
    parser.add_argument("--no-save", action="store_true", help="Don't save JSONL results")
    parser.add_argument("--config", type=str, help="Path to config YAML file")
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    classify_config = config.get("classify", {})
    
    # Get settings
    settings = get_settings()
    
    # Determine input directory
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        # Try relative to news_text_dir
        settings = get_settings()
        input_dir = Path(settings.news_text_dir) / args.input_dir
        if not input_dir.exists():
            raise FileNotFoundError(f"Directory not found: {args.input_dir}")
    
    # Get parameters
    limit_files = args.limit or classify_config.get("limit_files")
    workers = args.workers or classify_config.get("workers") or settings.openai_classify_workers
    save_jsonl = not args.no_save
    
    logger.info(f"Classifying files in {input_dir}...")
    logger.info(f"Limit: {limit_files}, Workers: {workers}, Save JSONL: {save_jsonl}")
    
    try:
        result = classify_texts_from_dir(
            input_dir,
            save_jsonl=save_jsonl,
            limit_files=limit_files,
            workers=workers,
        )
        
        logger.info(f"Classification complete:")
        logger.info(f"  Total files: {result['count']}")
        logger.info(f"  Positives: {result['positives']}")
        logger.info(f"  Saved file: {result.get('saved_file', 'N/A')}")
        
        # Export positives if configured
        export_positives = classify_config.get("export_positives", True)
        if export_positives and result['positives'] > 0:
            from pathlib import Path
            import shutil
            
            positives_dir = Path(settings.positive_text_dir) / input_dir.name
            positives_dir.mkdir(parents=True, exist_ok=True)
            
            copied = 0
            for r in result.get("results", []):
                if r.get("is_dat"):
                    src = input_dir / r["file"]
                    if src.exists():
                        shutil.copy2(src, positives_dir / src.name)
                        copied += 1
            
            logger.info(f"Exported {copied} positive files to {positives_dir}")
            
    except Exception as e:
        logger.error(f"Classification failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

