"""Master pipeline script to run all processing steps sequentially."""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from app.config import get_settings
from app.ingest.cryptopanic import ingest_cryptopanic
from app.analyze.gpt import classify_texts_from_dir, format_texts_from_dir
from app.enrich.alpha import enrich_folder_with_alpha
from app.enrich.coingecko import enrich_folder_with_coingecko
from app.utils.dedupe import dedupe_folder
from scripts.config_loader import load_config

# Import CSV export logic
import csv
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def _extract_url_from_txt(json_file: Path) -> str:
    """Extract URL from corresponding .txt or .orig.txt file."""
    base_name = json_file.stem
    if base_name.endswith(".orig"):
        base_name = base_name[:-5]
    
    parent_dir = json_file.parent
    trash_dir = parent_dir / "_dedup_trash"
    
    txt_candidates = [
        json_file.with_suffix(".orig.txt"),
        json_file.with_suffix(".txt"),
        parent_dir / f"{base_name}.orig.txt",
        parent_dir / f"{base_name}.txt",
        trash_dir / f"{base_name}.orig.txt",
        trash_dir / f"{base_name}.txt",
        trash_dir / json_file.with_suffix(".orig.txt").name,
        trash_dir / json_file.with_suffix(".txt").name,
    ]
    
    for txt_file in txt_candidates:
        if txt_file.exists():
            try:
                content = txt_file.read_text(encoding="utf-8", errors="ignore")
                lines = content.splitlines()
                if lines and lines[0].startswith("URL:"):
                    return lines[0].split("URL:", 1)[1].strip()
            except Exception:
                continue
    return ""


def export_csv(input_dir: Path, output_file: str | None = None, exclude_no_token: bool = True) -> Path:
    """Export JSON files to CSV."""
    json_files = sorted([p for p in input_dir.glob("*.json") if p.is_file()])
    if not json_files:
        raise ValueError(f"No JSON files found in {input_dir}")
    
    all_data: list[dict[str, str]] = []
    all_fields: set[str] = set()
    
    for json_file in json_files:
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            
            token_value = (data.get("Token") or "").strip().upper()
            if exclude_no_token and (not token_value or token_value == "N/A"):
                continue
            
            url = _extract_url_from_txt(json_file)
            data["URL"] = url
            
            all_data.append(data)
            all_fields.update(data.keys())
        except Exception as e:
            logger.warning(f"Failed to parse {json_file.name}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No valid JSON data found")
    
    sorted_fields = sorted(all_fields)
    if "URL" in sorted_fields:
        sorted_fields.remove("URL")
        sorted_fields.insert(0, "URL")
    
    if output_file:
        csv_path = input_dir / output_file
    else:
        csv_path = input_dir / f"{input_dir.name}_combined_final.csv"
    
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sorted_fields)
        writer.writeheader()
        for row in all_data:
            complete_row = {field: str(row.get(field, "")) for field in sorted_fields}
            writer.writerow(complete_row)
    
    return csv_path


async def run_pipeline(config_path: str | None = None, pipeline_config_path: str | None = None) -> None:
    """Run the complete pipeline."""
    # Load configs
    config = load_config(config_path)
    pipeline_config = load_config(pipeline_config_path) if pipeline_config_path else {}
    
    # Get settings
    settings = get_settings()
    
    # Pipeline configuration
    pipeline_settings = pipeline_config.get("pipeline", {})
    skip_steps = set(pipeline_settings.get("skip_steps", []))
    continue_from_dir = pipeline_settings.get("continue_from_dir")
    
    # Step 1: Ingest
    ingest_config = pipeline_config.get("ingest", {})
    hours = ingest_config.get("hours", 24)
    
    if "ingest" not in skip_steps:
        logger.info("=" * 60)
        logger.info("STEP 1: INGEST")
        logger.info("=" * 60)
        try:
            result = await ingest_cryptopanic(hours=hours)
            logger.info(f"Ingestion complete: {result}")
            news_dir = Path(result.get("saved_dir", ""))
        except Exception as e:
            logger.error(f"Ingestion failed: {e}", exc_info=True)
            raise
    else:
        logger.info("Skipping ingest step")
        if continue_from_dir:
            news_dir = Path(continue_from_dir)
        else:
            # Find latest news directory
            base_dir = Path(settings.news_text_dir)
            if base_dir.exists():
                dirs = [p for p in base_dir.iterdir() if p.is_dir()]
                if dirs:
                    news_dir = max(dirs, key=lambda p: p.stat().st_mtime)
                else:
                    raise FileNotFoundError("No news directories found")
            else:
                raise FileNotFoundError(f"News directory not found: {base_dir}")
    
    # Step 2: Classify
    classify_config = pipeline_config.get("classify", {})
    
    if "classify" not in skip_steps:
        logger.info("=" * 60)
        logger.info("STEP 2: CLASSIFY")
        logger.info("=" * 60)
        try:
            result = classify_texts_from_dir(
                news_dir,
                save_jsonl=True,
                limit_files=classify_config.get("limit_files"),
                workers=classify_config.get("workers") or settings.openai_classify_workers,
            )
            logger.info(f"Classification: {result['count']} files, {result['positives']} positives")
            
            # Export positives
            if classify_config.get("export_positives", True) and result['positives'] > 0:
                import shutil
                positives_dir = Path(settings.positive_text_dir) / news_dir.name
                positives_dir.mkdir(parents=True, exist_ok=True)
                
                copied = 0
                for r in result.get("results", []):
                    if r.get("is_dat"):
                        src = news_dir / r["file"]
                        if src.exists():
                            shutil.copy2(src, positives_dir / src.name)
                            copied += 1
                
                logger.info(f"Exported {copied} positives to {positives_dir}")
                work_dir = positives_dir
            else:
                work_dir = news_dir
        except Exception as e:
            logger.error(f"Classification failed: {e}", exc_info=True)
            raise
    else:
        logger.info("Skipping classify step")
        if continue_from_dir:
            work_dir = Path(continue_from_dir)
        else:
            work_dir = Path(settings.positive_text_dir) / news_dir.name
            if not work_dir.exists():
                raise FileNotFoundError(f"Work directory not found: {work_dir}")
    
    # Step 3: Format
    format_config = pipeline_config.get("format", {})
    
    if "format" not in skip_steps:
        logger.info("=" * 60)
        logger.info("STEP 3: FORMAT")
        logger.info("=" * 60)
        try:
            result = format_texts_from_dir(
                work_dir,
                limit_files=format_config.get("limit_files"),
                orig_only=format_config.get("orig_only", True),
            )
            logger.info(f"Formatting: {result['saved']} saved, {result['errors']} errors")
        except Exception as e:
            logger.error(f"Formatting failed: {e}", exc_info=True)
            raise
    else:
        logger.info("Skipping format step")
    
    # Step 4: Enrich
    enrich_config = pipeline_config.get("enrich", {})
    
    if "enrich" not in skip_steps:
        logger.info("=" * 60)
        logger.info("STEP 4: ENRICH")
        logger.info("=" * 60)
        try:
            as_of = None
            if enrich_config.get("as_of"):
                as_of = datetime.fromisoformat(enrich_config["as_of"].replace("Z", "+00:00"))
                if as_of.tzinfo is None:
                    as_of = as_of.replace(tzinfo=timezone.utc)
            
            enrich_stock = enrich_config.get("enrich_stock", True)
            enrich_token = enrich_config.get("enrich_token", True)
            
            if enrich_stock:
                logger.info("Enriching stocks...")
                stock_result = await enrich_folder_with_alpha(
                    work_dir,
                    as_of=as_of,
                    limit_files=enrich_config.get("limit_files"),
                )
                logger.info(f"Stock enrichment: {stock_result.get('saved', 0)} saved")
            
            if enrich_token:
                logger.info("Enriching tokens...")
                token_result = await enrich_folder_with_coingecko(
                    work_dir,
                    as_of=as_of,
                    limit_files=enrich_config.get("limit_files"),
                )
                logger.info(f"Token enrichment: {token_result.get('saved', 0)} saved")
        except Exception as e:
            logger.error(f"Enrichment failed: {e}", exc_info=True)
            raise
    else:
        logger.info("Skipping enrich step")
    
    # Step 5: Deduplicate
    dedup_config = pipeline_config.get("dedup", {})
    
    if "dedup" not in skip_steps:
        logger.info("=" * 60)
        logger.info("STEP 5: DEDUPLICATE")
        logger.info("=" * 60)
        try:
            result = dedupe_folder(
                work_dir,
                keep=dedup_config.get("keep", "largest"),
                require_all=dedup_config.get("require_all", True),
                remove_duplicates=dedup_config.get("remove_duplicates", False),
                include_related=dedup_config.get("include_related", True),
                dry_run=dedup_config.get("dry_run", False),
            )
            logger.info(f"Deduplication: {result['groups_deduped']} groups deduplicated, {result['kept_count']} kept")
        except Exception as e:
            logger.error(f"Deduplication failed: {e}", exc_info=True)
            raise
    else:
        logger.info("Skipping dedup step")
    
    # Step 6: Export CSV
    export_config = pipeline_config.get("export_csv", {})
    
    if "export_csv" not in skip_steps:
        logger.info("=" * 60)
        logger.info("STEP 6: EXPORT CSV")
        logger.info("=" * 60)
        try:
            csv_path = export_csv(
                work_dir,
                output_file=export_config.get("output_file"),
                exclude_no_token=export_config.get("exclude_no_token", True),
            )
            logger.info(f"CSV exported to: {csv_path}")
        except Exception as e:
            logger.error(f"CSV export failed: {e}", exc_info=True)
            raise
    else:
        logger.info("Skipping export_csv step")
    
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Final output directory: {work_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run complete DAT processing pipeline")
    parser.add_argument("--config", type=str, help="Path to main config YAML file")
    parser.add_argument("--pipeline-config", type=str, default="pipeline_config.yaml",
                       help="Path to pipeline config YAML file (default: pipeline_config.yaml)")
    args = parser.parse_args()
    
    try:
        asyncio.run(run_pipeline(config_path=args.config, pipeline_config_path=args.pipeline_config))
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

