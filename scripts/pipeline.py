#!/usr/bin/env python3
"""Complete pipeline script that runs all 6 steps sequentially.

Usage:
    python scripts/pipeline.py [--config CONFIG_FILE] [--pipeline-config PIPELINE_CONFIG]

Example:
    python scripts/pipeline.py
    python scripts/pipeline.py --pipeline-config pipeline_config.yaml
    python scripts/pipeline.py --config config.yaml --pipeline-config pipeline_config.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import os
import shutil
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.config import get_settings
from app.ingest.cryptopanic import ingest_cryptopanic
from app.analyze.gpt import classify_texts_from_dir, format_texts_from_dir
from app.enrich.alpha import enrich_folder_with_alpha
from app.enrich.coingecko import enrich_folder_with_coingecko
from app.utils.dedupe import dedupe_folder
from scripts.config_loader import get_settings_from_config


def load_pipeline_config(config_path: str | Path | None = None) -> dict:
    """Load pipeline-specific configuration from YAML file."""
    if not config_path:
        return {}
    
    config_path = Path(config_path)
    if not config_path.exists():
        print(f"Warning: Pipeline config file not found: {config_path}")
        return {}
    
    try:
        import yaml
        with config_path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        print("Warning: PyYAML not installed. Install with: pip install pyyaml")
        return {}
    except Exception as e:
        print(f"Warning: Failed to load pipeline config: {e}")
        return {}


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


def step_1_ingest(pipeline_config: dict, main_config_path: str | None) -> Path | None:
    """Step 1: Ingest news from CryptoPanic."""
    print("\n" + "="*60)
    print("STEP 1: Ingesting news from CryptoPanic")
    print("="*60)
    
    hours = pipeline_config.get("ingestion", {}).get("hours", 24)
    if hours < 1 or hours > 720:
        print(f"Error: hours must be between 1 and 720, got {hours}")
        return None
    
    # Load main config if provided
    if main_config_path:
        config = get_settings_from_config(main_config_path)
        for key, value in config.items():
            os.environ[key] = str(value)
    
    print(f"Ingesting news from last {hours} hours...")
    result = asyncio.run(ingest_cryptopanic(hours=hours))
    
    saved_dir = result.get("saved_dir")
    if not saved_dir:
        print("Error: Ingestion failed or no directory created")
        return None
    
    print(f"✓ Ingested {result.get('inserted', 0)} articles")
    print(f"✓ Saved to: {saved_dir}")
    
    return Path(saved_dir)


def step_2_classify(input_dir: Path, pipeline_config: dict, main_config_path: str | None) -> Path | None:
    """Step 2: Classify DAT events."""
    print("\n" + "="*60)
    print("STEP 2: Classifying DAT events")
    print("="*60)
    
    if main_config_path:
        config = get_settings_from_config(main_config_path)
        for key, value in config.items():
            os.environ[key] = str(value)
    
    settings = get_settings()
    output_dir = Path(pipeline_config.get("directories", {}).get("positive_text_dir", settings.positive_text_dir)) / input_dir.name
    
    class_config = pipeline_config.get("classification", {})
    limit = class_config.get("limit_files")
    workers = class_config.get("workers")
    
    print(f"Classifying files in: {input_dir}")
    print(f"Output directory: {output_dir}")
    
    result = classify_texts_from_dir(
        input_dir,
        save_jsonl=True,
        limit_files=limit,
        workers=workers,
    )
    
    print(f"✓ Processed {result.get('count', 0)} files")
    print(f"✓ Found {result.get('positives', 0)} DAT events")
    
    # Copy positive files
    output_dir.mkdir(parents=True, exist_ok=True)
    positives_copied = 0
    for r in result.get("results", []):
        if r.get("is_dat"):
            src = input_dir / r["file"]
            if src.exists():
                shutil.copy2(src, output_dir / src.name)
                positives_copied += 1
    
    print(f"✓ Copied {positives_copied} positive files to: {output_dir}")
    
    if result.get("positives", 0) == 0:
        print("Warning: No DAT events found. Pipeline stopping.")
        return None
    
    return output_dir


def step_3_format(input_dir: Path, pipeline_config: dict, main_config_path: str | None) -> Path:
    """Step 3: Format text files to JSON."""
    print("\n" + "="*60)
    print("STEP 3: Formatting structured data")
    print("="*60)
    
    if main_config_path:
        config = get_settings_from_config(main_config_path)
        for key, value in config.items():
            os.environ[key] = str(value)
    
    fmt_config = pipeline_config.get("formatting", {})
    limit = fmt_config.get("limit_files")
    orig_only = fmt_config.get("orig_only", True)
    
    print(f"Formatting files in: {input_dir}")
    
    result = format_texts_from_dir(
        input_dir,
        limit_files=limit,
        orig_only=orig_only,
    )
    
    print(f"✓ Processed {result.get('saved', 0)} files")
    print(f"✓ Errors: {result.get('errors', 0)}")
    
    return input_dir  # Files are modified in-place


def step_4_enrich(input_dir: Path, pipeline_config: dict, main_config_path: str | None) -> Path:
    """Step 4: Enrich with price data."""
    print("\n" + "="*60)
    print("STEP 4: Enriching with price data")
    print("="*60)
    
    if main_config_path:
        config = get_settings_from_config(main_config_path)
        for key, value in config.items():
            os.environ[key] = str(value)
    
    enrich_config = pipeline_config.get("enrichment", {})
    limit = enrich_config.get("limit_files")
    as_of_str = enrich_config.get("as_of")
    
    as_of_dt = None
    if as_of_str:
        try:
            as_of_dt = datetime.fromisoformat(as_of_str.replace("Z", "+00:00"))
            if as_of_dt.tzinfo is None:
                as_of_dt = as_of_dt.replace(tzinfo=timezone.utc)
        except Exception as e:
            print(f"Warning: Invalid as_of timestamp: {e}, using current time")
    
    print(f"Enriching files in: {input_dir}")
    
    async def run_enrichment():
        print("  Enriching with Alpha Vantage (stocks)...")
        stock_result = await enrich_folder_with_alpha(input_dir, as_of=as_of_dt, limit_files=limit)
        print(f"  ✓ Stock enrichment: {stock_result.get('processed', 0)} files")
        
        print("  Enriching with CoinGecko (tokens)...")
        token_result = await enrich_folder_with_coingecko(input_dir, as_of=as_of_dt, limit_files=limit)
        print(f"  ✓ Token enrichment: {token_result.get('processed', 0)} files")
        
        return {"stocks": stock_result, "tokens": token_result}
    
    result = asyncio.run(run_enrichment())
    print(f"✓ Enrichment complete")
    
    return input_dir  # Files are modified in-place


def step_5_dedup(input_dir: Path, pipeline_config: dict, main_config_path: str | None) -> Path:
    """Step 5: Deduplicate files."""
    print("\n" + "="*60)
    print("STEP 5: Deduplicating files")
    print("="*60)
    
    if main_config_path:
        config = get_settings_from_config(main_config_path)
        for key, value in config.items():
            os.environ[key] = str(value)
    
    dedup_config = pipeline_config.get("deduplication", {})
    keep = dedup_config.get("keep", "largest")
    remove = dedup_config.get("remove_duplicates", False)
    require_all = dedup_config.get("require_all", True)
    include_related = dedup_config.get("include_related", True)
    
    print(f"Deduplicating files in: {input_dir}")
    print(f"  Keep strategy: {keep}")
    
    result = dedupe_folder(
        input_dir,
        keep=keep,
        require_all=require_all,
        remove_duplicates=remove,
        include_related=include_related,
        dry_run=False,
    )
    
    print(f"✓ Total files: {result.get('total_files', 0)}")
    print(f"✓ Unique groups: {result.get('unique_groups', 0)}")
    print(f"✓ Duplicates found: {result.get('duplicates_found', 0)}")
    
    return input_dir  # Files are modified in-place


def step_6_export_csv(input_dir: Path, pipeline_config: dict, main_config_path: str | None) -> Path:
    """Step 6: Export to CSV."""
    print("\n" + "="*60)
    print("STEP 6: Exporting to CSV")
    print("="*60)
    
    if main_config_path:
        config = get_settings_from_config(main_config_path)
        for key, value in config.items():
            os.environ[key] = str(value)
    
    import csv
    import json
    
    export_config = pipeline_config.get("export", {})
    output_file = export_config.get("output_file")
    exclude_no_token = export_config.get("exclude_no_token", True)
    
    json_files = sorted([p for p in input_dir.glob("*.json") if p.is_file()])
    if not json_files:
        print("Error: No JSON files found")
        return input_dir
    
    print(f"Exporting {len(json_files)} JSON files...")
    
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
            print(f"Warning: Failed to parse {json_file.name}: {e}")
            continue
    
    if not all_data:
        print("Error: No valid JSON data found")
        return input_dir
    
    sorted_fields = sorted(all_fields)
    if "URL" in sorted_fields:
        sorted_fields.remove("URL")
        sorted_fields.insert(0, "URL")
    
    if output_file:
        csv_path = input_dir / output_file
    else:
        csv_path = input_dir / f"{input_dir.name}_combined.csv"
    
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fields)
            writer.writeheader()
            for row in all_data:
                complete_row = {field: row.get(field, "") for field in sorted_fields}
                writer.writerow(complete_row)
    except Exception as e:
        print(f"Error: Failed to write CSV: {e}")
        return input_dir
    
    print(f"✓ Exported {len(all_data)} rows to: {csv_path}")
    print(f"✓ Columns: {len(sorted_fields)}")
    
    return input_dir


def main():
    parser = argparse.ArgumentParser(description="Run complete DAT pipeline")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to main YAML config file (API keys, etc.)",
    )
    parser.add_argument(
        "--pipeline-config",
        type=str,
        default="pipeline_config.yaml",
        help="Path to pipeline configuration file (default: pipeline_config.yaml)",
    )
    
    args = parser.parse_args()
    
    # Load pipeline config
    pipeline_config = load_pipeline_config(args.pipeline_config)
    
    # Check if we should continue from an existing directory
    continue_from_dir = pipeline_config.get("continue_from_dir")
    skip_steps = pipeline_config.get("skip_steps", [])
    
    print("="*60)
    print("DAT Pipeline - Complete Workflow")
    print("="*60)
    
    # Step 1: Ingest
    if 1 not in skip_steps:
        if continue_from_dir:
            input_dir = Path(continue_from_dir)
            if not input_dir.exists():
                print(f"Error: Continue directory does not exist: {input_dir}")
                sys.exit(1)
            print(f"\nContinuing from existing directory: {input_dir}")
        else:
            input_dir = step_1_ingest(pipeline_config, args.config)
            if not input_dir:
                print("Pipeline failed at Step 1")
                sys.exit(1)
    else:
        print("\nSkipping Step 1 (Ingest)")
        if continue_from_dir:
            input_dir = Path(continue_from_dir)
        else:
            print("Error: Must provide continue_from_dir when skipping Step 1")
            sys.exit(1)
    
    # Step 2: Classify
    if 2 not in skip_steps:
        input_dir = step_2_classify(input_dir, pipeline_config, args.config)
        if not input_dir:
            print("Pipeline failed at Step 2")
            sys.exit(1)
    else:
        print("\nSkipping Step 2 (Classify)")
    
    # Step 3: Format
    if 3 not in skip_steps:
        input_dir = step_3_format(input_dir, pipeline_config, args.config)
    else:
        print("\nSkipping Step 3 (Format)")
    
    # Step 4: Enrich
    if 4 not in skip_steps:
        input_dir = step_4_enrich(input_dir, pipeline_config, args.config)
    else:
        print("\nSkipping Step 4 (Enrich)")
    
    # Step 5: Dedup
    if 5 not in skip_steps:
        input_dir = step_5_dedup(input_dir, pipeline_config, args.config)
    else:
        print("\nSkipping Step 5 (Deduplicate)")
    
    # Step 6: Export CSV
    if 6 not in skip_steps:
        input_dir = step_6_export_csv(input_dir, pipeline_config, args.config)
    else:
        print("\nSkipping Step 6 (Export CSV)")
    
    print("\n" + "="*60)
    print("Pipeline Complete!")
    print("="*60)
    print(f"Final output directory: {input_dir}")
    print(f"CSV file: {input_dir / f'{input_dir.name}_combined.csv'}")


if __name__ == "__main__":
    main()

