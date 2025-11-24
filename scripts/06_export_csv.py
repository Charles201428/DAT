#!/usr/bin/env python3
"""Step 6: Export JSON files to CSV format.

Usage:
    python scripts/06_export_csv.py --input-dir DIR [--output-file FILE] [--include-no-token] [--config CONFIG_FILE]

Example:
    python scripts/06_export_csv.py --input-dir positive_DAT/20251120_190823Z
    python scripts/06_export_csv.py --input-dir positive_DAT/20251120_190823Z --output-file custom.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.config_loader import get_settings_from_config


def _extract_url_from_txt(json_file: Path) -> str:
    """Extract URL from corresponding .txt or .orig.txt file."""
    base_name = json_file.stem
    if base_name.endswith(".orig"):
        base_name = base_name[:-5]  # Remove ".orig"
    
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


def main():
    parser = argparse.ArgumentParser(description="Export JSON files to CSV")
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Directory containing .json files to export",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default=None,
        help="Output CSV filename (default: {dir_name}_combined.csv)",
    )
    parser.add_argument(
        "--include-no-token",
        action="store_true",
        help="Include entries where Token is N/A (default: exclude them)",
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
    
    # Get all JSON files
    json_files = sorted([p for p in input_dir.glob("*.json") if p.is_file()])
    if not json_files:
        print(f"Error: No JSON files found in {input_dir}")
        sys.exit(1)
    
    print(f"Exporting {len(json_files)} JSON files from: {input_dir}")
    
    # Collect all data
    all_data: list[dict[str, str]] = []
    all_fields: set[str] = set()
    
    for json_file in json_files:
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            
            # Filter out entries where Token is N/A if requested
            token_value = (data.get("Token") or "").strip().upper()
            if not args.include_no_token and (not token_value or token_value == "N/A"):
                continue
            
            # Add URL from corresponding text file
            url = _extract_url_from_txt(json_file)
            data["URL"] = url
            
            all_data.append(data)
            all_fields.update(data.keys())
        except Exception as e:
            print(f"Warning: Failed to parse {json_file.name}: {e}")
            continue
    
    if not all_data:
        print("Error: No valid JSON data found (or all filtered out)")
        sys.exit(1)
    
    # Sort fields, but put URL first
    sorted_fields = sorted(all_fields)
    if "URL" in sorted_fields:
        sorted_fields.remove("URL")
        sorted_fields.insert(0, "URL")
    
    # Determine output filename
    if args.output_file:
        csv_path = input_dir / args.output_file
    else:
        csv_path = input_dir / f"{input_dir.name}_combined.csv"
    
    # Write CSV file
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fields)
            writer.writeheader()
            for row in all_data:
                complete_row = {field: row.get(field, "") for field in sorted_fields}
                writer.writerow(complete_row)
    except Exception as e:
        print(f"Error: Failed to write CSV: {e}")
        sys.exit(1)
    
    print(f"\nExport complete!")
    print(f"  CSV file: {csv_path}")
    print(f"  Rows: {len(all_data)}")
    print(f"  Columns: {len(sorted_fields)}")
    print(f"  Excluded no-token entries: {not args.include_no_token}")


if __name__ == "__main__":
    main()

