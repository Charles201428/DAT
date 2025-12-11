"""Step 6: Export JSON files to CSV."""
from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path

from scripts.config_loader import load_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _extract_url_from_txt(json_file: Path) -> str:
    """Extract URL from corresponding .txt or .orig.txt file.
    Checks both main folder and _dedup_trash folder.
    """
    # Get base name (remove .orig.json or .json)
    base_name = json_file.stem
    if base_name.endswith(".orig"):
        base_name = base_name[:-5]  # Remove ".orig"
    
    parent_dir = json_file.parent
    trash_dir = parent_dir / "_dedup_trash"
    
    # List of possible txt file locations to check (in order of preference)
    txt_candidates = [
        json_file.with_suffix(".orig.txt"),  # Same name with .orig.txt
        json_file.with_suffix(".txt"),  # Same name with .txt
        parent_dir / f"{base_name}.orig.txt",  # Base name with .orig.txt
        parent_dir / f"{base_name}.txt",  # Base name with .txt
        trash_dir / f"{base_name}.orig.txt",  # In trash with .orig.txt
        trash_dir / f"{base_name}.txt",  # In trash with .txt
        trash_dir / json_file.with_suffix(".orig.txt").name,  # Original name in trash
        trash_dir / json_file.with_suffix(".txt").name,  # Original name in trash
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Export JSON files to CSV")
    parser.add_argument("--input-dir", type=str, required=True, help="Directory containing .json files")
    parser.add_argument("--output-file", type=str, help="Output CSV filename (default: {dir_name}_combined.csv)")
    parser.add_argument("--include-no-token", action="store_true", help="Include entries where Token is N/A")
    parser.add_argument("--config", type=str, help="Path to config YAML file")
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    export_config = config.get("export_csv", {})
    
    # Determine input directory
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        raise FileNotFoundError(f"Directory not found: {args.input_dir}")
    
    # Get all JSON files
    json_files = sorted([p for p in input_dir.glob("*.json") if p.is_file()])
    if not json_files:
        raise ValueError(f"No JSON files found in {input_dir}")
    
    # Get parameters
    exclude_no_token = not args.include_no_token and export_config.get("exclude_no_token", True)
    output_file = args.output_file or export_config.get("output_file")
    
    logger.info(f"Exporting {len(json_files)} JSON files to CSV...")
    logger.info(f"Exclude no token: {exclude_no_token}")
    
    # Collect all data and determine all possible fields
    all_data: list[dict[str, str]] = []
    all_fields: set[str] = set()
    
    for json_file in json_files:
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            
            # Filter out entries where Token is N/A if requested
            token_value = (data.get("Token") or "").strip().upper()
            if exclude_no_token and (not token_value or token_value == "N/A"):
                continue
            
            # Add URL from corresponding text file
            url = _extract_url_from_txt(json_file)
            data["URL"] = url
            
            all_data.append(data)
            all_fields.update(data.keys())
        except Exception as e:
            logger.warning(f"Failed to parse {json_file.name}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No valid JSON data found (or all filtered out)")
    
    # Sort fields for consistent column order, but put URL first
    sorted_fields = sorted(all_fields)
    if "URL" in sorted_fields:
        sorted_fields.remove("URL")
        sorted_fields.insert(0, "URL")
    
    # Determine output filename
    if output_file:
        csv_path = input_dir / output_file
    else:
        csv_path = input_dir / f"{input_dir.name}_combined.csv"
    
    # Write CSV file
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fields)
            writer.writeheader()
            for row in all_data:
                # Ensure all fields are present (fill missing with empty string)
                complete_row = {field: str(row.get(field, "")) for field in sorted_fields}
                writer.writerow(complete_row)
    except Exception as e:
        raise RuntimeError(f"Failed to write CSV: {e}")
    
    logger.info(f"CSV export complete:")
    logger.info(f"  Output file: {csv_path}")
    logger.info(f"  Rows: {len(all_data)}")
    logger.info(f"  Columns: {len(sorted_fields)}")


if __name__ == "__main__":
    main()

