# Standalone Scripts

This directory contains standalone Python scripts that replicate the functionality of the API endpoints. These scripts can be run independently and support both YAML config files and command-line arguments.

## Quick Start: Pipeline Script

For automated end-to-end processing, use the **pipeline script** that runs all 6 steps sequentially:

```bash
# Simple usage (uses defaults)
python scripts/pipeline.py

# With custom pipeline config
python scripts/pipeline.py --pipeline-config pipeline_config.yaml

# With both main config and pipeline config
python scripts/pipeline.py --config config.yaml --pipeline-config pipeline_config.yaml
```

See [Pipeline Configuration](#pipeline-configuration) below for details.

## Setup

1. **Install dependencies** (if not already installed):
   ```bash
   pip install -r requirements.txt
   ```

2. **Create config file** (optional):
   ```bash
   cp config.yaml.example config.yaml
   # Edit config.yaml with your API keys and settings
   ```

   Alternatively, you can use environment variables (`.env` file) as before.

## Scripts Overview

### Pipeline Script (Recommended)

**`pipeline.py`** - Runs all 6 steps automatically in sequence. See [Pipeline Configuration](#pipeline-configuration) section.

### Individual Scripts

The individual scripts follow a pipeline workflow where each step takes the output directory from the previous step:

1. **`01_ingest.py`** - Ingest news from CryptoPanic API
2. **`02_classify.py`** - Binary classification of DAT events using GPT
3. **`03_format.py`** - Extract structured data from text files
4. **`04_enrich.py`** - Enrich JSON files with stock and token prices
5. **`05_dedup.py`** - Deduplicate JSON files
6. **`06_export_csv.py`** - Export JSON files to CSV

## Usage Examples

### Complete Pipeline

```bash
# Step 1: Ingest news (last 30 days)
python scripts/01_ingest.py --hours 720

# Step 2: Classify (use output directory from step 1)
python scripts/02_classify.py --input-dir news_text/20251120_190823Z

# Step 3: Format (use output directory from step 2)
python scripts/03_format.py --input-dir positive_DAT/20251120_190823Z

# Step 4: Enrich (use same directory as step 3)
python scripts/04_enrich.py --input-dir positive_DAT/20251120_190823Z

# Step 5: Deduplicate (use same directory)
python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z --keep largest

# Step 6: Export to CSV (use same directory)
python scripts/06_export_csv.py --input-dir positive_DAT/20251120_190823Z
```

### Using Config File

```bash
# All scripts support --config flag
python scripts/01_ingest.py --hours 24 --config config.yaml
python scripts/02_classify.py --input-dir news_text/20251120_190823Z --config config.yaml
```

### Advanced Options

```bash
# Classify with custom worker count
python scripts/02_classify.py --input-dir news_text/20251120_190823Z --workers 20 --limit 100

# Format with limit
python scripts/03_format.py --input-dir positive_DAT/20251120_190823Z --limit 50

# Enrich with specific timestamp
python scripts/04_enrich.py --input-dir positive_DAT/20251120_190823Z --as-of "2025-11-20T00:00:00Z"

# Deduplicate with dry-run first
python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z --dry-run
python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z --keep largest --remove

# Export with custom filename
python scripts/06_export_csv.py --input-dir positive_DAT/20251120_190823Z --output-file custom_export.csv
```

## Pipeline Configuration

The pipeline script uses a dedicated `pipeline_config.yaml` file (see `pipeline_config.yaml.example` for template) that allows you to configure:

- **Time period** for ingestion (hours to look back)
- **Step-specific settings** (limits, workers, deduplication strategy, etc.)
- **Skip steps** for resuming from a specific point
- **Continue from existing directory** to skip ingestion

Example `pipeline_config.yaml`:

```yaml
ingestion:
  hours: 720  # Last 30 days

classification:
  workers: 10
  limit_files: null  # Process all files

enrichment:
  limit_files: null

deduplication:
  keep: "largest"
  remove_duplicates: false

export:
  exclude_no_token: true
```

### Advanced Pipeline Features

**Skip Steps**: Resume from a specific step
```yaml
skip_steps: [1, 2]  # Skip ingestion and classification
```

**Continue from Directory**: Skip ingestion and use existing directory
```yaml
continue_from_dir: "news_text/20251120_190823Z"
```

## Configuration

### YAML Config File

Create `config.yaml` from `config.yaml.example`:

```yaml
api_keys:
  cryptopanic_token: "your_token_here"
  openai_api_key: "sk-proj-..."
  alphavantage_api_key: "your_key_here"
  coingecko_api_key: "CG-your_pro_key_here"

cryptopanic:
  base: "https://cryptopanic.com/api/growth/v2/posts/"
  require_keyword: "treasury"  # Optional local filter

openai:
  classify_workers: 10

directories:
  news_text_dir: "news_text"
  positive_text_dir: "positive_DAT"
```

### Environment Variables

You can still use `.env` file or environment variables. Environment variables take precedence over YAML config.

## Script Details

### 01_ingest.py

Ingests news articles from CryptoPanic API and saves them to timestamped directories.

**Arguments:**
- `--hours`: Number of hours to look back (1-720, default: 24)
- `--config`: Path to YAML config file (optional)

**Output:** `news_text/YYYYMMDD_HHMMSSZ/` directory

### 02_classify.py

Classifies text files as DAT events using GPT. Copies positive classifications to output directory.

**Arguments:**
- `--input-dir`: Directory containing .txt files (required)
- `--output-dir`: Output directory (default: `positive_DAT/{input_dir_name}`)
- `--limit`: Limit number of files to process (optional)
- `--workers`: Number of parallel workers (optional, default: from config)
- `--config`: Path to YAML config file (optional)

**Output:** `positive_DAT/YYYYMMDD_HHMMSSZ/` directory with positive files

### 03_format.py

Extracts structured data from text files into JSON format using GPT.

**Arguments:**
- `--input-dir`: Directory containing .orig.txt or .txt files (required)
- `--limit`: Limit number of files to process (optional)
- `--orig-only`: Only process .orig.txt files (default: True)
- `--config`: Path to YAML config file (optional)

**Output:** JSON files (`.orig.json` or `.json`) in the same directory

### 04_enrich.py

Enriches JSON files with stock prices (Alpha Vantage) and token prices (CoinGecko).

**Arguments:**
- `--input-dir`: Directory containing .json files (required)
- `--limit`: Limit number of files to process (optional)
- `--as-of`: ISO timestamp for enrichment (optional, default: now UTC)
- `--config`: Path to YAML config file (optional)

**Output:** Enriched JSON files (modified in-place)

### 05_dedup.py

Deduplicates JSON files based on stock ticker, token, and announcement date.

**Arguments:**
- `--input-dir`: Directory containing .json files (required)
- `--keep`: Strategy: `largest`, `newest`, `most_filled`, or `first` (default: `largest`)
- `--remove`: Delete duplicates instead of moving to `_dedup_trash`
- `--dry-run`: Only report without modifying files
- `--require-all`: Require stock, token, and date to deduplicate (default: True)
- `--include-related`: Also move/delete sibling files (default: True)
- `--config`: Path to YAML config file (optional)

**Output:** Deduplicated files (duplicates moved to `_dedup_trash/` or deleted)

### 06_export_csv.py

Exports JSON files to CSV format with URL column.

**Arguments:**
- `--input-dir`: Directory containing .json files (required)
- `--output-file`: Output CSV filename (optional, default: `{dir_name}_combined.csv`)
- `--include-no-token`: Include entries where Token is N/A (default: exclude them)
- `--config`: Path to YAML config file (optional)

**Output:** CSV file in the input directory

## Notes

- All scripts can be run independently if you have the required input directories
- Scripts automatically print the next step command after completion
- Config file values are overridden by environment variables
- Scripts use the same underlying functions as the API endpoints, ensuring consistency

