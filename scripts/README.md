# Standalone Scripts

This directory contains standalone Python scripts that can be run independently without the FastAPI server. These scripts directly call the underlying functions from the `app` modules, making them more efficient than API wrappers.

## Overview

The scripts follow a 6-step pipeline:

1. **01_ingest.py** - Fetch news from CryptoPanic API
2. **02_classify.py** - Classify text files as DAT events using GPT
3. **03_format.py** - Extract structured data from text files into JSON
4. **04_enrich.py** - Enrich JSON files with stock and token price data
5. **05_dedup.py** - Deduplicate JSON files
6. **06_export_csv.py** - Combine JSON files into CSV

## Quick Start

### Prerequisites

- Python 3.11+ with dependencies installed (`pip install -r requirements.txt`)
- API keys configured in `.env` file:
  - `CRYPTOPANIC_TOKEN`
  - `OPENAI_API_KEY`
  - `ALPHAVANTAGE_API_KEY`
  - `COINGECKO_API_KEY`

### Using the Pipeline Script (Recommended)

The easiest way to run the complete pipeline:

```bash
# Simple usage (uses defaults from pipeline_config.yaml)
python scripts/pipeline.py

# With custom pipeline config
python scripts/pipeline.py --pipeline-config my_pipeline_config.yaml

# With both main config and pipeline config
python scripts/pipeline.py --config config.yaml --pipeline-config pipeline_config.yaml
```

### Running Individual Scripts

You can also run each step individually:

```bash
# Step 1: Ingest news
python scripts/01_ingest.py --hours 720

# Step 2: Classify (after ingestion creates news_text/ directory)
python scripts/02_classify.py --input-dir news_text/20251120_190823Z

# Step 3: Format (after classification creates positive_DAT/ directory)
python scripts/03_format.py --input-dir positive_DAT/20251120_190823Z

# Step 4: Enrich
python scripts/04_enrich.py --input-dir positive_DAT/20251120_190823Z

# Step 5: Deduplicate
python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z --keep largest

# Step 6: Export to CSV
python scripts/06_export_csv.py --input-dir positive_DAT/20251120_190823Z
```

## Configuration

### Config Files

1. **config.yaml** (optional) - Main configuration for individual scripts
2. **pipeline_config.yaml** (optional) - Pipeline-specific configuration

Copy the `.example` files and customize:

```bash
cp config.yaml.example config.yaml
cp pipeline_config.yaml.example pipeline_config.yaml
```

### Environment Variables

API keys and other settings are loaded from `.env` file (see `.env.example`). The scripts use `app.config.get_settings()` which reads from environment variables.

## Script Details

### 01_ingest.py

Fetches news articles from CryptoPanic API and saves them to `news_text/YYYYMMDD_HHMMSSZ/` directories.

**Arguments:**
- `--hours` - Hours to look back (default: 24, max: 720)
- `--config` - Path to config YAML file

**Example:**
```bash
python scripts/01_ingest.py --hours 168  # Last 7 days
```

### 02_classify.py

Classifies text files as DAT events using GPT-4o-mini. Positive classifications are optionally exported to `positive_DAT/` directory.

**Arguments:**
- `--input-dir` - Directory containing .txt files (required)
- `--limit` - Limit number of files to process
- `--workers` - Number of parallel workers (default: from config)
- `--no-save` - Don't save JSONL results
- `--config` - Path to config YAML file

**Example:**
```bash
python scripts/02_classify.py --input-dir news_text/20251120_190823Z --workers 10
```

### 03_format.py

Extracts structured data from text files into JSON format using GPT.

**Arguments:**
- `--input-dir` - Directory containing .orig.txt files (required)
- `--limit` - Limit number of files to process
- `--allow-txt` - Also process .txt files (not just .orig.txt)
- `--config` - Path to config YAML file

**Example:**
```bash
python scripts/03_format.py --input-dir positive_DAT/20251120_190823Z
```

### 04_enrich.py

Enriches JSON files with stock prices (Alpha Vantage) and token prices (CoinGecko).

**Arguments:**
- `--input-dir` - Directory containing .json files (required)
- `--limit` - Limit number of files to process
- `--as-of` - ISO datetime string (default: now UTC)
- `--stock-only` - Only enrich stock prices
- `--token-only` - Only enrich token prices
- `--config` - Path to config YAML file

**Example:**
```bash
python scripts/04_enrich.py --input-dir positive_DAT/20251120_190823Z
```

### 05_dedup.py

Deduplicates JSON files based on Stock Ticker, Token, and Raise Ann. Date.

**Arguments:**
- `--input-dir` - Directory containing .json files (required)
- `--keep` - Strategy: largest, newest, most_filled, first (default: largest)
- `--remove-duplicates` - Delete duplicates instead of moving to trash
- `--no-related` - Don't move/delete related files (.orig.txt, etc.)
- `--dry-run` - Only report, don't modify files
- `--config` - Path to config YAML file

**Example:**
```bash
python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z --keep largest
```

### 06_export_csv.py

Combines JSON files into a single CSV file with URL column.

**Arguments:**
- `--input-dir` - Directory containing .json files (required)
- `--output-file` - Output CSV filename (default: {dir_name}_combined.csv)
- `--include-no-token` - Include entries where Token is N/A
- `--config` - Path to config YAML file

**Example:**
```bash
python scripts/06_export_csv.py --input-dir positive_DAT/20251120_190823Z
```

## Pipeline Configuration

The `pipeline.py` script supports advanced configuration via `pipeline_config.yaml`:

### Skipping Steps

To resume from a specific step:

```yaml
pipeline:
  skip_steps: [ingest, classify]  # Skip ingestion and classification
```

### Continuing from Existing Directory

To continue processing an existing directory:

```yaml
pipeline:
  continue_from_dir: "positive_DAT/20251120_190823Z"
  skip_steps: [ingest, classify]
```

### Step-Specific Settings

Each step can be configured independently:

```yaml
ingest:
  hours: 720  # 30 days

classify:
  workers: 10  # More parallel workers

enrich:
  enrich_stock: true
  enrich_token: true

dedup:
  keep: largest
  remove_duplicates: false
```

## Differences from API Endpoints

These scripts **directly import and call** the underlying functions:

- ✅ No HTTP overhead
- ✅ No API server required
- ✅ Better error handling (exceptions instead of HTTP status codes)
- ✅ Faster execution
- ✅ Can be integrated into other Python workflows

The underlying functions are:
- `app.ingest.cryptopanic.ingest_cryptopanic()`
- `app.analyze.gpt.classify_texts_from_dir()`
- `app.analyze.gpt.format_texts_from_dir()`
- `app.enrich.alpha.enrich_folder_with_alpha()`
- `app.enrich.coingecko.enrich_folder_with_coingecko()`
- `app.utils.dedupe.dedupe_folder()`

## Troubleshooting

### Import Errors

Make sure you're running scripts from the project root:

```bash
# Correct
python scripts/01_ingest.py

# Wrong
cd scripts && python 01_ingest.py
```

### API Key Errors

Ensure your `.env` file is configured correctly and contains all required API keys.

### File Not Found Errors

Make sure the input directories exist. The scripts will try to find directories relative to configured base paths, but you may need to provide full paths.

## Examples

### Full Pipeline Run

```bash
# Run complete pipeline with default settings
python scripts/pipeline.py

# Run with custom time range (30 days)
# Edit pipeline_config.yaml: ingest.hours = 720
python scripts/pipeline.py
```

### Resuming from Enrichment

```bash
# Edit pipeline_config.yaml:
# pipeline.skip_steps = [ingest, classify, format]
# pipeline.continue_from_dir = "positive_DAT/20251120_190823Z"

python scripts/pipeline.py
```

### Processing Specific Directory

```bash
# Process a specific directory through remaining steps
python scripts/04_enrich.py --input-dir positive_DAT/20251120_190823Z
python scripts/05_dedup.py --input-dir positive_DAT/20251120_190823Z
python scripts/06_export_csv.py --input-dir positive_DAT/20251120_190823Z
```

