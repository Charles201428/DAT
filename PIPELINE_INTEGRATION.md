# Crypto Treasury Parser - Pipeline Integration

This document describes how the Crypto Treasury Parser was integrated into the Pipeline Framework to enable orchestrated execution, S3 storage, and Airflow DAG generation.

## Overview

The Crypto Treasury Parser was originally a standalone script-based application with 6 processing steps. It has been integrated into the Pipeline Framework by wrapping each step as a `BaseTask` subclass, enabling:

- **S3 storage** for all intermediate and final results
- **DSL-based dependency declaration** (e.g., `"ingest>>classify>>format>>enrich>>dedupe>>export"`)
- **Direct pipeline execution** via `Pipeline.run()`
- **Automatic Airflow DAG generation** via `Pipeline.to_airflow_dag()`

## S3 Storage Structure

Each pipeline run creates a unique directory in S3 with the structure:

```
s3://{bucket}/{prefix}/{run_id}/
    ├── news_text/              # Raw ingested news
    │   ├── 12345.txt
    │   └── 12346.txt
    ├── positive_DAT/           # Classified positives + extracted JSON
    │   ├── 12345.orig.txt
    │   ├── 12345.orig.json
    │   └── classifications.jsonl
    └── final/                  # Final exports
        └── 20241230_120000Z_treasury_export.csv
```

**Example:**
```
s3://my-bucket/crypto_treasury/20241230_120000Z/news_text/12345.txt
s3://my-bucket/crypto_treasury/20241230_120000Z/positive_DAT/12345.orig.json
s3://my-bucket/crypto_treasury/20241230_120000Z/final/treasury_export.csv
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Pipeline Framework                           │
├─────────────────────────────────────────────────────────────────────┤
│  BaseTask  │  Pipeline  │  DSLParser  │  AirflowDAGGenerator        │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     Task Classes (tasks/)                           │
├─────────────────────────────────────────────────────────────────────┤
│  TreasuryS3Storage    │  IngestCryptoPanicTask  │  ClassifyTask     │
│  FormatTask           │  EnrichTask             │  DedupeTask       │
│  ExportCSVTask        │                                             │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS S3 Storage                              │
├─────────────────────────────────────────────────────────────────────┤
│  S3Singleton (from src.utils.gaia_utils.s3_utils)                   │
│  - upload_file_with_custom_key()                                    │
│  - list_objects()                                                   │
│  - download_file_as_string()                                        │
│  - delete_object()                                                  │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow

Each task passes `run_id`, `s3_bucket`, and `s3_prefix` to downstream tasks:

```
IngestTask ──► ClassifyTask ──► FormatTask ──► EnrichTask ──► DedupeTask ──► ExportCSVTask
    │              │               │              │              │              │
    ▼              ▼               ▼              ▼              ▼              ▼
Creates:       Reads from:     Reads from:   Reads from:    Reads from:    Reads from:
run_id         news_text/      positive_DAT/ positive_DAT/  positive_DAT/  positive_DAT/
               Writes to:      Writes to:    Writes to:     Deletes from:  Writes to:
               positive_DAT/   positive_DAT/ positive_DAT/  positive_DAT/  final/
```

## Files Created

| File | Purpose |
|------|---------|
| `tasks/__init__.py` | Package init, exports all task classes |
| `tasks/treasury_storage.py` | `TreasuryS3Storage` - S3 operations for Treasury pipeline |
| `tasks/ingest_task.py` | Fetches news and uploads to S3 |
| `tasks/classify_task.py` | Classifies texts and saves positives to S3 |
| `tasks/format_task.py` | Extracts JSON from texts, saves to S3 |
| `tasks/enrich_task.py` | Enriches with price data, saves to S3 |
| `tasks/dedupe_task.py` | Removes duplicates from S3 |
| `tasks/export_task.py` | Creates CSV and uploads to S3 |
| `treasury_pipeline.py` | Main entry point with Pipeline framework |

## Usage

### 1. Command Line Execution

```bash
# Basic usage (S3 bucket required)
python treasury_pipeline.py --s3-bucket my-bucket

# With custom prefix and hours
python treasury_pipeline.py --s3-bucket my-bucket --s3-prefix treasury/prod --hours 720

# With configuration file
python treasury_pipeline.py --s3-bucket my-bucket --config pipeline_config.yaml

# With options
python treasury_pipeline.py --s3-bucket my-bucket --hours 48 --limit-files 100 --workers 5
```

**Available CLI options:**
- `--s3-bucket`: S3 bucket name (required)
- `--s3-prefix`: S3 prefix path (default: "crypto_treasury")
- `--hours`: Hours to look back for news (max 720, default: 24)
- `--config`: Path to pipeline config YAML file
- `--limit-files`: Maximum files to process per step
- `--workers`: Parallel workers for GPT classification
- `--no-stock`: Skip stock price enrichment
- `--no-token`: Skip token price enrichment
- `--keep-all-tokens`: Include entries with Token=N/A in CSV

### 2. Programmatic Execution

```python
from treasury_pipeline import run_pipeline, create_treasury_pipeline

# Simple execution
results = run_pipeline(
    s3_bucket="my-bucket",
    hours=24
)

# Create pipeline with custom configuration
pipeline = create_treasury_pipeline(
    s3_bucket="my-bucket",
    s3_prefix="crypto_treasury/prod",
    hours=48,
    limit_files=100,
    enrich_stock=True,
    enrich_token=True,
)

# Run the pipeline
results = pipeline.run()

# Check results
print(pipeline.get_status())
```

### 3. Airflow DAG Generation

Create a DAG file in your Airflow dags folder:

```python
# dags/crypto_treasury_dag.py
from treasury_pipeline import create_treasury_dag

dag = create_treasury_dag(
    s3_bucket="my-data-bucket",
    dag_id="crypto_treasury_dag",
    schedule_interval="@daily",  # or cron: "0 6 * * *"
    s3_prefix="crypto_treasury",
    hours=24,
)
```

**With TaskFlow API (Airflow 2.9+):**

```python
dag = create_treasury_dag(
    s3_bucket="my-data-bucket",
    dag_id="crypto_treasury_dag",
    schedule_interval="@daily",
    hours=24,
    use_taskflow=True,
)
```

## TreasuryS3Storage Class

The `TreasuryS3Storage` class provides S3 operations for the Treasury pipeline:

```python
from tasks import TreasuryS3Storage

storage = TreasuryS3Storage(
    bucket="my-bucket",
    prefix="crypto_treasury",
    run_id="20241230_120000Z"  # auto-generated if not provided
)

# Save text file
s3_key = storage.save_text(content, "12345.txt", stage="news_text")

# Save JSON file
s3_key = storage.save_json(data, "12345.json", stage="positive_DAT")

# Save JSONL file
s3_key = storage.save_jsonl(records, "classifications.jsonl", stage="positive_DAT")

# Save CSV file
s3_key = storage.save_csv(data_list, "export.csv", stage="final")

# Get S3 path for current run
path = storage.get_run_path("final")  # s3://bucket/prefix/run_id/final
```

## Task Classes

### IngestCryptoPanicTask

Fetches news from CryptoPanic API and uploads to S3.

```python
task = IngestCryptoPanicTask(
    hours=24,
    s3_bucket="my-bucket",
    s3_prefix="crypto_treasury"
)
```

**Output:** `{"run_id": "...", "s3_bucket": "...", "s3_path": "...", "inserted": N}`

### ClassifyTask

Downloads texts from S3, classifies with GPT, uploads positives.

```python
task = ClassifyTask(
    limit_files=None,
    workers=10,
)
```

**Input:** `run_id`, `s3_bucket`, `s3_prefix` from upstream  
**Output:** `{"run_id": "...", "s3_path": "...", "count": N, "positives": N}`

### FormatTask

Downloads positive texts from S3, extracts JSON, uploads.

```python
task = FormatTask(
    limit_files=None,
)
```

**Output:** `{"run_id": "...", "s3_path": "...", "saved": N, "errors": N}`

### EnrichTask

Downloads JSON from S3, adds price data, uploads enriched data.

```python
task = EnrichTask(
    enrich_stock=True,
    enrich_token=True,
    limit_files=None,
)
```

**Output:** `{"run_id": "...", "s3_path": "...", "stock_saved": N, "token_saved": N}`

### DedupeTask

Identifies duplicates in S3 and deletes them.

```python
task = DedupeTask(
    keep="largest",        # largest/newest/most_filled/first
    require_all=True,
)
```

**Output:** `{"run_id": "...", "groups_deduped": N, "kept_count": N, "duplicate_count": N}`

### ExportCSVTask

Reads JSON from S3, creates combined CSV, uploads to final stage.

```python
task = ExportCSVTask(
    output_file=None,       # Auto-generate name
    exclude_no_token=True,
)
```

**Output:** `{"run_id": "...", "csv_s3_key": "s3://...", "rows": N}`

## Configuration

### Pipeline Config (YAML)

```yaml
# pipeline_config.yaml

# S3 configuration
s3:
  bucket: my-data-bucket
  prefix: crypto_treasury

# Pipeline settings
pipeline:
  skip_steps: []  # Options: ingest, classify, format, enrich, dedup, export_csv

# Step configurations
ingest:
  hours: 720

classify:
  limit_files: null
  workers: 10

enrich:
  enrich_stock: true
  enrich_token: true

dedup:
  keep: largest
  require_all: true

export_csv:
  exclude_no_token: true
```

### Environment Variables

Required API keys (set via environment or `.env`):
- `CRYPTOPANIC_TOKEN`: CryptoPanic API token
- `OPENAI_API_KEY`: OpenAI API key
- `ALPHAVANTAGE_API_KEY`: Alpha Vantage API key
- AWS credentials (via IAM role or environment variables)

## DSL Syntax

The pipeline uses DSL (Domain Specific Language) to define task dependencies:

```python
pipeline.set_dsl("ingest_cryptopanic>>classify>>format>>enrich>>dedupe>>export_csv")
```

**DSL Operators:**
- `>>`: Sequential dependency (B runs after A)
- `[A,B]`: Parallel execution (A and B run simultaneously)

## Comparison: Local vs S3 Storage

| Aspect | Local Storage (old) | S3 Storage (new) |
|--------|---------------------|------------------|
| **Location** | `news_text/`, `positive_DAT/` folders | S3 bucket |
| **Organization** | By timestamp folder | By run_id prefix |
| **Persistence** | Local disk | Cloud storage |
| **Scalability** | Limited by disk | Unlimited |
| **Cost** | Free | S3 pricing |
| **Access** | Local only | Any AWS-connected system |

## Troubleshooting

### S3 Permission Errors

Ensure your AWS credentials have permissions for:
- `s3:PutObject`
- `s3:GetObject`
- `s3:ListBucket`
- `s3:DeleteObject`

### Missing Run Data

If downstream tasks fail with "run_id not found", check that upstream tasks completed successfully and returned the correct output format.

### API Rate Limits

- CryptoPanic: May throttle with 4xx/5xx errors
- OpenAI: Use `workers` parameter to control parallelism
- Alpha Vantage: 5 calls/minute on free tier

## Future Improvements

1. **Add retry logic** to tasks for transient API failures
2. **Implement checkpointing** to resume from failed steps
3. **Add metrics collection** for monitoring pipeline performance
4. **Support partial pipeline runs** (e.g., start from format step)
5. **Add S3 lifecycle policies** for automatic data cleanup
