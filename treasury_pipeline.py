"""
Treasury Pipeline - Main Entry Point

This module provides the main entry point for running the Crypto Treasury Parser
as a Pipeline framework pipeline with S3 storage. It supports:
- Direct pipeline execution via Pipeline.run()
- Automatic Airflow DAG generation via Pipeline.to_airflow_dag()
- DSL-based dependency declaration
- S3 storage for all intermediate and final results

Usage:
    # Direct execution
    python treasury_pipeline.py --s3-bucket my-bucket --hours 24
    
    # With custom config
    python treasury_pipeline.py --config pipeline_config.yaml
    
    # Generate Airflow DAG (when imported in Airflow)
    from treasury_pipeline import create_treasury_dag
    dag = create_treasury_dag(s3_bucket="my-bucket")
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure the parent directories are in path
_script_dir = Path(__file__).parent
_framework_dir = _script_dir.parent
if str(_script_dir) not in sys.path:
    sys.path.insert(0, str(_script_dir))
if str(_framework_dir) not in sys.path:
    sys.path.insert(0, str(_framework_dir))

from src.domain.data_integration.framework.pipeline.pipeline import Pipeline

from tasks import (
    IngestCryptoPanicTask,
    ClassifyTask,
    FormatTask,
    EnrichTask,
    DedupeTask,
    ExportCSVTask,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_treasury_pipeline(
    s3_bucket: str,
    s3_prefix: str = "crypto_treasury",
    hours: int = 24,
    limit_files: Optional[int] = None,
    classify_workers: Optional[int] = None,
    enrich_stock: bool = True,
    enrich_token: bool = True,
    dedupe_keep: str = "largest",
    dedupe_require_all: bool = True,
    exclude_no_token: bool = True,
) -> Pipeline:
    """
    Create a Treasury Pipeline with all tasks configured for S3 storage.
    
    This pipeline processes crypto treasury news through 6 stages:
    1. Ingest: Fetch news from CryptoPanic API → S3
    2. Classify: Use GPT to identify DAT events → S3
    3. Format: Extract structured data from texts → S3
    4. Enrich: Add stock/token price data → S3
    5. Dedupe: Remove duplicate entries → S3
    6. Export: Generate final CSV → S3
    
    S3 Path Structure:
        s3://{bucket}/{prefix}/{run_id}/news_text/
        s3://{bucket}/{prefix}/{run_id}/positive_DAT/
        s3://{bucket}/{prefix}/{run_id}/final/
    
    Args:
        s3_bucket: S3 bucket name (required)
        s3_prefix: S3 prefix path (default: "crypto_treasury")
        hours: Hours to look back for news ingestion (max 720)
        limit_files: Max files to process per step (None = all)
        classify_workers: Parallel workers for GPT classification
        enrich_stock: Enrich with Alpha Vantage stock prices
        enrich_token: Enrich with CoinGecko token prices
        dedupe_keep: Strategy for keeping files (largest/newest/most_filled/first)
        dedupe_require_all: Require all three key fields to deduplicate
        exclude_no_token: Exclude entries where Token is N/A from CSV
    
    Returns:
        Configured Pipeline instance
    """
    if not s3_bucket:
        raise ValueError("s3_bucket is required for Treasury Pipeline")
    
    # Create pipeline
    pipeline = Pipeline(name="crypto_treasury_pipeline", max_workers=1)
    
    # Add tasks with S3 configuration
    pipeline.add_task(IngestCryptoPanicTask(
        hours=hours,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
    ))
    pipeline.add_task(ClassifyTask(
        limit_files=limit_files,
        workers=classify_workers,
    ))
    pipeline.add_task(FormatTask(
        limit_files=limit_files,
    ))
    pipeline.add_task(EnrichTask(
        enrich_stock=enrich_stock,
        enrich_token=enrich_token,
        limit_files=limit_files,
    ))
    pipeline.add_task(DedupeTask(
        keep=dedupe_keep,
        require_all=dedupe_require_all,
    ))
    pipeline.add_task(ExportCSVTask(
        exclude_no_token=exclude_no_token,
    ))
    
    # Set DSL for task dependencies (sequential pipeline)
    pipeline.set_dsl("ingest_cryptopanic>>classify>>format>>enrich>>dedupe>>export_csv")
    
    return pipeline


def create_treasury_dag(
    s3_bucket: str,
    dag_id: str = "crypto_treasury_dag",
    schedule_interval: str = "@daily",
    start_date: Optional[datetime] = None,
    s3_prefix: str = "crypto_treasury",
    hours: int = 24,
    use_taskflow: bool = False,
    **kwargs
):
    """
    Create an Airflow DAG for the Treasury Pipeline with S3 storage.
    
    This function is intended to be called from an Airflow DAG file.
    
    Example usage in a DAG file:
        from treasury_pipeline import create_treasury_dag
        dag = create_treasury_dag(
            s3_bucket="my-data-bucket",
            dag_id="my_treasury_dag",
            schedule_interval="0 6 * * *",  # 6 AM daily
            hours=48,
        )
    
    Args:
        s3_bucket: S3 bucket name (required)
        dag_id: Airflow DAG ID
        schedule_interval: Cron expression or Airflow preset
        start_date: DAG start date (default: 2024-01-01)
        s3_prefix: S3 prefix path
        hours: Hours to look back for news ingestion
        use_taskflow: If True, use TaskFlow API (Airflow 2.9+)
        **kwargs: Additional arguments passed to create_treasury_pipeline()
    
    Returns:
        Airflow DAG object
    """
    if start_date is None:
        start_date = datetime(2024, 1, 1)
    
    # Create pipeline with S3 configuration
    pipeline = create_treasury_pipeline(
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        hours=hours,
        **kwargs
    )
    
    # Generate Airflow DAG
    dag = pipeline.to_airflow_dag(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        start_date=start_date,
        max_active_runs=1,
        catchup=False,
        use_taskflow=use_taskflow,
    )
    
    return dag


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    if not config_path:
        return {}
    
    path = Path(config_path)
    if not path.exists():
        logger.warning(f"Config file not found: {path}")
        return {}
    
    try:
        import yaml
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        logger.warning("PyYAML not installed, skipping config file")
        return {}
    except Exception as e:
        logger.warning(f"Failed to load config: {e}")
        return {}


def run_pipeline(
    s3_bucket: str,
    s3_prefix: str = "crypto_treasury",
    hours: int = 24,
    config_path: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """
    Run the Treasury Pipeline directly with S3 storage.
    
    Args:
        s3_bucket: S3 bucket name (required)
        s3_prefix: S3 prefix path
        hours: Hours to look back for news ingestion
        config_path: Optional path to pipeline config YAML
        **kwargs: Additional arguments passed to create_treasury_pipeline()
    
    Returns:
        Pipeline execution results
    """
    if not s3_bucket:
        raise ValueError("s3_bucket is required")
    
    # Load config if provided
    config = load_config(config_path)
    
    # Merge config with kwargs (kwargs take precedence)
    pipeline_config = config.get("pipeline", {})
    ingest_config = config.get("ingest", {})
    classify_config = config.get("classify", {})
    enrich_config = config.get("enrich", {})
    dedupe_config = config.get("dedup", {})
    export_config = config.get("export_csv", {})
    s3_config = config.get("s3", {})
    
    # Build final configuration
    final_config = {
        "s3_bucket": s3_bucket or s3_config.get("bucket"),
        "s3_prefix": kwargs.get("s3_prefix", s3_config.get("prefix", s3_prefix)),
        "hours": kwargs.get("hours", ingest_config.get("hours", hours)),
        "limit_files": kwargs.get("limit_files", classify_config.get("limit_files")),
        "classify_workers": kwargs.get("classify_workers", classify_config.get("workers")),
        "enrich_stock": kwargs.get("enrich_stock", enrich_config.get("enrich_stock", True)),
        "enrich_token": kwargs.get("enrich_token", enrich_config.get("enrich_token", True)),
        "dedupe_keep": kwargs.get("dedupe_keep", dedupe_config.get("keep", "largest")),
        "dedupe_require_all": kwargs.get("dedupe_require_all", dedupe_config.get("require_all", True)),
        "exclude_no_token": kwargs.get("exclude_no_token", export_config.get("exclude_no_token", True)),
    }
    
    # Create and run pipeline
    pipeline = create_treasury_pipeline(**final_config)
    
    logger.info("=" * 60)
    logger.info("STARTING TREASURY PIPELINE (S3 Storage)")
    logger.info("=" * 60)
    logger.info(f"S3 Bucket: {final_config['s3_bucket']}")
    logger.info(f"S3 Prefix: {final_config['s3_prefix']}")
    logger.info(f"Hours: {final_config['hours']}")
    
    results = pipeline.run()
    
    logger.info("=" * 60)
    logger.info("TREASURY PIPELINE COMPLETE")
    logger.info("=" * 60)
    
    # Extract final output path
    final_result = results.get("export_csv", (False, {}))
    if isinstance(final_result, tuple) and len(final_result) >= 2:
        success, data = final_result
        if success and isinstance(data, dict):
            logger.info(f"Final CSV: {data.get('csv_s3_key', 'N/A')}")
            logger.info(f"Total rows: {data.get('rows', 0)}")
    
    return results


def main():
    """Main entry point for CLI execution."""
    parser = argparse.ArgumentParser(
        description="Run the Crypto Treasury Pipeline with S3 storage"
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        required=True,
        help="S3 bucket name (required)"
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="crypto_treasury",
        help="S3 prefix path (default: crypto_treasury)"
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Hours to look back for news (max 720, default: 24)"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to pipeline config YAML file"
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=None,
        help="Maximum files to process per step"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Parallel workers for GPT classification"
    )
    parser.add_argument(
        "--no-stock",
        action="store_true",
        help="Skip stock price enrichment"
    )
    parser.add_argument(
        "--no-token",
        action="store_true",
        help="Skip token price enrichment"
    )
    parser.add_argument(
        "--keep-all-tokens",
        action="store_true",
        help="Include entries with Token=N/A in CSV"
    )
    
    args = parser.parse_args()
    
    try:
        run_pipeline(
            s3_bucket=args.s3_bucket,
            s3_prefix=args.s3_prefix,
            hours=args.hours,
            config_path=args.config,
            limit_files=args.limit_files,
            classify_workers=args.workers,
            enrich_stock=not args.no_stock,
            enrich_token=not args.no_token,
            exclude_no_token=not args.keep_all_tokens,
        )
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
