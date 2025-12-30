"""
ExportCSVTask - Exports JSON files to a combined CSV file.

This task reads JSON files from S3 and creates a CSV export.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add parent directory to path for imports
_task_dir = Path(__file__).parent.parent
if str(_task_dir) not in sys.path:
    sys.path.insert(0, str(_task_dir))

from src.domain.data_integration.framework.base.task import BaseTask
from .treasury_storage import TreasuryS3Storage


class ExportCSVTask(BaseTask):
    """
    Task to export JSON files to CSV in S3.
    
    This task reads all JSON files from the positive_DAT stage,
    combines them into a single CSV, and uploads to the final stage.
    
    Input (from kwargs):
        - run_id: Pipeline run identifier
        - s3_bucket: S3 bucket name
        - s3_prefix: S3 prefix
    
    Output:
        - run_id, s3_bucket, s3_prefix: Passed through
        - s3_path: S3 path to final stage
        - csv_s3_key: Full S3 key to the CSV file
        - rows: Number of rows in the CSV
    """
    
    def __init__(
        self,
        output_file: Optional[str] = None,
        exclude_no_token: bool = True,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the ExportCSVTask.
        
        Args:
            output_file: Custom output filename (None = auto-generate)
            exclude_no_token: If True, exclude entries where Token is N/A
            config: Optional configuration dictionary
        """
        super().__init__(name="export_csv", config=config or {})
        self.output_file = output_file
        self.exclude_no_token = exclude_no_token
    
    def load_config(self) -> Dict[str, Any]:
        """Override to return empty config - we use app.config instead."""
        return {}
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the CSV export task.
        
        Args:
            **kwargs: Must contain run_id, s3_bucket, s3_prefix from upstream
        
        Returns:
            Dict with run_id, s3_path, csv_s3_key, rows
        """
        # Get upstream parameters
        run_id = kwargs.get("run_id")
        s3_bucket = kwargs.get("s3_bucket")
        s3_prefix = kwargs.get("s3_prefix", "crypto_treasury")
        
        if not run_id or not s3_bucket:
            raise ValueError("ExportCSVTask requires 'run_id' and 's3_bucket' from upstream task")
        
        # Create storage with same run_id
        storage = TreasuryS3Storage(
            bucket=s3_bucket,
            prefix=s3_prefix,
            run_id=run_id,
            logger_instance=self.logger
        )
        
        self.logger.info(f"[ExportCSVTask] Exporting CSV from run {run_id}")
        
        # Export to CSV
        results = self._export_csv_to_s3(storage)
        
        rows = results.get("rows", 0)
        csv_s3_key = results.get("csv_s3_key", "")
        
        self.logger.info(f"[ExportCSVTask] Exported {rows} rows to {csv_s3_key}")
        
        return {
            "run_id": run_id,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "s3_path": storage.get_run_path("final"),
            "csv_s3_key": csv_s3_key,
            "rows": rows,
        }
    
    def _export_csv_to_s3(self, storage: TreasuryS3Storage) -> Dict[str, Any]:
        """
        Read JSON files from S3 and export combined CSV.
        """
        from src.utils.gaia_utils.s3_utils import S3Singleton
        
        # List JSON files in positive_DAT stage
        positive_prefix = f"{storage.prefix}/{storage.run_id}/positive_DAT/"
        
        try:
            files = S3Singleton.list_objects(
                bucket_name=storage.bucket,
                prefix=positive_prefix
            ) or []
        except Exception as e:
            self.logger.error(f"Failed to list S3 objects: {e}")
            files = []
        
        # Filter to .json files (not .jsonl)
        json_files = [f for f in files if f.endswith(".json") and not f.endswith(".jsonl")]
        
        if not json_files:
            self.logger.warning(f"No JSON files found in {positive_prefix}")
            return {"rows": 0, "csv_s3_key": ""}
        
        self.logger.info(f"[ExportCSVTask] Processing {len(json_files)} files")
        
        # Load all JSON data
        all_data: List[Dict[str, Any]] = []
        
        for s3_key in json_files:
            filename = s3_key.split("/")[-1]
            try:
                content = S3Singleton.download_file_as_string(
                    bucket_name=storage.bucket,
                    s3_key=s3_key
                )
                if not content:
                    continue
                
                data = json.loads(content)
                
                # Filter by token if requested
                token_value = (data.get("Token") or "").strip().upper()
                if self.exclude_no_token and (not token_value or token_value == "N/A"):
                    continue
                
                # Try to extract URL from corresponding .orig.txt file
                txt_key = s3_key.replace(".json", ".txt")
                url = self._extract_url_from_s3(storage.bucket, txt_key)
                if url:
                    data["URL"] = url
                
                all_data.append(data)
                
            except Exception as e:
                self.logger.warning(f"Failed to load {filename}: {e}")
        
        if not all_data:
            self.logger.warning("No valid JSON data found after filtering")
            return {"rows": 0, "csv_s3_key": ""}
        
        # Generate output filename
        output_file = self.output_file or f"{storage.run_id}_treasury_export.csv"
        
        # Save CSV to S3
        csv_s3_key = storage.save_csv(
            data=all_data,
            filename=output_file,
            stage="final"
        )
        
        return {
            "rows": len(all_data),
            "csv_s3_key": f"s3://{storage.bucket}/{csv_s3_key}" if csv_s3_key else "",
        }
    
    def _extract_url_from_s3(self, bucket: str, txt_key: str) -> str:
        """Extract URL from text file in S3."""
        from src.utils.gaia_utils.s3_utils import S3Singleton
        
        try:
            content = S3Singleton.download_file_as_string(
                bucket_name=bucket,
                s3_key=txt_key
            )
            if content:
                lines = content.splitlines()
                if lines and lines[0].startswith("URL:"):
                    return lines[0].split("URL:", 1)[1].strip()
        except Exception:
            pass
        return ""
