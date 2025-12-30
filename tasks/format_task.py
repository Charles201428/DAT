"""
FormatTask - GPT-based structured data extraction.

This task extracts structured data from news texts into JSON format.
Results are saved to S3. Self-contained module with no dependencies on app/.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Add parent directory to path for imports
_task_dir = Path(__file__).parent.parent
if str(_task_dir) not in sys.path:
    sys.path.insert(0, str(_task_dir))

from src.domain.data_integration.framework.base.task import BaseTask
from .treasury_storage import TreasuryS3Storage
from .config import get_settings
from .prompts import FORMAT_SYSTEM_PROMPT


class FormatTask(BaseTask):
    """
    Task to extract structured data from news texts using GPT.
    
    This task reads positive texts from S3 and creates JSON files
    with extracted fields like Stock Ticker, Token, Raise Amount, etc.
    
    Input (from kwargs):
        - run_id: Pipeline run identifier
        - s3_bucket: S3 bucket name
        - s3_prefix: S3 prefix
    
    Output:
        - run_id, s3_bucket, s3_prefix: Passed through
        - s3_path: S3 path to positive_DAT (with JSON files)
        - saved: Number of JSON files created
        - errors: Number of errors encountered
    """
    
    def __init__(
        self,
        limit_files: Optional[int] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the FormatTask.
        
        Args:
            limit_files: Maximum number of files to process (None = all)
            config: Optional configuration dictionary
        """
        super().__init__(name="format", config=config or {})
        self.limit_files = limit_files
    
    def load_config(self) -> Dict[str, Any]:
        """Override to return empty config - we use local config module instead."""
        return {}
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the formatting task.
        
        Args:
            **kwargs: Must contain run_id, s3_bucket, s3_prefix from upstream
        
        Returns:
            Dict with run_id, s3_path, saved, errors
        """
        settings = get_settings()
        
        # Get upstream parameters
        run_id = kwargs.get("run_id")
        s3_bucket = kwargs.get("s3_bucket")
        s3_prefix = kwargs.get("s3_prefix", "crypto_treasury")
        
        if not run_id or not s3_bucket:
            raise ValueError("FormatTask requires 'run_id' and 's3_bucket' from upstream task")
        
        # Create storage with same run_id
        storage = TreasuryS3Storage(
            bucket=s3_bucket,
            prefix=s3_prefix,
            run_id=run_id,
            logger_instance=self.logger
        )
        
        self.logger.info(f"[FormatTask] Formatting texts from run {run_id}")
        
        # Download positive texts, extract to JSON, upload
        results = self._format_from_s3(storage, settings)
        
        saved = results.get("saved", 0)
        errors = results.get("errors", 0)
        
        self.logger.info(f"[FormatTask] Created {saved} JSON files, {errors} errors")
        
        return {
            "run_id": run_id,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "s3_path": storage.get_run_path("positive_DAT"),
            "saved": saved,
            "errors": errors,
        }
    
    def _format_from_s3(self, storage: TreasuryS3Storage, settings) -> Dict[str, Any]:
        """
        Download positive texts from S3, extract structured data, upload JSON.
        """
        from openai import OpenAI
        from src.utils.gaia_utils.s3_utils import S3Singleton
        
        # List .orig.txt files in positive_DAT stage
        positive_prefix = f"{storage.prefix}/{storage.run_id}/positive_DAT/"
        
        try:
            files = S3Singleton.list_objects(
                bucket_name=storage.bucket,
                prefix=positive_prefix
            ) or []
        except Exception as e:
            self.logger.error(f"Failed to list S3 objects: {e}")
            files = []
        
        # Filter to .orig.txt files
        orig_files = [f for f in files if f.endswith(".orig.txt")]
        if self.limit_files:
            orig_files = orig_files[:self.limit_files]
        
        if not orig_files:
            self.logger.warning(f"No .orig.txt files found in {positive_prefix}")
            return {"saved": 0, "errors": 0}
        
        self.logger.info(f"[FormatTask] Processing {len(orig_files)} files")
        
        client = OpenAI(api_key=settings.openai_api_key)
        saved = 0
        errors = 0
        
        for s3_key in orig_files:
            filename = s3_key.split("/")[-1]
            try:
                # Download content
                content = S3Singleton.download_file_as_string(
                    bucket_name=storage.bucket,
                    s3_key=s3_key
                )
                
                if not content:
                    errors += 1
                    continue
                
                # Call GPT for extraction
                completion = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": FORMAT_SYSTEM_PROMPT},
                        {"role": "user", "content": content[:12000]},
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                )
                
                data = completion.choices[0].message.content or "{}"
                parsed_data = json.loads(data)
                
                # Save JSON to S3
                json_filename = filename.replace(".orig.txt", ".orig.json")
                storage.save_json(parsed_data, json_filename, stage="positive_DAT")
                saved += 1
                
            except Exception as e:
                self.logger.warning(f"Formatting failed for {filename}: {e}")
                errors += 1
        
        return {"saved": saved, "errors": errors}
