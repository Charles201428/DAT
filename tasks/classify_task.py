"""
ClassifyTask - GPT classification of news texts.

This task classifies news texts as DAT (Digital Asset Treasury) events or not.
Results are saved to S3. Self-contained module with no dependencies on app/.
"""

from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add parent directory to path for imports
_task_dir = Path(__file__).parent.parent
if str(_task_dir) not in sys.path:
    sys.path.insert(0, str(_task_dir))

from src.domain.data_integration.framework.base.task import BaseTask
from .treasury_storage import TreasuryS3Storage
from .config import get_settings
from .prompts import CLASSIFY_SYSTEM_PROMPT


class ClassifyTask(BaseTask):
    """
    Task to classify news texts as DAT events using GPT.
    
    This task reads news from S3 (via run_id from upstream),
    classifies them, and saves positive results to S3.
    
    Input (from kwargs):
        - run_id: Pipeline run identifier
        - s3_bucket: S3 bucket name
        - s3_prefix: S3 prefix
    
    Output:
        - run_id: Same run_id (passed through)
        - s3_bucket, s3_prefix: Same (passed through)
        - s3_path: S3 path to positive_DAT
        - count: Total files processed
        - positives: Number of positive classifications
    """
    
    def __init__(
        self,
        limit_files: Optional[int] = None,
        workers: Optional[int] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the ClassifyTask.
        
        Args:
            limit_files: Maximum number of files to process (None = all)
            workers: Number of parallel workers for GPT calls
            config: Optional configuration dictionary
        """
        super().__init__(name="classify", config=config or {})
        self.limit_files = limit_files
        self.workers = workers
    
    def load_config(self) -> Dict[str, Any]:
        """Override to return empty config - we use local config module instead."""
        return {}
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the classification task.
        
        Args:
            **kwargs: Must contain run_id, s3_bucket, s3_prefix from upstream
        
        Returns:
            Dict with run_id, s3_path, count, positives
        """
        settings = get_settings()
        
        # Get upstream parameters
        run_id = kwargs.get("run_id")
        s3_bucket = kwargs.get("s3_bucket")
        s3_prefix = kwargs.get("s3_prefix", "crypto_treasury")
        
        if not run_id or not s3_bucket:
            raise ValueError("ClassifyTask requires 'run_id' and 's3_bucket' from upstream task")
        
        workers = self.workers or settings.openai_classify_workers
        
        # Create storage with same run_id
        storage = TreasuryS3Storage(
            bucket=s3_bucket,
            prefix=s3_prefix,
            run_id=run_id,
            logger_instance=self.logger
        )
        
        self.logger.info(f"[ClassifyTask] Classifying news from run {run_id}")
        
        # Download news texts from S3, classify, and upload positives
        results = self._classify_from_s3(storage, settings, workers)
        
        count = results.get("count", 0)
        positives = results.get("positives", 0)
        
        self.logger.info(f"[ClassifyTask] Classified {count} files, {positives} positives")
        
        return {
            "run_id": run_id,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "s3_path": storage.get_run_path("positive_DAT"),
            "count": count,
            "positives": positives,
            "results": results.get("results", []),
        }
    
    def _classify_from_s3(
        self,
        storage: TreasuryS3Storage,
        settings,
        workers: int,
    ) -> Dict[str, Any]:
        """
        Download texts from S3, classify them, and upload positives.
        """
        from openai import OpenAI
        from src.utils.gaia_utils.s3_utils import S3Singleton
        
        # List files in news_text stage
        news_prefix = f"{storage.prefix}/{storage.run_id}/news_text/"
        
        try:
            # Get list of objects in S3
            files = S3Singleton.list_objects(
                bucket_name=storage.bucket,
                prefix=news_prefix
            ) or []
        except Exception as e:
            self.logger.error(f"Failed to list S3 objects: {e}")
            files = []
        
        if not files:
            self.logger.warning(f"No files found in {news_prefix}")
            return {"count": 0, "positives": 0, "results": []}
        
        # Filter to .txt files and apply limit
        txt_files = [f for f in files if f.endswith(".txt")]
        if self.limit_files:
            txt_files = txt_files[:self.limit_files]
        
        self.logger.info(f"[ClassifyTask] Processing {len(txt_files)} files")
        
        # Classify in parallel
        results: List[Dict[str, Any]] = []
        
        def classify_single(s3_key: str) -> Dict[str, Any]:
            """Classify a single file from S3."""
            filename = s3_key.split("/")[-1]
            try:
                # Download content from S3
                content = S3Singleton.download_file_as_string(
                    bucket_name=storage.bucket,
                    s3_key=s3_key
                )
                
                if not content:
                    return {"file": filename, "is_dat": False}
                
                # Call GPT for classification
                client = OpenAI(api_key=settings.openai_api_key)
                completion = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
                        {"role": "user", "content": content[:12000]},
                    ],
                    temperature=0.0,
                    response_format={"type": "json_object"},
                )
                
                data = json.loads(completion.choices[0].message.content)
                is_dat = bool(data.get("is_dat"))
                
                return {"file": filename, "is_dat": is_dat, "content": content}
                
            except Exception as e:
                self.logger.warning(f"Classification failed for {filename}: {e}")
                return {"file": filename, "is_dat": False}
        
        # Execute classification in parallel
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_file = {
                executor.submit(classify_single, f): f
                for f in txt_files
            }
            
            for future in as_completed(future_to_file):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    s3_key = future_to_file[future]
                    self.logger.warning(f"Task failed for {s3_key}: {e}")
        
        # Sort results by filename
        results.sort(key=lambda x: x["file"])
        
        # Upload positive classifications to positive_DAT stage
        positives = 0
        classification_log = []
        
        for r in results:
            classification_log.append({"file": r["file"], "is_dat": r["is_dat"]})
            
            if r.get("is_dat") and r.get("content"):
                # Save positive text to S3
                # Use .orig.txt extension for positive files
                orig_filename = r["file"].replace(".txt", ".orig.txt")
                storage.save_text(r["content"], orig_filename, stage="positive_DAT")
                positives += 1
        
        # Save classification log as JSONL
        storage.save_jsonl(
            classification_log,
            "classifications.jsonl",
            stage="positive_DAT"
        )
        
        return {
            "count": len(results),
            "positives": positives,
            "results": classification_log,
        }
