"""
DedupeTask - Wraps the deduplication function.

This task removes duplicate JSON files based on stock ticker, token, and date.
Operates on S3 data.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

# Add parent directory to path for imports
_task_dir = Path(__file__).parent.parent
if str(_task_dir) not in sys.path:
    sys.path.insert(0, str(_task_dir))

from src.domain.data_integration.framework.base.task import BaseTask
from .treasury_storage import TreasuryS3Storage


@dataclass
class FileInfo:
    """Information about a JSON file for deduplication."""
    s3_key: str
    filename: str
    size: int
    key: Optional[Tuple[str, str, str]]  # (ticker, token, date)
    fields: Dict[str, Any]


class DedupeTask(BaseTask):
    """
    Task to deduplicate JSON files in S3.
    
    This task groups files by (Stock Ticker, Token, Raise Ann. Date) 
    and keeps only one per group, deleting the rest from S3.
    
    Input (from kwargs):
        - run_id: Pipeline run identifier
        - s3_bucket: S3 bucket name
        - s3_prefix: S3 prefix
    
    Output:
        - run_id, s3_bucket, s3_prefix: Passed through
        - s3_path: S3 path to positive_DAT
        - groups_deduped: Number of duplicate groups processed
        - kept_count: Number of files kept
        - duplicate_count: Number of duplicates removed
    """
    
    def __init__(
        self,
        keep: Literal["largest", "newest", "most_filled", "first"] = "largest",
        require_all: bool = True,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the DedupeTask.
        
        Args:
            keep: Strategy for keeping files: 'largest', 'newest', 'most_filled', 'first'
            require_all: If True, require all three key fields to deduplicate
            config: Optional configuration dictionary
        """
        super().__init__(name="dedupe", config=config or {})
        self.keep = keep
        self.require_all = require_all
    
    def load_config(self) -> Dict[str, Any]:
        """Override to return empty config - we use app.config instead."""
        return {}
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the deduplication task.
        
        Args:
            **kwargs: Must contain run_id, s3_bucket, s3_prefix from upstream
        
        Returns:
            Dict with run_id, s3_path, groups_deduped, kept_count, duplicate_count
        """
        # Get upstream parameters
        run_id = kwargs.get("run_id")
        s3_bucket = kwargs.get("s3_bucket")
        s3_prefix = kwargs.get("s3_prefix", "crypto_treasury")
        
        if not run_id or not s3_bucket:
            raise ValueError("DedupeTask requires 'run_id' and 's3_bucket' from upstream task")
        
        # Create storage with same run_id
        storage = TreasuryS3Storage(
            bucket=s3_bucket,
            prefix=s3_prefix,
            run_id=run_id,
            logger_instance=self.logger
        )
        
        self.logger.info(f"[DedupeTask] Deduplicating data from run {run_id}")
        
        # Run deduplication
        results = self._dedupe_from_s3(storage)
        
        groups_deduped = results.get("groups_deduped", 0)
        kept_count = results.get("kept_count", 0)
        duplicate_count = results.get("duplicate_count", 0)
        
        self.logger.info(
            f"[DedupeTask] Deduped {groups_deduped} groups, "
            f"kept {kept_count}, removed {duplicate_count}"
        )
        
        return {
            "run_id": run_id,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "s3_path": storage.get_run_path("positive_DAT"),
            "groups_deduped": groups_deduped,
            "kept_count": kept_count,
            "duplicate_count": duplicate_count,
            "strategy": self.keep,
        }
    
    def _dedupe_from_s3(self, storage: TreasuryS3Storage) -> Dict[str, Any]:
        """
        Load JSON files from S3, deduplicate, and delete duplicates.
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
            return {"groups_deduped": 0, "kept_count": 0, "duplicate_count": 0}
        
        self.logger.info(f"[DedupeTask] Analyzing {len(json_files)} files")
        
        # Load all files and extract keys
        file_infos: List[FileInfo] = []
        
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
                
                # Extract key fields
                ticker = self._normalize_symbol(data.get("Stock Ticker"))
                token = self._normalize_symbol(data.get("Token"))
                date = self._parse_date(data.get("Raise Ann. Date"))
                
                key = None
                if ticker and token and date:
                    key = (ticker, token, date)
                elif not self.require_all and (ticker or token) and date:
                    key = (ticker or "", token or "", date)
                
                file_infos.append(FileInfo(
                    s3_key=s3_key,
                    filename=filename,
                    size=len(content),
                    key=key,
                    fields=data
                ))
                
            except Exception as e:
                self.logger.warning(f"Failed to load {filename}: {e}")
        
        # Group by key
        groups: Dict[Tuple[str, str, str], List[FileInfo]] = {}
        for info in file_infos:
            if info.key:
                groups.setdefault(info.key, []).append(info)
        
        # Find duplicates and pick winners
        to_delete: List[str] = []
        kept: List[str] = []
        groups_deduped = 0
        
        for key, entries in groups.items():
            if len(entries) <= 1:
                if entries:
                    kept.append(entries[0].s3_key)
                continue
            
            groups_deduped += 1
            winner = self._pick_winner(entries)
            kept.append(winner.s3_key)
            
            for entry in entries:
                if entry.s3_key != winner.s3_key:
                    to_delete.append(entry.s3_key)
                    # Also mark related .orig.txt for deletion
                    related_txt = entry.s3_key.replace(".json", ".txt")
                    if related_txt in files:
                        to_delete.append(related_txt)
        
        # Delete duplicates from S3
        deleted = 0
        for s3_key in to_delete:
            try:
                success = S3Singleton.delete_object(
                    bucket_name=storage.bucket,
                    s3_key=s3_key
                )
                if success:
                    deleted += 1
                    self.logger.debug(f"Deleted: {s3_key}")
            except Exception as e:
                self.logger.warning(f"Failed to delete {s3_key}: {e}")
        
        return {
            "groups_deduped": groups_deduped,
            "kept_count": len(kept),
            "duplicate_count": deleted,
        }
    
    def _normalize_symbol(self, val: Optional[str]) -> Optional[str]:
        """Normalize stock ticker or token symbol."""
        if not val:
            return None
        s = str(val).strip()
        if not s or s.upper() == "N/A":
            return None
        if s.startswith("$"):
            s = s[1:]
        return s.upper()
    
    def _parse_date(self, value: Optional[str]) -> Optional[str]:
        """Return date as YYYY-MM-DD string or None."""
        if not value:
            return None
        s = str(value).strip()
        if not s or s.upper() == "N/A":
            return None
        
        fmts = ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d %b %Y", "%d-%b-%y")
        for fmt in fmts:
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                continue
        
        try:
            return s.split("T")[0]
        except Exception:
            return None
    
    def _score_filled_fields(self, data: Dict[str, Any]) -> int:
        """Count non-empty, non-N/A fields."""
        score = 0
        for v in data.values():
            if v is None:
                continue
            if isinstance(v, str) and (v.strip() == "" or v.strip().upper() == "N/A"):
                continue
            score += 1
        return score
    
    def _pick_winner(self, entries: List[FileInfo]) -> FileInfo:
        """Pick the winning file to keep based on strategy."""
        if not entries:
            raise ValueError("No entries to pick from")
        
        if self.keep == "largest":
            return max(entries, key=lambda e: e.size)
        elif self.keep == "most_filled":
            return max(entries, key=lambda e: (self._score_filled_fields(e.fields), e.size))
        elif self.keep == "first":
            return sorted(entries, key=lambda e: e.filename)[0]
        else:  # newest - use filename (contains timestamp)
            return sorted(entries, key=lambda e: e.filename)[-1]
