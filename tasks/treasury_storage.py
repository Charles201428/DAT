"""
Treasury S3 Storage - Extended S3 storage for Crypto Treasury Parser

This module provides S3 storage functionality for the Treasury pipeline,
extending the base S3Storage with support for text files, CSV files,
and batch run organization.
"""

from __future__ import annotations

import csv
import io
import json
import os
import tempfile
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

from src.utils.gaia_utils.s3_utils import S3Singleton

logger = logging.getLogger(__name__)


class TreasuryS3Storage:
    """
    S3 Storage class for Treasury Pipeline.
    
    Organizes files by run_id (timestamp) with the structure:
        {prefix}/{run_id}/news_text/{filename}
        {prefix}/{run_id}/positive_DAT/{filename}
        {prefix}/{run_id}/final/{filename}
    
    Supports:
        - JSON files (structured data)
        - Text files (news articles)
        - CSV files (final exports)
        - JSONL files (classification logs)
    """
    
    def __init__(
        self,
        bucket: str,
        prefix: str = "crypto_treasury",
        run_id: Optional[str] = None,
        logger_instance: Optional[logging.Logger] = None
    ):
        """
        Initialize Treasury S3 Storage.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 prefix/path (default: "crypto_treasury")
            run_id: Unique run identifier (default: auto-generated timestamp)
            logger_instance: Logger instance
        """
        if not bucket:
            raise ValueError("S3 bucket is required")
        
        self.bucket = bucket
        self.prefix = prefix
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
        self.logger = logger_instance or logger
    
    @classmethod
    def from_config(
        cls,
        config: Dict[str, Any],
        run_id: Optional[str] = None,
        logger_instance: Optional[logging.Logger] = None
    ) -> "TreasuryS3Storage":
        """
        Create TreasuryS3Storage from config dictionary.
        
        Args:
            config: Config dict with 's3.bucket' and optional 's3.prefix'
            run_id: Optional run identifier
            logger_instance: Logger instance
        
        Returns:
            TreasuryS3Storage instance
        """
        s3_config = config.get("s3", {})
        bucket = s3_config.get("bucket")
        prefix = s3_config.get("prefix", "crypto_treasury")
        
        return cls(
            bucket=bucket,
            prefix=prefix,
            run_id=run_id,
            logger_instance=logger_instance
        )
    
    def generate_s3_path(
        self,
        filename: str,
        stage: str = "data",
    ) -> str:
        """
        Generate S3 path for a file.
        
        Args:
            filename: Name of the file
            stage: Pipeline stage (news_text, positive_DAT, final, etc.)
        
        Returns:
            S3 key path: {prefix}/{run_id}/{stage}/{filename}
        """
        return f"{self.prefix}/{self.run_id}/{stage}/{filename}"
    
    def _upload_file(self, local_path: str, s3_key: str) -> bool:
        """Upload a local file to S3."""
        try:
            self.logger.info(f"☁️  Uploading to S3: s3://{self.bucket}/{s3_key}")
            
            success = S3Singleton.upload_file_with_custom_key(
                bucket_name=self.bucket,
                file_name=local_path,
                s3_key=s3_key
            )
            
            if success:
                self.logger.info(f"✅ File uploaded: s3://{self.bucket}/{s3_key}")
            else:
                self.logger.error(f"❌ Failed to upload: s3://{self.bucket}/{s3_key}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"❌ Error uploading to S3: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _upload_content(self, content: str, s3_key: str, suffix: str = ".txt") -> bool:
        """Upload string content to S3 via temp file."""
        temp_file = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=suffix, delete=False, encoding="utf-8"
            ) as f:
                temp_file = f.name
                f.write(content)
            
            return self._upload_file(temp_file, s3_key)
            
        finally:
            if temp_file and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception:
                    pass
    
    def save_text(
        self,
        content: str,
        filename: str,
        stage: str = "news_text",
    ) -> str:
        """
        Save text content to S3.
        
        Args:
            content: Text content to save
            filename: Filename (e.g., "12345.txt")
            stage: Pipeline stage
        
        Returns:
            S3 key if successful, empty string otherwise
        """
        s3_key = self.generate_s3_path(filename, stage)
        success = self._upload_content(content, s3_key, suffix=".txt")
        return s3_key if success else ""
    
    def save_json(
        self,
        data: Any,
        filename: str,
        stage: str = "positive_DAT",
        indent: int = 2,
    ) -> str:
        """
        Save JSON data to S3.
        
        Args:
            data: Data to serialize as JSON
            filename: Filename (e.g., "12345.json")
            stage: Pipeline stage
            indent: JSON indentation
        
        Returns:
            S3 key if successful, empty string otherwise
        """
        if data is None:
            self.logger.warning("No data to save")
            return ""
        
        s3_key = self.generate_s3_path(filename, stage)
        content = json.dumps(data, indent=indent, ensure_ascii=False)
        success = self._upload_content(content, s3_key, suffix=".json")
        return s3_key if success else ""
    
    def save_jsonl(
        self,
        records: List[Dict[str, Any]],
        filename: str,
        stage: str = "positive_DAT",
    ) -> str:
        """
        Save JSONL (JSON Lines) data to S3.
        
        Args:
            records: List of records to save
            filename: Filename (e.g., "classifications.jsonl")
            stage: Pipeline stage
        
        Returns:
            S3 key if successful, empty string otherwise
        """
        if not records:
            self.logger.warning("No records to save")
            return ""
        
        s3_key = self.generate_s3_path(filename, stage)
        content = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
        success = self._upload_content(content, s3_key, suffix=".jsonl")
        return s3_key if success else ""
    
    def save_csv(
        self,
        data: List[Dict[str, Any]],
        filename: str,
        stage: str = "final",
        fieldnames: Optional[List[str]] = None,
    ) -> str:
        """
        Save CSV data to S3.
        
        Args:
            data: List of dictionaries to save as CSV
            filename: Filename (e.g., "treasury_export.csv")
            stage: Pipeline stage
            fieldnames: Optional field order (default: sorted keys)
        
        Returns:
            S3 key if successful, empty string otherwise
        """
        if not data:
            self.logger.warning("No data to save as CSV")
            return ""
        
        # Determine fieldnames
        if fieldnames is None:
            all_fields = set()
            for row in data:
                all_fields.update(row.keys())
            fieldnames = sorted(all_fields)
            # Put URL first if present
            if "URL" in fieldnames:
                fieldnames.remove("URL")
                fieldnames.insert(0, "URL")
        
        # Generate CSV content
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            complete_row = {field: str(row.get(field, "")) for field in fieldnames}
            writer.writerow(complete_row)
        
        s3_key = self.generate_s3_path(filename, stage)
        success = self._upload_content(output.getvalue(), s3_key, suffix=".csv")
        return s3_key if success else ""
    
    def get_run_path(self, stage: str = "") -> str:
        """
        Get the S3 path for the current run.
        
        Args:
            stage: Optional stage suffix
        
        Returns:
            S3 path like "s3://bucket/prefix/run_id/stage"
        """
        base = f"s3://{self.bucket}/{self.prefix}/{self.run_id}"
        if stage:
            return f"{base}/{stage}"
        return base
    
    def save_batch_texts(
        self,
        texts: Dict[str, str],
        stage: str = "news_text",
    ) -> Dict[str, str]:
        """
        Save multiple text files to S3.
        
        Args:
            texts: Dict of {filename: content}
            stage: Pipeline stage
        
        Returns:
            Dict of {filename: s3_key} for successful uploads
        """
        results = {}
        for filename, content in texts.items():
            s3_key = self.save_text(content, filename, stage)
            if s3_key:
                results[filename] = s3_key
        return results
    
    def save_batch_json(
        self,
        items: Dict[str, Any],
        stage: str = "positive_DAT",
    ) -> Dict[str, str]:
        """
        Save multiple JSON files to S3.
        
        Args:
            items: Dict of {filename: data}
            stage: Pipeline stage
        
        Returns:
            Dict of {filename: s3_key} for successful uploads
        """
        results = {}
        for filename, data in items.items():
            s3_key = self.save_json(data, filename, stage)
            if s3_key:
                results[filename] = s3_key
        return results

