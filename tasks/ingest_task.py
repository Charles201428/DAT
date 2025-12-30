"""
IngestCryptoPanicTask - Fetches news from CryptoPanic API.

This task fetches news from CryptoPanic API and saves them to S3.
Self-contained module with no dependencies on app/.
"""

from __future__ import annotations

import asyncio
import hashlib
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

# Add parent directory to path for imports
_task_dir = Path(__file__).parent.parent
if str(_task_dir) not in sys.path:
    sys.path.insert(0, str(_task_dir))

from src.domain.data_integration.framework.base.task import BaseTask
from .treasury_storage import TreasuryS3Storage
from .config import get_settings


def _hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class IngestCryptoPanicTask(BaseTask):
    """
    Task to ingest news from CryptoPanic API.
    
    This task fetches news and saves them directly to S3.
    
    Output:
        - run_id: Unique identifier for this pipeline run
        - s3_path: S3 path to the ingested news
        - inserted: Number of news items inserted
    """
    
    def __init__(
        self,
        hours: int = 24,
        s3_bucket: Optional[str] = None,
        s3_prefix: str = "crypto_treasury",
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the IngestCryptoPanicTask.
        
        Args:
            hours: Number of hours to look back for news (max 720 = 30 days)
            s3_bucket: S3 bucket name (required)
            s3_prefix: S3 prefix path (default: "crypto_treasury")
            config: Optional configuration dictionary
        """
        super().__init__(name="ingest_cryptopanic", config=config or {})
        self.hours = hours
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
    
    def load_config(self) -> Dict[str, Any]:
        """Override to return empty config - we use local config module instead."""
        return {}
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the ingestion task.
        
        Args:
            **kwargs: Additional arguments (can include s3_bucket override)
        
        Returns:
            Dict with:
                - run_id: Unique pipeline run identifier
                - s3_path: S3 path to news_text
                - inserted: Number of items inserted
        """
        settings = get_settings()
        
        # Get S3 bucket from kwargs, instance, or raise error
        s3_bucket = kwargs.get("s3_bucket") or self.s3_bucket
        if not s3_bucket:
            raise ValueError(
                "S3 bucket is required. Provide via s3_bucket parameter or pipeline config."
            )
        
        # Create storage with new run_id
        run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
        storage = TreasuryS3Storage(
            bucket=s3_bucket,
            prefix=self.s3_prefix,
            run_id=run_id,
            logger_instance=self.logger
        )
        
        self.logger.info(f"[IngestCryptoPanicTask] Ingesting news for last {self.hours} hours")
        self.logger.info(f"[IngestCryptoPanicTask] S3 path: {storage.get_run_path('news_text')}")
        
        # Run ingestion and upload to S3
        inserted = asyncio.run(self._ingest_to_s3(settings, storage))
        
        self.logger.info(f"[IngestCryptoPanicTask] Ingested {inserted} items")
        
        return {
            "run_id": run_id,
            "s3_bucket": s3_bucket,
            "s3_prefix": self.s3_prefix,
            "s3_path": storage.get_run_path("news_text"),
            "inserted": inserted,
        }
    
    async def _ingest_to_s3(
        self,
        settings,
        storage: TreasuryS3Storage,
    ) -> int:
        """
        Fetch news from CryptoPanic and upload directly to S3.
        """
        if not settings.cryptopanic_token:
            self.logger.warning("CRYPTOPANIC_TOKEN missing; skipping ingestion")
            return 0
        
        # Build request params
        params: dict[str, Any] = {"auth_token": settings.cryptopanic_token}
        if settings.cryptopanic_public:
            params["public"] = "true"
        
        # Filter
        allowed_filters = {"rising", "hot", "bullish", "bearish", "important", "saved", "lol"}
        f = (settings.cryptopanic_filter or "").strip().lower()
        if f in allowed_filters:
            params["filter"] = f
        
        # Kind
        try:
            k = (settings.cryptopanic_kind or "all").strip().lower()
        except Exception:
            k = "all"
        if k in {"news", "media"}:
            params["kind"] = k
        
        # Currencies
        currencies = (settings.cryptopanic_currencies or "").strip()
        if currencies and not currencies.startswith("#"):
            params["currencies"] = currencies
        
        headers = {"User-Agent": settings.user_agent}
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.hours)
        
        # Pagination config
        default_pages = max(1, settings.cryptopanic_pages)
        if self.hours >= 168:  # 7+ days
            pages_left = max(default_pages, 100)
        else:
            pages_left = default_pages
        
        inserted = 0
        next_url = settings.cryptopanic_base
        
        async with httpx.AsyncClient(headers=headers) as client:
            while pages_left:
                try:
                    resp = await client.get(next_url, params=params, timeout=30)
                    resp.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    if status in (401, 403, 429):
                        self.logger.error(f"CryptoPanic auth/rate error {status}: {exc}")
                        break
                    self.logger.error(f"CryptoPanic HTTP error {status}: {exc}")
                    break
                
                data: dict[str, Any] = resp.json()
                results: list[dict[str, Any]] = data.get("results", [])
                if not results:
                    break
                
                all_older = True
                for item in results:
                    published_str = item.get("published_at") or item.get("created_at")
                    try:
                        published = datetime.fromisoformat(
                            published_str.replace("Z", "+00:00")
                        ) if published_str else None
                    except Exception:
                        published = None
                    
                    if published and published < cutoff:
                        continue
                    if published is None or published >= cutoff:
                        all_older = False
                    
                    title = item.get("title") or ""
                    src_url = item.get("original_url") or item.get("url") or ""
                    if not src_url:
                        src_url = f"cryptopanic:{item.get('id')}"
                    
                    content_obj = item.get("content") or {}
                    content_clean = content_obj.get("clean") if isinstance(content_obj, dict) else None
                    content = f"{title}\n{content_clean or item.get('description') or ''}"
                    
                    # Optional local keyword filter
                    req = settings.cryptopanic_require_keyword
                    if req:
                        body_text = f"{title}\n{content}".lower()
                        if req.lower() not in body_text:
                            continue
                    
                    # Save to S3 (include URL as first line)
                    filename = f"{item.get('id') or _hash_content(src_url)}.txt"
                    file_content = f"URL: {src_url}\n{content}"
                    
                    s3_key = storage.save_text(file_content, filename, stage="news_text")
                    if s3_key:
                        inserted += 1
                
                next_link = data.get("next")
                if next_link and not all_older:
                    next_url = next_link
                    params = {}
                    pages_left -= 1
                    continue
                break
        
        self.logger.info(f"CryptoPanic: uploaded {inserted} news items to S3")
        return inserted
