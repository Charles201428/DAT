"""
EnrichTask - Price data enrichment.

This task enriches JSON files with stock and token price data.
Results are saved to S3. Self-contained module with no dependencies on app/.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta, timezone
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


class EnrichTask(BaseTask):
    """
    Task to enrich JSON files with stock and token price data.
    
    This task reads JSON files from S3, adds price performance metrics,
    and saves the enriched data back to S3.
    
    Input (from kwargs):
        - run_id: Pipeline run identifier
        - s3_bucket: S3 bucket name
        - s3_prefix: S3 prefix
    
    Output:
        - run_id, s3_bucket, s3_prefix: Passed through
        - s3_path: S3 path to positive_DAT
        - stock_saved: Number of files enriched with stock data
        - token_saved: Number of files enriched with token data
    """
    
    def __init__(
        self,
        enrich_stock: bool = True,
        enrich_token: bool = True,
        limit_files: Optional[int] = None,
        as_of: Optional[datetime] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the EnrichTask.
        
        Args:
            enrich_stock: Whether to enrich with Alpha Vantage stock prices
            enrich_token: Whether to enrich with Alpha Vantage token prices
            limit_files: Maximum number of files to process (None = all)
            as_of: Reference datetime for price lookups (None = now)
            config: Optional configuration dictionary
        """
        super().__init__(name="enrich", config=config or {})
        self.enrich_stock = enrich_stock
        self.enrich_token = enrich_token
        self.limit_files = limit_files
        self.as_of = as_of
    
    def load_config(self) -> Dict[str, Any]:
        """Override to return empty config - we use local config module instead."""
        return {}
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Execute the enrichment task.
        
        Args:
            **kwargs: Must contain run_id, s3_bucket, s3_prefix from upstream
        
        Returns:
            Dict with run_id, s3_path, stock_saved, token_saved
        """
        # Get upstream parameters
        run_id = kwargs.get("run_id")
        s3_bucket = kwargs.get("s3_bucket")
        s3_prefix = kwargs.get("s3_prefix", "crypto_treasury")
        
        if not run_id or not s3_bucket:
            raise ValueError("EnrichTask requires 'run_id' and 's3_bucket' from upstream task")
        
        # Create storage with same run_id
        storage = TreasuryS3Storage(
            bucket=s3_bucket,
            prefix=s3_prefix,
            run_id=run_id,
            logger_instance=self.logger
        )
        
        self.logger.info(f"[EnrichTask] Enriching data from run {run_id}")
        
        # Run enrichment
        results = asyncio.run(self._enrich_from_s3(storage))
        
        stock_saved = results.get("stock_saved", 0)
        token_saved = results.get("token_saved", 0)
        
        self.logger.info(f"[EnrichTask] Stock: {stock_saved}, Token: {token_saved}")
        
        return {
            "run_id": run_id,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "s3_path": storage.get_run_path("positive_DAT"),
            "stock_saved": stock_saved,
            "token_saved": token_saved,
        }
    
    async def _enrich_from_s3(self, storage: TreasuryS3Storage) -> Dict[str, Any]:
        """
        Download JSON files from S3, enrich with price data, upload back.
        """
        from src.utils.gaia_utils.s3_utils import S3Singleton
        
        settings = get_settings()
        
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
        if self.limit_files:
            json_files = json_files[:self.limit_files]
        
        if not json_files:
            self.logger.warning(f"No JSON files found in {positive_prefix}")
            return {"stock_saved": 0, "token_saved": 0}
        
        self.logger.info(f"[EnrichTask] Processing {len(json_files)} files")
        
        # Load all JSON data
        items: Dict[str, Dict[str, Any]] = {}
        for s3_key in json_files:
            filename = s3_key.split("/")[-1]
            try:
                content = S3Singleton.download_file_as_string(
                    bucket_name=storage.bucket,
                    s3_key=s3_key
                )
                if content:
                    items[filename] = json.loads(content)
            except Exception as e:
                self.logger.warning(f"Failed to load {filename}: {e}")
        
        as_of = self.as_of or datetime.now(timezone.utc)
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        
        stock_saved = 0
        token_saved = 0
        
        # Price caches
        stock_cache: Dict[str, Dict[str, Any]] = {}
        token_cache: Dict[str, Dict[str, Any]] = {}
        
        headers = {"User-Agent": settings.user_agent}
        
        async with httpx.AsyncClient(headers=headers) as client:
            for filename, data in items.items():
                ticker = (data.get("Stock Ticker") or "").strip().lstrip("$").upper() or None
                ann_date = self._parse_date(data.get("Raise Ann. Date"))
                token = self._normalize_symbol(data.get("Token"))
                
                # Enrich with stock prices
                if self.enrich_stock and ticker and ann_date and settings.alphavantage_api_key:
                    series = stock_cache.get(ticker)
                    if series is None:
                        series = await self._fetch_stock_series(client, settings, ticker)
                        stock_cache[ticker] = series
                    
                    if series:
                        self._apply_stock_metrics(data, series, ann_date)
                        stock_saved += 1
                
                # Enrich with token prices
                if self.enrich_token and token and ann_date and settings.alphavantage_api_key:
                    series_t = token_cache.get(token)
                    if series_t is None:
                        series_t = await self._fetch_token_series(client, settings, token)
                        token_cache[token] = series_t
                    
                    if series_t:
                        self._apply_token_metrics(data, series_t, ann_date)
                        token_saved += 1
                
                # Save enriched data back to S3
                storage.save_json(data, filename, stage="positive_DAT")
        
        return {"stock_saved": stock_saved, "token_saved": token_saved}
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str or str(date_str).strip().upper() == "N/A":
            return None
        s = str(date_str).strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%y"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
        return None
    
    def _normalize_symbol(self, val: Optional[str]) -> Optional[str]:
        """Normalize token symbol."""
        if not val:
            return None
        s = str(val).strip().upper()
        return s if s and s != "N/A" else None
    
    async def _fetch_stock_series(
        self, client: httpx.AsyncClient, settings, ticker: str
    ) -> Dict[str, Any]:
        """Fetch stock price series from Alpha Vantage."""
        try:
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": ticker,
                "apikey": settings.alphavantage_api_key,
                "outputsize": "compact",
            }
            resp = await client.get(settings.alphavantage_base, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "Note" in data or "Information" in data:
                return {}
            return data.get("Time Series (Daily)", {})
        except Exception as e:
            self.logger.warning(f"Failed to fetch stock data for {ticker}: {e}")
            return {}
    
    async def _fetch_token_series(
        self, client: httpx.AsyncClient, settings, token: str
    ) -> Dict[str, Any]:
        """Fetch token price series from Alpha Vantage."""
        try:
            params = {
                "function": "DIGITAL_CURRENCY_DAILY",
                "symbol": token,
                "market": "USD",
                "apikey": settings.alphavantage_api_key,
            }
            resp = await client.get(settings.alphavantage_base, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "Note" in data or "Information" in data:
                return {}
            return data.get("Time Series (Digital Currency Daily)", {})
        except Exception as e:
            self.logger.warning(f"Failed to fetch token data for {token}: {e}")
            return {}
    
    def _nearest_close(self, series: Dict[str, Any], target: datetime) -> Optional[float]:
        """Get the nearest close price on or before target date."""
        if not series:
            return None
        target_date = target.date()
        dates = sorted(series.keys())
        for d in reversed(dates):
            try:
                dt = datetime.strptime(d, "%Y-%m-%d").date()
            except Exception:
                continue
            if dt <= target_date:
                val = series.get(d)
                if isinstance(val, dict):
                    close = val.get("4. close") or val.get("4b. close (USD)")
                    return float(close) if close else None
        return None
    
    def _pct(self, a: Optional[float], b: Optional[float]) -> str:
        """Calculate percentage change."""
        if a is None or b is None or a == 0:
            return "N/A"
        return f"{((b - a) / a) * 100:.2f}%"
    
    def _apply_stock_metrics(
        self, data: Dict[str, Any], series: Dict[str, Any], ann_date: datetime
    ):
        """Apply stock performance metrics to data."""
        d = self._nearest_close(series, ann_date)
        d_minus_1 = self._nearest_close(series, ann_date - timedelta(days=1))
        d_minus_7 = self._nearest_close(series, ann_date - timedelta(days=7))
        d_minus_30 = self._nearest_close(series, ann_date - timedelta(days=30))
        d_plus_1 = self._nearest_close(series, ann_date + timedelta(days=1))
        d_plus_7 = self._nearest_close(series, ann_date + timedelta(days=7))
        d_plus_30 = self._nearest_close(series, ann_date + timedelta(days=30))
        
        def _set(k: str, v: str):
            if data.get(k) in (None, "", "N/A"):
                data[k] = v
        
        _set("Share Price on Ann. Date", f"{d:.2f}" if d else "N/A")
        _set("1D Stock Perf", self._pct(d, d_plus_1))
        _set("7D Stock Perf", self._pct(d, d_plus_7))
        _set("30D Stock Perf", self._pct(d, d_plus_30))
        _set("D Stock Perf", self._pct(d_minus_1, d))
        _set("-7D Stock Perf", self._pct(d_minus_7, d))
        _set("-7 to -1D Stock Perf", self._pct(d_minus_7, d_minus_1))
        _set("-30D Stock Perf (to D-1)", self._pct(d_minus_30, d_minus_1))
    
    def _apply_token_metrics(
        self, data: Dict[str, Any], series: Dict[str, Any], ann_date: datetime
    ):
        """Apply token performance metrics to data."""
        td = self._nearest_close(series, ann_date)
        td_minus_1 = self._nearest_close(series, ann_date - timedelta(days=1))
        td_minus_7 = self._nearest_close(series, ann_date - timedelta(days=7))
        td_plus_1 = self._nearest_close(series, ann_date + timedelta(days=1))
        td_plus_7 = self._nearest_close(series, ann_date + timedelta(days=7))
        
        def _set(k: str, v: str):
            if data.get(k) in (None, "", "N/A"):
                data[k] = v
        
        _set("Token Price on Ann. Date", f"{td:.2f}" if td else "N/A")
        _set("1D Token Perf", self._pct(td, td_plus_1))
        _set("7D Token Perf", self._pct(td, td_plus_7))
        _set("D Token Perf", self._pct(td_minus_1, td))
        _set("-7D Token Perf", self._pct(td_minus_7, td))
        _set("-7 to -1D Token Perf", self._pct(td_minus_7, td_minus_1))
