from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings


logger = logging.getLogger(__name__)


# Common token symbol to CoinGecko ID mapping
TOKEN_ID_MAP: dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "BNB": "binancecoin",
    "ADA": "cardano",
    "XRP": "ripple",
    "DOGE": "dogecoin",
    "TON": "the-open-network",
    "AVAX": "avalanche-2",
    "DOT": "polkadot",
    "MATIC": "matic-network",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "ATOM": "cosmos",
    "LTC": "litecoin",
    "ETC": "ethereum-classic",
    "XLM": "stellar",
    "ALGO": "algorand",
    "VET": "vechain",
    "FIL": "filecoin",
    "TRX": "tron",
    "EOS": "eos",
    "AAVE": "aave",
    "MKR": "maker",
    "COMP": "compound-governance-token",
    "YFI": "yearn-finance",
    "SNX": "havven",
    "SUSHI": "sushi",
    "CRV": "curve-dao-token",
    "1INCH": "1inch",
    "BAL": "balancer",
    "ZEC": "zcash",
    "DASH": "dash",
    "XMR": "monero",
    "ZEN": "zencash",
}


def _token_to_coingecko_id(token: str | None) -> str | None:
    """Convert token symbol (e.g., BTC) to CoinGecko ID (e.g., bitcoin)."""
    if not token:
        return None
    token_upper = token.strip().upper()
    return TOKEN_ID_MAP.get(token_upper)


def _parse_date(date_str: str | None) -> datetime | None:
    """Parse date string into datetime, handling multiple formats."""
    if not date_str or date_str.strip().upper() == "N/A":
        return None
    s = date_str.strip()
    # Try several common formats first
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%y", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%d %b %Y"):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    # Fallback to dateutil if available
    try:
        from dateutil import parser as dateparser  # type: ignore

        dt = dateparser.parse(s)
        if dt is not None and dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _pct(a: float | None, b: float | None) -> str:
    """Calculate percentage change."""
    if a is None or b is None or a == 0:
        return "N/A"
    return f"{((b - a) / a) * 100:.2f}%"


async def _fetch_coingecko_history(
    client: httpx.AsyncClient, coin_id: str, date: datetime
) -> dict[str, Any] | None:
    """Fetch CoinGecko historical price data for a specific date.
    
    Uses /coins/{id}/history endpoint with dd-mm-yyyy date format.
    Note: Free tier supports data up to 365 days back.
    Pro API supports up to 10 years of historical data.
    """
    settings = get_settings()
    
    # Detect Pro API key (starts with "CG-") and use appropriate endpoint
    api_key = settings.coingecko_api_key
    is_pro_api = api_key and api_key.startswith("CG-")
    
    if is_pro_api:
        base_url = "https://pro-api.coingecko.com/api/v3"
        api_header = "x-cg-pro-api-key"
    else:
        base_url = "https://api.coingecko.com/api/v3"
        api_header = "x-cg-demo-api-key"
    
    # CoinGecko history endpoint: GET /coins/{id}/history
    # date format: dd-mm-yyyy (per official docs)
    date_str = date.strftime("%d-%m-%Y")
    url = f"{base_url}/coins/{coin_id}/history"
    
    params: dict[str, str] = {"date": date_str, "localization": "false"}
    
    # Add API key if available
    headers: dict[str, str] = {}
    if api_key:
        headers[api_header] = api_key
    
    try:
        resp = await client.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        try:
            error_body = exc.response.text
            error_json = exc.response.json() if exc.response.headers.get("content-type", "").startswith("application/json") else None
        except Exception:
            error_body = str(exc)
            error_json = None
        
        if status == 429:
            logger.warning("CoinGecko rate limit hit for %s: %s", coin_id, error_body[:500])
        elif status == 400:
            # 400 usually means invalid date (future date, too old >365 days, or date format issue)
            # Log at WARNING level to see actual error message
            if error_json:
                logger.warning("CoinGecko 400 error for %s on %s: %s", coin_id, date_str, error_json)
            else:
                logger.warning("CoinGecko 400 error for %s on %s: %s", coin_id, date_str, error_body[:500])
        else:
            logger.warning("CoinGecko API error %s for %s: %s", status, coin_id, error_body[:500])
        return None
    except Exception as exc:
        logger.warning("CoinGecko fetch failed for %s: %s", coin_id, exc)
        return None


async def _fetch_coingecko_market_chart_range(
    client: httpx.AsyncClient, coin_id: str, from_date: datetime, to_date: datetime
) -> dict[str, Any] | None:
    """Fetch CoinGecko historical price data using market_chart/range endpoint.
    
    This endpoint uses UNIX timestamps and can fetch multiple days at once.
    More efficient than calling /history multiple times.
    Per CoinGecko docs: https://docs.coingecko.com/docs/2-get-historical-data
    """
    settings = get_settings()
    
    # Detect Pro API key (starts with "CG-") and use appropriate endpoint
    api_key = settings.coingecko_api_key
    is_pro_api = api_key and api_key.startswith("CG-")
    
    if is_pro_api:
        base_url = "https://pro-api.coingecko.com/api/v3"
        api_header = "x-cg-pro-api-key"
    else:
        base_url = "https://api.coingecko.com/api/v3"
        api_header = "x-cg-demo-api-key"
    
    # Convert dates to UNIX timestamps (start of day UTC)
    from_ts = int(from_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    to_ts = int(to_date.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())
    
    url = f"{base_url}/coins/{coin_id}/market_chart/range"
    params: dict[str, str] = {
        "vs_currency": "usd",
        "from": str(from_ts),
        "to": str(to_ts),
    }
    
    # Add API key if available
    headers: dict[str, str] = {}
    if api_key:
        headers[api_header] = api_key
    
    try:
        resp = await client.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        try:
            error_body = exc.response.text
            error_json = exc.response.json() if exc.response.headers.get("content-type", "").startswith("application/json") else None
        except Exception:
            error_body = str(exc)
            error_json = None
        
        if status == 429:
            logger.warning("CoinGecko rate limit hit for %s range: %s", coin_id, error_body[:500])
        elif status == 400:
            logger.warning("CoinGecko 400 error for %s range (%s to %s): %s", coin_id, from_date.date(), to_date.date(), error_json or error_body[:500])
        else:
            logger.warning("CoinGecko API error %s for %s range: %s", status, coin_id, error_body[:500])
        return None
    except Exception as exc:
        logger.warning("CoinGecko fetch failed for %s range: %s", coin_id, exc)
        return None


def _extract_price_from_market_chart(data: dict[str, Any] | None, target_date: datetime) -> float | None:
    """Extract USD price from CoinGecko market_chart/range response for a specific date.
    
    The response format is: {"prices": [[timestamp_ms, price], ...]}
    Returns the price closest to the target date (or exact match if available).
    """
    if not data:
        return None
    try:
        prices = data.get("prices", [])
        if not prices:
            return None
        
        # Convert target date to milliseconds timestamp
        target_ts_ms = int(target_date.timestamp() * 1000)
        
        # Find the closest price point (prices are sorted by timestamp)
        closest_price = None
        min_diff = float('inf')
        
        for ts_ms, price in prices:
            diff = abs(ts_ms - target_ts_ms)
            if diff < min_diff:
                min_diff = diff
                closest_price = price
        
        return float(closest_price) if closest_price is not None else None
    except Exception:
        return None


def _extract_price_from_history(data: dict[str, Any] | None) -> float | None:
    """Extract USD price from CoinGecko history response."""
    if not data:
        return None
    try:
        market_data = data.get("market_data", {})
        current_price = market_data.get("current_price", {})
        if isinstance(current_price, dict):
            return current_price.get("usd")
        return None
    except Exception:
        return None


async def enrich_folder_with_coingecko(
    dir_path: Path, *, as_of: datetime | None = None, limit_files: int | None = None
) -> dict[str, Any]:
    """Enrich JSON files with CoinGecko token price data."""
    settings = get_settings()
    
    files = sorted([p for p in dir_path.glob("*.json") if p.is_file()])
    if limit_files is not None:
        files = files[: max(0, int(limit_files))]
    as_of = as_of or datetime.now(timezone.utc)
    # Use today's date (UTC) as the maximum fetchable date (CoinGecko can't provide future data)
    # Also skip "today" as CoinGecko might not have today's data yet (usually has data up to yesterday)
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    
    # Check if using Pro API (supports up to 10 years) or free tier (365 days)
    api_key = settings.coingecko_api_key
    is_pro_api = api_key and api_key.startswith("CG-")
    if is_pro_api:
        # Pro API supports up to 10 years of historical data
        min_date = today - timedelta(days=3650)  # ~10 years
    else:
        # Free tier supports historical data up to 365 days back
        min_date = today - timedelta(days=365)

    token_cache: dict[str, dict[str, float | None]] = {}  # coin_id -> {date_str: price}
    
    saved = 0
    skipped = 0
    outputs: list[str] = []

    headers = {"User-Agent": settings.user_agent}
    async with httpx.AsyncClient(headers=headers) as client:
        for p in files:
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                skipped += 1
                continue

            token = (data.get("Token") or "").strip().upper() or None
            ann_date = _parse_date(data.get("Raise Ann. Date"))
            
            # If no announcement date, set it to yesterday (most recent available data)
            # This allows us to calculate token performance even without an announcement date
            if not ann_date:
                # Use yesterday as reference date (most recent available data)
                reference_date = datetime.combine(yesterday, datetime.min.time()).replace(tzinfo=timezone.utc)
                # Update the JSON with the reference date
                data["Raise Ann. Date"] = yesterday.strftime("%Y-%m-%d")
                logger.debug("No announcement date for %s, setting to reference date: %s", token, reference_date.date())
            else:
                reference_date = ann_date

            if not token:
                skipped += 1
                continue

            coin_id = _token_to_coingecko_id(token)
            if not coin_id:
                logger.debug("No CoinGecko ID mapping for token: %s", token)
                skipped += 1
                continue

            # Fetch prices for reference date and related dates
            dates_to_fetch = {
                "ann": reference_date,
                "ann_minus_1": reference_date - timedelta(days=1),
                "ann_minus_7": reference_date - timedelta(days=7),
                "ann_plus_1": reference_date + timedelta(days=1),
                "ann_plus_7": reference_date + timedelta(days=7),
            }
            
            prices: dict[str, float | None] = {}
            
            for key, target_date in dates_to_fetch.items():
                # Don't fetch future dates beyond today (CoinGecko limitation)
                target_date_only = target_date.date()
                if target_date_only > today:
                    # Skip future dates beyond today
                    prices[key] = None
                    continue
                # Skip dates that are too old (CoinGecko free tier limitation)
                if target_date_only < min_date:
                    logger.debug("Skipping date %s for %s (too old, min: %s)", target_date_only, coin_id, min_date)
                    prices[key] = None
                    continue
                    
                date_str = target_date.strftime("%Y-%m-%d")
                cache_key = f"{coin_id}:{date_str}"
                
                if cache_key in token_cache:
                    prices[key] = token_cache[cache_key]
                else:
                    history_data = await _fetch_coingecko_history(client, coin_id, target_date)
                    price = _extract_price_from_history(history_data)
                    token_cache[cache_key] = price
                    prices[key] = price
                    # Small delay to respect rate limits (free tier: 10-50 calls/min)
                    import asyncio
                    await asyncio.sleep(1.2)  # ~50 calls/min max

            def _set(k: str, v: str):
                cur = data.get(k)
                if cur in (None, "", "N/A"):
                    data[k] = v

            # Set token price on announcement date
            ann_price = prices.get("ann")
            _set("Token Price on Ann. Date", f"{ann_price:.2f}" if ann_price is not None else "N/A")
            
            # Calculate performance metrics
            ann_p = prices.get("ann")
            ann_minus_1_p = prices.get("ann_minus_1")
            ann_minus_7_p = prices.get("ann_minus_7")
            ann_plus_1_p = prices.get("ann_plus_1")
            ann_plus_7_p = prices.get("ann_plus_7")
            
            # Forward-looking performance (AFTER announcement)
            _set("1D Token Perf", _pct(ann_p, ann_plus_1_p) if ann_plus_1_p is not None else "N/A")
            _set("7D Token Perf", _pct(ann_p, ann_plus_7_p) if ann_plus_7_p is not None else "N/A")
            
            # Day-of-announcement performance (D-1 to D)
            _set("D Token Perf", _pct(ann_minus_1_p, ann_p))
            
            # Backward-looking performance (BEFORE announcement)
            _set("-7D Token Perf", _pct(ann_minus_7_p, ann_p))
            _set("-7 to -1D Token Perf", _pct(ann_minus_7_p, ann_minus_1_p))

            # Write back in place
            p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            outputs.append(str(p))
            saved += 1

    return {"saved": saved, "skipped": skipped, "outputs": outputs}

