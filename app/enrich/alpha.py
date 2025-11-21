from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings


def _parse_date(date_str: str | None) -> datetime | None:
    if not date_str or date_str.strip().upper() == "N/A":
        return None
    s = date_str.strip()
    # Try several common formats first
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%y", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
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
    if a is None or b is None or a == 0:
        return "N/A"
    return f"{((b - a) / a) * 100:.2f}%"


def _nearest_close(series: dict[str, Any], target: datetime) -> float | None:
    if not series:
        return None
    # series keys are dates as strings YYYY-MM-DD
    # Find the latest date ON OR BEFORE target.date()
    target_date = target.date()
    dates = sorted(series.keys())
    for d in reversed(dates):
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
        except Exception:
            continue
        if dt <= target_date:
            val = series.get(d)
            try:
                # For stock: '4. close'; for crypto daily (USD): '4b. close (USD)'
                close = (val.get("4. close") if isinstance(val, dict) else None) or (
                    val.get("4b. close (USD)") if isinstance(val, dict) else None
                )
                return float(close) if close is not None else None
            except Exception:
                return None
    # If nothing on/before (very old target), return None
    return None


async def _fetch_alpha(client: httpx.AsyncClient, params: dict[str, str]) -> dict[str, Any] | None:
    settings = get_settings()
    try:
        resp = await client.get(settings.alphavantage_base, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Alpha Vantage returns note on throttling
        if isinstance(data, dict) and ("Note" in data or "Information" in data):
            return None
        return data
    except Exception:
        return None


async def enrich_folder_with_alpha(dir_path: Path, *, as_of: datetime | None = None, limit_files: int | None = None) -> dict[str, Any]:
    settings = get_settings()
    if not settings.alphavantage_api_key:
        return {"saved": 0, "skipped": 0, "outputs": [], "error": "ALPHAVANTAGE_API_KEY missing"}

    files = sorted([p for p in dir_path.glob("*.json") if p.is_file()])
    if limit_files is not None:
        files = files[: max(0, int(limit_files))]
    as_of = as_of or datetime.now(timezone.utc)

    stock_cache: dict[str, dict[str, Any]] = {}
    token_cache: dict[str, dict[str, Any]] = {}

    def _token_to_symbol(sym: str | None) -> str | None:
        if not sym:
            return None
        return sym.strip().upper()

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

            ticker = (data.get("Stock Ticker") or "").strip().lstrip("$").upper() or None
            ann_date = _parse_date(data.get("Raise Ann. Date"))
            token = _token_to_symbol(data.get("Token"))

            # STOCK: TIME_SERIES_DAILY_ADJUSTED
            if ticker and ann_date:
                series = stock_cache.get(ticker)
                if series is None:
                    params = {
                        # Use free daily endpoint
                        "function": "TIME_SERIES_DAILY",
                        "symbol": ticker,
                        "apikey": settings.alphavantage_api_key or "",
                        "outputsize": "compact",
                    }
                    data_json = await _fetch_alpha(client, params)
                    series = (data_json or {}).get("Time Series (Daily)") or {}
                    stock_cache[ticker] = series

                # Anchor on announcement date
                d = _nearest_close(series, ann_date)
                d_minus_1 = _nearest_close(series, ann_date - timedelta(days=1))
                d_minus_7 = _nearest_close(series, ann_date - timedelta(days=7))
                d_minus_30 = _nearest_close(series, ann_date - timedelta(days=30))
                d_plus_1 = _nearest_close(series, ann_date + timedelta(days=1))
                d_plus_7 = _nearest_close(series, ann_date + timedelta(days=7))
                d_plus_30 = _nearest_close(series, ann_date + timedelta(days=30))

                def _set(k: str, v: str):
                    cur = data.get(k)
                    if cur in (None, "", "N/A"):
                        data[k] = v

                _set("Share Price on Ann. Date", f"{d:.2f}" if d is not None else "N/A")
                
                # Forward-looking performance (AFTER announcement)
                _set("1D Stock Perf", _pct(d, d_plus_1) if d_plus_1 is not None else "N/A")
                _set("7D Stock Perf", _pct(d, d_plus_7) if d_plus_7 is not None else "N/A")
                _set("30D Stock Perf", _pct(d, d_plus_30) if d_plus_30 is not None else "N/A")
                
                # Day-of-announcement performance (D-1 to D)
                _set("D Stock Perf", _pct(d_minus_1, d))
                
                # Backward-looking performance (BEFORE announcement)
                _set("-7D Stock Perf", _pct(d_minus_7, d))
                _set("-7 to -1D Stock Perf", _pct(d_minus_7, d_minus_1))
                _set("-30D Stock Perf (to D-1)", _pct(d_minus_30, d_minus_1))

            # TOKEN: DIGITAL_CURRENCY_DAILY (USD)
            if token and ann_date:
                series_t = token_cache.get(token)
                if series_t is None:
                    params_t = {
                        "function": "DIGITAL_CURRENCY_DAILY",
                        "symbol": token,
                        "market": "USD",
                        "apikey": settings.alphavantage_api_key or "",
                    }
                    data_t = await _fetch_alpha(client, params_t)
                    series_t = (data_t or {}).get("Time Series (Digital Currency Daily)") or {}
                    token_cache[token] = series_t

                # Anchor on announcement date
                td = _nearest_close(series_t, ann_date)
                td_minus_1 = _nearest_close(series_t, ann_date - timedelta(days=1))
                td_minus_7 = _nearest_close(series_t, ann_date - timedelta(days=7))
                td_plus_1 = _nearest_close(series_t, ann_date + timedelta(days=1))
                td_plus_7 = _nearest_close(series_t, ann_date + timedelta(days=7))

                def _set_t(k: str, v: str):
                    cur = data.get(k)
                    if cur in (None, "", "N/A"):
                        data[k] = v

                _set_t("Token Price on Ann. Date", f"{td:.2f}" if td is not None else "N/A")
                
                # Forward-looking performance (AFTER announcement)
                _set_t("1D Token Perf", _pct(td, td_plus_1) if td_plus_1 is not None else "N/A")
                _set_t("7D Token Perf", _pct(td, td_plus_7) if td_plus_7 is not None else "N/A")
                
                # Day-of-announcement performance (D-1 to D)
                _set_t("D Token Perf", _pct(td_minus_1, td))
                
                # Backward-looking performance (BEFORE announcement)
                _set_t("-7D Token Perf", _pct(td_minus_7, td))
                _set_t("-7 to -1D Token Perf", _pct(td_minus_7, td_minus_1))

            # Write back in place
            p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            outputs.append(str(p))
            saved += 1

    return {"saved": saved, "skipped": skipped, "outputs": outputs}


