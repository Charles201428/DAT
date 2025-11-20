from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
import shutil
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.analyze.gpt import analyze_docs, classify_docs, classify_texts_from_dir, format_texts_from_dir
from app.enrich.stocks import enrich_folder_with_yfinance
from app.enrich.alpha import enrich_folder_with_alpha
from app.enrich.coingecko import enrich_folder_with_coingecko, _token_to_coingecko_id, _fetch_coingecko_history, _extract_price_from_history, _pct
from app.utils.dedupe import dedupe_folder
from app.db.session import get_session
from app.config import get_settings
import httpx


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analyze", tags=["analyze"])


@router.post("")
async def analyze(limit: int = Query(default=20, ge=1, le=200), session: AsyncSession = Depends(get_session)) -> dict[str, int]:
    try:
        inserted = await analyze_docs(session, limit=limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"inserted_events": inserted}


@router.post("/classify")
async def classify(
    limit: int = Query(default=20, ge=1, le=1000),
    save: bool = Query(default=False),
    out_dir: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> dict[str, object]:
    try:
        results = await classify_docs(session, limit=limit)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    saved_file = None
    if save:
        settings = get_settings()
        base_dir = Path(out_dir) if out_dir else Path(settings.news_text_dir)
        target_dir = base_dir
        if not out_dir:
            # choose latest timestamped subdir if exists
            if base_dir.exists():
                dirs = [p for p in base_dir.iterdir() if p.is_dir()]
                if dirs:
                    target_dir = max(dirs, key=lambda p: p.stat().st_mtime)
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
            jf = target_dir / f"classifications_{ts}.jsonl"
            with jf.open("w", encoding="utf-8") as f:
                for r in results:
                    # minimal JSONL: doc_id, is_dat
                    f.write(f"{{\"doc_id\":\"{r['doc_id']}\",\"is_dat\":{str(bool(r['is_dat'])).lower()}}}\n")
            saved_file = str(jf)
        except Exception as exc:  # pragma: no cover
            raise HTTPException(status_code=500, detail=f"failed to write results: {exc}")

    return {"count": len(results), "positives": sum(1 for r in results if r.get("is_dat")), "saved_file": saved_file, "results": results}


@router.post("/classify-local")
async def classify_local(
    dir: str | None = Query(default=None),
    save: bool = Query(default=True),
    export_positives: bool = Query(default=True),
    limit_files: int | None = Query(default=None, ge=1, le=2000),
    workers: int | None = Query(default=None, ge=1, le=50, description="Number of parallel workers"),
) -> dict[str, object]:
    settings = get_settings()
    base_dir = Path(settings.news_text_dir)
    target_dir = Path(dir) if dir else base_dir
    if not dir:
        if base_dir.exists():
            dirs = [p for p in base_dir.iterdir() if p.is_dir()]
            if dirs:
                target_dir = max(dirs, key=lambda p: p.stat().st_mtime)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    result = classify_texts_from_dir(target_dir, save_jsonl=save, limit_files=limit_files, workers=workers)
    if export_positives:
        positives_dir = Path(settings.positive_text_dir) / target_dir.name
        positives_dir.mkdir(parents=True, exist_ok=True)
        for r in result.get("results", []):
            if r.get("is_dat"):
                src = target_dir / r["file"]
                if src.exists():
                    shutil.copy2(src, positives_dir / src.name)
    return result


@router.post("/format-local")
async def format_local(
    dir: str,
    limit_files: int | None = Query(default=None, ge=1, le=2000),
    orig_only: bool = Query(default=True),
) -> dict[str, object]:
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    return format_texts_from_dir(target_dir, limit_files=limit_files, orig_only=orig_only)

@router.post("/enrich-stock")
async def enrich_stock(
    dir: str,
    as_of: str | None = Query(default=None, description="ISO time, default now UTC"),
    limit_files: int | None = Query(default=None, ge=1, le=5000),
) -> dict[str, object]:
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    dt = None
    if as_of:
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid as_of: {exc}")
    return enrich_folder_with_yfinance(target_dir, as_of=dt, limit_files=limit_files)

@router.post("/enrich-stock-av")
async def enrich_stock_alpha(
    dir: str,
    as_of: str | None = Query(default=None, description="ISO time, default now UTC"),
    limit_files: int | None = Query(default=None, ge=1, le=5000),
) -> dict[str, object]:
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    dt = None
    if as_of:
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid as_of: {exc}")
    return await enrich_folder_with_alpha(target_dir, as_of=dt, limit_files=limit_files)

@router.post("/enrich-token-cg")
async def enrich_token_coingecko(
    dir: str,
    as_of: str | None = Query(default=None, description="ISO time, default now UTC"),
    limit_files: int | None = Query(default=None, ge=1, le=5000),
) -> dict[str, object]:
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    dt = None
    if as_of:
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid as_of: {exc}")
    return await enrich_folder_with_coingecko(target_dir, as_of=dt, limit_files=limit_files)

@router.post("/enrich-full")
async def enrich_full(
    dir: str,
    as_of: str | None = Query(default=None, description="ISO time, default now UTC"),
    limit_files: int | None = Query(default=None, ge=1, le=5000),
) -> dict[str, object]:
    """Enrich with both Alpha Vantage (stocks) and CoinGecko (tokens)."""
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    dt = None
    if as_of:
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid as_of: {exc}")
    
    # First enrich stocks with Alpha Vantage
    stock_result = await enrich_folder_with_alpha(target_dir, as_of=dt, limit_files=limit_files)
    # Then enrich tokens with CoinGecko
    token_result = await enrich_folder_with_coingecko(target_dir, as_of=dt, limit_files=limit_files)
    
    return {
        "stocks": stock_result,
        "tokens": token_result,
    }

@router.post("/format-and-enrich-av")
async def format_and_enrich_alpha(
    dir: str,
    limit_files: int | None = Query(default=None, ge=1, le=2000),
    orig_only: bool = Query(default=True),
    as_of: str | None = Query(default=None),
) -> dict[str, object]:
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    fmt_result = format_texts_from_dir(target_dir, limit_files=limit_files, orig_only=orig_only)
    dt = None
    if as_of:
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid as_of: {exc}")
    enr_result = await enrich_folder_with_alpha(target_dir, as_of=dt, limit_files=limit_files)
    return {"format": fmt_result, "enrich": enr_result}

@router.post("/format-and-enrich")
async def format_and_enrich(
    dir: str,
    limit_files: int | None = Query(default=None, ge=1, le=2000),
    orig_only: bool = Query(default=True),
    as_of: str | None = Query(default=None),
) -> dict[str, object]:
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    # Step 1: GPT formatting
    fmt_result = format_texts_from_dir(target_dir, limit_files=limit_files, orig_only=orig_only)
    # Step 2: yfinance enrichment (in-place)
    dt = None
    if as_of:
        try:
            from datetime import datetime, timezone
            dt = datetime.fromisoformat(as_of.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"invalid as_of: {exc}")
    enr_result = enrich_folder_with_yfinance(target_dir, as_of=dt, limit_files=limit_files)
    return {"format": fmt_result, "enrich": enr_result}

@router.get("/test-coingecko")
async def test_coingecko(
    token: str = Query(default="BTC", description="Token symbol (e.g., BTC, ETH)"),
    base_date: str = Query(default=None, description="Base date in YYYY-MM-DD format (default: 7 days ago)"),
) -> dict[str, object]:
    """Test CoinGecko API by fetching token prices and calculating performance metrics."""
    from datetime import datetime, timezone, timedelta
    
    # Parse base date or use 7 days ago as default
    if base_date:
        try:
            base_dt = datetime.strptime(base_date, "%Y-%m-%d")
            base_dt = base_dt.replace(tzinfo=timezone.utc)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid base_date format: {exc}. Use YYYY-MM-DD")
    else:
        base_dt = datetime.now(timezone.utc) - timedelta(days=7)
    
    # Convert token symbol to CoinGecko ID
    coin_id = _token_to_coingecko_id(token.upper())
    if not coin_id:
        raise HTTPException(status_code=400, detail=f"Unknown token symbol: {token}. Supported: BTC, ETH, SOL, etc.")
    
    # Calculate dates to fetch
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    base_date_only = base_dt.date()
    
    if base_date_only > yesterday:
        raise HTTPException(status_code=400, detail=f"Base date {base_date} is too recent. Use a date up to yesterday.")
    
    dates_to_fetch = {
        "base": base_dt,
        "base_minus_1": base_dt - timedelta(days=1),
        "base_minus_3": base_dt - timedelta(days=3),
        "base_minus_7": base_dt - timedelta(days=7),
        "base_plus_1": base_dt + timedelta(days=1),
        "base_plus_3": base_dt + timedelta(days=3),
        "base_plus_7": base_dt + timedelta(days=7),
    }
    
    settings = get_settings()
    headers = {"User-Agent": settings.user_agent}
    prices: dict[str, float | None] = {}
    
    fetch_details: dict[str, str] = {}
    async with httpx.AsyncClient(headers=headers) as client:
        for key, target_date in dates_to_fetch.items():
            target_date_only = target_date.date()
            # Skip future dates
            if target_date_only > yesterday:
                prices[key] = None
                fetch_details[key] = f"Skipped: future date ({target_date_only} > {yesterday})"
                continue
            
            try:
                history_data = await _fetch_coingecko_history(client, coin_id, target_date)
                if history_data is None:
                    prices[key] = None
                    fetch_details[key] = f"API returned None for {target_date_only} (check server logs for details)"
                else:
                    price = _extract_price_from_history(history_data)
                    prices[key] = price
                    fetch_details[key] = f"Success: ${price:,.2f}" if price else f"No price in response for {target_date_only}"
                # Small delay to respect rate limits
                import asyncio
                await asyncio.sleep(1.2)
            except Exception as exc:
                prices[key] = None
                fetch_details[key] = f"Exception: {exc}"
                continue
    
    # Calculate performance metrics
    base_price = prices.get("base")
    base_minus_1_price = prices.get("base_minus_1")
    base_minus_3_price = prices.get("base_minus_3")
    base_minus_7_price = prices.get("base_minus_7")
    base_plus_1_price = prices.get("base_plus_1")
    base_plus_3_price = prices.get("base_plus_3")
    base_plus_7_price = prices.get("base_plus_7")
    
    result = {
        "token": token.upper(),
        "coin_id": coin_id,
        "base_date": base_date_only.isoformat(),
        "today": today.isoformat(),
        "yesterday": yesterday.isoformat(),
        "prices": {
            "base": base_price,
            "base_minus_1": base_minus_1_price,
            "base_minus_3": base_minus_3_price,
            "base_minus_7": base_minus_7_price,
            "base_plus_1": base_plus_1_price,
            "base_plus_3": base_plus_3_price,
            "base_plus_7": base_plus_7_price,
        },
        "fetch_details": fetch_details,
        "performance": {
            "1D_perf": _pct(base_price, base_plus_1_price) if base_plus_1_price and base_price else "N/A",
            "3D_perf": _pct(base_price, base_plus_3_price) if base_plus_3_price and base_price else "N/A",
            "7D_perf": _pct(base_price, base_plus_7_price) if base_plus_7_price and base_price else "N/A",
            "-1D_perf": _pct(base_minus_1_price, base_price) if base_minus_1_price and base_price else "N/A",
            "-3D_perf": _pct(base_minus_3_price, base_price) if base_minus_3_price and base_price else "N/A",
            "-7D_perf": _pct(base_minus_7_price, base_price) if base_minus_7_price and base_price else "N/A",
        },
    }
    
    return result


@router.post("/dedup")
async def dedup(
    dir: str = Query(..., description="Folder containing JSON files to deduplicate"),
    keep: str = Query(default="largest", pattern="^(largest|newest|most_filled|first)$"),
    require_all: bool = Query(default=True, description="Require stock, token, and date to deduplicate"),
    remove_duplicates: bool = Query(default=False, description="If true, delete duplicates instead of moving to _dedup_trash"),
    include_related: bool = Query(default=True, description="Also move/delete sibling files sharing the same base stem (e.g., .orig.txt)"),
    dry_run: bool = Query(default=True, description="If true, only report; do not modify files"),
) -> dict[str, object]:
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    try:
        result = dedupe_folder(
            target_dir,
            keep=keep,
            require_all=require_all,
            remove_duplicates=remove_duplicates,
            include_related=include_related,
            dry_run=dry_run,
        )
        return result
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))

@router.post("/json-to-csv")
async def json_to_csv(
    dir: str = Query(..., description="Folder containing JSON files to combine"),
    output_file: str | None = Query(default=None, description="Output CSV filename (default: {dir_name}_combined.csv)"),
) -> dict[str, object]:
    """Combine all JSON files in a directory into a single CSV file."""
    target_dir = Path(dir)
    if not target_dir.exists():
        raise HTTPException(status_code=400, detail=f"directory not found: {target_dir}")
    
    # Get all JSON files
    json_files = sorted([p for p in target_dir.glob("*.json") if p.is_file()])
    if not json_files:
        raise HTTPException(status_code=400, detail=f"no JSON files found in {target_dir}")
    
    # Collect all data and determine all possible fields
    all_data: list[dict[str, Any]] = []
    all_fields: set[str] = set()
    
    for json_file in json_files:
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            all_data.append(data)
            all_fields.update(data.keys())
        except Exception as exc:
            logger.warning("Failed to parse %s: %s", json_file.name, exc)
            continue
    
    if not all_data:
        raise HTTPException(status_code=400, detail="no valid JSON data found")
    
    # Sort fields for consistent column order
    sorted_fields = sorted(all_fields)
    
    # Determine output filename
    if output_file:
        csv_path = target_dir / output_file
    else:
        csv_path = target_dir / f"{target_dir.name}_combined.csv"
    
    # Write CSV file
    try:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=sorted_fields)
            writer.writeheader()
            for row in all_data:
                # Ensure all fields are present (fill missing with empty string)
                complete_row = {field: row.get(field, "") for field in sorted_fields}
                writer.writerow(complete_row)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to write CSV: {exc}")
    
    return {
        "csv_file": str(csv_path),
        "rows": len(all_data),
        "columns": len(sorted_fields),
        "fields": sorted_fields,
    }
