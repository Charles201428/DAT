from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from pathlib import Path
import shutil
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession

from app.analyze.gpt import analyze_docs, classify_docs, classify_texts_from_dir, format_texts_from_dir
from app.enrich.stocks import enrich_folder_with_yfinance
from app.enrich.alpha import enrich_folder_with_alpha
from app.db.session import get_session
from app.config import get_settings


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
    result = classify_texts_from_dir(target_dir, save_jsonl=save, limit_files=limit_files)
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

