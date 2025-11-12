from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from pathlib import Path
import httpx
from bs4 import BeautifulSoup
from app.config import get_settings

from app.db.models import SourceDoc
from app.db.session import get_session


router = APIRouter(prefix="/news", tags=["news"])


@router.get("")
async def list_news(
    since: datetime | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    stmt = select(SourceDoc).order_by(SourceDoc.fetched_at.desc()).limit(limit)
    rows = (await session.execute(stmt)).scalars().all()
    results: list[dict[str, Any]] = []
    for d in rows:
        results.append(
            {
                "id": str(d.id),
                "url": d.url,
                "fetched_at": d.fetched_at,
            }
        )
    return results


@router.get("/{doc_id}")
async def get_news_item(doc_id: str, session: AsyncSession = Depends(get_session)) -> dict[str, Any]:
    try:
        doc_uuid = UUID(doc_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid id")

    stmt = select(SourceDoc).where(SourceDoc.id == doc_uuid)
    doc = (await session.execute(stmt)).scalars().first()
    if not doc:
        raise HTTPException(status_code=404, detail="not found")
    return {
        "id": str(doc.id),
        "url": doc.url,
        "fetched_at": doc.fetched_at,
        "raw_text": doc.raw_text,
    }


@router.post("/fetch-original")
async def fetch_original_texts(
    dir: str = Query(..., description="Folder containing .txt files with first line 'URL: ...'"),
    limit_files: int | None = Query(default=None, ge=1, le=5000),
) -> dict[str, int]:
    settings = get_settings()
    base = Path(dir)
    if not base.exists() or not base.is_dir():
        raise HTTPException(status_code=400, detail=f"directory not found: {dir}")
    files = sorted([p for p in base.glob("*.txt") if p.is_file()])
    if limit_files is not None:
        files = files[:limit_files]
    headers = {"User-Agent": settings.user_agent}
    saved = 0
    skipped = 0
    async with httpx.AsyncClient(headers=headers, timeout=30) as client:
        for p in files:
            try:
                text = p.read_text(encoding="utf-8", errors="ignore").splitlines()
                if not text or not text[0].startswith("URL:"):
                    skipped += 1
                    continue
                url = text[0].split("URL:", 1)[1].strip()
                if not url:
                    skipped += 1
                    continue
                resp = await client.get(url)
                resp.raise_for_status()
                html = resp.text
                soup = BeautifulSoup(html, "html.parser")
                for tag in soup(["script", "style", "noscript"]):
                    tag.decompose()
                body_text = soup.get_text("\n", strip=True)
                out = p.with_suffix("")
                out = out.parent / f"{out.name}.orig.txt"
                out.write_text(f"URL: {url}\n{body_text}", encoding="utf-8")
                saved += 1
            except Exception:
                skipped += 1
                continue
    return {"saved": saved, "skipped": skipped}


