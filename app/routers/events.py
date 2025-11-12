from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Event
from app.db.session import get_session
from app.ingest.cryptopanic import ingest_cryptopanic


router = APIRouter(prefix="/events", tags=["events"])


@router.get("")
async def list_events(
    since: datetime | None = Query(default=None),
    token: str | None = Query(default=None),
    min_confidence: int = Query(default=0, ge=0, le=100),
    limit: int = Query(default=50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
) -> list[dict[str, Any]]:
    stmt = select(Event).order_by(Event.created_at.desc())
    if since is not None:
        stmt = stmt.where(Event.created_at >= since)
    if min_confidence:
        stmt = stmt.where(Event.confidence >= min_confidence)
    # Don't apply limit yet if we need to filter in Python
    rows = (await session.execute(stmt)).scalars().all()
    results: list[dict[str, Any]] = []
    # Optional token filter (Python-side for DB-agnostic behavior)
    if token is not None:
        token_upper = token.upper()
        rows = [e for e in rows if (e.tokens and any(t.upper() == token_upper for t in (e.tokens or [])))]

    # Apply limit after filtering
    rows = rows[:limit]

    for e in rows:
        results.append(
            {
                "id": str(e.id),
                "created_at": e.created_at,
                "company": {
                    "name": e.company_name,
                    "ticker": e.company_ticker,
                    "exchange": e.company_exchange,
                    "country": e.company_country,
                },
                "transaction": {
                    "action": e.action.value,
                    "token": e.tokens or [],
                    "chain": e.chains or [],
                    "amount_token": float(e.amount_token) if e.amount_token is not None else None,
                    "amount_usd": float(e.amount_usd) if e.amount_usd is not None else None,
                    "financing_type": e.financing_type.value,
                    "vehicle": {"type": e.vehicle_type.value, "name": e.vehicle_name},
                    "execution": e.execution.value,
                    "announcement_date": e.announcement_date,
                    "effective_date": e.effective_date,
                },
                "discovery": {
                    "source_type": e.source_type.value,
                    "source_url": e.source_url,
                    "first_seen_at": e.first_seen_at,
                },
                "validation": {"sec_form": e.sec_form.value, "notes": e.notes},
                "confidence": e.confidence,
            }
        )
    return results


@router.post("/ingest")
async def trigger_ingest(hours: int = Query(default=24, ge=1, le=168)) -> dict[str, object]:
    result = await ingest_cryptopanic(hours=hours)
    return result


@router.delete("")
async def clear_events(session: AsyncSession = Depends(get_session)) -> dict[str, int]:
    # Delete events first due to FK, then source docs
    del_events = await session.execute(delete(Event))
    # SourceDoc imported inline to avoid circular import at module top
    from app.db.models import SourceDoc  # noqa: WPS433

    del_docs = await session.execute(delete(SourceDoc))
    await session.commit()
    return {"deleted_events": del_events.rowcount or 0, "deleted_source_docs": del_docs.rowcount or 0}


