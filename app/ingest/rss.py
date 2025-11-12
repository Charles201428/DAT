from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from typing import Iterable

import feedparser
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db.models import Action, Event, Execution, FinancingType, SecForm, SourceDoc, SourceType, VehicleType
from app.db.session import session_context
from app.parsing.classifier import classify_text
from app.parsing.extractor import extract_fields


logger = logging.getLogger(__name__)


def _hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def _fetch_text(url: str, client: httpx.AsyncClient) -> str:
    resp = await client.get(url, timeout=30)
    resp.raise_for_status()
    return resp.text


async def _upsert_raw_doc(session: AsyncSession, url: str, text: str) -> SourceDoc:
    h = _hash_content(text)
    existing = await session.execute(select(SourceDoc).where(SourceDoc.content_hash == h))
    doc = existing.scalar_one_or_none()
    if doc:
        return doc
    doc = SourceDoc(url=url, content_hash=h, raw_text=text)
    session.add(doc)
    await session.flush()
    return doc


async def ingest_feeds() -> int:
    settings = get_settings()
    headers = {"User-Agent": settings.user_agent}
    async with httpx.AsyncClient(headers=headers) as client:
        # MVP: EDGAR 8-K current feed
        feed_url = settings.edgar_rss_base
        parsed = feedparser.parse(feed_url)
        if not parsed.entries:
            logger.info("RSS: no entries")
            return 0
        async with session_context() as session:
            inserted = 0
            for entry in parsed.entries[:50]:  # cap for MVP
                link = entry.get("link")
                title = entry.get("title", "")
                if not link:
                    continue
                try:
                    text = await _fetch_text(link, client)
                except Exception as exc:  # pragma: no cover
                    logger.warning("fetch failed %s: %s", link, exc)
                    continue

                doc = await _upsert_raw_doc(session, link, text)

                cls = classify_text(title + "\n" + text)
                if not cls.is_dat or cls.score < settings.min_classifier_score:
                    continue

                # Extremely simple extractor for MVP
                company_name = entry.get("author", "Unknown Company")
                extr = extract_fields(text)

                event = Event(
                    company_name=company_name,
                    company_ticker=None,
                    company_exchange=None,
                    company_country=None,
                    action=Action.announce_purchase,
                    tokens=extr.tokens or None,
                    chains=None,
                    amount_token=None,
                    amount_usd=extr.amount_usd,
                    financing_type=FinancingType.unknown,
                    vehicle_type=VehicleType.unknown,
                    vehicle_name=None,
                    execution=Execution.unknown,
                    announcement_date=extr.announcement_date or datetime.now(timezone.utc).date(),
                    effective_date=None,
                    source_type=SourceType.sec_filing,
                    source_url=link,
                    sec_form=SecForm.eight_k,
                    confidence=cls.score,
                    notes=title[:500],
                    raw_doc_id=doc.id,
                )
                session.add(event)
                inserted += 1
            await session.commit()
            logger.info("RSS: inserted %s events", inserted)
            return inserted


