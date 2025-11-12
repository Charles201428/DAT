from __future__ import annotations

import hashlib
import logging
from typing import Any

import httpx
from pathlib import Path
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db.models import SourceDoc
from app.db.session import session_context
from app.parsing.classifier import classify_text
from app.parsing.extractor import extract_fields


logger = logging.getLogger(__name__)


def _hash_content(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def _upsert_raw_doc(session: AsyncSession, url: str, text: str) -> SourceDoc:
    h = _hash_content(text)
    # Prefer URL uniqueness first
    existing_by_url = await session.execute(select(SourceDoc).where(SourceDoc.url == url))
    doc = existing_by_url.scalar_one_or_none()
    if doc:
        return doc
    # Then dedupe by content hash
    existing_by_hash = await session.execute(select(SourceDoc).where(SourceDoc.content_hash == h))
    doc2 = existing_by_hash.scalar_one_or_none()
    if doc2:
        return doc2
    # Insert new record and handle potential race/duplicate
    doc_new = SourceDoc(url=url, content_hash=h, raw_text=text)
    session.add(doc_new)
    try:
        await session.flush()
        return doc_new
    except IntegrityError:
        # Retrieve the existing row that caused the conflict
        existing_conflict = await session.execute(select(SourceDoc).where(SourceDoc.url == url))
        doc3 = existing_conflict.scalar_one_or_none()
        if doc3:
            return doc3
        existing_conflict_hash = await session.execute(select(SourceDoc).where(SourceDoc.content_hash == h))
        doc4 = existing_conflict_hash.scalar_one_or_none()
        if doc4:
            return doc4
        raise


async def ingest_cryptopanic(hours: int = 24) -> dict[str, Any]:
    settings = get_settings()
    if not settings.cryptopanic_token:
        logger.warning("CRYPTOPANIC_TOKEN missing; skipping ingestion")
        return {"inserted": 0, "saved_dir": ""}

    # Use minimal params and honor size if provided
    params: dict[str, Any] = {"auth_token": settings.cryptopanic_token}
    if settings.cryptopanic_public:
        params["public"] = "true"
    # Sanitize optional params from .env (ignore placeholders or invalid values)
    allowed_filters = {"rising", "hot", "bullish", "bearish", "important", "saved", "lol"}
    f = (settings.cryptopanic_filter or "").strip().lower()
    if f in allowed_filters:
        params["filter"] = f

    # Kind is optional; only send when it's a strict value and not default 'all'
    try:
        k = (settings.cryptopanic_kind or "all").strip().lower()
    except Exception:
        k = "all"
    if k in {"news", "media"}:  # omit 'all' to reduce query surface
        params["kind"] = k

    # Currencies: ignore if looks like a placeholder
    currencies = (settings.cryptopanic_currencies or "").strip()
    if currencies and not currencies.startswith("#"):
        params["currencies"] = currencies
    # Avoid size param for now due to intermittent 5xx; default page size is acceptable
    headers = {"User-Agent": settings.user_agent}
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    # Prepare local batch directory
    batch_label = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    batch_dir = Path(settings.news_text_dir) / batch_label
    async with httpx.AsyncClient(headers=headers) as client:
        inserted = 0
        next_url = settings.cryptopanic_base
        pages_left = max(1, getattr(settings, "cryptopanic_pages", 1))
        # Ensure batch directory exists
        try:
            batch_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        async with session_context() as session:
            while pages_left:
                try:
                    resp = await client.get(next_url, params=params, timeout=30)
                    resp.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    status = exc.response.status_code
                    body = exc.response.text
                    if status in (401, 403, 429):
                        logger.error("CryptoPanic auth/rate error %s: %s | body=%s", status, exc, body)
                        break
                    logger.error(
                        "CryptoPanic HTTP error %s: %s | url=%s | params=%s | body=%s",
                        status,
                        exc,
                        next_url,
                        params,
                        body,
                    )
                    break

                data: dict[str, Any] = resp.json()
                results: list[dict[str, Any]] = data.get("results", [])
                if not results:
                    break

                all_older = True
                for item in results:
                    published_str = item.get("published_at") or item.get("created_at")
                    try:
                        published = datetime.fromisoformat(published_str.replace("Z", "+00:00")) if published_str else None
                    except Exception:
                        published = None
                    if published and published < cutoff:
                        # Older than cutoff; skip
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
                        body = f"{title}\n{content}".lower()
                        if req.lower() not in body:
                            continue

                    # Store raw doc
                    await _upsert_raw_doc(session, src_url, content)
                    # Write to batch folder (include original URL as first line)
                    try:
                        safe_name = f"{item.get('id') or _hash_content(src_url)}.txt"
                        file_payload = f"URL: {src_url}\n{content}"
                        (batch_dir / safe_name).write_text(file_payload, encoding="utf-8")
                    except Exception:
                        pass
                    inserted += 1

                next_link = data.get("next")
                if next_link and not all_older:
                    next_url = next_link
                    params = {}
                    pages_left -= 1
                    continue
                break
            await session.commit()
        logger.info("CryptoPanic: inserted %s raw docs into %s", inserted, str(batch_dir))
        return {"inserted": inserted, "saved_dir": str(batch_dir)}


