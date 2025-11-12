from __future__ import annotations

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.config import get_settings
from app.db.session import init_db
from app.config import get_settings
from app.routers.events import router as events_router
from app.routers.news import router as news_router
from app.routers.analyze import router as analyze_router


logger = logging.getLogger(__name__)
app = FastAPI(title="Crypto Treasury Parser")
scheduler = AsyncIOScheduler()


@app.on_event("startup")
async def on_startup() -> None:
    settings = get_settings()
    logging.basicConfig(level=getattr(logging, settings.log_level, logging.INFO))
    await init_db()

    # Schedule CryptoPanic ingestion (disabled by default)
    if settings.enable_scheduler:
        try:
            from app.ingest.cryptopanic import ingest_cryptopanic

            scheduler.add_job(ingest_cryptopanic, "interval", minutes=30, id="cryptopanic_ingest")
            scheduler.start()
            logger.info("Scheduler started with CryptoPanic ingest every 30 minutes")
        except Exception as exc:  # pragma: no cover
            logger.warning("Scheduler not started: %s", exc)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    if scheduler.running:
        scheduler.shutdown(wait=False)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


app.include_router(events_router)
app.include_router(news_router)
app.include_router(analyze_router)


