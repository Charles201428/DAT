from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class Company(BaseModel):
    name: str
    ticker: str | None = None
    exchange: str | None = None
    country: str | None = None


class EventCreate(BaseModel):
    company: Company
    transaction: dict
    discovery: dict
    validation: dict | None = None
    raw_doc_id: str | None = None


class EventOut(BaseModel):
    id: str
    created_at: datetime
    company: Company
    transaction: dict
    discovery: dict
    validation: dict | None = None
    confidence: int = 0


