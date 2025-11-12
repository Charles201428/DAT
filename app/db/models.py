from __future__ import annotations

import enum
import uuid
from datetime import date, datetime

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Boolean,
    Date,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Action(str, enum.Enum):
    announce_purchase = "announce_purchase"
    purchase = "purchase"
    mandate = "mandate"
    explore = "explore"


class FinancingType(str, enum.Enum):
    convertible = "convertible"
    equity = "equity"
    PIPE = "PIPE"
    ATM = "ATM"
    cash_reserves = "cash_reserves"
    debt = "debt"
    unknown = "unknown"


class Execution(str, enum.Enum):
    open_market = "open_market"
    otc = "otc"
    staking = "staking"
    validator = "validator"
    unknown = "unknown"


class SourceType(str, enum.Enum):
    sec_filing = "sec_filing"
    ir_press = "ir_press"
    exchange_notice = "exchange_notice"
    news = "news"


class SecForm(str, enum.Enum):
    eight_k = "8-K"
    six_k = "6-K"
    ten_q = "10-Q"
    ten_k = "10-K"
    none_ = "none"


class VehicleType(str, enum.Enum):
    parent = "parent"
    subsidiary = "subsidiary"
    spv = "spv"
    unknown = "unknown"


class SourceDoc(Base):
    __tablename__ = "source_docs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    url: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    raw_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    events: Mapped[list[Event]] = relationship(back_populates="raw_doc")  # type: ignore[name-defined]


class Event(Base):
    __tablename__ = "events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Company
    company_name: Mapped[str] = mapped_column(String(256), nullable=False)
    company_ticker: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    company_exchange: Mapped[str | None] = mapped_column(String(64), nullable=True)
    company_country: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # Transaction
    action: Mapped[Action] = mapped_column(Enum(Action, name="action_enum"), nullable=False)
    tokens: Mapped[list[str] | None] = mapped_column(JSONB().with_variant(JSON, "sqlite"), nullable=True)
    chains: Mapped[list[str] | None] = mapped_column(JSONB().with_variant(JSON, "sqlite"), nullable=True)
    amount_token: Mapped[float | None] = mapped_column(Numeric(38, 18), nullable=True)
    amount_usd: Mapped[float | None] = mapped_column(Numeric(38, 2), nullable=True)
    financing_type: Mapped[FinancingType] = mapped_column(
        Enum(FinancingType, name="financing_type_enum"), nullable=False, default=FinancingType.unknown
    )
    vehicle_type: Mapped[VehicleType] = mapped_column(
        Enum(VehicleType, name="vehicle_type_enum"), nullable=False, default=VehicleType.unknown
    )
    vehicle_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    execution: Mapped[Execution] = mapped_column(
        Enum(Execution, name="execution_enum"), nullable=False, default=Execution.unknown
    )
    announcement_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    # Discovery
    source_type: Mapped[SourceType] = mapped_column(Enum(SourceType, name="source_type_enum"), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)

    # Validation
    sec_form: Mapped[SecForm] = mapped_column(Enum(SecForm, name="sec_form_enum"), nullable=False, default=SecForm.none_)
    confidence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Relations
    raw_doc_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey("source_docs.id"), nullable=True)
    raw_doc: Mapped[SourceDoc | None] = relationship(back_populates="events")


