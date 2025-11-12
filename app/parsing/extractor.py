from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Iterable


AMOUNT_RE = re.compile(r"\$\s?([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{1,2})?)", re.I)
TOKEN_RE = re.compile(r"\b(BTC|Bitcoin|ETH|Ether|Solana|SOL|BNB|TON|AVAX)\b", re.I)
DATE_RE = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+\d{4}\b", re.I)


@dataclass
class Extracted:
    tokens: list[str]
    amount_usd: float | None
    announcement_date: date | None


def _parse_money(s: str) -> float | None:
    s = s.replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def extract_fields(text: str) -> Extracted:
    tokens = [m.group(1).upper() for m in TOKEN_RE.finditer(text)]
    amount_usd = None
    for m in AMOUNT_RE.finditer(text):
        amount_usd = _parse_money(m.group(1))
        if amount_usd is not None:
            break
    announcement_date = None
    m = DATE_RE.search(text)
    if m:
        # leave as string for MVP; parser to date can be added later
        announcement_date = None
    return Extracted(tokens=list(dict.fromkeys(tokens)), amount_usd=amount_usd, announcement_date=announcement_date)


