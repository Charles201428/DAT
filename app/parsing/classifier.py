from __future__ import annotations

from dataclasses import dataclass


KEYWORDS_TREASURY = {
    "treasury",
    "digital asset treasury",
    "reserve",
    "treasury policy",
}

KEYWORDS_ACTION = {
    "acquire",
    "purchase",
    "allocate",
    "hold",
    "mandate",
    "explore",
}

KEYWORDS_FINANCING = {
    "registered direct",
    "private placement",
    "pipe",
    "convertible",
    "atm offering",
    "credit facility",
}

KEYWORDS_TOKENS = {
    "btc",
    "bitcoin",
    "eth",
    "ether",
    "ethereum",
    "sol",
    "solana",
    "bnb",
    "ton",
    "avax",
    "native token",
    "token",
}


@dataclass
class ClassificationResult:
    is_dat: bool
    score: int


def classify_text(text: str, *, instrument_present: bool = False) -> ClassificationResult:
    t = text.lower()

    has_token = any(k in t for k in KEYWORDS_TOKENS)
    has_treasury = any(k in t for k in KEYWORDS_TREASURY)
    has_action = any(k in t for k in KEYWORDS_ACTION)
    has_fin = any(k in t for k in KEYWORDS_FINANCING)

    score = 0
    score += 30 if has_token else 0
    score += 30 if has_treasury else 0
    score += 20 if has_action else 0
    score += 10 if has_fin else 0
    score += 20 if instrument_present else 0

    # Consider DAT when we either cross 60 or we have instruments plus treasury/financing language
    is_dat = score >= 60 or (instrument_present and (has_treasury or has_fin))
    return ClassificationResult(is_dat=is_dat, score=min(score, 100))


