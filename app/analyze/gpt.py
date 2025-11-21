from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from openai import OpenAI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pathlib import Path

from app.config import get_settings
from app.db.models import Action, Event, Execution, FinancingType, SecForm, SourceDoc, SourceType, VehicleType


logger = logging.getLogger(__name__)


SYSTEM_PROMPT = (
    "You are a precise information extraction agent. Given a news article, extract a Digital Asset Treasury (DAT) "
    "event if present and return STRICT JSON ONLY matching this schema: {\n"
    "  'is_dat': boolean,\n"
    "  'company': {'name': string|null, 'ticker': string|null, 'exchange': string|null, 'country': string|null},\n"
    "  'transaction': {\n"
    "    'action': 'announce_purchase'|'purchase'|'mandate'|'explore'|null,\n"
    "    'token': string[]|null, 'chain': string[]|null,\n"
    "    'amount_token': number|null, 'amount_usd': number|null,\n"
    "    'financing_type': 'convertible'|'equity'|'PIPE'|'ATM'|'cash_reserves'|'debt'|'unknown',\n"
    "    'vehicle': {'type': 'parent'|'subsidiary'|'spv'|'unknown', 'name': string|null},\n"
    "    'execution': 'open_market'|'otc'|'staking'|'validator'|'unknown',\n"
    "    'announcement_date': string|null, 'effective_date': string|null\n"
    "  },\n"
    "  'notes': string|null\n"
    "}\n"
    "Return ONLY JSON."
)

CLASSIFY_SYSTEM_PROMPT = (
    "You are a precise binary classifier for Digital Asset Treasury (DAT) events.\n"
    "Classify news text as DAT or not using the following strict definition.\n\n"
    "WHAT IS A DAT EVENT (positives)\n"
    "A news item is DAT=TRUE ONLY IF:\n"
    "1) A named company/entity (preferably public) decides or executes balance-sheet actions involving SPECIFIC cryptoassets (BTC, ETH, SOL, BNB, TON, AVAX, etc.), AND\n"
    "2) The specific token(s) involved are clearly identifiable in the article (e.g., Bitcoin/BTC, Ethereum/ETH, Solana/SOL, etc.).\n\n"
    "Valid DAT events include:\n"
    "1) Treasury allocation / policy: formal decision/policy to hold tokens as treasury/reserves; updates (increase/decrease, diversify, add staking/validator to treasury policy).\n"
    "2) Acquisition / disposal: purchases/sales for treasury with amounts/value/timing/counterparties; plans/authorizations (e.g., \"up to $X of token Y\"), board approvals, or mandates to external managers.\n"
    "3) Financing directly tied to DAT: raises/facilities (registered direct, ATM, PIPE, convertible, loans, credit lines) where use of proceeds explicitly includes building/expanding token treasury; formation of subsidiaries/SPVs specifically to hold/acquire tokens.\n"
    "4) Treasury operations: staking/validator/custody setup as part of treasury management (not product/research), yield policies, custody changes for treasury assets, or corporate treasury wallet disclosure.\n"
    "Favor credible sources: SEC/stock-exchange filings, company IR/press releases, audited reports, tier-1 media citing primary docs.\n\n"
    "REQUIREMENTS (hard gates - ALL must be met)\n"
    "Return DAT=TRUE only if:\n"
    "1) The issuer is a publicly listed company with a valid stock ticker and exchange identifiable in the article or via authoritative sources.\n"
    "2) A specific token (BTC, ETH, SOL, BNB, etc.) is clearly mentioned and identifiable in the article.\n"
    "If the issuer is private/unlisted, ticker/exchange cannot be resolved, OR no specific token is mentioned, return DAT=FALSE.\n\n"
    "WHAT IS NOT A DAT EVENT (negatives)\n"
    "- Product/partnership/tech news (integrations, NFTs, accepting crypto payments) without balance-sheet holding.\n"
    "- Client/custody services without the company holding tokens on its own balance sheet.\n"
    "- Fund/ETF flows unless the corporate parent states treasury exposure.\n"
    "- Venture investments in token/equity of Web3 startups unless the company says it will hold project tokens as treasury.\n"
    "- Generic market commentary (price targets, \"exploring blockchain\") with no treasury decision.\n"
    "- Unattributed rumors, social posts, or founder/CEO personal holdings (not corporate).\n\n"
    "EDGE CASES\n"
    "- Exploring/considering: TRUE if on-the-record/IR/filing states exploring treasury allocation (board review/policy draft/RFP). If vague press speculation, FALSE.\n"
    "- Stablecoins: TRUE only if framed as treasury policy (e.g., allocate X% to USDC as reserves). Payments float only → FALSE.\n"
    "- Mining/validators: holding self-produced tokens is TRUE only if framed as treasury strategy (e.g., retain Y% as reserves). Pure inventory awaiting sale → FALSE.\n"
    "- Private companies: TRUE if disclosure is credible and clearly about corporate treasury.\n"
    "- Wallet disclosures: TRUE if disclosed as treasury holdings (not demo/test).\n\n"
    "OUTPUT\n"
    "Respond with STRICT JSON ONLY: { 'is_dat': boolean }.\n"
)


def _make_client() -> OpenAI:
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY not configured")
    return OpenAI(api_key=settings.openai_api_key)


async def analyze_docs(session: AsyncSession, limit: int = 20) -> int:
    """Analyze recent SourceDocs and create Events for DAT items."""
    client = _make_client()
    rows = (await session.execute(select(SourceDoc).order_by(SourceDoc.fetched_at.desc()).limit(limit))).scalars().all()
    inserted = 0
    for doc in rows:
        text = doc.raw_text or ""
        if not text:
            continue
        try:
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": text[:12000]},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            content = completion.choices[0].message.content
            data: dict[str, Any] = json.loads(content)
        except Exception as exc:  # pragma: no cover
            logger.warning("GPT analysis skipped doc %s: %s", doc.id, exc)
            continue

        if not data.get("is_dat"):
            continue

        company = data.get("company") or {}
        txn = data.get("transaction") or {}
        vehicle = (txn.get("vehicle") or {}) if isinstance(txn, dict) else {}

        event = Event(
            company_name=(company.get("name") or "Unknown Company") if isinstance(company, dict) else "Unknown Company",
            company_ticker=(company.get("ticker") if isinstance(company, dict) else None),
            company_exchange=(company.get("exchange") if isinstance(company, dict) else None),
            company_country=(company.get("country") if isinstance(company, dict) else None),
            action=Action((txn.get("action") or "announce_purchase")),
            tokens=(txn.get("token") if isinstance(txn, dict) else None),
            chains=(txn.get("chain") if isinstance(txn, dict) else None),
            amount_token=(txn.get("amount_token") if isinstance(txn, dict) else None),
            amount_usd=(txn.get("amount_usd") if isinstance(txn, dict) else None),
            financing_type=FinancingType(txn.get("financing_type", "unknown")),
            vehicle_type=VehicleType(vehicle.get("type", "unknown")),
            vehicle_name=(vehicle.get("name") if isinstance(vehicle, dict) else None),
            execution=Execution(txn.get("execution", "unknown")),
            announcement_date=(txn.get("announcement_date") if isinstance(txn, dict) else None),
            effective_date=(txn.get("effective_date") if isinstance(txn, dict) else None),
            source_type=SourceType.news,
            source_url=doc.url,
            sec_form=SecForm.none_,
            confidence=0,
            notes=(data.get("notes") if isinstance(data, dict) else None),
            raw_doc_id=doc.id,
        )
        session.add(event)
        inserted += 1
    await session.commit()
    return inserted


async def classify_docs(session: AsyncSession, limit: int = 20) -> list[dict[str, Any]]:
    """Binary classify recent SourceDocs as DAT or not, without creating events."""
    client = _make_client()
    rows = (await session.execute(select(SourceDoc).order_by(SourceDoc.fetched_at.desc()).limit(limit))).scalars().all()
    results: list[dict[str, Any]] = []
    for doc in rows:
        text = doc.raw_text or ""
        if not text:
            results.append({"doc_id": str(doc.id), "is_dat": False})
            continue
        try:
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
                    {"role": "user", "content": text[:12000]},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            content = completion.choices[0].message.content
            data: dict[str, Any] = json.loads(content)
            is_dat = bool(data.get("is_dat"))
        except Exception as exc:  # pragma: no cover
            logger.warning("GPT classification skipped doc %s: %s", doc.id, exc)
            is_dat = False
        results.append({"doc_id": str(doc.id), "is_dat": is_dat})
    return results


def _classify_single_file(p: Path) -> dict[str, Any]:
    """Classify a single file. Used for parallel processing.
    Creates its own OpenAI client (thread-safe per OpenAI docs).
    """
    client = _make_client()
    try:
        text = p.read_text(encoding="utf-8", errors="ignore")
    except Exception as exc:  # pragma: no cover
        logger.warning("skip file %s: %s", p, exc)
        return {"file": p.name, "is_dat": False}
    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": CLASSIFY_SYSTEM_PROMPT},
                {"role": "user", "content": text[:12000]},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        content = completion.choices[0].message.content
        data: dict[str, Any] = json.loads(content)
        is_dat = bool(data.get("is_dat"))
    except Exception as exc:  # pragma: no cover
        logger.warning("GPT classification failed for %s: %s", p.name, exc)
        is_dat = False
    return {"file": p.name, "is_dat": is_dat}


def classify_texts_from_dir(
    dir_path: Path,
    *,
    save_jsonl: bool = True,
    limit_files: int | None = None,
    workers: int | None = None,
) -> dict[str, Any]:
    """Classify .txt files under dir_path using GPT, optionally saving JSONL.

    When limit_files is provided, only the first N files (alphabetically) are processed.
    Uses parallel workers (default from config) to speed up processing.
    """
    settings = get_settings()
    num_workers = workers if workers is not None else settings.openai_classify_workers
    
    files = sorted([p for p in dir_path.glob("*.txt") if p.is_file()])
    if limit_files is not None:
        files = files[: max(0, int(limit_files))]
    
    results: list[dict[str, Any]] = []
    
    # Use ThreadPoolExecutor for parallel processing
    # OpenAI client is thread-safe, but creating per-task avoids any potential issues
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(_classify_single_file, p): p
            for p in files
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_file):
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:  # pragma: no cover
                file_path = future_to_file[future]
                logger.warning("Classification task failed for %s: %s", file_path, exc)
                results.append({"file": file_path.name, "is_dat": False})
    
    # Sort results by filename to maintain consistent ordering
    results.sort(key=lambda x: x["file"])

    saved_file: str | None = None
    if save_jsonl:
        try:
            from datetime import datetime, timezone

            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
            jf = dir_path / f"classifications_{ts}.jsonl"
            with jf.open("w", encoding="utf-8") as f:
                for r in results:
                    f.write(
                        f"{json.dumps({'file': r['file'], 'is_dat': bool(r['is_dat'])})}\n"
                    )
            saved_file = str(jf)
        except Exception as exc:  # pragma: no cover
            logger.warning("failed to write JSONL: %s", exc)

    return {
        "count": len(results),
        "positives": sum(1 for r in results if r.get("is_dat")),
        "saved_file": saved_file,
        "results": results,
    }


FORMAT_SYSTEM_PROMPT = (
    "You are a precise information extraction agent.\n"
    "Given a news article about a public company and digital assets, extract the following fields.\n"
    "If a field is not available or unclear, output the string 'N/A'.\n\n"
    "IMPORTANT: For the 'Token' field, extract the specific cryptocurrency token mentioned (e.g., BTC, Bitcoin, ETH, Ethereum, SOL, Solana, BNB, Binance Coin, etc.). "
    "Look for explicit mentions of token names, ticker symbols, or cryptocurrency names in the article. "
    "If multiple tokens are mentioned, extract the primary one. If no specific token is mentioned, output 'N/A'.\n\n"
    "Return STRICT JSON ONLY with exactly these keys: {\n"
    "  'Stock Ticker': string,\n"
    "  'Stock Name': string,\n"
    "  'Token': string (e.g., 'BTC', 'ETH', 'SOL', 'BNB', or 'N/A' if not specified),\n"
    "  'Raise Ann. Date': string,\n"
    "  'Type of Raise': string,\n"
    "  'Country(HQ)': string,\n"
    "  'Stock Exchange': string,\n"
    "  'FDV of Token': string,\n"
    "  'FDV Group': string,\n"
    "  'Raise Amount Announced': string,\n"
    "  'Locked/not': string,\n"
    "  'Implied No. Tokens': string,\n"
    "  'No. Tokens Already Held (latest known before ann.)': string,\n"
    "  'Outstanding Shares': string,\n"
    "  'Share Price on Ann. Date': string,\n"
    "  'Stock Market Cap on Ann. Date': string,\n"
    "  'Token Price on Ann. Date': string,\n"
    "  'Token Value Already Held (latest known before ann.)': string,\n"
    "  'Implied Token Value': string,\n"
    "  'Multiple (MC/Token Value) on Ann. Date': string,\n"
    "  'Implied Multiple (MC/Implied Token Value) on Ann. Date': string,\n"
    "  '-30D Stock Perf (to D-1)': string,\n"
    "  '-7D Stock Perf': string,\n"
    "  'D Stock Perf': string,\n"
    "  '1D Stock Perf': string,\n"
    "  '7D Stock Perf': string,\n"
    "  '30D Stock Perf': string,\n"
    "  '-7D Token Perf': string,\n"
    "  'D Token Perf': string,\n"
    "  '1D Token Perf': string,\n"
    "  '7D Token Perf': string,\n"
    "  'Month': string,\n"
    "  '-7 to -1D Stock Perf': string,\n"
    "  '-7 to -1D Token Perf': string,\n"
    "  'w/mNAV': string\n"
    "}\n"
    "Return ONLY JSON."
)


def format_texts_from_dir(
    dir_path: Path,
    *,
    limit_files: int | None = None,
    orig_only: bool = True,
) -> dict[str, Any]:
    """Format .orig.txt files (or .txt if allowed) into JSON with the specified schema."""
    client = _make_client()
    files = sorted([p for p in dir_path.glob("*.orig.txt")])
    if not files and not orig_only:
        files = sorted([p for p in dir_path.glob("*.txt")])
    if limit_files is not None:
        files = files[: max(0, int(limit_files))]

    saved = 0
    errors = 0
    outputs: list[str] = []
    for p in files:
        try:
            text = p.read_text(encoding="utf-8", errors="ignore")
            # Include URL if present on first line
            content = text[:12000]
            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": FORMAT_SYSTEM_PROMPT},
                    {"role": "user", "content": content},
                ],
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            data = completion.choices[0].message.content or "{}"
            # Save JSON next to file, with .json extension
            out = p.with_suffix("")
            out = out.parent / f"{out.name}.json"
            out.write_text(data, encoding="utf-8")
            outputs.append(str(out))
            saved += 1
        except Exception:
            errors += 1
            continue
    return {"saved": saved, "errors": errors, "outputs": outputs}


