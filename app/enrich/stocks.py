from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import json
import yfinance as yf
from dateutil import parser as dateparser


def _parse_date(date_str: str | None) -> datetime | None:
	if not date_str or date_str.strip().upper() == "N/A":
		return None
	# Common explicit formats
	for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%y", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
		try:
			dt = datetime.strptime(date_str.strip(), fmt)
			# If naive, assume UTC
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=timezone.utc)
			return dt
		except Exception:
			continue
	# Fallback: dateutil, try dayfirst first, then yearfirst
	try:
		dt = dateparser.parse(date_str.strip(), dayfirst=True, yearfirst=False)
		if dt is not None:
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=timezone.utc)
			return dt
	except Exception:
		pass
	try:
		dt = dateparser.parse(date_str.strip(), dayfirst=False, yearfirst=True)
		if dt is not None:
			if dt.tzinfo is None:
				dt = dt.replace(tzinfo=timezone.utc)
			return dt
	except Exception:
		pass
	return None


def _nearest_close(df, target: datetime) -> float | None:
	if df is None or df.empty:
		return None
	# Normalize to date index (yfinance uses tz-aware DatetimeIndex)
	idx = df.index
	# Find first index >= target
	for ts in idx:
		if ts >= target:
			try:
				return float(df.loc[ts]["Close"])
			except Exception:
				continue
	# Fallback to last available close if target is beyond data
	try:
		return float(df.iloc[-1]["Close"])
	except Exception:
		return None


def _pct(a: float | None, b: float | None) -> str:
	if a is None or b is None or a == 0:
		return "N/A"
	return f"{((b - a) / a) * 100:.2f}%"


def _clean_ticker(t: str | None) -> str | None:
	if not t:
		return None
	s = t.strip().upper()
	if s.startswith("$"):
		s = s[1:]
	return s or None


def _token_to_yahoo(symbol: str | None) -> str | None:
	if not symbol:
		return None
	s = symbol.strip().upper()
	mapping = {
		"BTC": "BTC-USD",
		"ETH": "ETH-USD",
		"SOL": "SOL-USD",
		"BNB": "BNB-USD",
		"XRP": "XRP-USD",
		"TON": "TON-USD",
		"AVAX": "AVAX-USD",
		"DOGE": "DOGE-USD",
		"ADA": "ADA-USD",
		"TRX": "TRX-USD",
		"SUI": "SUI-USD",
		"FET": "FET-USD",
		"TAO": "TAO-USD",
		"BONK": "BONK-USD",
	}
	return mapping.get(s)


def enrich_folder_with_yfinance(dir_path: Path, *, as_of: datetime | None = None, limit_files: int | None = None) -> dict[str, Any]:
	"""Read *.json fact cards and fill share price and stock perf metrics using yfinance.
	Updates JSON files in place (no separate .stock.json output).
	"""
	files = sorted([p for p in dir_path.glob("*.json") if p.is_file()])
	if limit_files is not None:
		files = files[: max(0, int(limit_files))]
	as_of = as_of or datetime.now(timezone.utc)

	saved = 0
	skipped = 0
	outputs: list[str] = []

	for p in files:
		try:
			data = json.loads(p.read_text(encoding="utf-8"))
		except Exception:
			skipped += 1
			continue

		ticker = _clean_ticker(data.get("Stock Ticker"))
		ann_date = _parse_date(data.get("Raise Ann. Date"))
		# Proceed even if stock ticker missing; we can still fill token metrics

		# STOCK enrichment
		if ticker and ann_date:
			try:
				tkr = yf.Ticker(ticker)
				hist = tkr.history(start=(ann_date - timedelta(days=2)).date(), end=(as_of + timedelta(days=1)).date(), auto_adjust=False)
			except Exception:
				hist = None

			price_d = _nearest_close(hist, ann_date)
			price_d1 = _nearest_close(hist, ann_date + timedelta(days=1))
			price_d7 = _nearest_close(hist, ann_date + timedelta(days=7))
			price_d30 = _nearest_close(hist, ann_date + timedelta(days=30))

			# Overwrite when empty or 'N/A'
			def _set(k: str, v: str):
				cur = data.get(k)
				if cur in (None, "", "N/A"):
					data[k] = v

			_set("Share Price on Ann. Date", f"{price_d:.2f}" if price_d is not None else "N/A")
			_set("1D Stock Perf", _pct(price_d, price_d1))
			_set("7D Stock Perf", _pct(price_d, price_d7))
			_set("30D Stock Perf", _pct(price_d, price_d30))

		# TOKEN enrichment
		token_symbol = (data.get("Token") or "").strip()
		yahoo_token = _token_to_yahoo(token_symbol)
		if yahoo_token and ann_date:
			tkr = yf.Ticker(ticker)
			try:
				ttkr = yf.Ticker(yahoo_token)
				thist = ttkr.history(start=(ann_date - timedelta(days=2)).date(), end=(as_of + timedelta(days=1)).date(), auto_adjust=False)
			except Exception:
				thist = None

			tp_d = _nearest_close(thist, ann_date)
			tp_d1 = _nearest_close(thist, ann_date + timedelta(days=1))
			tp_d7 = _nearest_close(thist, ann_date + timedelta(days=7))
			# Overwrite when empty or 'N/A'
			def _set_t(k: str, v: str):
				cur = data.get(k)
				if cur in (None, "", "N/A"):
					data[k] = v

			_set_t("Token Price on Ann. Date", f"{tp_d:.2f}" if tp_d is not None else "N/A")
			_set_t("1D Token Perf", _pct(tp_d, tp_d1))
			_set_t("7D Token Perf", _pct(tp_d, tp_d7))

		# Write back in place
		p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
		outputs.append(str(p))
		saved += 1

	return {"saved": saved, "skipped": skipped, "outputs": outputs}


