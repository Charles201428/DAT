from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DATE_KEYS = ("Raise Ann. Date",)
STOCK_KEYS = ("Stock Ticker",)
TOKEN_KEYS = ("Token",)


def _normalize_symbol(val: Optional[str]) -> Optional[str]:
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    # Many files store ticker as "MSTR" or "$MSTR"
    if s.startswith("$"):
        s = s[1:]
    return s.upper()


def _parse_date(value: Optional[str]) -> Optional[str]:
    """Return date as YYYY-MM-DD string or None."""
    if not value:
        return None
    s = str(value).strip()
    if not s or s.upper() == "N/A":
        return None
    # try common formats
    fmts = ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y", "%d %b %Y", "%d-%b-%y", "%Y-%m-%dT%H:%M:%SZ")
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue
    # last resort: naive split by space or T
    try:
        return s.split("T")[0]
    except Exception:
        return None


def _extract_key(triple: Tuple[str, str, str]) -> Optional[Tuple[str, str, str]]:
    stock, token, date = triple
    if not stock or not token or not date:
        return None
    return (stock, token, date)

@dataclass
class FileInfo:
    path: Path
    size: int
    mtime: float
    stem: str
    key: Optional[Tuple[str, str, str]]
    fields: Dict[str, Any]


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        text = path.read_text(encoding="utf-8")
        return json.loads(text)
    except Exception:
        return None


def _collect_json_files(root: Path) -> List[Path]:
    return [p for p in sorted(root.iterdir()) if p.is_file() and p.suffix.lower() == ".json"]


def _group_by_key(files: Iterable[Path], require_all: bool = True) -> Dict[Tuple[str, str, str], List[FileInfo]]:
    groups: Dict[Tuple[str, str, str], List[FileInfo]] = {}
    for p in files:
        data = _load_json(p)
        if data is None or not isinstance(data, dict):
            continue
        stock_raw = None
        token_raw = None
        date_raw = None
        for k in STOCK_KEYS:
            if k in data:
                stock_raw = data.get(k)
                break
        for k in TOKEN_KEYS:
            if k in data:
                token_raw = data.get(k)
                break
        for k in DATE_KEYS:
            if k in data:
                date_raw = data.get(k)
                break
        stock = _normalize_symbol(stock_raw)
        token = _normalize_symbol(token_raw)
        date = _parse_date(str(date_raw) if date_raw is not None else None)
        key = _extract_key(((stock or ""), (token or ""), (date or "")))
        if key is None:
            if require_all:
                continue
            else:
                continue
        info = FileInfo(
            path=p,
            size=p.stat().st_size,
            mtime=p.stat().st_mtime,
            stem=p.stem,
            key=key,
            fields=data,
        )
        groups.setdefault(key, []).append(info)  # type: ignore
    return groups


def _score_filled_fields(data: Dict[str, Any]) -> int:
    """Higher is better: count non-empty, non-'N/A' fields."""
    score = 0
    for v in data.values():
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        if isinstance(v, str) and v.strip().upper() == "N/A":
            continue
        score += 1
    return score


def _pick_winner(entries: List[FileInfo], strategy: str) -> FileInfo:
    if not entries:
        raise ValueError("No entries to pick from")
    if strategy == "largest":
        return max(entries, key=lambda e: (e.size, e.mtime))
    if strategy == "newest":
        return max(entries, key=lambda e: (e.mtime, e.size))
    if strategy == "most_filled":
        return max(entries, key=lambda e: (_score_filled_fields(e.fields), e.size, e.mtime))
    # default fallback: first by path
    return sorted(entries, key=lambda e: (e.path.name, ))[0]


def _related_candidates(p: Path) -> List[Path]:
    """Find sibling files that share the same stem prefix (e.g., '1234' or '1234.orig')."""
    parent = p.parent
    stem = p.stem  # e.g., "1234" or "1234.orig"
    base = stem
    # If stem ends with .orig, consider both '1234.orig' and '1234'
    bases = {base}
    if "." in base:
        bases.add(base.split(".", 1)[0])
    out: List[Path] = []
    for q in parent.iterdir():
        if not q.is_file():
            continue
        qstem = q.stem
        if q == p:
            continue
        if qstem == base or qstem in bases or any(qstem.startswith(b + ".") for b in bases):
            out.append(q)
    return out


def dedupe_folder(
    folder: Path,
    *,
    keep: str = "largest",
    require_all: bool = True,
    remove_duplicates: bool = False,
    include_related: bool = True,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """
    Group JSON files in `folder` by (Stock Ticker, Token, Raise Ann. Date) and keep only one per group.
    - keep: 'largest' | 'newest' | 'most_filled' | 'first'
    - require_all: if True, only dedupe when all three key fields are present and non-empty
    - remove_duplicates: if True, delete duplicates; otherwise move them to a _dedup_trash/ subdir
    - include_related: also move/delete sibling files sharing the same base stem (e.g., .orig.txt)
    - dry_run: if True, compute plan only without modifying files
    """
    folder = Path(folder)
    if not folder.exists() or not folder.is_dir():
        raise FileNotFoundError(f"Folder not found: {folder}")

    files = _collect_json_files(folder)
    groups = _group_by_key(files, require_all=require_all)

    # Prepare trash directory if needed
    trash_dir = folder / "_dedup_trash"
    to_remove: List[Path] = []
    kept: List[Path] = []
    groups_processed = 0

    for key, entries in groups.items():
        if len(entries) <= 1:
            continue
        groups_processed += 1
        winner = _pick_winner(entries, keep)
        kept.append(winner.path)
        for e in entries:
            if e.path == winner.path:
                continue
            to_remove.append(e.path)
            if include_related:
                to_remove.extend([q for q in _related_candidates(e.path) if q.exists()])

    executed = []
    if not dry_run and to_remove:
        # make unique while preserving order
        seen = set()
        deduped_remove = []
        for p in to_remove:
            if str(p) in seen:
                continue
            seen.add(str(p))
            deduped_remove.append(p)
        to_remove = deduped_remove
        if not remove_duplicates:
            trash_dir.mkdir(exist_ok=True, parents=True)
        # Move or delete
        for p in to_remove:
            if not p.exists():
                continue
            if remove_duplicates:
                p.unlink(missing_ok=True)
                executed.append({"action": "deleted", "path": str(p)})
            else:
                dest = trash_dir / p.name
                # avoid overwrite
                i = 1
                final = dest
                while final.exists():
                    final = dest.with_name(f"{dest.stem}__{i}{dest.suffix}")
                    i += 1
                shutil.move(str(p), str(final))
                executed.append({"action": "moved", "from": str(p), "to": str(final)})

    return {
        "folder": str(folder),
        "groups_considered": len(groups),
        "groups_deduped": groups_processed,
        "kept_count": len(kept),
        "kept": [str(p) for p in kept],
        "duplicate_count": len(to_remove),
        "duplicate_actions": executed if not dry_run else [str(p) for p in to_remove],
        "strategy": keep,
        "require_all": require_all,
        "remove_duplicates": remove_duplicates,
        "dry_run": dry_run,
        "include_related": include_related,
    }


