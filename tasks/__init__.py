"""
Crypto Treasury Parser Tasks

Task wrapper classes that integrate with the pipeline framework.
Self-contained module with S3 storage - no dependencies on app/.
"""

from .config import TreasurySettings, get_settings
from .prompts import CLASSIFY_SYSTEM_PROMPT, FORMAT_SYSTEM_PROMPT
from .treasury_storage import TreasuryS3Storage
from .ingest_task import IngestCryptoPanicTask
from .classify_task import ClassifyTask
from .format_task import FormatTask
from .enrich_task import EnrichTask
from .dedupe_task import DedupeTask
from .export_task import ExportCSVTask

__all__ = [
    # Configuration
    "TreasurySettings",
    "get_settings",
    # Prompts
    "CLASSIFY_SYSTEM_PROMPT",
    "FORMAT_SYSTEM_PROMPT",
    # Storage
    "TreasuryS3Storage",
    # Tasks
    "IngestCryptoPanicTask",
    "ClassifyTask",
    "FormatTask",
    "EnrichTask",
    "DedupeTask",
    "ExportCSVTask",
]
