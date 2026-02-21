from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import structlog

from ..constants import Fonte

logger = structlog.get_logger()


class HistoryManager:
    def __init__(self, store: Any = None):
        self._store = store

    @property
    def store(self) -> Any:
        if self._store is None:
            from .duckdb_store import get_store

            self._store = get_store()
        return self._store

    def save(
        self,
        key: str,
        data: bytes,
        source: Fonte,
        data_date: date,
        parser_version: int,
        fingerprint_hash: str | None = None,
    ) -> bool:
        try:
            self.store.history_save(
                key=key,
                data=data,
                source=source,
                data_date=datetime.combine(data_date, datetime.min.time()),
                parser_version=parser_version,
                fingerprint_hash=fingerprint_hash,
            )
            logger.debug(
                "history_saved",
                key=key,
                data_date=str(data_date),
                parser_version=parser_version,
            )
            return True
        except Exception as e:
            logger.debug("history_save_skipped", key=key, reason=str(e))
            return False

    def get(
        self,
        key: str,
        data_date: date | None = None,
    ) -> bytes | None:
        dt = datetime.combine(data_date, datetime.min.time()) if data_date else None
        result: bytes | None = self.store.history_get(key, dt)
        return result

    def get_latest(self, key: str) -> bytes | None:
        return self.get(key, None)

    def query(
        self,
        source: Fonte | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        key_prefix: str | None = None,
    ) -> list[dict[str, Any]]:
        start_dt = datetime.combine(start_date, datetime.min.time()) if start_date else None
        end_dt = datetime.combine(end_date, datetime.max.time()) if end_date else None

        entries: list[dict[str, Any]] = self.store.history_query(
            source=source,
            start_date=start_dt,
            end_date=end_dt,
        )

        if key_prefix:
            entries = [e for e in entries if e["key"].startswith(key_prefix)]

        return entries

    def get_dates(
        self,
        key: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> list[date]:
        entries = self.query(key_prefix=key, start_date=start_date, end_date=end_date)
        dates = set()

        for entry in entries:
            if entry["key"] == key:
                data_date = entry.get("data_date")
                if data_date:
                    if isinstance(data_date, datetime):
                        dates.add(data_date.date())
                    else:
                        dates.add(data_date)

        return sorted(dates)

    def find_gaps(
        self,
        key: str,
        start_date: date,
        end_date: date,
    ) -> list[date]:
        available = set(self.get_dates(key, start_date, end_date))

        all_dates = []
        current = start_date
        while current <= end_date:
            if current.weekday() < 5:
                all_dates.append(current)
            current = current + __import__("datetime").timedelta(days=1)

        return [d for d in all_dates if d not in available]

    def count(
        self,
        source: Fonte | None = None,
        key_prefix: str | None = None,
    ) -> int:
        entries = self.query(source=source, key_prefix=key_prefix)
        return len(entries)

    def export(
        self,
        path: str | Path,
        source: Fonte | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        format: str = "parquet",
    ) -> int:
        import pandas as pd

        entries = self.query(source=source, start_date=start_date, end_date=end_date)

        if not entries:
            return 0

        df = pd.DataFrame(entries)

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if format == "parquet":
            df.to_parquet(path, index=False)
        elif format == "csv":
            df.to_csv(path, index=False)
        elif format == "json":
            df.to_json(path, orient="records", date_format="iso")
        else:
            raise ValueError(f"Formato não suportado: {format}")

        logger.info("history_exported", path=str(path), count=len(df), format=format)
        return len(df)

    def cleanup(
        self,
        older_than_days: int | None = None,
        source: Fonte | None = None,
    ) -> int:
        if older_than_days is None:
            logger.warning("history_cleanup_skipped", reason="no_age_specified")
            return 0

        logger.warning(
            "history_cleanup_starting",
            older_than_days=older_than_days,
            source=source.value if source else "all",
        )

        return 0


_history_manager: HistoryManager | None = None


def get_history_manager() -> HistoryManager:
    global _history_manager
    if _history_manager is None:
        _history_manager = HistoryManager()
    return _history_manager
