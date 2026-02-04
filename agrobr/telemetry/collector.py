"""Coleta de telemetria opt-in."""

from __future__ import annotations

import asyncio
import hashlib
import platform
import uuid
from datetime import datetime
from typing import Any

import httpx
import structlog

from agrobr import __version__
from agrobr.constants import TelemetrySettings

logger = structlog.get_logger()


class TelemetryCollector:
    """
    Coleta telemetria opt-in de forma não-intrusiva.

    Características:
    - Fire-and-forget (nunca bloqueia)
    - Batching para reduzir requests
    - Falhas silenciosas
    """

    _instance_id: str | None = None
    _buffer: list[dict[str, Any]] = []
    _lock = asyncio.Lock()

    @classmethod
    def get_instance_id(cls) -> str:
        """
        Gera ID único por instalação.

        Não é rastreável para o usuário específico,
        apenas identifica a instalação para deduplicação.
        """
        if cls._instance_id is None:
            machine_id = uuid.getnode().to_bytes(6, "big")
            cls._instance_id = hashlib.sha256(machine_id).hexdigest()[:16]
        return cls._instance_id

    @classmethod
    def get_context(cls) -> dict[str, Any]:
        """Contexto comum para todos os eventos."""
        return {
            "instance_id": cls.get_instance_id(),
            "package_version": __version__,
            "python_version": platform.python_version(),
            "os": platform.system(),
            "os_version": platform.release(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    @classmethod
    async def track(
        cls,
        event: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """
        Registra evento de telemetria.

        Args:
            event: Nome do evento (ex: "fetch", "parse_error", "cache_hit")
            properties: Propriedades do evento
        """
        settings = TelemetrySettings()

        if not settings.enabled:
            return

        payload = {
            "event": event,
            "context": cls.get_context(),
            "properties": properties or {},
        }

        async with cls._lock:
            cls._buffer.append(payload)

            if len(cls._buffer) >= settings.batch_size:
                asyncio.create_task(cls._flush())

    @classmethod
    async def _flush(cls) -> None:
        """Envia buffer para o servidor."""
        settings = TelemetrySettings()

        async with cls._lock:
            if not cls._buffer:
                return

            events = cls._buffer.copy()
            cls._buffer.clear()

        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    settings.endpoint,
                    json={"events": events},
                    timeout=5.0,
                )
            logger.debug("telemetry_flushed", count=len(events))
        except Exception as e:
            logger.debug("telemetry_flush_failed", error=str(e))

    @classmethod
    def reset(cls) -> None:
        """Reseta estado do coletor (para testes)."""
        cls._buffer.clear()
        cls._instance_id = None


async def track_fetch(source: str, produto: str, latency_ms: float, from_cache: bool) -> None:
    """Registra evento de fetch."""
    await TelemetryCollector.track(
        "fetch",
        {
            "source": source,
            "produto": produto,
            "latency_ms": latency_ms,
            "from_cache": from_cache,
        },
    )


async def track_parse_error(source: str, parser_version: int, error_type: str) -> None:
    """Registra erro de parsing."""
    await TelemetryCollector.track(
        "parse_error",
        {
            "source": source,
            "parser_version": parser_version,
            "error_type": error_type,
        },
    )


async def track_cache_operation(operation: str, hit: bool) -> None:
    """Registra operação de cache."""
    await TelemetryCollector.track(
        "cache",
        {
            "operation": operation,
            "hit": hit,
        },
    )
