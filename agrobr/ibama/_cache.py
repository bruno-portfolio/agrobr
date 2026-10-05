from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import NamedTuple
from weakref import WeakKeyDictionary

from agrobr import _log
from agrobr.constants import CacheSettings
from agrobr.utils import atomic, tasks
from agrobr.utils.warnings import warn_once

from . import client
from .models import CACHE_TTL

logger = _log.get_logger(__name__)

_LOCKS: WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = WeakKeyDictionary()


class Coleta(NamedTuple):
    conteudo: bytes
    url: str
    fetched_at: datetime
    from_cache: bool


def _arquivos() -> tuple[Path, Path]:
    pasta = CacheSettings().cache_dir / "ibama"
    return pasta / "termo_embargo.csv", pasta / "termo_embargo.json"


def _ler(agora: datetime) -> Coleta | None:
    csv_path, manifesto_path = _arquivos()
    try:
        manifesto = json.loads(manifesto_path.read_text(encoding="utf-8"))
        fetched_at = datetime.fromisoformat(manifesto["fetched_at"])
        if not timedelta(0) <= agora - fetched_at < CACHE_TTL:
            return None
        conteudo = csv_path.read_bytes()
    except (OSError, ValueError, KeyError, TypeError):
        return None
    if hashlib.sha256(conteudo).hexdigest() != manifesto.get("sha256"):
        logger.warning("ibama_cache_corrompido", path=str(csv_path))
        return None
    return Coleta(conteudo, str(manifesto.get("source_url", "")), fetched_at, True)


def _gravar(coleta: Coleta) -> None:
    csv_path, manifesto_path = _arquivos()
    manifesto = {
        "source_url": coleta.url,
        "fetched_at": coleta.fetched_at.isoformat(),
        "sha256": hashlib.sha256(coleta.conteudo).hexdigest(),
        "size_bytes": len(coleta.conteudo),
    }
    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with atomic.atomic_output(csv_path) as tmp:
            tmp.write_bytes(coleta.conteudo)
        with atomic.atomic_output(manifesto_path) as tmp:
            tmp.write_text(json.dumps(manifesto, ensure_ascii=False), encoding="utf-8")
        warn_once(
            "ibama_cache_pii",
            f"IBAMA: o cache de 1 hora em {csv_path.parent} guarda o CSV inteiro da fonte, com nome e "
            "CPF/CNPJ dos embargados; use_cache=False não grava, e apagar a pasta remove o arquivo.",
        )
    except OSError as exc:
        logger.warning("ibama_cache_gravacao_falhou", path=str(csv_path), error=str(exc))


async def _baixar() -> Coleta:
    conteudo, url = await client.fetch_embargos_csv()
    return Coleta(conteudo, url, datetime.now(UTC), False)


async def obter_embargos_csv(*, use_cache: bool = True) -> Coleta:
    """CSV de embargos, do cache em disco enquanto a coleta tiver menos de `CACHE_TTL`.

    Com `use_cache=False`, baixa de novo e não lê nem grava o cache. Chamadas simultâneas no mesmo
    loop esperam a primeira em vez de baixar o arquivo várias vezes.
    """
    if not use_cache:
        return await _baixar()
    async with _LOCKS.setdefault(asyncio.get_running_loop(), asyncio.Lock()):
        coleta = await tasks.to_thread_ate_o_fim(_ler, datetime.now(UTC))
        if coleta is None:
            coleta = await _baixar()
            await tasks.to_thread_ate_o_fim(_gravar, coleta)
        return coleta
