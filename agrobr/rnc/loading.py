from __future__ import annotations

import time
from dataclasses import dataclass

from agrobr import _log, contracts
from agrobr.exceptions import ContractViolationError, ParseError

from . import acquisition, client, parser, snapshot

logger = _log.get_logger(__name__)


@dataclass(frozen=True)
class RncTable:
    acquisition: acquisition.CSVAcquisition
    parsed: parser.RncParsedTable
    from_cache: bool
    fetch_ms: int
    parse_ms: int
    cache_status: str


def _parse(captured: acquisition.CSVAcquisition) -> parser.RncParsedTable:
    parse = (
        parser.parse_registradas_bundle
        if captured.kind == "registradas"
        else parser.parse_protegidas_bundle
    )
    parsed = parse(captured.content)
    expected = captured.search.reported_total
    if expected is not None and expected != len(parsed.frame):
        raise ParseError(
            source="rnc",
            parser_version=parser.PARSER_VERSION,
            reason=(
                f"Total da pesquisa {expected} incompatível com as {len(parsed.frame)} "
                f"linhas do CSV de {captured.kind}"
            ),
        )
    contracts.validate_dataset(parsed.frame, f"rnc_{captured.kind}")
    return parsed


def _cached(kind: str) -> RncTable | None:
    captured = snapshot.read_acquisition(kind)
    if captured is None:
        return None
    started = time.monotonic()
    try:
        parsed = _parse(captured)
    except (ParseError, ContractViolationError) as error:
        logger.warning("rnc_cached_population_invalid", kind=kind, error=type(error).__name__)
        return None
    return RncTable(
        acquisition=captured,
        parsed=parsed,
        from_cache=True,
        fetch_ms=0,
        parse_ms=int((time.monotonic() - started) * 1000),
        cache_status="hit",
    )


def _store(captured: acquisition.CSVAcquisition, use_cache: bool) -> str:
    if not use_cache:
        return "bypassed"
    try:
        snapshot.write_acquisition(captured)
    except OSError as error:
        logger.warning("rnc_cache_write_failed", kind=captured.kind, error=type(error).__name__)
        return "write_failed"
    return "stored"


async def load(kind: acquisition.Family, use_cache: bool) -> RncTable:
    async with snapshot.acquisition_lock(kind):
        if use_cache:
            cached = _cached(kind)
            if cached is not None:
                return cached
        started = time.monotonic()
        fetch = (
            client.fetch_registradas_bundle
            if kind == "registradas"
            else client.fetch_protegidas_bundle
        )
        captured = await fetch()
        fetch_ms = int((time.monotonic() - started) * 1000)
        if captured.kind != kind:
            raise ParseError(
                source="rnc",
                parser_version=parser.PARSER_VERSION,
                reason="Família da aquisição diferente da consulta RNC/SNPC",
            )
        started = time.monotonic()
        parsed = _parse(captured)
        parse_ms = int((time.monotonic() - started) * 1000)
        return RncTable(
            acquisition=captured,
            parsed=parsed,
            from_cache=False,
            fetch_ms=fetch_ms,
            parse_ms=parse_ms,
            cache_status=_store(captured, use_cache),
        )
