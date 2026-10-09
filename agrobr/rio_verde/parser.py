from __future__ import annotations

import io
from typing import Any

import pandas as pd
from pydantic import ValidationError

from agrobr import _log
from agrobr.exceptions import ParseError
from agrobr.normalize.regions import remover_acentos

from . import models

logger = _log.get_logger(__name__)

PARSER_VERSION = 2


def _check_pdfplumber() -> Any:
    try:
        import pdfplumber

        return pdfplumber
    except ImportError:
        raise ImportError(
            "pdfplumber é necessário para rio_verde. Instale com: pip install agrobr[pdf]"
        ) from None


def _summary_width(table: list[list[str | None]]) -> int | None:
    header = remover_acentos(" ".join(cell or "" for cell in table[0])).lower()
    if not all(
        label in header for label in ("empresa", "cultivar", "g.m.", "ciclo", "produtividade")
    ):
        return None
    return 10 if "estimado" in header else 9


def _parse_summary_table(table: list[list[str | None]], safra: str) -> list[dict[str, Any]]:
    width = _summary_width(table)
    if width is None:
        return []
    records = []
    for row in table[3:]:
        cells = [" ".join(cell.split()) for cell in row if cell and cell.strip()]
        if not cells:
            continue
        try:
            if len(cells) != width:
                raise ValueError(f"esperadas {width} células, recebidas {len(cells)}")
            yields = [
                None if value == "-" else float(value.replace(",", ".")) for value in cells[-5:]
            ]
            average = yields[4]
            if average is None:
                raise ValueError("produtividade média ausente")
            record = models.EnsaioSoja(
                safra=safra,
                empresa=cells[0],
                cultivar=cells[1],
                grupo_maturacao=cells[2],
                ciclo_dias=int(cells[-6]),
                produtividade_1_epoca_sc_ha=yields[0],
                produtividade_2_epoca_sc_ha=yields[1],
                produtividade_3_epoca_sc_ha=yields[2],
                produtividade_4_epoca_sc_ha=yields[3],
                produtividade_media_sc_ha=average,
            )
        except (ValueError, ValidationError) as exc:
            raise ParseError(
                source="rio_verde",
                parser_version=PARSER_VERSION,
                reason=f"Linha inválida na tabela de cultivares ({safra}): {cells}: {exc}",
            ) from exc
        records.append(record.model_dump())
    return records


def _records_to_df(records: list[dict[str, Any]], safra: str) -> pd.DataFrame:
    if not records:
        raise ParseError(
            source="rio_verde",
            parser_version=PARSER_VERSION,
            reason=f"Nenhum registro extraído da tabela de cultivares (safra {safra})",
        )
    df = pd.DataFrame(records, columns=models.COLUNAS_SAIDA).astype({"ciclo_dias": "Int64"})
    logger.info("rio_verde_parse_ok", safra=safra, records=len(df))
    return df


def parse_ensaio_soja(data: bytes, safra: str) -> pd.DataFrame:
    pdfplumber = _check_pdfplumber()
    try:
        pdf = pdfplumber.open(io.BytesIO(data))
    except Exception as exc:
        raise ParseError(
            source="rio_verde",
            parser_version=PARSER_VERSION,
            reason=f"Falha ao abrir PDF: {exc}",
        ) from exc
    records: list[dict[str, Any]] = []
    try:
        for page in pdf.pages:
            text = remover_acentos(page.extract_text() or "").lower()
            if records and "resultados" in text:
                break
            if not all(label in text for label in ("empresa", "cultivar", "ciclo", "media")):
                continue
            for table in page.extract_tables():
                records.extend(_parse_summary_table(table, safra))
    finally:
        pdf.close()
    return _records_to_df(records, safra)
