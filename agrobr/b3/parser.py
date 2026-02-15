from __future__ import annotations

import re
from datetime import date, datetime

import pandas as pd
import structlog
from bs4 import BeautifulSoup

from agrobr.exceptions import ParseError

from .models import (
    COLUNAS_SAIDA,
    TICKERS_AGRO,
    UNIDADES,
    parse_numero_br,
    parse_vencimento,
)

logger = structlog.get_logger()

PARSER_VERSION = 1

_RE_ATUALIZADO = re.compile(r"ATUALIZADO EM:\s*(\d{2}/\d{2}/\d{4})")


def _extrair_data_referencia(html: str) -> date | None:
    m = _RE_ATUALIZADO.search(html)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%d/%m/%Y").date()


def _extrair_ticker(texto: str) -> tuple[str, str]:
    parts = texto.split("-", 1)
    ticker = parts[0].strip()
    descricao = parts[1].strip() if len(parts) > 1 else ""
    return ticker, descricao


def parse_ajustes_html(html: str) -> pd.DataFrame:
    data_ref = _extrair_data_referencia(html)
    if data_ref is None:
        logger.info("b3_sem_pregao", reason="ATUALIZADO EM ausente")
        return pd.DataFrame(columns=COLUNAS_SAIDA)

    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="tblDadosAjustes")
    if table is None:
        raise ParseError(
            source="b3",
            parser_version=PARSER_VERSION,
            reason="Tabela tblDadosAjustes nao encontrada",
        )

    rows = table.find_all("tr")  # type: ignore[union-attr]
    if len(rows) < 2:
        raise ParseError(
            source="b3",
            parser_version=PARSER_VERSION,
            reason=f"Tabela com apenas {len(rows)} rows",
        )

    records: list[dict[str, object]] = []
    current_ticker = ""
    current_desc = ""
    in_agro = False

    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < 6:
            continue

        col1 = cells[0].get_text(strip=True)
        if col1:
            current_ticker, current_desc = _extrair_ticker(col1)
            in_agro = current_ticker in TICKERS_AGRO

        if not in_agro:
            continue

        vct_raw = cells[1].get_text(strip=True)
        if not vct_raw:
            continue

        try:
            vct_ano, vct_mes = parse_vencimento(vct_raw)
        except (KeyError, ValueError, IndexError):
            continue

        records.append(
            {
                "data": data_ref,
                "ticker": current_ticker,
                "descricao": current_desc,
                "vencimento_codigo": vct_raw.strip(),
                "vencimento_mes": vct_mes,
                "vencimento_ano": vct_ano,
                "ajuste_anterior": parse_numero_br(cells[2].get_text(strip=True)),
                "ajuste_atual": parse_numero_br(cells[3].get_text(strip=True)),
                "variacao": parse_numero_br(cells[4].get_text(strip=True)),
                "ajuste_por_contrato": parse_numero_br(cells[5].get_text(strip=True)),
                "unidade": UNIDADES.get(current_ticker, ""),
            }
        )

    df = pd.DataFrame(records, columns=COLUNAS_SAIDA)

    if not df.empty:
        df["data"] = pd.to_datetime(df["data"])
        for col in ["ajuste_anterior", "ajuste_atual", "variacao", "ajuste_por_contrato"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("b3_parse_ok", records=len(df), data_ref=str(data_ref))
    return df
