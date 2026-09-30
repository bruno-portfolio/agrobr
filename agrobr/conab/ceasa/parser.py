from __future__ import annotations

import re
import warnings
from datetime import datetime
from typing import Any

import pandas as pd

from agrobr.exceptions import ParseError

from .models import (
    COLUNAS_SAIDA,
    PRODUTO_PARA_CATEGORIA,
    parse_ceasa_uf,
    parse_produto_unidade,
)

PARSER_VERSION = 1

_RE_DATA_HEADER = re.compile(r"\((\d{2}/\d{2}/\d{4})\)")
_RE_SUFIXO_DATA = re.compile(r"\s*\(\d{2}/\d{2}/\d{4}\).*$")


def _ceasa_do_cabecalho(col_name: str) -> str:
    nome = _RE_SUFIXO_DATA.sub("", col_name).replace("\r", " - ")
    return re.sub(r"\s+", " ", nome).strip()


def _validar_identidade_ceasas(ceasas_por_coluna: list[str], ceasas_list: list[str]) -> None:
    catalogo = set(ceasas_list)
    ausentes = [nome for nome in ceasas_por_coluna if nome not in catalogo]
    duplicadas = {nome for nome in ceasas_por_coluna if ceasas_por_coluna.count(nome) > 1}
    if not ceasas_por_coluna or ausentes or duplicadas:
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason=(
                "Cabeçalhos de preço não identificam CEASAs do catálogo: "
                f"ausentes={sorted(ausentes)[:5]} duplicadas={sorted(duplicadas)[:5]}"
            ),
        )


def parse_precos(precos_json: dict[str, Any], ceasas_json: dict[str, Any]) -> pd.DataFrame:
    resultset = precos_json.get("resultset") if isinstance(precos_json, dict) else None
    if not isinstance(resultset, list):
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason="Resposta de preços sem a lista 'resultset'",
        )
    if not resultset:
        return pd.DataFrame(columns=COLUNAS_SAIDA)

    ceasas_list = [row[1] for row in ceasas_json.get("resultset", [])]
    if not ceasas_list:
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason="Lista de CEASAs vazia",
        )

    metadata = precos_json.get("metadata", [])
    datas_por_ceasa: list[datetime | None] = []
    ceasas_por_coluna: list[str] = []
    for i, col in enumerate(metadata):
        if i == 0:
            continue
        col_name = col.get("colName", "")
        m = _RE_DATA_HEADER.search(col_name)
        datas_por_ceasa.append(datetime.strptime(m.group(1), "%d/%m/%Y") if m else None)
        ceasas_por_coluna.append(_ceasa_do_cabecalho(col_name))
    _validar_identidade_ceasas(ceasas_por_coluna, ceasas_list)

    records: list[dict[str, object]] = []
    fora_da_tabela: set[str] = set()
    for row in resultset:
        produto, unidade = parse_produto_unidade(row[0])
        categoria = PRODUTO_PARA_CATEGORIA.get(produto)
        if categoria is None:
            fora_da_tabela.add(produto)

        for col_idx in range(1, len(row)):
            preco = row[col_idx]
            if preco is None:
                continue

            ceasa_idx = col_idx - 1
            ceasa_name = ceasas_por_coluna[ceasa_idx]
            ceasa_uf = parse_ceasa_uf(ceasa_name) or ""
            data = datas_por_ceasa[ceasa_idx] if ceasa_idx < len(datas_por_ceasa) else None

            records.append(
                {
                    "data": pd.Timestamp(data) if data else pd.NaT,
                    "produto": produto,
                    "categoria": categoria,
                    "unidade": unidade,
                    "ceasa": ceasa_name,
                    "ceasa_uf": ceasa_uf,
                    "preco": float(preco),
                }
            )

    if fora_da_tabela:
        warnings.warn(
            "conab_ceasa: produtos fora da tabela de categorias do agrobr saem com categoria nula: "
            f"{sorted(fora_da_tabela)}",
            UserWarning,
            stacklevel=2,
        )

    return pd.DataFrame(records, columns=COLUNAS_SAIDA)
