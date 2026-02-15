from __future__ import annotations

import re
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


def parse_precos(precos_json: dict[str, Any], ceasas_json: dict[str, Any]) -> pd.DataFrame:
    resultset = precos_json.get("resultset", [])
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
    for i, col in enumerate(metadata):
        if i == 0:
            continue
        m = _RE_DATA_HEADER.search(col.get("colName", ""))
        datas_por_ceasa.append(datetime.strptime(m.group(1), "%d/%m/%Y") if m else None)

    records: list[dict[str, object]] = []
    for row in resultset:
        produto, unidade = parse_produto_unidade(row[0])
        categoria = PRODUTO_PARA_CATEGORIA.get(produto, "HORTALICAS")

        for col_idx in range(1, len(row)):
            preco = row[col_idx]
            if preco is None:
                continue

            ceasa_idx = col_idx - 1
            ceasa_name = (
                ceasas_list[ceasa_idx] if ceasa_idx < len(ceasas_list) else f"CEASA_{col_idx}"
            )
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

    if not records:
        return pd.DataFrame(columns=COLUNAS_SAIDA)

    return pd.DataFrame(records, columns=COLUNAS_SAIDA)


def parse_lista_produtos(json_data: dict[str, Any]) -> list[str]:
    produtos = []
    for row in json_data.get("resultset", []):
        nome = row[0]
        if isinstance(nome, str):
            produto, _ = parse_produto_unidade(nome)
            produtos.append(produto)
    return sorted(produtos)


def parse_lista_ceasas(json_data: dict[str, Any]) -> list[dict[str, str]]:
    result = []
    for row in json_data.get("resultset", []):
        name = row[1]
        uf = parse_ceasa_uf(name)
        result.append({"nome": name, "uf": uf or ""})
    return sorted(result, key=lambda x: x["nome"])
