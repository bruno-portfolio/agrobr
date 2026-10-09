from __future__ import annotations

import re
import warnings
from datetime import datetime
from typing import Any

import pandas as pd

from agrobr import contracts
from agrobr.exceptions import ParseError

from .models import (
    COLUNAS_SAIDA,
    PRODUTO_PARA_CATEGORIA,
    parse_ceasa_uf,
    parse_produto_unidade,
)

PARSER_VERSION = 1

_RE_CABECALHO_PRECO = re.compile(
    r"(?P<instituicao>[^\r\n]+)\r(?P<cidade>[^\r\n]+)\r"
    r"\((?P<data>\d{2}/\d{2}/\d{4})\)/Preco \(R\$\)"
)


def _ceasa_do_cabecalho(col_name: str) -> tuple[str, datetime]:
    m = _RE_CABECALHO_PRECO.fullmatch(col_name) if isinstance(col_name, str) else None
    if m is None or not m.group("instituicao").strip() or not m.group("cidade").strip():
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason=f"Cabeçalho de preço fora do formato publicado: {col_name!r}",
        )
    try:
        data = datetime.strptime(m.group("data"), "%d/%m/%Y")
    except ValueError as exc:
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason=f"Cabeçalho de preço com data inválida: {col_name!r}",
        ) from exc
    nome = f"{m.group('instituicao').strip()} - {m.group('cidade').strip()}"
    return re.sub(r"\s+", " ", nome), data


def _validar_identidade_ceasas(ceasas_por_coluna: list[str]) -> None:
    duplicadas = {nome for nome in ceasas_por_coluna if ceasas_por_coluna.count(nome) > 1}
    if not ceasas_por_coluna or duplicadas:
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason=(
                "Cabeçalhos de preço não identificam CEASAs únicas: "
                f"duplicadas={sorted(duplicadas)[:5]}"
            ),
        )


def parse_precos(precos_json: dict[str, Any]) -> pd.DataFrame:
    resultset = precos_json.get("resultset") if isinstance(precos_json, dict) else None
    if not isinstance(resultset, list):
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason="Resposta de preços sem a lista 'resultset'",
        )
    if not resultset:
        return contracts.get_contract("preco_atacado").empty_frame()[COLUNAS_SAIDA]

    metadata = precos_json.get("metadata", [])
    if not isinstance(metadata, list):
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason="Cabeçalhos de preço sem lista de metadata",
        )

    datas_por_ceasa: list[datetime] = []
    ceasas_por_coluna: list[str] = []
    for col in metadata[1:]:
        col_name = col.get("colName", "") if isinstance(col, dict) else ""
        nome, data = _ceasa_do_cabecalho(col_name)
        datas_por_ceasa.append(data)
        ceasas_por_coluna.append(nome)
    _validar_identidade_ceasas(ceasas_por_coluna)
    for index, column in enumerate(metadata):
        if (
            not isinstance(column, dict)
            or type(column.get("colIndex")) is not int
            or column["colIndex"] != index
        ):
            raise ParseError(
                source="conab_ceasa",
                parser_version=PARSER_VERSION,
                reason=f"Cabeçalhos de preço: metadata[{index}].colIndex deve ser {index}",
            )
    if any(len(row) != len(metadata) for row in resultset):
        raise ParseError(
            source="conab_ceasa",
            parser_version=PARSER_VERSION,
            reason="Quantidade de preços por linha diverge dos cabeçalhos de CEASA",
        )

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
            data = datas_por_ceasa[ceasa_idx]

            records.append(
                {
                    "data": pd.Timestamp(data),
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

    if not records:
        return contracts.get_contract("preco_atacado").empty_frame()[COLUNAS_SAIDA]
    return pd.DataFrame(records, columns=COLUNAS_SAIDA)
