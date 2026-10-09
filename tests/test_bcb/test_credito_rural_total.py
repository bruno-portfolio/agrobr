from __future__ import annotations

import gzip
import hashlib
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import bcb, contracts
from agrobr.bcb import client, models, parser
from agrobr.exceptions import ContractViolationError, InvalidParameterError, ParseError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "bcb" / "sicor_regiao_uf_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
RECURSOS = {recurso["file"]: recurso for recurso in MANIFESTO["resources"]}
MESES_REGIAO_UF = sorted(nome for nome in RECURSOS if nome.startswith("RegiaoUF_"))
TOTAIS_SICOR = {
    ("2022", "custeio"): (942825, Decimal("207709040433.19")),
    ("2022", "investimento"): (1023854, Decimal("100448934321.77")),
    ("2022", "comercializacao"): (22988, Decimal("32986781718.21")),
    ("2022", "industrializacao"): (1732, Decimal("21849654291.96")),
    ("2023", "custeio"): (964287, Decimal("225139296410.76")),
    ("2023", "investimento"): (1129634, Decimal("101930864138.82")),
    ("2023", "comercializacao"): (33610, Decimal("51413811887.88")),
    ("2023", "industrializacao"): (1868, Decimal("28089305375.65")),
}
CAMPOS = {
    "custeio": ("QtdCusteio", "VlCusteio"),
    "investimento": ("QtdInvestimento", "VlInvestimento"),
    "comercializacao": ("QtdComercializacao", "VlComercializacao"),
    "industrializacao": ("QtdIndustrializacao", "VlIndustrializacao"),
}


def corpo(nome: str, decimal: bool = False) -> list[dict[str, Any]]:
    bruto = gzip.decompress((GOLDEN / nome).read_bytes())
    assert hashlib.sha256(bruto).hexdigest() == RECURSOS[nome]["sha256"]
    extra: dict[str, Any] = {"parse_float": Decimal} if decimal else {}
    return list(json.loads(bruto, **extra)["value"])


def servir(monkeypatch: pytest.MonkeyPatch, nomes: list[str]) -> AsyncMock:
    registros = [registro for nome in nomes for registro in corpo(nome)]
    fetch = AsyncMock(return_value={"value": registros})
    monkeypatch.setattr(client, "_fetch_odata", fetch)
    return fetch


def na_safra_2022_23(registro: dict[str, Any]) -> bool:
    ano, mes = int(registro["AnoEmissao"]), int(registro["MesEmissao"])
    return (ano, mes) >= (2022, 7) and (ano, mes) <= (2023, 6)


def oraculo(
    registros: list[dict[str, Any]], chave: Any = lambda registro: (registro["nomeUF"],)
) -> dict[tuple[Any, ...], tuple[int, Decimal]]:
    somas: dict[tuple[Any, ...], tuple[int, Decimal]] = {}
    for registro in registros:
        for finalidade, (campo_qtd, campo_valor) in CAMPOS.items():
            if registro[campo_qtd] == 0 and registro[campo_valor] == 0:
                continue
            item = (*chave(registro), finalidade)
            qtd, valor = somas.get(item, (0, Decimal(0)))
            somas[item] = (qtd + registro[campo_qtd], valor + registro[campo_valor])
    return somas


def obtido(frame: pd.DataFrame, colunas: list[str]) -> dict[tuple[Any, ...], tuple[int, Decimal]]:
    return {
        (*(getattr(linha, coluna) for coluna in colunas), linha.finalidade): (
            int(linha.qtd_contratos),
            Decimal(str(linha.valor)),
        )
        for linha in frame.itertuples()
    }


async def test_total_da_safra_por_uf_e_finalidade_confere_com_os_corpos(
    monkeypatch: pytest.MonkeyPatch,
):
    fetch = servir(monkeypatch, MESES_REGIAO_UF)
    with sem_excecao():
        frame, meta = await bcb.credito_rural_total(safra="2022/23", return_meta=True)

    registros = [registro for nome in MESES_REGIAO_UF for registro in corpo(nome, decimal=True)]
    por_ano = oraculo(registros, lambda registro: (registro["AnoEmissao"],))
    assert por_ano == TOTAIS_SICOR
    esperado = oraculo([registro for registro in registros if na_safra_2022_23(registro)])
    assert obtido(frame, ["uf"]) == esperado
    assert len(esperado) == 104
    assert sorted({uf for uf, _ in esperado}) == sorted(models.UF_CODES)
    assert sorted(
        (uf, finalidade)
        for uf in models.UF_CODES
        for finalidade in CAMPOS
        if (uf, finalidade) not in esperado
    ) == [
        ("AM", "industrializacao"),
        ("AP", "comercializacao"),
        ("AP", "industrializacao"),
        ("RR", "industrializacao"),
    ]
    assert set(frame["safra"]) == {"2022/23"}
    assert set(frame["agregacao"]) == {"uf"} and frame["programa"].isna().all()
    assert list(frame.columns) == contracts.get_contract("bcb_credito_rural_total").list_columns()
    assert meta.source_details == {
        "meses": {"primeiro": "2022-07", "ultimo": "2023-06", "quantidade": 12}
    }
    assert (meta.schema_version, meta.attempted_sources, meta.selected_source) == (
        "1.0",
        ["bcb_odata"],
        "bcb_odata",
    )
    chamada = fetch.await_args.kwargs
    assert chamada["endpoint"] == "RegiaoUF"
    assert chamada["select"] == list(models.SICOR_TOTAL_CAMPOS)
    assert chamada["filters"] == [client._safra_odata_filter(2022)]


async def test_total_por_programa_de_uma_uf_e_uma_finalidade(monkeypatch: pytest.MonkeyPatch):
    servir(monkeypatch, MESES_REGIAO_UF)
    with sem_excecao():
        programa = await bcb.credito_rural_total(safra="2022/23", uf="MT", agregacao="programa")
        industrializacao = await bcb.credito_rural_total(
            safra="2022/23", finalidade="industrializacao"
        )

    registros = [
        registro
        for nome in MESES_REGIAO_UF
        for registro in corpo(nome, decimal=True)
        if na_safra_2022_23(registro)
    ]
    esperado = oraculo(
        [registro for registro in registros if registro["nomeUF"] == "MT"],
        lambda registro: (registro["cdPrograma"],),
    )
    assert obtido(programa, ["cd_programa"]) == esperado
    assert set(programa["uf"]) == {"MT"}
    assert dict(zip(programa["cd_programa"], programa["programa"], strict=True)) == {
        codigo: models.SICOR_PROGRAMAS[codigo] for codigo, _ in esperado
    }
    esperado_industrializacao = {
        chave: valor
        for chave, valor in oraculo(registros).items()
        if chave[1] == "industrializacao"
    }
    assert obtido(industrializacao, ["uf"]) == esperado_industrializacao
    assert len(esperado_industrializacao) == 24


async def test_total_por_uf_fecha_com_a_soma_por_produto_e_com_os_municipios(
    monkeypatch: pytest.MonkeyPatch,
):
    servir(monkeypatch, ["RegiaoUF_2023_01.json.gz"])
    with sem_excecao():
        frame = await bcb.credito_rural_total(safra="2022/23")
    total = obtido(frame, ["uf"])

    for finalidade, entidade in (
        ("custeio", "CusteioRegiaoUFProduto"),
        ("investimento", "InvestRegiaoUFProduto"),
        ("comercializacao", "ComercRegiaoUFProduto"),
    ):
        produtos = parser.parse_credito_rural(
            corpo(f"{entidade}_2023_01.json.gz"), finalidade=finalidade
        )
        somas = produtos.groupby("uf").agg(qtd=("qtd_contratos", "sum"), valor=("valor", "sum"))
        assert {
            (uf, finalidade): (int(linha.qtd), round(linha.valor, 2))
            for uf, linha in somas.iterrows()
        } == {
            chave: (qtd, float(valor))
            for chave, (qtd, valor) in total.items()
            if chave[1] == finalidade
        }, finalidade

    municipios = corpo("CusteioInvestimentoComercialIndustrialSemFiltros_2023_01.json.gz", True)
    assert total == oraculo(municipios)
    assert len(total) == 89


async def test_sem_safra_junho_e_julho_saem_em_safras_diferentes(monkeypatch: pytest.MonkeyPatch):
    meses = ["RegiaoUF_2022_06.json.gz", "RegiaoUF_2022_07.json.gz"]
    fetch = servir(monkeypatch, meses)
    with sem_excecao():
        frame = await bcb.credito_rural_total()

    junho, julho = (corpo(nome, decimal=True) for nome in meses)
    assert obtido(frame, ["safra", "uf"]) == {
        **{("2021/22", *chave): valor for chave, valor in oraculo(junho).items()},
        **{("2022/23", *chave): valor for chave, valor in oraculo(julho).items()},
    }
    assert fetch.await_args.kwargs["filters"] is None


async def test_linha_da_regiao_uf_sem_campo_recusa_o_layout(monkeypatch: pytest.MonkeyPatch):
    registros = corpo("RegiaoUF_2023_01.json.gz")
    del registros[0]["VlIndustrializacao"]
    monkeypatch.setattr(client, "_fetch_odata", AsyncMock(return_value={"value": registros}))
    with levanta_exatamente(ParseError, match=re.escape("RegiaoUF sem ['VlIndustrializacao']")):
        await bcb.credito_rural_total(safra="2022/23")


async def test_valor_negativo_viola_o_contrato(monkeypatch: pytest.MonkeyPatch):
    registros = corpo("RegiaoUF_2023_01.json.gz")
    amapa = [registro for registro in registros if registro["nomeUF"] == "AP"]
    assert all(registro["QtdComercializacao"] == 0 for registro in amapa)
    amapa[0].update(QtdComercializacao=1, VlComercializacao=-1.0)
    monkeypatch.setattr(client, "_fetch_odata", AsyncMock(return_value={"value": registros}))
    with levanta_exatamente(ContractViolationError, match="below minimum 0"):
        await bcb.credito_rural_total(safra="2022/23")


async def test_safra_sem_registros_devolve_as_nove_colunas(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(client, "_fetch_odata", AsyncMock(return_value={"value": []}))
    with sem_excecao():
        frame, meta = await bcb.credito_rural_total(safra="2030/31", return_meta=True)
    assert frame.empty
    assert list(frame.columns) == contracts.get_contract("bcb_credito_rural_total").list_columns()
    assert meta.source_details == {"meses": None}


async def test_recusas_antes_da_rede(monkeypatch: pytest.MonkeyPatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "_fetch_odata", fetch)
    with collect_failures() as check:
        for argumentos, motivo in [
            ({"agregacao": "municipio"}, "agregacao inválida: 'municipio'"),
            ({"finalidade": "exportacao"}, "Finalidade inválida: 'exportacao'"),
            ({"uf": "XX"}, "UF inválida: 'XX'"),
            ({"safra": "2022/24"}, "safra inválida: '2022/24'"),
        ]:
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                await bcb.credito_rural_total(**argumentos)
    fetch.assert_not_awaited()
