from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import warnings
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import bcb, datasets
from agrobr.bcb import sgs_models
from agrobr.exceptions import InvalidParameterError
from tests import helpers

ORACULO = Path(__file__).parents[1] / "golden_data/bcb/oraculo_20260923"
MANIFEST = json.loads((ORACULO / "manifest.json").read_text(encoding="utf-8"))
CASOS = {caso["id"]: caso for caso in MANIFEST["cases"]}
SHA = {recurso["file"]: recurso["sha256"] for recurso in MANIFEST["resources"]}
MOEDAS = ["AUD", "CAD", "CHF", "DKK", "EUR", "GBP", "JPY", "NOK", "SEK", "USD"]


def corpo(nome: str) -> Any:
    conteudo = (ORACULO / nome).read_bytes()
    assert hashlib.sha256(conteudo).hexdigest() == SHA[nome]
    return json.loads(conteudo)


async def servir(
    monkeypatch: pytest.MonkeyPatch,
    caso: str | dict[str, Any],
    chamada: Callable[[], Awaitable[Any]],
    avisos: list[warnings.WarningMessage] | None = None,
    diagnosticos: list[str] | None = None,
) -> Any:
    seen = helpers.install_replay_http(
        monkeypatch, CASOS[caso] if isinstance(caso, str) else caso, ORACULO
    )
    try:
        with warnings.catch_warnings(record=True) as registrados, helpers.sem_excecao():
            warnings.simplefilter("always")
            resultado = await chamada()
    finally:
        helpers.assert_replay_served(seen)
    assert [str(aviso.message) for aviso in registrados if aviso.category is UserWarning] == (
        diagnosticos or []
    )
    if avisos is not None:
        avisos.extend(registrados)
    return resultado


def valores(frame: pd.DataFrame, colunas: list[str]) -> list[tuple[Any, ...]]:
    return [
        tuple(None if pd.isna(valor) else valor for valor in linha)
        for linha in frame[colunas].itertuples(index=False, name=None)
    ]


def caso_sgs(codigo: int) -> dict[str, Any]:
    return next(
        caso
        for caso in MANIFEST["cases"]
        if re.search(rf"bcdata\.sgs\.{codigo}/", caso["requests"][0]["match"]["path"])
    )


@pytest.mark.parametrize("camada", ["fonte", "dataset"])
@pytest.mark.parametrize(("alias", "codigo"), list(sgs_models.SGS_SERIES.items()))
async def test_sgs_publica_as_17_series_do_corpo_oficial(
    alias: str, codigo: int, camada: str, monkeypatch: pytest.MonkeyPatch
):
    consultar = bcb.sgs if camada == "fonte" else datasets.series_economicas
    caso = caso_sgs(codigo)
    itens = corpo(caso["requests"][0]["file"])
    com_fim = any("dataFim" in item for item in itens)
    extras = sorted({campo for item in itens for campo in item} - {"data", "valor", "dataFim"})

    frame = await servir(
        monkeypatch,
        caso["id"],
        lambda: consultar(alias, ultimos=3),
        diagnosticos=[f"SGS retornou campos adicionais: {extras}"] if extras else [],
    )

    esperado = sorted(
        (
            datetime.strptime(item["data"], "%d/%m/%Y"),
            None if item["valor"] == "" else float(item["valor"]),
            codigo,
            alias,
            *([datetime.strptime(item["dataFim"], "%d/%m/%Y")] if com_fim else []),
        )
        for item in itens
    )
    colunas = ["data", "valor", "codigo", "nome_serie", *(["data_fim"] if com_fim else [])]
    assert frame.columns.tolist() == colunas
    assert valores(frame, colunas) == esperado


@pytest.mark.parametrize("camada", ["fonte", "dataset"])
@pytest.mark.parametrize("moeda", MOEDAS)
async def test_ptax_publica_os_boletins_de_cada_moeda(
    moeda: str, camada: str, monkeypatch: pytest.MonkeyPatch
):
    consultar = bcb.ptax if camada == "fonte" else datasets.cotacoes_cambio
    caso = CASOS[f"ptax_{moeda.lower()}_dia"]
    cotacoes = next(p for p in caso["requests"] if p["match"]["params"].get("@m") == f"'{moeda}'")

    frame = await servir(
        monkeypatch,
        caso["id"],
        lambda: consultar(data="22/09/2026", moeda=moeda, boletim="todos"),
    )

    esperado = [
        (
            item["cotacaoCompra"],
            item["cotacaoVenda"],
            pd.Timestamp(item["dataHoraCotacao"]),
            pd.Timestamp(item["dataHoraCotacao"][:10]),
            moeda,
            item["paridadeCompra"],
            item["paridadeVenda"],
            item["tipoBoletim"],
        )
        for item in corpo(cotacoes["file"])["value"]
    ]
    colunas = [
        "cotacao_compra",
        "cotacao_venda",
        "data_hora",
        "data",
        "moeda",
        "paridade_compra",
        "paridade_venda",
        "tipo_boletim",
    ]
    assert valores(frame, colunas) == esperado
    assert len(esperado) == 5


@pytest.mark.parametrize("camada", ["fonte", "dataset"])
async def test_ptax_catalogo_publica_as_moedas_oficiais(
    camada: str, monkeypatch: pytest.MonkeyPatch
):
    consultar = bcb.ptax_moedas if camada == "fonte" else datasets.moedas_cambio
    caso = CASOS["ptax_usd_dia"]
    paginas = [p for p in caso["requests"] if p["match"]["path"].endswith("/Moedas")]
    frame = await servir(monkeypatch, {"requests": paginas}, consultar)

    esperado = sorted(
        (item["simbolo"], item["nomeFormatado"], item["tipoMoeda"])
        for pagina in paginas
        for item in corpo(pagina["file"])["value"]
    )
    assert valores(frame, ["moeda", "nome", "tipo_moeda"]) == esperado
    assert [simbolo for simbolo, *_ in esperado] == MOEDAS


async def test_sicor_comercializacao_publica_a_soma_do_corpo_oficial(
    monkeypatch: pytest.MonkeyPatch,
):
    caso = CASOS["sicor_comercializacao_soja_mt"]
    registros = corpo(caso["requests"][0]["file"])["value"]

    frame = await servir(
        monkeypatch,
        caso["id"],
        lambda: bcb.credito_rural("soja", safra="2024/25", finalidade="comercializacao", uf="MT"),
    )

    assert valores(frame, ["safra", "uf", "finalidade", "agregacao"]) == [
        ("2024/25", "MT", "comercializacao", "uf")
    ]
    assert frame["valor"].iloc[0] == pytest.approx(
        math.fsum(r["VlComerc"] for r in registros), rel=1e-12
    )
    assert frame["qtd_contratos"].iloc[0] == sum(r["QtdComerc"] for r in registros)


async def test_ipa_agropecuario_vira_ipa_agricola_com_aviso(monkeypatch: pytest.MonkeyPatch):
    caso = caso_sgs(7460)
    esperado = sorted(
        (datetime.strptime(item["data"], "%d/%m/%Y"), float(item["valor"]))
        for item in corpo(caso["requests"][0]["file"])
    )

    avisos: list[warnings.WarningMessage] = []
    canonico = await servir(
        monkeypatch, caso["id"], lambda: bcb.sgs("ipa_agricola", ultimos=3), avisos
    )
    assert avisos == []
    antigo = await servir(
        monkeypatch, caso["id"], lambda: bcb.sgs("ipa_agropecuario", ultimos=3), avisos
    )

    for frame in (canonico, antigo):
        assert valores(frame, ["data", "valor"]) == esperado
        assert frame["nome_serie"].tolist() == ["ipa_agricola"] * 3
    assert [(aviso.category, str(aviso.message)) for aviso in avisos] == [
        (
            FutureWarning,
            "codigo='ipa_agropecuario' está depreciado: a série 7460 é o IPA-DI por origem de "
            "produtos agrícolas, sem os pecuários. Use codigo='ipa_agricola'",
        )
    ]


def programas_oficiais() -> dict[str, str]:
    conteudo = (ORACULO / "dominio_Programa.csv").read_bytes()
    assert hashlib.sha256(conteudo).hexdigest() == SHA["dominio_Programa.csv"]
    linhas = csv.reader(io.StringIO(conteudo.decode("cp1252")), delimiter=";")
    return {
        codigo: descricao.split(" - ", 1)[0].replace('"', "").strip()
        for codigo, descricao, *_vigencia in list(linhas)[1:]
    }


@pytest.mark.parametrize("caso", sorted(c for c in CASOS if c.startswith("sicor_custeio_")))
async def test_credito_rural_publica_cada_cultura_pela_chave_pedida(
    caso: str, monkeypatch: pytest.MonkeyPatch
):
    produto, uf = caso.removeprefix("sicor_custeio_").rsplit("_", 1)
    consultar = (
        datasets.credito_rural
        if produto in datasets.get_dataset("credito_rural").info.products
        else bcb.credito_rural
    )
    registros = corpo(CASOS[caso]["requests"][0]["file"])["value"]
    oficiais = programas_oficiais()

    frame = await servir(
        monkeypatch,
        caso,
        lambda: consultar(produto, safra="2024/25", uf=uf.upper(), agregacao="programa"),
    )

    esperado: dict[tuple[str, str], list[float]] = {}
    for registro in registros:
        chave = (oficiais[registro["cdPrograma"]], registro["cdPrograma"])
        esperado.setdefault(chave, []).append(registro["VlCusteio"])
    assert valores(frame, ["safra", "produto", "uf", "programa", "cd_programa"]) == [
        ("2024/25", produto, uf.upper(), *chave) for chave in sorted(esperado)
    ]
    assert frame["valor"].tolist() == pytest.approx(
        [math.fsum(esperado[c]) for c in sorted(esperado)], rel=1e-12
    )
    assert {registro["nomeUF"] for registro in registros} == {uf.upper()}


async def test_credito_rural_publica_a_mesma_chave_para_grafias_equivalentes(
    monkeypatch: pytest.MonkeyPatch,
):
    publicados = {}
    for grafia in ["algodao", "Algodão", "ALGODAO", " algodão ", '"ALGODÃO"']:
        frame = await servir(
            monkeypatch,
            "sicor_custeio_algodao_mt",
            lambda grafia=grafia: bcb.credito_rural(grafia, safra="2024/25", uf="MT"),
        )
        publicados[grafia] = frame["produto"].unique().tolist()
    assert publicados == dict.fromkeys(publicados, ["algodao"])


async def test_credito_rural_casa_com_a_estimativa_safra_pela_safra_uf_e_produto(
    monkeypatch: pytest.MonkeyPatch,
):
    credito = await servir(
        monkeypatch,
        "sicor_custeio_milho_mt",
        lambda: datasets.credito_rural("milho", safra="2024/25", uf="MT", agregacao="programa"),
    )
    estimativa = pd.DataFrame(
        {"safra": ["2024/25"], "uf": ["MT"], "produto": ["milho"], "producao": [51_000.0]}
    )

    cruzado = credito.merge(estimativa, on=["safra", "uf", "produto"])

    assert len(cruzado) == len(credito) > 0


async def test_safra_do_credito_rural_so_aceita_anos_consecutivos(monkeypatch: pytest.MonkeyPatch):
    caso = CASOS["sicor_custeio_milho_mt"]
    total = math.fsum(r["VlCusteio"] for r in corpo(caso["requests"][0]["file"])["value"])
    for safra in ["2024/25", "2024/2025", "2025", " 2024/25 "]:
        frame = await servir(
            monkeypatch,
            caso["id"],
            lambda safra=safra: datasets.credito_rural(
                "milho", safra=safra, uf="MT", agregacao="programa"
            ),
        )
        assert frame["safra"].unique().tolist() == ["2024/25"]
        assert frame["valor"].sum() == pytest.approx(total, rel=1e-12)
    seen = helpers.install_replay_http(monkeypatch, {"requests": []}, ORACULO)
    with helpers.collect_failures() as check:
        for safra in [
            "2023/25",
            "2024/2023",
            "2024/2026",
            "23/24",
            "abc",
            "",
            "2024/",
            "0000",
            2025,
        ]:
            with (
                check(safra),
                helpers.levanta_exatamente(
                    InvalidParameterError, match=re.escape(f"safra inválida: {safra!r};")
                ),
            ):
                await bcb.credito_rural("milho", safra=safra, uf="MT")
    assert seen == {"served": [], "unmatched": []}
