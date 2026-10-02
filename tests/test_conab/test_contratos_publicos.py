from __future__ import annotations

import importlib
import inspect
import json
from io import BytesIO
from pathlib import Path
from types import ModuleType
from unittest import mock

import pandas as pd
import pytest

from agrobr import conab, datasets, sync
from agrobr.conab import api
from agrobr.conab._serie_historica import api as serie_api
from agrobr.exceptions import InvalidParameterError


@pytest.mark.parametrize("interno_primeiro", [False, True])
def test_caminhos_privados_preservam_fachadas(interno_primeiro):
    modulos = [
        conab,
        importlib.import_module("agrobr.conab._custo_producao"),
        importlib.import_module("agrobr.conab._serie_historica"),
    ]
    for modulo in reversed(modulos) if interno_primeiro else modulos:
        importlib.reload(modulo)
    assert isinstance(modulos[1], ModuleType)
    assert isinstance(modulos[2], ModuleType)
    assert conab.custo_producao is modulos[1].custo_producao
    assert conab.serie_historica is modulos[2].serie_historica
    assert conab.produtos_serie_historica is modulos[2].produtos_disponiveis
    assert "produtos_serie_historica" in conab.__all__
    assert sync.conab.produtos_serie_historica() == conab.produtos_serie_historica()
    assert {p["produto"] for p in conab.produtos_serie_historica()} >= {"soja", "milho", "cafe"}
    for nome in ("custo_producao", "serie_historica"):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(f"agrobr.conab.{nome}")
    assert conab.ceasa.__all__ == []
    assert conab.ceasa.precos is conab.ceasa_precos


@pytest.mark.parametrize(
    "funcao",
    [
        conab.safras,
        conab.balanco,
        conab.brasil_total,
        conab.serie_historica,
        datasets.balanco,
        datasets.estimativa_safra,
        datasets.serie_historica_safra,
    ],
)
def test_flags_publicas_exigem_nome(funcao):
    parametros = inspect.signature(funcao).parameters
    assert parametros["as_polars"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parametros["return_meta"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize(
    "filtros",
    [
        {"ano_inicio": True},
        {"ano_inicio": "2020"},
        {"ano_fim": 2020.5},
        {"ano_inicio": 2024, "ano_fim": 2020},
    ],
)
@pytest.mark.parametrize("funcao", [conab.serie_historica, datasets.serie_historica_safra])
async def test_anos_invalidos_falham_antes_da_rede(funcao, filtros, monkeypatch):
    download = mock.AsyncMock(side_effect=AssertionError("consulta inválida tentou a rede"))
    monkeypatch.setattr(serie_api.client, "download_xls", download)
    with pytest.raises(InvalidParameterError, match="ano_inicio|ano_fim"):
        await funcao("soja", **filtros)
    download.assert_not_awaited()


@pytest.mark.parametrize("funcao", [conab.serie_historica, datasets.serie_historica_safra])
@pytest.mark.parametrize("nome", ["inicio", "fim"])
async def test_periodo_anual_recusa_nomes_de_data(funcao, nome):
    with pytest.raises(TypeError, match=nome):
        await funcao("soja", **{nome: 2020})


@pytest.mark.parametrize("endpoint", ["safras", "balanco", "brasil_total"])
async def test_vazios_preservam_colunas_tipos_e_metadados(endpoint, monkeypatch):
    parser = mock.MagicMock(version=3)
    parser.parse_safra_produto.return_value = []
    parser.parse_suprimento.return_value = []
    parser.parse_brasil_total.return_value = []
    monkeypatch.setattr(api, "ConabParserV1", mock.Mock(return_value=parser))
    monkeypatch.setattr(
        api.client,
        "fetch_safra_xlsx",
        mock.AsyncMock(return_value=(BytesIO(b"sem linhas"), {"safra": "2025/26"})),
    )
    argumentos = () if endpoint == "brasil_total" else ("soja",)
    frame, meta = await getattr(conab, endpoint)(*argumentos, return_meta=True)
    esperado = {
        "safras": [
            "fonte",
            "produto",
            "safra",
            "uf",
            "area_plantada",
            "area_colhida",
            "produtividade",
            "producao",
            "levantamento",
            "data_publicacao",
        ],
        "balanco": [
            "produto",
            "safra",
            "estoque_inicial",
            "producao",
            "importacao",
            "suprimento",
            "consumo",
            "exportacao",
            "estoque_final",
            "demanda_total",
            "levantamento",
            "unidade",
        ],
        "brasil_total": [
            "produto",
            "rotulo",
            "grupo",
            "safra",
            "area_plantada",
            "produtividade",
            "producao",
            "unidade_area",
            "unidade_producao",
        ],
    }[endpoint]
    assert frame.empty
    assert frame.columns.tolist() == meta.columns == esperado
    assert meta.records_count == 0
    assert frame["producao"].dtype == "float64"
    assert frame["produto"].dtype == pd.Series(["soja"]).dtype
    if endpoint == "safras":
        assert frame["levantamento"].dtype == "Int64"
        assert str(frame["data_publicacao"].dtype) == "datetime64[ns]"


async def test_safras_normaliza_acento_antes_de_consultar_e_filtrar(monkeypatch):
    parser = mock.MagicMock(version=3)
    parser.parse_safra_produto.return_value = []
    monkeypatch.setattr(api, "ConabParserV1", mock.Mock(return_value=parser))
    download = mock.AsyncMock(return_value=(BytesIO(b"sem linhas"), {"safra": "2025/26"}))
    monkeypatch.setattr(api.client, "fetch_safra_xlsx", download)
    await conab.safras(" FEIJÃO ")
    download.assert_awaited_once()
    assert parser.parse_safra_produto.call_args.kwargs["produto"] == "feijao"


@pytest.mark.parametrize("endpoint", ["safras", "balanco", "brasil_total"])
async def test_vazio_tem_os_dtypes_do_boletim_oficial_preenchido(endpoint, monkeypatch):
    golden = Path(__file__).resolve().parents[1] / "golden_data/conab/safra_2025_26_agosto"
    corpo = (golden / "response.xlsx").read_bytes()
    publicacao = json.loads((golden / "provenance.json").read_text(encoding="utf-8"))
    monkeypatch.setattr(
        api.client,
        "fetch_safra_xlsx",
        mock.AsyncMock(side_effect=lambda **_kwargs: (BytesIO(corpo), publicacao.copy())),
    )
    args = {"safras": ("soja",), "balanco": ("milho",), "brasil_total": ()}[endpoint]
    consultar = getattr(conab, endpoint)
    cheio, _ = await consultar(*args, safra="2025/26", levantamento=11, return_meta=True)
    if endpoint == "safras":
        linha = cheio.set_index("uf").loc["RR"]
        assert linha["produto"] == "soja"
        assert linha["area_plantada"] == 150
        assert linha["producao"] == 513
        assert linha["produtividade"] == 3420
    elif endpoint == "balanco":
        linha = cheio.set_index("safra").loc["2025/26"]
        assert linha["produto"] == "MILHO"
        assert linha["producao"] == 142955
        assert linha["exportacao"] == 46500
        assert linha["levantamento"] == "ago/26"
        assert linha["unidade"] == "mil_ton"
    else:
        linha = cheio[cheio["rotulo"].eq("SOJA")].set_index("safra").loc["2025/26"]
        assert linha["produto"] == "soja"
        assert linha["producao"] == 180463.5
        assert linha["unidade_producao"] == "mil_ton"
    metodo = {
        "safras": "parse_safra_produto",
        "balanco": "parse_suprimento",
        "brasil_total": "parse_brasil_total",
    }[endpoint]
    monkeypatch.setattr(api.ConabParserV1, metodo, lambda _self, **_kwargs: [])
    vazio, meta = await consultar(*args, safra="2025/26", levantamento=11, return_meta=True)
    assert vazio.empty
    assert meta.records_count == 0
    assert list(vazio.columns) == list(cheio.columns)
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
