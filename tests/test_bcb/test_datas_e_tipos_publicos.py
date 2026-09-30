from __future__ import annotations

import inspect
import json
from datetime import UTC, date, datetime
from unittest.mock import Mock

import httpx
import pandas as pd
import pytest

from agrobr import bcb, cepea, contracts, datasets
from agrobr.bcb import focus_parser, focus_query, ptax_parser, ptax_query, sgs_query
from agrobr.contracts import bcb_sgs
from agrobr.utils import time as time_utils


def entrada(formato, ano, mes, dia):
    valor = date(ano, mes, dia)
    return {
        "iso": valor.isoformat(),
        "br": valor.strftime("%d/%m/%Y"),
        "date": valor,
        "datetime": datetime(ano, mes, dia, 23, 59),
    }[formato]


@pytest.mark.parametrize("formato", ["iso", "br", "date", "datetime"])
@pytest.mark.parametrize("consulta", [bcb.sgs, datasets.series_economicas])
async def test_sgs_aceita_datas_e_alias_sem_mudar_a_consulta_publicada(
    formato, consulta, sgs_http, sgs_captures
):
    pedidos = sgs_http()
    frame, meta = await consulta(
        " IPCA ",
        inicio=entrada(formato, 2024, 1, 1),
        fim=entrada(formato, 2024, 12, 31),
        return_meta=True,
    )
    publicado = json.loads(sgs_captures["bodies"]["monthly433"])
    assert frame["valor"].tolist() == [float(item["valor"]) for item in publicado]
    assert frame["data"].tolist() == [
        pd.Timestamp(datetime.strptime(item["data"], "%d/%m/%Y")) for item in publicado
    ]
    assert len(frame) == 12
    assert pedidos[0].url.params["dataInicial"] == "01/01/2024"
    assert pedidos[0].url.params["dataFinal"] == "31/12/2024"
    assert meta.schema_version == meta.contract_version == "3.0"


@pytest.mark.parametrize("formato", ["iso", "br", "date", "datetime"])
@pytest.mark.parametrize("consulta", [bcb.focus, datasets.expectativas_mercado])
async def test_focus_aceita_datas_e_periodicidade_normalizada(
    formato, consulta, focus_http, focus_captures
):
    pedidos = focus_http()
    with pytest.warns(UserWarning, match="max_registros"):
        frame = await consulta(
            "Balança comercial",
            periodicidade="ANUAL",
            inicio=entrada(formato, 2026, 8, 28),
            top=6,
            max_registros=6,
        )
    publicado = json.loads(focus_captures["bodies"]["annual_api_ge"])["value"]
    assert len(frame) == 6
    assert frame["media"].tolist() == [item["Media"] for item in publicado]
    assert frame["data_referencia"].tolist() == [item["DataReferencia"] for item in publicado]
    assert pedidos[0].url.params["$filter"].endswith("Data ge '2026-08-28'")


@pytest.mark.parametrize("formato", ["iso", "br", "date", "datetime"])
@pytest.mark.parametrize("consulta", [bcb.ptax, datasets.cotacoes_cambio])
async def test_ptax_aceita_datas_e_preserva_o_boletim_oficial(
    formato, consulta, ptax_http, ptax_captures
):
    corpo = ptax_captures["bodies"]["usd_period"]
    trace = ptax_http(
        lambda _request, index: httpx.Response(
            200, content=corpo if index == 1 else b'{"value":[]}'
        )
    )
    frame = await consulta(
        inicio=entrada(formato, 2026, 9, 3), fim=entrada(formato, 2026, 9, 6), boletim="Fechamento"
    )
    publicado = [item for item in json.loads(corpo)["value"] if item["tipoBoletim"] == "Fechamento"]
    assert len(frame) == len(publicado) > 0
    assert frame["cotacao_venda"].tolist() == [item["cotacaoVenda"] for item in publicado]
    assert trace["quotes"][0].url.params["@di"] == "'09-03-2026'"
    assert trace["quotes"][0].url.params["@df"] == "'09-06-2026'"


def test_data_brasileira_ambigua_e_primeiro_de_fevereiro():
    sgs = sgs_query.build_query(433, data_inicial="01/02/2024", reference_date=date(2024, 2, 29))
    ptax = ptax_query.build_query(
        data="01/02/2024", boletim="intermediário", reference_date=date(2024, 2, 29)
    )
    focus = focus_query.build_query(data_inicial="01/02/2024")
    assert sgs.inicio == ptax.inicio == focus.data_inicial == date(2024, 2, 1)
    assert sgs_query.format_date(sgs.inicio) == "01/02/2024"
    assert ptax_query.format_date(ptax.inicio) == "02-01-2024"
    assert "Data ge '2024-02-01'" in focus.filter
    assert ptax.boletim == "intermediario"


@pytest.mark.parametrize(
    "fonte,consulta",
    [
        ("sgs", bcb.sgs),
        ("sgs", datasets.series_economicas),
        ("ptax", bcb.ptax),
        ("ptax", datasets.cotacoes_cambio),
    ],
)
async def test_fim_padrao_e_proveniencia_seguem_a_data_de_brasilia_na_virada(
    fonte, consulta, monkeypatch, sgs_http, sgs_captures, ptax_http, ptax_captures
):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2027, 1, 1, 1, tzinfo=UTC))
    modulo = sgs_query if fonte == "sgs" else ptax_query
    construir = Mock(wraps=modulo.build_query)
    monkeypatch.setattr(modulo, "build_query", construir)
    if fonte == "sgs":
        corpo = sgs_captures["bodies"]["monthly433"]
        pedidos = sgs_http(lambda _request, _index: httpx.Response(200, content=corpo))
        frame, meta = await consulta(433, inicio="2024-01-01", return_meta=True)
        assert frame["valor"].tolist() == [float(row["valor"]) for row in json.loads(corpo)]
        assert pedidos[0].url.params["dataFinal"] == "31/12/2026"
    else:
        corpo = ptax_captures["bodies"]["usd_day"]
        pedidos = ptax_http(
            lambda _request, index: httpx.Response(
                200, content=corpo if index == 1 else b'{"value":[]}'
            )
        )
        frame, meta = await consulta(inicio="2026-09-01", boletim="todos", return_meta=True)
        assert frame["cotacao_venda"].tolist() == [
            row["cotacaoVenda"] for row in json.loads(corpo)["value"]
        ]
        assert pedidos["quotes"][0].url.params["@df"] == "'12-31-2026'"
    assert construir.call_args_list
    assert all(
        chamada.kwargs["reference_date"] == date(2026, 12, 31)
        for chamada in construir.call_args_list
    )
    assert meta.source_details["query"]["fim"] == "2026-12-31"
    assert meta.source_details["query"]["reference_date"] == "2026-12-31"
    assert meta.source_details["query"]["defaulted_fields"] == ["fim"]


@pytest.mark.parametrize(
    "consulta,args",
    [
        (bcb.sgs, (1,)),
        (bcb.focus, ()),
        (bcb.ptax, ()),
        (datasets.series_economicas, (1,)),
        (datasets.expectativas_mercado, ()),
        (datasets.cotacoes_cambio, ()),
    ],
)
@pytest.mark.parametrize("nome", ["data_inicial", "data_final"])
async def test_nome_de_data_antigo_e_recusado_sem_alias(consulta, args, nome):
    with pytest.raises(TypeError, match=nome):
        await consulta(*args, **{nome: "2024-01-01"})


@pytest.mark.parametrize(
    "consulta",
    [
        bcb.sgs,
        bcb.focus,
        bcb.ptax,
        bcb.ptax_moedas,
        bcb.credito_rural,
        bcb.credito_rural_total,
        cepea.indicador,
        datasets.series_economicas,
        datasets.expectativas_mercado,
        datasets.cotacoes_cambio,
        datasets.moedas_cambio,
        datasets.credito_rural,
        datasets.preco_diario,
    ],
)
def test_flags_publicas_exigem_nome(consulta):
    assinatura = inspect.signature(consulta)
    for nome in ("as_polars", "return_meta"):
        assert assinatura.parameters[nome].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize("fonte", ["focus", "ptax", "moedas"])
def test_dtypes_do_vazio_iguais_a_captura_real(fonte, focus_captures, ptax_captures):
    if fonte == "focus":
        registros = focus_parser.parse_page(
            focus_captures["bodies"]["annual_api_ge"], "anual"
        ).records
        cheio = focus_parser.build_frame(registros)
        vazio = focus_parser.build_frame([])
        contrato = contracts.get_contract("bcb_focus")
    elif fonte == "ptax":
        registros = ptax_parser.parse_quotes_page(ptax_captures["bodies"]["usd_day"], "USD").records
        cheio = ptax_parser.build_quotes_frame(registros)
        vazio = ptax_parser.build_quotes_frame([])
        contrato = contracts.get_contract("bcb_ptax")
    else:
        registros = ptax_parser.parse_currencies_page(ptax_captures["bodies"]["currencies"]).records
        cheio = ptax_parser.build_currencies_frame(registros)
        vazio = ptax_parser.build_currencies_frame([])
        contrato = contracts.get_contract("bcb_ptax_moedas")
    assert not cheio.empty
    assert cheio.dtypes.equals(vazio.dtypes)
    assert cheio.dtypes.equals(contrato.empty_frame().dtypes)


async def test_sgs_vazio_tem_os_tipos_da_serie_real(sgs_http, sgs_captures):
    sgs_http()
    cheio = await bcb.sgs(1, ultimos=3)
    vazio = await bcb.sgs(1, inicio="06/01/2024", fim="07/01/2024")
    publicado = json.loads(sgs_captures["bodies"]["last3_sdk"])
    assert cheio["valor"].tolist() == [float(item["valor"]) for item in publicado]
    assert len(cheio) == 3 and vazio.empty
    assert cheio.dtypes.equals(vazio.dtypes)
    assert str(cheio["codigo"].dtype) == "Int64"
    assert bcb_sgs.BCB_SGS_V2.to_dict()["constraints"]["integer_dtype"] == "int64"
    assert contracts.get_contract("bcb_sgs").to_dict()["constraints"]["integer_dtype"] == "Int64"


@pytest.mark.parametrize("fonte", ["focus", "ptax", "moedas"])
async def test_polars_vazio_tem_tipos_da_captura_real(
    fonte, focus_http, focus_captures, ptax_http, ptax_captures
):
    pl = pytest.importorskip("polars")
    if fonte == "focus":
        focus_http()
        with pytest.warns(UserWarning, match="max_registros"):
            cheio = await bcb.focus(
                "Balança comercial", inicio="2026-08-28", top=6, max_registros=6, as_polars=True
            )
        publicado = json.loads(focus_captures["bodies"]["annual_api_ge"])["value"]
        assert cheio["media"].to_list() == [row["Media"] for row in publicado]
        focus_http(lambda _request, _index: httpx.Response(200, content=b'{"value":[]}'))
        vazio = await bcb.focus(as_polars=True)
    elif fonte == "ptax":
        corpo = ptax_captures["bodies"]["usd_day"]
        ptax_http(
            lambda _request, index: httpx.Response(
                200, content=corpo if index == 1 else b'{"value":[]}'
            )
        )
        cheio = await bcb.ptax(data="04/09/2026", boletim="todos", as_polars=True)
        assert cheio["cotacao_venda"].to_list() == [
            row["cotacaoVenda"] for row in json.loads(corpo)["value"]
        ]
        ptax_http(lambda _request, _index: httpx.Response(200, content=b'{"value":[]}'))
        vazio = await bcb.ptax(data="04/09/2026", as_polars=True)
    else:
        ptax_http()
        cheio = await bcb.ptax_moedas(as_polars=True)
        assert cheio["moeda"].to_list() == [
            row["simbolo"] for row in json.loads(ptax_captures["bodies"]["currencies"])["value"]
        ]
        ptax_http(catalog=lambda _request, _index: httpx.Response(200, content=b'{"value":[]}'))
        vazio = await bcb.ptax_moedas(as_polars=True)
    assert len(cheio) > 0 and len(vazio) == 0
    assert cheio.schema == vazio.schema
    assert pl.Null not in vazio.schema.values()
