from __future__ import annotations

import asyncio
import hashlib
import json
import re
import warnings

import httpx
import pandas as pd
import pytest

from agrobr import bcb, contracts
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.sync import bcb as sync_bcb
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao
from tests.test_bcb.ptax_replay import encode

BOLETIM_DESCONHECIDO = (
    "Página PTAX quotes 0 (offset 0): PTAX linha 1: tipo_boletim ausente ou não reconhecido; "
    "valores preservados."
)


async def com_avisos(chamada):
    with warnings.catch_warnings(record=True) as registrados, sem_excecao():
        warnings.simplefilter("always")
        resultado = await chamada
    return resultado, [str(aviso.message) for aviso in registrados if aviso.category is UserWarning]


@pytest.mark.parametrize(
    "moeda,name,unit",
    [("USD", "usd_day", "USD/USD"), ("EUR", "eur_day", "USD/EUR"), ("JPY", "jpy_day", "JPY/USD")],
)
async def test_public_quotes_replay_preserves_raw_values_units_and_resource_manifest(
    moeda, name, unit, ptax_http, ptax_captures
):
    body = ptax_captures["bodies"][name]
    trace = ptax_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    (frame, meta), avisos = await com_avisos(
        bcb.ptax(data="04/09/2026", moeda=moeda, boletim="todos", return_meta=True)
    )
    assert avisos == [] and meta.validation_warnings == []
    raw = json.loads(body)["value"]
    assert len(frame) == len(raw) == 5
    assert frame.columns.tolist() == [
        "cotacao_compra",
        "cotacao_venda",
        "data_hora",
        "data",
        "moeda",
        "paridade_compra",
        "paridade_venda",
        "tipo_boletim",
    ]
    for external, column in {
        "cotacaoCompra": "cotacao_compra",
        "cotacaoVenda": "cotacao_venda",
        "paridadeCompra": "paridade_compra",
        "paridadeVenda": "paridade_venda",
        "tipoBoletim": "tipo_boletim",
    }.items():
        assert frame[column].tolist() == [row[external] for row in raw]
    assert frame["data_hora"].tolist() == [pd.Timestamp(row["dataHoraCotacao"]) for row in raw]
    assert frame["data_hora"].dt.tz is None and frame["data"].equals(
        frame["data_hora"].dt.normalize()
    )
    assert meta.source == meta.selected_source == "bcb_ptax" and meta.attempted_sources == [
        "bcb_ptax"
    ]
    assert meta.schema_version == meta.contract_version == "2.0" and meta.parser_version == 2
    assert meta.records_count == 5 and meta.columns == frame.columns.tolist()
    assert (
        meta.fetched_at.utcoffset().total_seconds()
        == meta.fetch_timestamp.utcoffset().total_seconds()
        == 0
    )
    assert meta.fetch_timestamp == meta.fetched_at
    details = meta.source_details
    assert (
        details["coverage"]["completeness"]
        == details["catalog"]["coverage"]["completeness"]
        == "unknown"
    )
    assert details["coverage"]["expected_count"] is None
    assert details["catalog"]["selected_currency"]["moeda"] == moeda
    assert details["catalog"]["schema_version"] == "1.0"
    assert details["units"]["paridade_compra"] == details["units"]["paridade_venda"] == unit
    assert (
        details["units"]["cotacao_compra"]
        == "domestic_currency_at_reference_date_per_unit_of_selected_currency"
    )
    assert details["units"]["converted"] is False
    assert details["published_clock"]["timezone"] is None
    resources = details["resources"]
    assert [resource["role"] for resource in resources] == [
        "catalog",
        "catalog",
        "quotes",
        "quotes",
    ]
    assert [resource["page_index"] for resource in resources] == [0, 1, 0, 1]
    assert all(resource["index_base"] == 0 and "content" not in resource for resource in resources)
    assert len(resources) == len(trace["all"])
    assert details["resource_bytes"] == sum(resource["size_bytes"] for resource in resources)
    contracts.validate_dataset(frame, "bcb_ptax")
    encoded = json.dumps(
        {key: details[key] for key in ["query", "resources"]},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert len(encoded) == meta.raw_content_size
    assert hashlib.sha256(encoded).hexdigest() == meta.raw_content_hash
    assert resources[2]["sha256"] == hashlib.sha256(body).hexdigest()


@pytest.mark.parametrize(
    "mode,name,legacy",
    [("day", "usd_day", "baseline_sdk_day"), ("period", "usd_period", "baseline_sdk_period")],
)
async def test_default_usd_closing_matches_legacy_values_and_timestamp(
    mode, name, legacy, ptax_http, ptax_captures
):
    body = ptax_captures["bodies"][name]
    ptax_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    kwargs = (
        {"data": "04/09/2026"}
        if mode == "day"
        else {"data_inicial": "03/09/2026", "data_final": "06/09/2026"}
    )
    (frame, meta), avisos = await com_avisos(bcb.ptax(**kwargs, return_meta=True))
    assert avisos == []
    expected = json.loads(ptax_captures["bodies"][legacy])["value"]
    assert frame["cotacao_compra"].tolist() == [row["cotacaoCompra"] for row in expected]
    assert frame["cotacao_venda"].tolist() == [row["cotacaoVenda"] for row in expected]
    assert frame["data_hora"].tolist() == [pd.Timestamp(row["dataHoraCotacao"]) for row in expected]
    assert frame["tipo_boletim"].eq("Fechamento PTAX" if mode == "day" else "Fechamento").all()
    assert meta.source_details["coverage"]["filtered_by_boletim_count"] == len(
        json.loads(body)["value"]
    ) - len(frame)


async def test_recusas_publicas_antes_do_catalogo(ptax_http, quote_row):
    trace = ptax_http()
    with collect_failures() as check:
        for argumentos, motivo in [
            ({"moeda": " USD"}, "moeda deve conter três letras ASCII, sem espaços"),
            ({"moeda": True}, "moeda deve conter três letras ASCII, sem espaços"),
            (
                {"boletim": "Fechamento"},
                "boletim deve ser todos, fechamento, abertura ou intermediario",
            ),
            ({"data": "31/02/2026"}, "data contém data inválida"),
            (
                {"data": "04/09/2026", "data_inicial": "03/09/2026"},
                "data não pode ser combinada com limites de período",
            ),
            (
                {"data_final": "01/01/0001"},
                "Período padrão PTAX excede o calendário representável",
            ),
            *(({"top": top}, "top deve ser inteiro positivo") for top in [0, True, 1.0]),
            ({"as_polars": 1}, "as_polars e return_meta devem ser booleanos"),
            ({"return_meta": "yes"}, "as_polars e return_meta devem ser booleanos"),
        ]:
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                await bcb.ptax(**argumentos)
        with check("ptax"), levanta_exatamente(TypeError, match="tipoMoeda"):
            await bcb.ptax(tipoMoeda="A")
        with check("ptax_moedas"), levanta_exatamente(TypeError, match="moeda"):
            await bcb.ptax_moedas(moeda="USD")
    assert trace["all"] == []
    quote_row.update(tipoBoletim="Abertura", cotacaoVenda="bad")
    ptax_http(lambda _request, _index: httpx.Response(200, content=encode([quote_row])))
    with levanta_exatamente(ParseError, match="Cotação PTAX inválida na linha 1"):
        await bcb.ptax(data="04/09/2026")


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize("empty", [False, True])
async def test_polars_quotes_preserve_ns_and_eight_columns(
    empty, return_meta, ptax_http, ptax_captures
):
    pl = pytest.importorskip("polars")
    body = ptax_captures["bodies"]["usd_day"]
    ptax_http(
        lambda _request, index: httpx.Response(
            200, content=encode([]) if empty or index > 1 else body
        )
    )
    result = await bcb.ptax(
        data="04/09/2026", boletim="todos", as_polars=True, return_meta=return_meta
    )
    frame = result[0] if return_meta else result
    assert isinstance(frame, pl.DataFrame) and frame.height == (0 if empty else 5)
    assert len(frame.columns) == 8
    assert frame["data_hora"].dtype == frame["data"].dtype == pl.Datetime("ns")
    assert frame["cotacao_compra"].dtype == pl.Float64
    assert frame["tipo_boletim"].dtype == pl.Utf8
    if not empty:
        assert frame["data_hora"].to_list()[-1].microsecond == 556874
    if return_meta:
        assert result[1].columns == frame.columns and result[1].records_count == frame.height


@pytest.mark.parametrize("empty", [False, True])
async def test_public_catalog_has_own_contract_and_no_quotes(empty, ptax_http):
    trace = ptax_http(
        catalog=(lambda _request, _index: httpx.Response(200, content=encode([])))
        if empty
        else None
    )
    (frame, meta), avisos = await com_avisos(bcb.ptax_moedas(return_meta=True))
    vazio = ["Catálogo PTAX corrente retornou vazio; nenhuma moeda foi validada."]
    assert avisos == (vazio if empty else [])
    assert meta.validation_warnings == (vazio if empty else [])
    assert len(frame) == (0 if empty else 10) and frame.columns.tolist() == [
        "moeda",
        "nome",
        "tipo_moeda",
    ]
    assert trace["quotes"] == []
    assert meta.source == meta.selected_source == "bcb_ptax_moedas"
    assert meta.schema_version == meta.contract_version == "1.0" and meta.parser_version == 2
    assert meta.source_details["coverage"]["completeness"] == "unknown"
    contracts.validate_dataset(frame, "bcb_ptax_moedas")


@pytest.mark.parametrize("label", [None, "", " "])
@pytest.mark.parametrize("return_meta", [False, True])
async def test_polars_quotes_all_nullable_labels_have_stable_text_schema(
    label, return_meta, ptax_http, quote_row
):
    pl = pytest.importorskip("polars")
    quote_row.update(tipoBoletim=label, dataHoraCotacao="2026-09-04 13:03:59.556874001")
    ptax_http(
        lambda _request, index: httpx.Response(
            200, content=encode([quote_row] if index == 1 else [])
        )
    )
    with pytest.warns(UserWarning):
        fetched = await bcb.ptax(
            data="04/09/2026", boletim="todos", as_polars=True, return_meta=return_meta
        )
    frame = fetched[0] if return_meta else fetched
    assert frame["tipo_boletim"].dtype == pl.Utf8
    assert frame["tipo_boletim"].to_list() == [label]
    assert frame["data_hora"].cast(pl.Int64).to_list() == [
        pd.Timestamp(quote_row["dataHoraCotacao"]).value
    ]
    if return_meta:
        assert fetched[1].columns == frame.columns and fetched[1].records_count == 1


async def test_unknown_catalog_type_keeps_parity_unit_uninterpreted(ptax_http, quote_row):
    currencies = [{"simbolo": "USD", "nomeFormatado": "Dólar publicado", "tipoMoeda": "C"}]
    ptax_http(
        lambda _request, index: httpx.Response(
            200, content=encode([quote_row] if index == 1 else [])
        ),
        catalog=lambda _request, index: httpx.Response(
            200, content=encode(currencies if index == 1 else [])
        ),
    )
    (frame, meta), avisos = await com_avisos(
        bcb.ptax(data="04/09/2026", boletim="todos", return_meta=True)
    )
    assert avisos == [
        "Página PTAX catalog 0 (offset 0): PTAX moeda linha 1: tipo_moeda novo; texto preservado."
    ]
    assert len(frame) == 1
    assert meta.source_details["units"]["paridade_compra"] is None
    assert meta.source_details["catalog"]["selected_currency"]["tipo_moeda"] == "C"


async def test_boletim_desconhecido_so_passa_sem_filtro_especifico(ptax_http, quote_row):
    with collect_failures() as check:
        for label in ["Boletim novo", "", " "]:
            quote_row["tipoBoletim"] = label
            ptax_http(
                lambda _request, index: httpx.Response(
                    200, content=encode([quote_row] if index == 1 else [])
                )
            )
            with check(("todos", label)):
                frame, avisos = await com_avisos(bcb.ptax(data="04/09/2026", boletim="todos"))
                assert avisos == [BOLETIM_DESCONHECIDO]
                assert frame.iloc[0]["tipo_boletim"] == label
            if not label.strip():
                ptax_http(lambda _request, _index: httpx.Response(200, content=encode([quote_row])))
                for boletim in ["fechamento", "abertura", "intermediario"]:
                    with (
                        check((boletim, label)),
                        levanta_exatamente(
                            ParseError,
                            match=re.escape(
                                "Boletim PTAX desconhecido ou nulo impede aplicar filtro específico"
                            ),
                        ),
                    ):
                        await bcb.ptax(data="04/09/2026", boletim=boletim)


async def test_historical_usd_preserves_pre_real_rates_and_published_clock(
    ptax_http, ptax_captures
):
    body = ptax_captures["bodies"]["usd_historical"]
    legacy = json.loads(ptax_captures["bodies"]["baseline_sdk_historical"])["value"][0]
    row = json.loads(body)["value"][-1]
    ptax_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    (frame, meta), avisos = await com_avisos(bcb.ptax(data="29/06/1994", return_meta=True))
    assert avisos == []
    assert len(frame) == 1
    assert frame.iloc[0]["cotacao_compra"] == legacy["cotacaoCompra"] == 2698.0
    assert frame.iloc[0]["cotacao_venda"] == legacy["cotacaoVenda"] == 2698.46
    assert frame.iloc[0]["data_hora"] == pd.Timestamp(row["dataHoraCotacao"])
    assert (
        meta.source_details["units"]["cotacao_compra"]
        == "domestic_currency_at_reference_date_per_unit_of_selected_currency"
    )
    assert meta.source_details["published_clock"]["timezone"] is None


async def test_concurrent_currencies_preserve_catalog_selection_and_metadata_copy(
    ptax_http, ptax_captures
):
    def respond(request, _index):
        name = "usd_day" if request.url.params["@m"] == "'USD'" else "eur_day"
        body = (
            ptax_captures["bodies"][name] if int(request.url.params["$skip"]) == 0 else encode([])
        )
        return httpx.Response(200, content=body)

    ptax_http(respond)
    with sem_excecao():
        usd, eur = await asyncio.gather(
            bcb.ptax(data="04/09/2026", moeda="USD", return_meta=True),
            bcb.ptax(data="04/09/2026", moeda="EUR", return_meta=True),
        )
    assert usd[0]["moeda"].eq("USD").all() and eur[0]["moeda"].eq("EUR").all()
    copied = usd[1].to_dict()
    copied["source_details"]["catalog"]["selected_currency"]["nome"] = "outside"
    assert usd[1].source_details["catalog"]["selected_currency"]["nome"] != "outside"
    assert eur[1].source_details["catalog"]["selected_currency"]["moeda"] == "EUR"


def test_sync_quotes_and_catalog_use_exported_public_apis(ptax_http, ptax_captures):
    body = ptax_captures["bodies"]["usd_day"]
    ptax_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    with sem_excecao():
        frame, meta = sync_bcb.ptax(data="04/09/2026", return_meta=True)
        catalog = sync_bcb.ptax_moedas()
    assert len(frame) == 1 and len(catalog) == 10
    assert meta.contract_version == "2.0"


def _sem_paginacao(url: httpx.URL) -> tuple[str, list[tuple[str, str]]]:
    url = url.copy_remove_param("$top").copy_remove_param("$skip")
    return url.path, sorted(url.params.multi_items())


async def test_source_url_e_a_consulta_sem_a_paginacao(ptax_http, ptax_captures):
    body = ptax_captures["bodies"]["eur_day"]
    trace = ptax_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    (_, meta), _ = await com_avisos(
        bcb.ptax(data="04/09/2026", moeda="EUR", boletim="todos", return_meta=True)
    )
    assert len(trace["quotes"]) == 2
    publicada = httpx.URL(meta.source_url)
    assert "$skip" not in publicada.params and "$top" not in publicada.params
    assert _sem_paginacao(publicada) == _sem_paginacao(trace["quotes"][0].url)
    (_, catalogo), _ = await com_avisos(bcb.ptax_moedas(return_meta=True))
    moedas = httpx.URL(catalogo.source_url)
    assert "$skip" not in moedas.params and "$top" not in moedas.params
    assert _sem_paginacao(moedas) == _sem_paginacao(trace["catalog"][-1].url)
