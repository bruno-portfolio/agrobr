from __future__ import annotations

import asyncio
import hashlib
import json
import re
import warnings
from datetime import datetime

import httpx
import pandas as pd
import pytest

from agrobr import bcb, contracts, datasets
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.sync import bcb as sync_bcb
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao


async def com_avisos(chamada):
    with warnings.catch_warnings(record=True) as registrados, sem_excecao():
        warnings.simplefilter("always")
        resultado = await chamada
    return resultado, [str(aviso.message) for aviso in registrados if aviso.category is UserWarning]


@pytest.mark.parametrize("fetch", [bcb.sgs, datasets.series_economicas])
@pytest.mark.parametrize("empty", [False, True])
async def test_polars_concat_serie_sem_alias_preserva_tipo_textual(fetch, empty, sgs_http):
    pl = pytest.importorskip("polars")
    sgs_http(
        lambda *_: httpx.Response(
            200, json=[] if empty else [{"data": "02/01/2024", "valor": "1.5"}]
        )
    )
    unknown = await fetch(999999, ultimos=1, as_polars=True)
    known = await fetch("selic", ultimos=1, as_polars=True)
    assert unknown.schema["nome_serie"] == known.schema["nome_serie"] == pl.Utf8
    combined = pl.concat([unknown, known])
    assert combined.schema["nome_serie"] == pl.Utf8
    assert combined.height == (0 if empty else 2)
    assert unknown["nome_serie"].null_count() == unknown.height


async def test_replay_public_history_matches_all_3767_independent_values(sgs_http, sgs_captures):
    sgs_http()
    (frame, meta), avisos = await com_avisos(
        bcb.sgs(1, data_inicial="01/01/2010", data_final="31/12/2024", return_meta=True)
    )
    assert avisos == [] and meta.validation_warnings == []
    assert len(frame) == 3767
    assert frame.columns.tolist() == ["data", "valor", "codigo", "nome_serie"]
    actual = dict(zip(frame["data"].dt.strftime("%Y-%m-%d"), frame["valor"], strict=True))
    assert actual == {key: float(value) for key, value in sgs_captures["oracles"]["union"].items()}
    assert not frame.duplicated(["codigo", "data"]).any()
    assert frame["data"].is_monotonic_increasing
    assert frame["codigo"].eq(1).all()
    assert frame["nome_serie"].eq("dolar_ptax_venda").all()
    assert meta.source == meta.selected_source == "bcb_sgs"
    assert meta.attempted_sources == ["bcb_sgs"]
    assert meta.schema_version == meta.contract_version == "2.1"
    assert meta.parser_version == 2
    assert meta.records_count == len(frame) and meta.columns == frame.columns.tolist()
    assert meta.fetched_at.utcoffset().total_seconds() == 0
    assert meta.fetch_timestamp == meta.fetched_at
    details = meta.source_details
    assert details["coverage"]["completeness"] == "unknown"
    assert details["coverage"]["expected_count"] is None
    assert details["hash_kind"] == "resource_manifest_sha256"
    assert len(details["resources"]) == 2
    assert all("content" not in resource for resource in details["resources"])
    contracts.validate_dataset(frame, "bcb_sgs")
    encoded = json.dumps(
        {key: details[key] for key in ["query", "resources"]},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert len(encoded) == meta.raw_content_size
    assert hashlib.sha256(encoded).hexdigest() == meta.raw_content_hash


async def test_replay_alias_monthly_keeps_saturday_and_negative_value(sgs_http):
    sgs_http()
    frame, avisos = await com_avisos(
        bcb.sgs("ipca", data_inicial="01/01/2024", data_final="31/12/2024")
    )
    assert avisos == []
    assert len(frame) == 12
    assert frame["codigo"].eq(433).all() and frame["nome_serie"].eq("ipca").all()
    by_date = frame.set_index("data")
    assert by_date.loc["2024-06-01", "valor"] == 0.21
    assert by_date.loc["2024-08-01", "valor"] == -0.02


async def test_recusas_publicas_antes_da_rede(sgs_http):
    requests = sgs_http()
    with collect_failures() as check:
        for codigo, argumentos, motivo in [
            *(
                (codigo, {}, "codigo deve ser inteiro positivo ou alias SGS")
                for codigo in [True, 1.0, 0]
            ),
            ("1", {}, "Serie '1' nao encontrada"),
            (2**63, {}, "Seleção SGS inválida ou intervalo invertido"),
            *(
                (1, {"ultimos": valor}, "ultimos deve ser inteiro positivo")
                for valor in [0, True, 3.0]
            ),
            (1, {"as_polars": 1}, "as_polars e return_meta devem ser booleanos"),
            (1, {"return_meta": "yes"}, "as_polars e return_meta devem ser booleanos"),
            (1, {"data_inicial": "31/02/2024"}, "data_inicial contém data inválida"),
            (
                1,
                {"data_inicial": "02/01/2024", "data_final": "01/01/2024"},
                "Seleção SGS inválida ou intervalo invertido",
            ),
        ]:
            with (
                check((codigo, argumentos)),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                await bcb.sgs(codigo, **argumentos)
        with check("assinatura"), levanta_exatamente(TypeError, match="data_inicio"):
            await bcb.sgs(1, data_inicio="01/01/2024")
    assert requests == []
    sgs_http(
        lambda _request, _index: httpx.Response(
            200,
            json=[{"data": "01/01/2024", "valor": "bad"}, {"data": "02/01/2024", "valor": "1.2"}],
        )
    )
    with levanta_exatamente(
        ParseError, match=re.escape("Observação SGS inválida na linha 1; campos: ['valor']")
    ):
        await bcb.sgs(1, data_inicial="01/01/2024", data_final="02/01/2024", ultimos=1)


@pytest.mark.parametrize("with_dates", [False, True])
async def test_replay_ultimos_returns_exact_last_three_dates_not_just_row_count(
    with_dates, sgs_http, sgs_captures
):
    requests = sgs_http()
    arguments = {"data_inicial": "01/01/2024", "data_final": "10/01/2024"} if with_dates else {}
    (frame, meta), avisos = await com_avisos(bcb.sgs(1, ultimos=3, return_meta=True, **arguments))
    assert avisos == []
    name = "dates_last3_sdk" if with_dates else "last3_sdk"
    rows = sorted(
        json.loads(sgs_captures["bodies"][name]),
        key=lambda row: datetime.strptime(row["data"], "%d/%m/%Y"),
    )[-3:]
    assert frame["data"].tolist() == [
        pd.Timestamp(datetime.strptime(row["data"], "%d/%m/%Y")) for row in rows
    ]
    assert frame["valor"].tolist() == [float(row["valor"]) for row in rows]
    assert ("/ultimos/" in requests[0].url.path) == (not with_dates)
    assert meta.source_details["coverage"]["returned_count"] == 3
    assert meta.source_details["coverage"]["received_count"] == (7 if with_dates else 3)


async def test_range_with_large_ultimos_is_not_limited_to_twenty(sgs_http):
    sgs_http()
    frame, avisos = await com_avisos(
        bcb.sgs(1, data_inicial="01/01/2010", data_final="31/12/2024", ultimos=1300)
    )
    assert avisos == []
    assert len(frame) == 1300
    assert frame["data"].iloc[-1] == pd.Timestamp("2024-12-31")


@pytest.mark.parametrize(
    "codigo,start,end,reference,value",
    [
        (433, "02/01/2024", "31/01/2024", "2024-01-01", 0.42),
        (22083, "15/11/2023", "31/12/2023", "2023-10-01", 156.39),
    ],
)
@pytest.mark.parametrize("return_meta", [False, True])
async def test_replay_period_reference_before_window_is_visible_with_warning(
    codigo, start, end, reference, value, return_meta, sgs_http
):
    sgs_http()
    resultado, avisos = await com_avisos(
        bcb.sgs(codigo, data_inicial=start, data_final=end, return_meta=return_meta)
    )
    frame = resultado[0] if return_meta else resultado
    assert frame.iloc[0]["data"] == pd.Timestamp(reference)
    assert frame.iloc[0]["valor"] == value
    assert len(avisos) == 1
    assert re.fullmatch(
        r"Bloco SGS [0-9a-f]{64}: 1 referências anteriores e 0 posteriores aos limites "
        r"diários foram preservadas; a resposta não informa frequência ou regras de seleção "
        r"da série\.",
        avisos[0],
    )
    if return_meta:
        meta = resultado[1]
        assert meta.validation_warnings == avisos
        assert meta.source_details["resources"][0]["reference_diagnostics"]["before_count"] == 1
        assert meta.source_details["resources"][0]["block"]["id"] in avisos[0]


async def test_explicit_json_null_preserves_nullable_float_without_zero(sgs_http):
    sgs_http(
        lambda _request, _index: httpx.Response(200, json=[{"data": "01/01/2024", "valor": None}])
    )
    frame, avisos = await com_avisos(
        bcb.sgs(999999999, data_inicial="01/01/2024", data_final="01/01/2024")
    )
    assert avisos == []
    assert pd.isna(frame.iloc[0]["valor"])
    assert pd.isna(frame.iloc[0]["nome_serie"])
    assert str(frame["valor"].dtype) == "float64"


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize("empty", [False, True])
async def test_polars_nonempty_and_declared_empty_preserve_four_column_schema(
    empty, return_meta, sgs_http
):
    pl = pytest.importorskip("polars")
    sgs_http()
    args = {"data_inicial": "06/01/2024", "data_final": "07/01/2024"} if empty else {"ultimos": 3}
    result = await bcb.sgs(1, as_polars=True, return_meta=return_meta, **args)
    frame = result[0] if return_meta else result
    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == ["data", "valor", "codigo", "nome_serie"]
    assert frame.height == (0 if empty else 3)
    assert frame["data"].dtype == pl.Datetime("ns")
    assert frame["valor"].dtype == pl.Float64
    assert frame["codigo"].dtype == pl.Int64
    if return_meta:
        assert result[1].records_count == frame.height
        assert result[1].columns == frame.columns


async def test_concurrent_series_do_not_mix_results_or_nested_metadata(sgs_http):
    sgs_http()
    (daily, monthly), avisos = await com_avisos(
        asyncio.gather(
            bcb.sgs(1, ultimos=3, return_meta=True),
            bcb.sgs(433, data_inicial="01/01/2024", data_final="31/12/2024", return_meta=True),
        )
    )
    assert avisos == []
    assert daily[0]["codigo"].eq(1).all()
    assert monthly[0]["codigo"].eq(433).all()
    exported = daily[1].to_dict()
    exported["source_details"]["resources"][0]["parameters"]["changed"] = "outside"
    assert "changed" not in daily[1].source_details["resources"][0]["parameters"]
    assert "changed" not in monthly[1].source_details["resources"][0]["parameters"]


def test_sync_public_sgs_replay_runs_full_pipeline_and_returns_meta(sgs_http):
    requests = sgs_http()
    with sem_excecao():
        frame, meta = sync_bcb.sgs(
            1, data_inicial="01/01/2010", data_final="31/12/2024", ultimos=3, return_meta=True
        )
    assert len(frame) == 3
    assert frame["data"].iloc[-1] == pd.Timestamp("2024-12-31")
    assert meta.schema_version == "2.1"
    assert len(requests) == 2


def _consulta(url: httpx.URL) -> tuple[str, list[tuple[str, str]]]:
    return url.path, sorted(url.params.multi_items())


async def test_source_url_e_a_consulta_inteira_e_nao_o_ultimo_bloco(sgs_http, sgs_captures):
    requests = sgs_http()
    (_, meta), _ = await com_avisos(
        bcb.sgs(1, data_inicial="01/01/2010", data_final="31/12/2024", return_meta=True)
    )
    capturas = {item["case"]: item["url"] for item in sgs_captures["manifest"]["artifacts"]}
    assert len(requests) == 2
    assert _consulta(httpx.URL(meta.source_url)) == _consulta(httpx.URL(capturas["long_sdk"]))
    assert [httpx.URL(item["url"]) for item in meta.source_details["resources"]] == [
        request.url for request in requests
    ]
