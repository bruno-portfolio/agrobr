from __future__ import annotations

import asyncio
import hashlib
import json
import re
import warnings

import httpx
import pytest

from agrobr import bcb, contracts
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.sync import bcb as sync_bcb
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao
from tests.test_bcb.focus_replay import encode

LIMITE_LOCAL = (
    "Limite local max_registros=6 encerrou a coleta Focus; cobertura não comprovada, "
    "total não informado pela fonte."
)


async def com_avisos(chamada):
    with warnings.catch_warnings(record=True) as registrados, sem_excecao():
        warnings.simplefilter("always")
        resultado = await chamada
    return resultado, [str(aviso.message) for aviso in registrados if aviso.category is UserWarning]


@pytest.mark.parametrize(
    "periodicidade,indicador,prefix",
    [("anual", "Balança comercial", "annual"), ("mensal", "IPCA", "monthly")],
)
async def test_public_exact_replay_preserves_values_identity_and_provenance(
    periodicidade, indicador, prefix, focus_http, focus_captures
):
    requests = focus_http()
    (frame, meta), avisos = await com_avisos(
        bcb.focus(
            indicador,
            periodicidade=periodicidade,
            inicio="2026-08-28",
            top=6,
            max_registros=6,
            return_meta=True,
        )
    )
    assert avisos == [LIMITE_LOCAL] and meta.validation_warnings == [LIMITE_LOCAL]
    assert len(requests) == 1 and len(frame) == 6
    raw = json.loads(focus_captures["bodies"][f"{prefix}_api_ge"])["value"]
    assert frame["media"].tolist() == [row["Media"] for row in raw]
    assert frame["data_referencia"].tolist() == [row["DataReferencia"] for row in raw]
    assert frame["base_calculo"].tolist() == [row["baseCalculo"] for row in raw]
    assert frame["periodicidade"].eq(periodicidade).all()
    assert len(frame.columns) == 12
    assert meta.source == meta.selected_source == "bcb_focus"
    assert meta.attempted_sources == ["bcb_focus"]
    assert meta.schema_version == meta.contract_version == "2.0" and meta.parser_version == 2
    assert meta.columns == frame.columns.tolist() and meta.records_count == 6
    assert meta.fetched_at.utcoffset().total_seconds() == 0
    assert meta.fetch_timestamp == meta.fetched_at
    details = meta.source_details
    assert details["coverage"]["completeness"] == "unknown"
    assert details["coverage"]["expected_count"] is None
    assert "records" not in details and "content" not in details["resources"][0]
    contracts.validate_dataset(frame, "bcb_focus")
    encoded = json.dumps(
        {key: details[key] for key in ["query", "resources"]},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert len(encoded) == meta.raw_content_size
    assert hashlib.sha256(encoded).hexdigest() == meta.raw_content_hash
    assert (
        details["resources"][0]["sha256"]
        == hashlib.sha256(focus_captures["bodies"][f"{prefix}_api_ge"]).hexdigest()
    )


async def test_padrao_anual_e_mensal_vazio_nao_trocam_de_entidade(focus_http):
    requests = focus_http(lambda _request, _index: httpx.Response(200, content=encode([])))
    frame, avisos = await com_avisos(bcb.focus())
    assert avisos == []
    assert requests[0].url.path.endswith("/ExpectativasMercadoAnuais")
    assert requests[0].url.params["$filter"] == "Indicador eq 'PIB Agropecuária'"
    assert frame.empty and len(frame.columns) == 12
    assert str(frame["data"].dtype) == "datetime64[ns]"
    assert str(frame["base_calculo"].dtype) == "Int64"
    requests = focus_http(lambda _request, _index: httpx.Response(200, content=encode([])))
    (frame, meta), avisos = await com_avisos(
        bcb.focus("ipca", periodicidade="mensal", return_meta=True)
    )
    assert frame.empty and len(requests) == 1 and avisos == []
    assert requests[0].url.path.endswith("/ExpectativaMercadoMensais")
    assert requests[0].url.params["$filter"] == "Indicador eq 'ipca'"
    assert meta.source_details["coverage"]["completeness"] == "unknown"


async def test_monthly_reference_in_2028_remains_text_and_negative_minimum(
    focus_http, focus_captures
):
    body = focus_captures["bodies"]["monthly_short"]
    focus_http(
        lambda _request, index: httpx.Response(200, content=body if index == 1 else encode([]))
    )
    frame, avisos = await com_avisos(bcb.focus("IPCA", periodicidade="mensal"))
    assert avisos == []
    assert frame["data_referencia"].tolist() == ["08/2028", "08/2028"]
    assert frame["data"].dt.strftime("%Y-%m-%d").tolist() == ["2026-08-28"] * 2
    assert frame["media"].tolist() == [0.1324, 0.09]
    assert frame["minimo"].tolist() == [-0.323, -0.323]
    assert frame["numero_respondentes"].tolist() == [81, 26]
    assert frame["indicador_detalhe"].isna().all()


async def test_recusas_publicas_antes_da_rede(focus_http, annual_row):
    requests = focus_http()
    with collect_failures() as check:
        for argumentos, motivo in [
            ({"indicador": " "}, "indicador deve ser texto não vazio"),
            ({"periodicidade": "trimestral"}, "periodicidade deve ser anual ou mensal"),
            *(({"top": top}, "top deve ser inteiro positivo") for top in [True, 0, 1.0]),
            *(
                ({"max_registros": valor}, "max_registros deve ser inteiro positivo")
                for valor in [0, True]
            ),
            ({"inicio": "2026-02-30"}, "inicio contém data inexistente"),
            ({"as_polars": 1}, "as_polars e return_meta devem ser booleanos"),
            ({"return_meta": "yes"}, "as_polars e return_meta devem ser booleanos"),
        ]:
            with (
                check(argumentos),
                levanta_exatamente(InvalidParameterError, match=re.escape(motivo)),
            ):
                await bcb.focus(**argumentos)
        with check("assinatura"), levanta_exatamente(TypeError, match="base_calculo"):
            await bcb.focus(base_calculo=0)
    assert requests == []
    rows = [annual_row, dict(annual_row, Indicador="IPCA", baseCalculo=1)]
    focus_http(lambda _request, _index: httpx.Response(200, content=encode(rows)))
    with levanta_exatamente(
        ParseError,
        match=re.escape("Registro Focus incompatível com indicador ou periodicidade solicitados"),
    ):
        await bcb.focus("Balança comercial", top=2, max_registros=1)


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize("empty", [False, True])
async def test_polars_full_pipeline_preserves_schema_and_meta(
    empty, return_meta, focus_http, focus_captures
):
    pl = pytest.importorskip("polars")
    body = focus_captures["bodies"]["monthly_api_ge"]
    focus_http(
        lambda _request, index: httpx.Response(
            200, content=encode([]) if empty or index > 1 else body
        )
    )
    result = await bcb.focus(
        "IPCA", periodicidade="mensal", as_polars=True, return_meta=return_meta
    )
    frame = result[0] if return_meta else result
    assert isinstance(frame, pl.DataFrame) and frame.height == (0 if empty else 6)
    assert len(frame.columns) == 12
    assert frame["data"].dtype == pl.Datetime("ns")
    assert frame["media"].dtype == pl.Float64
    assert frame["base_calculo"].dtype == frame["numero_respondentes"].dtype == pl.Int64
    assert frame["indicador_detalhe"].dtype == pl.Utf8
    assert frame["indicador_detalhe"].null_count() == frame.height
    if return_meta:
        assert result[1].columns == frame.columns and result[1].records_count == frame.height


async def test_inconsistent_published_statistics_warn_without_metadata(focus_http, annual_row):
    annual_row.update(Media=-5.0, DesvioPadrao=-1.0, Minimo=3.0, Maximo=2.0)
    focus_http(
        lambda _request, index: httpx.Response(
            200, content=encode([annual_row] if index == 1 else [])
        )
    )
    frame, avisos = await com_avisos(bcb.focus("Balança comercial"))
    assert avisos == [
        "Página Focus 0 (offset 0): Focus linha 1: desvio_padrao negativo; minimo maior que "
        "maximo; media fora dos extremos disponíveis; mediana fora dos extremos disponíveis; "
        "valores preservados."
    ]
    assert frame.iloc[0]["media"] == -5.0 and frame.iloc[0]["desvio_padrao"] == -1.0


async def test_concurrent_periodicities_and_metadata_are_isolated(focus_http):
    focus_http()
    selecao = {"inicio": "2026-08-28", "top": 6, "max_registros": 6, "return_meta": True}
    (annual, monthly), avisos = await com_avisos(
        asyncio.gather(
            bcb.focus("Balança comercial", periodicidade="anual", **selecao),
            bcb.focus("IPCA", periodicidade="mensal", **selecao),
        )
    )
    assert avisos == [LIMITE_LOCAL, LIMITE_LOCAL]
    assert annual[0]["periodicidade"].eq("anual").all()
    assert monthly[0]["periodicidade"].eq("mensal").all()
    exported = annual[1].to_dict()
    exported["source_details"]["resources"][0]["parameters"]["changed"] = "outside"
    assert "changed" not in annual[1].source_details["resources"][0]["parameters"]
    assert "changed" not in monthly[1].source_details["resources"][0]["parameters"]


def test_sync_public_focus_uses_monthly_source_pipeline(focus_http):
    requests = focus_http()
    with warnings.catch_warnings(record=True) as registrados, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = sync_bcb.focus(
            "IPCA",
            periodicidade="mensal",
            inicio="2026-08-28",
            top=6,
            max_registros=6,
            return_meta=True,
        )
    assert [str(aviso.message) for aviso in registrados] == [LIMITE_LOCAL]
    assert len(frame) == 6 and len(requests) == 1
    assert meta.schema_version == "2.0"


async def test_source_url_e_a_consulta_sem_a_paginacao(focus_http, focus_captures):
    capturas = {item["case"]: item for item in focus_captures["manifest"]["artifacts"]}
    por_skip = {"0": "monthly_page1", "3": "monthly_page2"}

    def paginas(request, _ordem):
        caso = por_skip[request.url.params["$skip"]]
        return httpx.Response(
            200, content=focus_captures["bodies"][caso], headers=capturas[caso]["headers"]
        )

    requests = focus_http(paginas)
    (_, meta), _ = await com_avisos(
        bcb.focus(
            "IPCA",
            periodicidade="mensal",
            inicio="2026-08-28",
            top=3,
            max_registros=6,
            return_meta=True,
        )
    )
    pedidas = [request.url for request in requests]
    assert [url.params["$skip"] for url in pedidas] == ["0", "3"]
    consulta = pedidas[0].copy_remove_param("$top").copy_remove_param("$skip")
    publicada = httpx.URL(meta.source_url)
    assert (publicada.path, sorted(publicada.params.multi_items())) == (
        consulta.path,
        sorted(consulta.params.multi_items()),
    )
    assert [item["url"] for item in meta.source_details["resources"]] == list(map(str, pedidas))
