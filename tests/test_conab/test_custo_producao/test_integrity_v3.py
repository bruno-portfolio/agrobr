from __future__ import annotations

import hashlib
import inspect
import json
import zipfile
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pandas as pd
import pytest
from bs4 import BeautifulSoup
from openpyxl import Workbook as ExcelWorkbook

from agrobr import datasets
from agrobr.conab._custo_producao import _acquisition as acquisition
from agrobr.conab._custo_producao import api, models
from agrobr.conab._custo_producao._context import context, select
from agrobr.conab._custo_producao._parse import number, parse_selected
from agrobr.conab._custo_producao._workbook import Aba, Workbook
from agrobr.contracts.conab_custos import CONAB_CUSTOS_V3
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError, ParseError
from tests import helpers

GOLDEN = Path(__file__).parents[2] / "golden_data" / "conab" / "custos_20260908"


def resource(cultura: str = "soja") -> models.RecursoCusto:
    name = (
        "serie-historica-custos-soja-1997-a-2025.xls"
        if cultura == "soja"
        else "serie-historica-custos-algodao-em-pluma-1998-a-2025.xls"
    )
    return models.RecursoCusto(
        planilha=name,
        cultura=cultura,
        titulo=name,
        pagina_url=acquisition.CATALOG_URL + "/" + name + "/view",
    )


def official(cultura: str = "soja", aba: str = "Barreiras-BA-2025"):
    book = Workbook((GOLDEN / f"{cultura}.xls").read_bytes())
    try:
        sheet = book.read(aba)
        return sheet, context(sheet, resource(cultura), book.names.index(aba))
    finally:
        book.close()


@pytest.mark.parametrize("reverse", [False, True])
def test_catalogo_com_duas_edicoes_exige_planilha_explicita(reverse):
    older = resource()
    newer = older.model_copy(update={"planilha": older.planilha.replace("2025", "2026")})
    resources = [newer, older] if reverse else [older, newer]
    with pytest.raises(InvalidParameterError, match="planilha= explicitamente"):
        api._resource(resources, None)
    assert api._resource(resources, older.planilha) is older


def mock_http(monkeypatch, *, workbook: bytes | None = None, pages: dict[str, bytes] | None = None):
    manifest = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    bodies = {
        v["receipt"]["url"]: workbook
        if workbook is not None and name.endswith(".xls")
        else (GOLDEN / name).read_bytes()
        for name, v in manifest.items()
    }
    bodies.update(pages or {})
    calls = []
    factory = httpx.AsyncClient

    def respond(request):
        calls.append(str(request.url))
        assert request.headers["accept-encoding"] == "identity"
        body = bodies[str(request.url)]
        return httpx.Response(
            200, headers={"content-length": str(len(body))}, stream=httpx.ByteStream(body)
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: factory(
        **kwargs, transport=httpx.MockTransport(respond)
    )
    monkeypatch.setattr(acquisition, "httpx", namespace)
    return calls


@pytest.mark.parametrize(
    "scenario,parameters",
    [
        ("test_catalog_descends_into_crop_subfolders", {}),
        ("test_catalog_reads_all_root_pages_before_tab", {}),
        ("test_catalog_duplicate_keeps_first_metadata", {}),
        ("test_catalog_real_conflict", {}),
        ("test_catalog_unknown_crop_is_refused", {}),
        ("test_catalog_cycle_fails_before_repeating_request", {}),
    ],
    ids=[
        "catalog_descends_into_crop_subfolders-0",
        "catalog_reads_all_root_pages_before_tab-0",
        "catalog_duplicate_keeps_first_metadata-0",
        "catalog_real_conflict-0",
        "catalog_unknown_crop_preserves_typed_empty-0",
        "catalog_cycle_fails_before_repeating_request-0",
    ],
)
async def test_catalogo_navegacao_e_consistencia(scenario: str, parameters: dict[str, Any]):
    with (
        helpers.collect_failures() as check,
        check((scenario, parameters)),
        helpers.isolated_dataset_case((scenario, parameters)) as monkeypatch,
    ):
        acquisition.clear()
        if scenario == "test_catalog_descends_into_crop_subfolders":
            calls = mock_http(monkeypatch)
            df = await api.catalogo_custos()
            expected = {
                "milho_1a_safra_serie_historica_1997-2025.xls": "milho",
                "milho_2a_safra_serie_historica_2005-2025.xls": "milho",
                "arroz_irrigado_serie_historica_2002-2025.xls": "arroz",
                "arroz_sequeiro_serie_historica_2001-2025.xls": "arroz",
                "feijao_1_safra_serie_historica-1998-2025.xls": "feijao",
                "feijao_2_e_3_safras_serie_historica_2013-2025.xls": "feijao",
            }
            crops = df[df["cultura"].isin({"milho", "arroz", "feijao"})].set_index("planilha")
            assert crops["cultura"].to_dict() == expected
            for name, crop in expected.items():
                assert (
                    crops.loc[name, "pagina_url"] == f"{acquisition.CATALOG_URL}/{crop}/{name}/view"
                )
            assert len(df) == 51
            assert df["planilha"].is_unique
            assert len(calls) == 7
        elif scenario == "test_catalog_reads_all_root_pages_before_tab":
            calls = mock_http(monkeypatch)
            await api.catalogo_custos()
            assert calls[:4] == [
                acquisition.CATALOG_URL,
                acquisition.CATALOG_URL + "?b_start:int=20",
                acquisition.CATALOG_URL + "?b_start:int=40",
                acquisition.TAB_URL,
            ]
        elif scenario == "test_catalog_duplicate_keeps_first_metadata":
            soup = BeautifulSoup((GOLDEN / "catalog_page40.html").read_bytes(), "lxml")
            article = soup.new_tag("article", attrs={"class": "entry"})
            anchor = soup.new_tag("a", title="File", href=resource().pagina_url)
            anchor.string = "Outra representação"
            article.append(anchor)
            soup.select_one("#content-core").append(article)
            mock_http(
                monkeypatch,
                pages={acquisition.CATALOG_URL + "?b_start:int=40": soup.encode("utf-8")},
            )
            df = await api.catalogo_custos("soja")
            selected = df[df["planilha"] == resource().planilha]
            assert selected["titulo"].tolist() == [resource().planilha]
            assert df["planilha"].is_unique
        elif scenario == "test_catalog_real_conflict":
            page = (GOLDEN / "catalog_milho.html").read_bytes()
            original = b"milho_1a_safra_serie_historica_1997-2025.xls/view"
            assert original in page
            page = page.replace(original, (resource().planilha + "/view").encode())
            mock_http(monkeypatch, pages={acquisition.CATALOG_URL + "/milho": page})
            with pytest.raises(ParseError, match="Identificador de recurso ambíguo"):
                await api.catalogo_custos()
        elif scenario == "test_catalog_unknown_crop_is_refused":
            mock_http(monkeypatch)
            with pytest.raises(
                InvalidParameterError,
                match="Nenhuma planilha para 'inexistente'; culturas disponíveis: .*'soja'",
            ):
                await api.catalogo_custos("inexistente")
        elif scenario == "test_catalog_cycle_fails_before_repeating_request":
            soup = BeautifulSoup((GOLDEN / "catalog.html").read_bytes(), "lxml")
            for anchor in soup.select("a.proximo"):
                anchor["href"] = acquisition.CATALOG_URL
            calls = mock_http(monkeypatch, pages={acquisition.CATALOG_URL: soup.encode("utf-8")})
            with pytest.raises(ParseError, match="Ciclo no catálogo"):
                await api.catalogo_custos()
            assert calls == [acquisition.CATALOG_URL]


@pytest.mark.parametrize("order", [(0, 1), (1, 0)])
def test_ambiguous_contexts_never_choose_by_order(order):
    _, first = official()
    second = first.model_copy(update={"aba": "Outra", "sistema": "Outro sistema"})
    candidates = [first, second]
    with pytest.raises(InvalidParameterError, match="2 candidatos"):
        select(
            [candidates[i] for i in order],
            models.ConsultaCusto(cultura="soja", local="Barreiras", ano=2025),
        )
    assert select(candidates, models.ConsultaCusto(aba="Outra")) is second


def test_missing_measure_propagates_null_without_zero():
    sheet, ctx = official()
    sheet.linhas[8][1] = ""
    row = next(r for r in parse_selected(sheet, ctx).observacoes if r.linha == 9)
    assert row.valor_ha is None
    assert row.valor_unidade_produto == 0


@pytest.mark.asyncio
async def test_public_total_uses_published_values(monkeypatch):
    mock_http(monkeypatch)
    result, meta = await api.custo_producao_total(
        "algodao", planilha=resource("algodao").planilha, aba="Barreiras-BA-2025", return_meta=True
    )
    assert result["coe_ha"] is None
    assert result["cv_ha"] == 14583.319999999998
    assert len(result["totais_publicados"]) == 4
    assert meta.records_count == 1


@pytest.mark.asyncio
async def test_meta_separa_o_manifesto_dos_bytes_recebidos(monkeypatch):
    calls = mock_http(monkeypatch)
    _, meta = await api.custo_producao_total(
        "algodao", planilha=resource("algodao").planilha, aba="Barreiras-BA-2025", return_meta=True
    )
    manifesto = json.dumps(
        meta.source_details["manifest"],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    digest = hashlib.sha256(manifesto).hexdigest()
    assert (meta.raw_content_hash, meta.raw_content_size) == (digest, len(manifesto))
    capturas = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    corpos = {v["receipt"]["url"]: (GOLDEN / nome).read_bytes() for nome, v in capturas.items()}
    planilhas = {v["receipt"]["url"] for nome, v in capturas.items() if nome.endswith(".xls")}
    assert meta.source_details["received_bytes"] == sum(len(corpos[url]) for url in calls)
    dados = sum(len(corpos[url]) for url in calls if url in planilhas)
    assert dados and meta.source_details["data_file_bytes"] == dados


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs", [{"ano": True}, {"ano": 0}, {"local": ""}, {"return_meta": 1}, {"as_polars": "yes"}]
)
async def test_guards_before_http(monkeypatch, kwargs):
    calls = mock_http(monkeypatch)
    with pytest.raises(InvalidParameterError):
        await api.custo_producao("soja", **kwargs)
    assert calls == []


@pytest.mark.asyncio
async def test_deterministic_rejected_before_http(monkeypatch):
    calls = mock_http(monkeypatch)
    async with deterministic("2025-01-01"):
        with pytest.raises(InvalidParameterError, match="snapshot"):
            await api.custo_producao("soja", ano=2025)
    assert calls == []


def test_wholly_missing_numbered_item_is_preserved():
    sheet, ctx = official()
    sheet.linhas[8][1:5] = ["", "", "", ""]
    rows = {row.linha: row for row in parse_selected(sheet, ctx).observacoes}
    assert 9 in rows
    row = rows[9]
    assert row.valor_ha is None
    assert row.valor_unidade_produto is None
    assert row.tipo_linha == "item"


def test_context_references_cannot_choose_last():
    sheet, _ = official()
    sheet.linhas[5][0] = "Mês/Ano: Março/2024"
    with pytest.raises(ParseError, match="Referências conflitantes"):
        context(sheet, resource(), 0)


@pytest.mark.asyncio
async def test_partial_short_chunk_is_in_receipt():
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"abc"
            raise httpx.ReadError("interrupted")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _request: httpx.Response(200, stream=Stream()))
    ) as http:
        acquired = acquisition.Acquisition()
        with pytest.raises(httpx.ReadError):
            await acquired._attempt(http, acquisition.CATALOG_URL, "catalog")
    assert acquired.receipts[0]["bytes"] == 3
    assert acquired.receipts[0]["sha256"] == hashlib.sha256(b"abc").hexdigest()
    assert acquired.receipts[0]["closed"]
    assert not acquired.receipts[0]["eof"]


@pytest.mark.asyncio
async def test_partial_http_206_rejected_even_with_eof():
    def response(_request):
        return httpx.Response(
            206, headers={"content-range": "bytes 0-2/8"}, stream=httpx.ByteStream(b"abc")
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as http:
        acquired = acquisition.Acquisition()
        with pytest.raises(ParseError, match="parcial"):
            await acquired._attempt(http, acquisition.CATALOG_URL, "workbook")
    assert acquired.receipts[0]["closed"]
    assert acquired.receipts[0]["bytes"] == 0


def test_percentage_scaling_overflow_is_layout_error():
    sheet = Aba("synthetic", [[1e308]], {(0, 0): "0.00%"})
    with pytest.raises(ParseError, match="Overflow de escala"):
        number(1e308, sheet, 0, 0, percentage=True)


@pytest.mark.parametrize(
    "scenario,parameters",
    [
        ("test_catalog_cache_reutiliza_catalogo_completo", {}),
        ("test_catalog_cache_respeita_ttl", {"elapsed": 3599, "expected_calls": 7}),
        ("test_catalog_cache_respeita_ttl", {"elapsed": 3600, "expected_calls": 14}),
        ("test_catalog_cache_bypass_ignora_leitura_e_gravacao", {"warm": False}),
        ("test_catalog_cache_bypass_ignora_leitura_e_gravacao", {"warm": True}),
        ("test_catalog_cache_copias_independentes", {"warm": False}),
        ("test_catalog_cache_copias_independentes", {"warm": True}),
        ("test_catalog_cache_falha_nao_publica_catalogo_parcial", {}),
    ],
    ids=[
        "catalog_cache_reutiliza_catalogo_completo-0",
        "catalog_cache_respeita_ttl-0",
        "catalog_cache_respeita_ttl-1",
        "catalog_cache_bypass_ignora_leitura_e_gravacao-0",
        "catalog_cache_bypass_ignora_leitura_e_gravacao-1",
        "catalog_cache_copias_independentes-0",
        "catalog_cache_copias_independentes-1",
        "catalog_cache_falha_nao_publica_catalogo_parcial-0",
    ],
)
async def test_cache_catalogo_ciclo_completo(scenario: str, parameters: dict[str, Any]):
    with (
        helpers.collect_failures() as check,
        check((scenario, parameters)),
        helpers.isolated_dataset_case((scenario, parameters)) as monkeypatch,
    ):
        acquisition.clear()
        if scenario == "test_catalog_cache_reutiliza_catalogo_completo":
            calls = mock_http(monkeypatch)
            first, first_meta = await api.catalogo_custos("milho", return_meta=True)
            second, second_meta = await api.catalogo_custos("arroz", return_meta=True)
            assert len(calls) == 7
            assert len(first) == len(second) == 2
            assert set(first.cultura) == {"milho"}
            assert set(second.cultura) == {"arroz"}
            assert first_meta.source_details["catalog_cache"] == "miss"
            assert second_meta.source_details["catalog_cache"] == "hit"
            assert not first_meta.from_cache and second_meta.from_cache
            assert first_meta.fetched_at == second_meta.fetched_at
            before = first_meta.source_details["manifest"]["acquisition"]
            after = second_meta.source_details["manifest"]["acquisition"]
            assert len(before["resources"]) == 7
            assert after["resources"] == [] and after["received_bytes"] == 0
            assert after["catalog_acquisition"] == before["catalog_acquisition"]
        elif scenario == "test_catalog_cache_respeita_ttl":
            elapsed = parameters["elapsed"]
            expected_calls = parameters["expected_calls"]
            instant = acquisition.now()
            monkeypatch.setattr(acquisition, "now", lambda: instant)
            calls = mock_http(monkeypatch)
            await api.catalogo_custos("milho")
            monkeypatch.setattr(acquisition, "now", lambda: instant + timedelta(seconds=elapsed))
            await api.catalogo_custos("milho")
            assert len(calls) == expected_calls
        elif scenario == "test_catalog_cache_bypass_ignora_leitura_e_gravacao":
            warm = parameters["warm"]
            calls = mock_http(monkeypatch)
            if warm:
                await api.catalogo_custos("milho")
            before = acquisition._catalog_cache

            def forbidden(*_args: object) -> None:
                raise AssertionError("cache accessed during bypass")

            monkeypatch.setattr(acquisition, "_get_cached_catalog", forbidden)
            monkeypatch.setattr(acquisition, "_store_catalog", forbidden)
            frame, meta = await api.catalogo_custos("milho", use_cache=False, return_meta=True)
            assert len(frame) == 2
            assert len(calls) == (14 if warm else 7)
            assert acquisition._catalog_cache is before
            assert meta.source_details["catalog_cache"] == "bypass"
            assert not meta.from_cache
        elif scenario == "test_catalog_cache_copias_independentes":
            warm = parameters["warm"]
            calls = mock_http(monkeypatch)
            if warm:
                await acquisition.Acquisition().catalog("milho")
            first = acquisition.Acquisition()
            resources = await first.catalog("milho")
            original_title = resources[0].titulo
            resources[0].titulo = "alterado"
            first.culturas_catalogo.append("inventada")
            first.catalog_snapshot.receipts[0]["sha256"] = "alterado"
            second = acquisition.Acquisition()
            observed = await second.catalog("milho")
            assert len(calls) == 7
            assert observed[0].titulo == original_title
            assert "inventada" not in second.culturas_catalogo
            assert second.catalog_snapshot.receipts[0]["sha256"] != "alterado"
        elif scenario == "test_catalog_cache_falha_nao_publica_catalogo_parcial":
            mock_http(monkeypatch, pages={acquisition.CATALOG_URL: b"<html>invalido</html>"})
            with pytest.raises(ParseError, match="Catálogo sem área de conteúdo"):
                await api.catalogo_custos()
            assert acquisition._catalog_cache is None
            calls = mock_http(monkeypatch)
            _, meta = await api.catalogo_custos(return_meta=True)
            assert len(calls) == 7
            assert meta.source_details["catalog_cache"] == "miss"


@pytest.mark.parametrize(
    "function_name", ["custo_producao", "custo_producao_total", "catalogo_custos"]
)
async def test_truncated_xlsx_preserves_acquisition(function_name, monkeypatch):
    stream = BytesIO()
    excel = ExcelWorkbook()
    excel.active.append(["Custo", "Valor"])
    excel.save(stream)
    excel.close()
    truncated = stream.getvalue()[:-22]
    mock_http(monkeypatch, workbook=truncated)
    with pytest.raises(ParseError, match="XLSX inválido") as caught:
        await getattr(api, function_name)("soja", planilha=resource().planilha)
    assert isinstance(caught.value.__cause__, zipfile.BadZipFile)
    details = caught.value.conab_custos_acquisition
    receipt = details["resources"][-1]
    assert receipt["role"] == "workbook"
    assert receipt["bytes"] == len(truncated)
    assert receipt["sha256"] == hashlib.sha256(truncated).hexdigest()
    assert receipt["closed"] and receipt["eof"]
    assert details["received_bytes"] >= len(truncated)


@pytest.mark.asyncio
@pytest.mark.parametrize("crop", ["cafe", "café"])
async def test_custo_producao_cafe_hint(monkeypatch, crop):
    calls = mock_http(monkeypatch)
    with pytest.raises(InvalidParameterError, match="Nenhuma planilha") as caught:
        await api.custo_producao(crop, uf="MG")
    assert "cafe_arabica" in str(caught.value)
    assert "cafe_conilon" in str(caught.value)
    assert "soja" not in str(caught.value)
    assert len(calls) == 7


def test_workbook_close_does_not_replace_primary():
    primary = ParseError(source="conab_custo", reason="original", parser_version=3)
    book = object.__new__(Workbook)

    def fail():
        raise OSError("close failed")

    book.excel = SimpleNamespace(close=fail)
    with pytest.raises(ParseError) as caught:
        try:
            raise primary
        finally:
            book.close()
    assert caught.value is primary
    assert "close failed" in primary.conab_workbook_close_error


def test_calamine_sem_formatos_falha_com_erro_de_parser(monkeypatch):
    pytest.importorskip("python_calamine")
    stream = BytesIO()
    excel = ExcelWorkbook()
    excel.active.append(["Custo", 0.25])
    excel.active["B1"].number_format = "0%"
    excel.save(stream)
    excel.close()
    factory = pd.ExcelFile

    def open_excel(data, *, engine=None):
        if engine != "calamine":
            raise ValueError("Falha do leitor primário")
        return factory(data, engine=engine)

    monkeypatch.setattr(pd, "ExcelFile", open_excel)
    with pytest.raises(ParseError, match="formatos de células"):
        Workbook(stream.getvalue())


def test_invalid_numeric_cell_fails_instead_of_skipping():
    sheet, ctx = official()
    sheet.linhas[8][1] = "inválido"
    with pytest.raises(ParseError, match="Medida inválida"):
        parse_selected(sheet, ctx)


@pytest.mark.asyncio
async def test_catalog_ignores_false_folder_and_duplicate_links(monkeypatch):
    soup = BeautifulSoup((GOLDEN / "catalog_tab.html").read_bytes(), "lxml")
    content = soup.select_one("#content-core")
    targets = [
        acquisition.CATALOG_URL + "/milho",
        acquisition.CATALOG_URL + "/ficha",
        acquisition.CATALOG_URL + "/ficha/view",
        acquisition.CATALOG_URL + "/pasta/outra",
        acquisition.CATALOG_URL + "/lista?b_start:int=20",
        acquisition.CATALOG_URL + "/arquivo.xls",
    ]
    for url in targets:
        content.append(soup.new_tag("a", attrs={"class": "internal-link"}, href=url))
    calls = mock_http(
        monkeypatch,
        pages={
            acquisition.TAB_URL: soup.encode("utf-8"),
            acquisition.CATALOG_URL + "/ficha": b"<html><body>Ficha</body></html>",
        },
    )
    df = await api.catalogo_custos()
    assert len(df) == 51
    assert calls.count(acquisition.CATALOG_URL + "/milho") == 1
    assert calls.count(acquisition.CATALOG_URL + "/ficha") == 1
    assert len(calls) == 8


def test_conflicting_filter_with_explicit_sheet_is_rejected():
    _, ctx = official()
    with pytest.raises(InvalidParameterError, match="0 candidatos"):
        select([ctx], models.ConsultaCusto(aba=ctx.aba, ano=2024))


def test_contract_rejects_changed_projection_and_empty_is_typed():
    empty = CONAB_CUSTOS_V3.empty_frame()
    assert CONAB_CUSTOS_V3.validate(empty) == (True, [])
    assert not CONAB_CUSTOS_V3.validate(empty[list(reversed(empty.columns))])[0]


@pytest.mark.parametrize(
    "funcao",
    [api.custo_producao, api.custo_producao_total, api.catalogo_custos, datasets.custo_producao],
)
def test_assinatura_usa_produto_e_flags_nomeadas(funcao):
    parametros = inspect.signature(funcao).parameters
    assert next(iter(parametros)) == "produto"
    flags = [nome for nome in ("as_polars", "return_meta") if nome in parametros]
    assert flags
    assert all(parametros[nome].kind is inspect.Parameter.KEYWORD_ONLY for nome in flags)


@pytest.mark.asyncio
async def test_catalogo_de_contextos_tem_data_em_datetime_e_texto_no_padrao(monkeypatch):
    mock_http(monkeypatch)
    df = await api.catalogo_custos("algodao", planilha=resource("algodao").planilha)
    assert str(df["data_referencia"].dtype) == "datetime64[ns]"
    assert df["data_referencia"].notna().any()
    assert df["aba"].dtype == pd.Series(["Barreiras-BA-2025"]).dtype
