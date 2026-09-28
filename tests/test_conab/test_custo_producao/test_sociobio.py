from __future__ import annotations

import json
from datetime import datetime

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from agrobr import conab, constants, datasets
from agrobr.conab.custo_producao import (
    _acquisition,
    _sociobio_api,
    _sociobio_context,
    _sociobio_parse,
    models,
)
from agrobr.conab.custo_producao._sociobio_workbook import WorkbookSociobio
from agrobr.contracts import conab_custos
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError, ParseError
from tests import helpers


@pytest.fixture
def old_sheet():
    book = WorkbookSociobio((helpers.SOCIOBIO_GOLDEN / "acai.xlsx").read_bytes())
    try:
        return book.read("Codajás-AM-2008")
    finally:
        book.close()


EXPECTED = json.loads((helpers.SOCIOBIO_GOLDEN / "expected.json").read_text(encoding="utf-8"))


async def test_catalogo_completo_e_cache_isolado_preservam_recurso_ativo(monkeypatch):
    calls = helpers.mock_sociobio_http(monkeypatch)
    frame, first = await conab.catalogo_sociobiodiversidade(return_meta=True)
    repeat, second = await conab.catalogo_sociobiodiversidade(return_meta=True)
    pd.testing.assert_frame_equal(frame, repeat)
    assert len(frame) == EXPECTED["resource_count"]
    active = frame.loc[frame.ativo].set_index("produto").planilha.to_dict()
    assert active == {row["canonical"]: row["planilha"] for row in EXPECTED["products"]}
    assert sorted(frame.produto.unique()) == sorted(
        datasets.list_products("custo_sociobiodiversidade")
    )
    assert len(calls) == 3
    assert first.from_cache is False
    assert second.from_cache is True
    assert _acquisition._get_cached_catalog("agricolas") is None
    resources = await _acquisition.Acquisition(family="sociobiodiversidade").catalog("acai")
    assert _sociobio_api._resource(resources, None).planilha == active["acai"]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"produto": "soja"},
        {"produto": "acai", "ano": True},
        {"produto": "acai", "uf": "XX"},
        {"produto": "acai", "use_cache": 1},
        {"produto": "acai", "return_meta": 1},
        {"produto": "acai", "as_polars": 1},
    ],
)
async def test_validacao_previa_nao_consulta_rede(monkeypatch, kwargs):
    calls = helpers.mock_sociobio_http(monkeypatch)
    with helpers.collect_failures() as check:
        with check("erro"), pytest.raises(InvalidParameterError):
            await conab.custo_sociobiodiversidade(**kwargs)
        with check("rede"):
            assert not calls


async def test_snapshot_recusado_antes_da_rede(monkeypatch):
    calls = helpers.mock_sociobio_http(monkeypatch)
    async with deterministic("2025-01-01"):
        with pytest.raises(InvalidParameterError, match="snapshot"):
            await conab.custo_sociobiodiversidade("acai")
    assert not calls


async def test_regiao_identificada_nao_descarta_terceira_medida_monetaria(monkeypatch):
    helpers.mock_sociobio_http(monkeypatch)
    inventory = await conab.catalogo_sociobiodiversidade("piacava")
    row = inventory.loc[inventory.aba.eq("Belmonte-BA-2016")].iloc[0]
    assert str(row.local) == "Litoral Sul e Extremo Sul da Bahia"
    assert str(row.uf) == "BA"
    assert json.loads(row.anos_publicados) == [2016]
    assert row.status == "unresolved"
    assert "Cabeçalhos monetários não reconhecidos: 3" in row.error
    with pytest.raises(ParseError, match="Cabeçalhos monetários não reconhecidos: 3") as caught:
        await conab.custo_sociobiodiversidade("piacava", aba=row.aba)
    recursos = caught.value.conab_custos_acquisition["resources"]
    assert [recurso["role"] for recurso in recursos] == ["workbook_metadata", "workbook"]


async def test_consulta_ambigua_nao_omite_abas_recusadas(monkeypatch):
    helpers.mock_sociobio_http(monkeypatch)
    with pytest.raises(ParseError, match="Amêndoa-Iporá-GO-2010"):
        await conab.custo_sociobiodiversidade("baru", uf="GO", ano=2010)


def test_contrato_rejeita_coluna_extra_ordem_e_infinito(old_sheet):
    resource = models.RecursoCusto(
        cultura="acai",
        planilha="literal",
        titulo="literal",
        pagina_url="https://www.gov.br/conab/literal",
    )
    context = _sociobio_context.context(old_sheet, resource, 0)
    parsed = _sociobio_parse.frame(_sociobio_parse.parse_selected(old_sheet, context).observacoes)
    for changed in [
        parsed.assign(extra=1),
        parsed[list(reversed(parsed.columns))],
        parsed.assign(valor=float("inf")),
    ]:
        valid, errors = conab_custos.CONAB_SOCIOBIO_V1.validate(changed)
        assert not valid and errors


@pytest.mark.parametrize("value", [True, float("inf"), "sem valor legível"])
def test_medida_invalida_nao_vira_zero_ou_nulo(old_sheet, value):
    old_sheet.linhas[9][1] = value
    resource = models.RecursoCusto(
        cultura="acai",
        planilha="literal",
        titulo="literal",
        pagina_url="https://www.gov.br/conab/literal",
    )
    context = _sociobio_context.context(old_sheet, resource, 0)
    with pytest.raises(ParseError, match="Medida inválida.*R10C2"):
        _sociobio_parse.parse_selected(old_sheet, context)


@pytest.mark.parametrize("year", [2008, 2024])
async def test_api_publica_aceite_sem_escolher_ano_do_arquivo(monkeypatch, year):
    calls = helpers.mock_sociobio_http(monkeypatch)
    frame, meta = await conab.custo_sociobiodiversidade("Açaí", uf="AM", ano=year, return_meta=True)
    assert frame.ano.eq(year).all()
    assert frame.planilha.eq("acai_serie_historica_2008-2025.xlsx").all()
    assert meta.raw_content_hash == EXPECTED["workbooks"]["acai_2025.xlsx"]["sha256"]
    assert meta.raw_content_size == (helpers.SOCIOBIO_GOLDEN / "acai_2025.xlsx").stat().st_size
    assert meta.selected_source == "conab_sociobio"
    assert meta.fetch_timestamp.tzinfo is not None
    assert len(calls) == 5
    if year == 2024:
        row = frame.loc[
            (frame.aba == "Boca do Acre-AM-2024") & (frame.item == "6 - Mão de obra")
        ].iloc[0]
        assert row.valor == 26103
        assert row.unidade_valor == "CUSTO POR SAFRA"
        assert row.produtividade == 17920
        assert row.unidade_produtividade == "kg"
    else:
        assert frame.aba.eq("Codajás-AM-2008").all()
        assert frame.produtividade.eq(1500).all()
        assert frame.unidade_produtividade.eq("kg/ha").all()
        assert frame.data_precos.eq(datetime(2008, 7, 11)).all()
        assert frame.unidade_valor.eq("R$/safra").all()


async def test_recursos_ativos_ambiguos_nao_escolhem_maior_ano(monkeypatch):
    original = (helpers.SOCIOBIO_GOLDEN / "catalog_tab.html").read_bytes()
    extra = (
        b'<a href="'
        + constants.CONAB_SOCIOBIO_CATALOG_URL.encode()
        + b'/acai_serie_historica_2008-2024.xlsx">Old</a>'
    )
    soup = BeautifulSoup(original, "lxml")
    soup.select_one("#content-core").append(BeautifulSoup(extra, "lxml").a)
    helpers.mock_sociobio_http(
        monkeypatch, {constants.CONAB_SOCIOBIO_TAB_URL: str(soup).encode("utf-8")}
    )
    with pytest.raises(InvalidParameterError, match="2 candidatas"):
        await conab.custo_sociobiodiversidade("acai", ano=2024)


@pytest.mark.parametrize("place", ["LOCAL: Codajás - ZZ", "LOCAL: Codajás (ZZ)"])
def test_uf_publicada_invalida_nao_e_substituida_pelo_nome(old_sheet, place):
    old_sheet.linhas[3][0] = place
    resource = models.RecursoCusto(
        cultura="acai", planilha="literal", titulo="literal", pagina_url="https://www.gov.br/conab/"
    )
    with pytest.raises(ParseError, match="UF"):
        _sociobio_context.context(old_sheet, resource, 0)


def test_local_sem_uf_no_cabecalho_e_nome_continua_recusado(old_sheet):
    old_sheet.nome = "Codajás-2008"
    old_sheet.linhas[3][0] = "LOCAL: Codajás"
    resource = models.RecursoCusto(
        cultura="acai", planilha="literal", titulo="literal", pagina_url="https://www.gov.br/conab/"
    )
    with pytest.raises(ParseError, match="Local/UF não reconhecido"):
        _sociobio_context.context(old_sheet, resource, 0)


@pytest.mark.parametrize("value", [0, "0", "0,00"])
def test_celula_orfa_zero_nao_e_descartada(old_sheet, value):
    old_sheet.linhas[9].append(value)
    resource = models.RecursoCusto(
        cultura="acai",
        planilha="literal",
        titulo="literal",
        pagina_url="https://www.gov.br/conab/literal",
    )
    context = _sociobio_context.context(old_sheet, resource, 0)
    with pytest.raises(ParseError, match="fora dos cabeçalhos mapeados"):
        _sociobio_parse.parse_selected(old_sheet, context)


async def test_catalogo_contextos_mantem_recusas_nominais(monkeypatch):
    helpers.mock_sociobio_http(monkeypatch)
    frame = await conab.catalogo_sociobiodiversidade("acai")
    expected = EXPECTED["workbooks"]["acai_2025.xlsx"]
    assert len(frame.loc[frame.status == "identified"]) == expected["identified_count"]
    assert set(frame.loc[frame.status == "unresolved", "aba"]) == {
        r["aba"] for r in expected["unresolved"]
    }
    assert frame.loc[frame.aba == "Igarapé-Miri-PA-2008", "status"].item() == "identified"
    tipos = frame.dtypes[["ano", "indice_aba", "produtividade", "data_precos"]]
    assert tipos.astype(str).tolist() == ["Int64", "Int64", "float64", "datetime64[ns]"]
