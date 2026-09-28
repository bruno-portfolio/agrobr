from __future__ import annotations

import hashlib
import json
from unittest.mock import Mock

import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.conab.progresso import api, client, parser
from agrobr.exceptions import SourceUnavailableError
from tests import helpers

GOLDEN = helpers.PROGRESSO_HISTORICAL_GOLDEN
METADATA = json.loads((GOLDEN / "metadata.json").read_text(encoding="utf-8"))
EXPECTED = json.loads((GOLDEN / "expected.json").read_text(encoding="utf-8"))


@pytest.fixture
def pages():
    return {
        METADATA["week_url"]: (GOLDEN / "week.html").read_bytes(),
        METADATA["metadata_url"]: (GOLDEN / "metadata.html").read_bytes(),
        METADATA["download_url"]: (GOLDEN / "response.xlsx").read_bytes(),
    }


@pytest.mark.parametrize("explicit_week", [True, False])
async def test_boletim_real_via_ficha_preserva_registros_e_contexto(
    monkeypatch, pages, explicit_week
):
    week_url = METADATA["week_url"]
    pages[client.BASE_URL] = f'<a href="{week_url}">Acompanhamento das Lavouras</a>'.encode()
    calls = helpers.mock_progresso_http(monkeypatch, pages)
    frame, meta = await api.progresso_safra(
        cultura=None, semana_url=week_url if explicit_week else None, return_meta=True
    )
    assert len(frame) == EXPECTED["total_records"]
    assert frame.columns.tolist() == EXPECTED["columns"]
    assert set(frame.cultura) == set(EXPECTED["cultures"])
    assert frame.semana_atual.eq(EXPECTED["week"]).all()
    assert meta.source_url == METADATA["download_url"]
    assert meta.records_count == len(frame)
    assert meta.selected_source == "conab_govbr"
    assert meta.schema_version == "2.0"
    contracts.validate_dataset(frame, "progresso_safra")
    assert hashlib.sha256(pages[METADATA["download_url"]]).hexdigest() == EXPECTED["sha256"]
    for sample in EXPECTED["samples"]:
        value = frame.loc[
            frame.cultura.eq(sample["culture"])
            & frame.estado.eq(sample["state"])
            & frame.operacao.eq(sample["operation"]),
            "pct_semana_atual",
        ].item()
        assert value == pytest.approx(sample["value"]), sample["value_cell"]
    expected_calls = [week_url, METADATA["metadata_url"], METADATA["download_url"]]
    assert calls == ([] if explicit_week else [client.BASE_URL]) + expected_calls


async def test_percentual_revisado_real_preserva_valor_e_marca_no_dataset(monkeypatch, pages):
    helpers.mock_progresso_http(monkeypatch, pages)
    frame, meta = await datasets.progresso_safra(
        "trigo", semana_url=METADATA["week_url"], return_meta=True
    )
    revised = frame.loc[frame.estado.eq("BA")].iloc[0]
    unchanged = frame.loc[frame.estado.eq("GO")].iloc[0]
    assert revised.pct_semana_anterior == EXPECTED["revision_cell"]["value"]
    assert bool(revised.revisado) is True
    assert bool(unchanged.revisado) is False
    assert str(frame.revisado.dtype) == "boolean"
    assert meta.schema_version == meta.contract_version == "2.0"
    contract = contracts.get_contract("progresso_safra")
    assert contract.validate(frame)[0]
    assert contract.validate(frame.drop(columns=["revisado"]))[0]


@pytest.mark.parametrize(
    "value, expected, revised",
    [(" 10% *  ", 0.1, True), ("10%", 0.1, False), (None, None, None), ("*", None, None)],
)
def test_revisado_distingue_valor_com_sem_nota_e_linha_sem_numero(value, expected, revised):
    raw = helpers.progresso_xlsx_cells({"C113": None, "D113": value, "E113": None, "F113": None})
    frame = parser.parse_progresso_xlsx(raw)
    row = frame.loc[frame.cultura.eq("Trigo") & frame.estado.eq("BA")].iloc[0]
    if expected is None:
        assert pd.isna(row.pct_semana_anterior) and pd.isna(row.revisado)
    else:
        assert row.pct_semana_anterior == pytest.approx(expected)
        assert bool(row.revisado) is revised


@pytest.mark.parametrize("links", ["", '<a href="one.xlsx">one</a><a href="two.xlsx">two</a>'])
async def test_ficha_sem_download_unico_falha_nominalmente(monkeypatch, pages, links):
    pages[METADATA["metadata_url"]] = f'<div id="content-core">{links}</div>'.encode()
    helpers.mock_progresso_http(monkeypatch, pages)
    read = Mock(side_effect=AssertionError("parser não deve ser chamado"))
    monkeypatch.setattr(parser, "parse_progresso_xlsx", read)
    with pytest.raises(SourceUnavailableError, match="downloads de planilha candidatos") as caught:
        await api.progresso_safra(semana_url=METADATA["week_url"])
    assert caught.value.url == METADATA["metadata_url"]
    read.assert_not_called()


@pytest.mark.parametrize("stage", ["metadata_url", "download_url"])
async def test_http_404_da_ficha_ou_arquivo_preserva_erro_tipado(monkeypatch, pages, stage):
    url = METADATA[stage]
    helpers.mock_progresso_http(monkeypatch, pages, statuses={url: 404})
    with pytest.raises(SourceUnavailableError, match="HTTP 404") as caught:
        await api.progresso_safra(semana_url=METADATA["week_url"])
    assert caught.value.url == url
