from __future__ import annotations

import json
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.anec import api, client, models, parser
from agrobr.exceptions import ParseError
from agrobr.utils.warnings import warn_once_reset
from tests.helpers import levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/anec"
MANIFESTO = json.loads(
    (GOLDEN / "quatro_produtos_20260926/manifest.json").read_text(encoding="utf-8")
)
QUATRO_PRODUTOS = {"soybean", "soybean_meal", "maize", "wheat"}


async def _consulta(funcao, edicao, **filtros):
    boletim = MANIFESTO["boletins"][edicao]
    artigo = models.ANECArticle.model_validate(boletim["artigo"])
    aquisicao = client.Aquisicao(
        (GOLDEN / boletim["pdf"]).read_bytes(),
        artigo.pdf_url,
        False,
        datetime.fromisoformat(boletim["recibo"]["fetched_at"]),
        {},
    )
    with (
        patch.object(api.client, "list_articles", new_callable=AsyncMock, return_value=[artigo]),
        patch.object(api.client, "_acquire_pdf", new_callable=AsyncMock, return_value=aquisicao),
    ):
        return await funcao(ano=2026, semana=artigo.week_year[0], use_cache=False, **filtros)


def _avisos_da_linha_total(avisos):
    return [str(aviso.message) for aviso in avisos if "linha TOTAL" in str(aviso.message)]


@pytest.mark.parametrize("edicao", ["2026-01", "2026-02"])
async def test_semanas_de_4_produtos_fecham_com_a_linha_total_publicada(edicao):
    boletim = MANIFESTO["boletins"][edicao]
    warn_once_reset()
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await _consulta(api.embarques, edicao, return_meta=True)
    somas = frame.groupby(["periodo", "produto"])["valor_ton"].sum()
    assert {
        f"{periodo}/{produto}": float(valor) for (periodo, produto), valor in somas.items()
    } == {coluna: float(valor) for coluna, valor in boletim["linha_total"].items()}
    santos = frame[(frame["porto"] == "SANTOS") & (frame["produto"] == "soybean")]
    assert {
        linha.periodo: [str(linha.data_inicio.date()), str(linha.data_fim.date()), linha.valor_ton]
        for linha in santos.itertuples()
    } == boletim["santos_soja"]
    assert meta.validation_warnings == []
    assert _avisos_da_linha_total(avisos) == []


@pytest.mark.parametrize("edicao", ["2026-01", "2026-02"])
async def test_mensal_das_semanas_de_4_produtos_le_os_produtos_pelo_nome(edicao):
    boletim = MANIFESTO["boletins"][edicao]
    with sem_excecao():
        frame = await _consulta(api.embarques_mensais, edicao)
    janeiro = frame[(frame["ano"] == 2025) & (frame["mes"] == 1)]
    assert (
        dict(zip(janeiro["produto"], janeiro["valor_ton"], strict=True))
        == boletim["mensal_janeiro_2025"]
    )
    assert janeiro["valor_ton"].sum() == boletim["total_products_impresso"]
    assert set(frame["produto"]) == QUATRO_PRODUTOS


async def test_coluna_sem_nome_de_produto_da_semana_14_recusada():
    with levanta_exatamente(ParseError, match="cada período precisa dos mesmos produtos nomeados"):
        await _consulta(api.embarques, "2026-14")


async def test_porto_perdido_na_leitura_vira_aviso_da_linha_total(monkeypatch):
    original = parser.resolve_port
    monkeypatch.setattr(
        parser,
        "resolve_port",
        lambda texto: None if texto.startswith("SÃO LUIS") else original(texto),
    )
    esperado = (
        "ANEC: no boletim 1/2026, a soma dos portos em soybean (last_week) dá 467757 t, e a linha "
        "TOTAL publicada, 524158 t; o agrobr repassa os valores por porto."
    )
    warn_once_reset()
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await _consulta(api.embarques, "2026-01", produto="soja", return_meta=True)
    assert set(frame["produto"]) == {"soybean"}
    assert meta.validation_warnings == [esperado]
    assert _avisos_da_linha_total(avisos) == [esperado]


@pytest.mark.parametrize(
    "pdf", ["weekly_w34_2026/response.pdf", "rotulos_20260926/weekly_w35_2026.pdf"]
)
def test_arredondamento_da_linha_total_nao_vira_aviso(pdf):
    with sem_excecao():
        relatorio = parser.parse_anec_pdf((GOLDEN / pdf).read_bytes())
    assert relatorio.avisos_da_linha_total == ()
