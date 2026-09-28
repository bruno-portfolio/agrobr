from __future__ import annotations

import json
import warnings
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.anec import api, client, models, parser
from tests.helpers import sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/anec/rotulos_20260926"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))


def periodos_do_pdf(conteudo: bytes) -> tuple[int, int, dict[str, tuple[date | None, date | None]]]:
    paginas = [parser._group_by_row(palavras) for palavras in parser._extract_pages_words(conteudo)]
    semanal = paginas[parser._find_page_with_header(paginas, parser.HEADER_WEEKLY)]
    cabecalho = next(
        y
        for y, linha in semanal
        if any(palavra["text"].upper() == "PORT" for palavra in linha)
        and any(palavra["text"].lower() == "soybean" for palavra in linha)
    )
    return parser._weekly_periods(semanal, cabecalho)


@pytest.mark.parametrize("edicao", sorted(MANIFESTO["boletins"]))
def test_semanas_do_boletim_ficam_a_ate_7_dias_da_edicao(edicao):
    boletim = MANIFESTO["boletins"][edicao]
    with sem_excecao():
        ano, semana, periodos = periodos_do_pdf((GOLDEN / boletim["pdf"]).read_bytes())
    assert (ano, semana) == tuple(int(parte) for parte in edicao.split("-"))
    assert periodos == {
        periodo: tuple(date.fromisoformat(dia) for dia in esperado) if esperado else (None, None)
        for periodo, esperado in boletim["esperado"].items()
    }


async def test_embarques_da_semana_35_de_2026_avisam_o_rotulo_errado():
    boletim = MANIFESTO["boletins"]["2026-35"]
    artigo = models.ANECArticle(
        id=693,
        cuid="cmtvv51ul180828otxdi95xatq",
        title_en="ANEC - 35.2026 Accumulated Exports",
        slug_en="anec-352026-accumulated-exports",
        created_at=datetime(2026, 9, 10, 18, 30, 53, tzinfo=UTC),
        pdf_url=boletim["recibo"]["url"],
        media_updated_at=datetime(2026, 9, 10, 18, 30, 5, tzinfo=UTC),
    )
    aquisicao = client.Aquisicao(
        (GOLDEN / boletim["pdf"]).read_bytes(),
        artigo.pdf_url,
        False,
        datetime(2026, 9, 25, 22, 56, tzinfo=UTC),
        {},
    )
    with (
        patch("agrobr.anec.client._acquire_latest", AsyncMock(return_value=(aquisicao, artigo))),
        warnings.catch_warnings(record=True) as avisos,
    ):
        warnings.simplefilter("always")
        frame, meta = await api.embarques(ano=2026, use_cache=False, return_meta=True)
    mensagem = (
        "Os rótulos das duas semanas do boletim 35/2026 não formam semanas consecutivas de 7 dias "
        "a até 7 dias da semana da edição; data_inicio e data_fim ficam nulas."
    )
    assert mensagem in [str(aviso.message) for aviso in avisos]
    assert mensagem in meta.validation_warnings
    assert frame["data_inicio"].isna().all() and frame["data_fim"].isna().all()
    assert set(frame["semana"]) == {35}
