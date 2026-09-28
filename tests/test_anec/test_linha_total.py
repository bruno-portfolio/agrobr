from __future__ import annotations

import hashlib
import json
import warnings
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.anec import api, client, models, parser
from agrobr.utils.warnings import warn_once_reset
from tests.helpers import sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/anec"
MANIFESTO = json.loads((GOLDEN / "linha_total_20260926/manifest.json").read_text(encoding="utf-8"))


def _corpo(edicao: str) -> bytes:
    boletim = MANIFESTO["boletins"][edicao]
    corpo = (GOLDEN / boletim["pdf"]).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == boletim["sha256"]
    return corpo


def _semanal(frame: pd.DataFrame) -> dict[str, dict[str, float | None]]:
    return {
        porto: {
            f"{linha.periodo}/{linha.produto}": None
            if pd.isna(linha.valor_ton)
            else float(linha.valor_ton)
            for linha in grupo.itertuples()
        }
        for porto, grupo in frame.groupby("porto", sort=False)
    }


@pytest.mark.parametrize("edicao", ["2026-36", "2025-14", "2025-09"])
def test_tabela_semanal_confere_com_o_oraculo_pdfium(edicao):
    boletim = MANIFESTO["boletins"][edicao]
    with sem_excecao():
        relatorio = parser.parse_anec_pdf(_corpo(edicao))

    assert _semanal(relatorio.weekly_shipments) == boletim["semanal"]
    assert relatorio.avisos_da_linha_total == ()


async def test_semana_36_de_2026_fecha_com_a_linha_total_sem_aviso_falso():
    boletim = MANIFESTO["boletins"]["2026-36"]
    artigo = models.ANECArticle.model_validate(boletim["artigo"])
    aquisicao = client.Aquisicao(
        _corpo("2026-36"),
        artigo.pdf_url,
        False,
        datetime.fromisoformat(boletim["recibo"]["fetched_at"]),
        {},
    )
    warn_once_reset()
    with (
        patch.object(api.client, "list_articles", new_callable=AsyncMock, return_value=[artigo]),
        patch.object(api.client, "_acquire_pdf", new_callable=AsyncMock, return_value=aquisicao),
        warnings.catch_warnings(record=True) as avisos,
        sem_excecao(),
    ):
        warnings.simplefilter("always")
        frame, meta = await api.embarques(ano=2026, semana=36, use_cache=False, return_meta=True)
        _, meta_dataset = await datasets.embarques_anec(
            ano=2026, semana=36, produto="farelo", use_cache=False, return_meta=True
        )

    somas = frame.groupby(["periodo", "produto"])["valor_ton"].sum()
    portos = frame.dropna(subset=["valor_ton"]).groupby(["periodo", "produto"]).size()
    for coluna, impresso in boletim["linha_total"].items():
        periodo, produto = coluna.split("/")
        soma = float(somas.get((periodo, produto), 0.0))
        assert abs(soma - (impresso or 0.0)) <= 0.5 * (portos.get((periodo, produto), 0) + 1), (
            coluna
        )
    assert boletim["linha_total"]["last_week/soybean_meal"] == 372958
    assert boletim["linha_total"]["current_week/ddgs"] == 58300
    assert meta.validation_warnings == []
    assert meta_dataset.validation_warnings == []
    assert [str(a.message) for a in avisos if "linha TOTAL" in str(a.message)] == []
