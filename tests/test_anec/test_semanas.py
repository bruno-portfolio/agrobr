from __future__ import annotations

import json
import warnings
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.anec import api, client, models, parser
from agrobr.exceptions import ParseError
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data/anec/semanas_20260925"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))


def _linhas(*textos: str) -> list[tuple[float, list[dict]]]:
    return [
        (
            float(y),
            [
                {"text": parte, "x0": float(x), "top": float(y)}
                for x, parte in enumerate(texto.split())
            ],
        )
        for y, texto in enumerate(textos)
    ]


@pytest.mark.parametrize("semana", sorted(MANIFESTO["transcricao_manual"]["boletins"]))
def test_datas_saem_dos_rotulos_transcritos_do_boletim(semana):
    boletim = MANIFESTO["transcricao_manual"]["boletins"][semana]
    report = parser.parse_anec_pdf((GOLDEN / boletim["pdf"]).read_bytes())
    semanal = report.weekly_shipments
    assert set(semanal["semana"]) == {int(semana)}
    assert set(semanal["ano"]) == {2026}
    for periodo, esperado in boletim["periodos"].items():
        linhas = semanal[semanal["periodo"] == periodo]
        assert linhas["data_inicio"].dt.date.unique().tolist() == [
            date.fromisoformat(esperado["data_inicio"])
        ]
        assert linhas["data_fim"].dt.date.unique().tolist() == [
            date.fromisoformat(esperado["data_fim"])
        ]


@pytest.mark.parametrize(
    ("rotulos", "edicao", "esperado"),
    [
        (
            "(27th to 02nd Jan ) (03rd to 09th Jan ) (metric tons)",
            "Week 01/2027",
            ((date(2026, 12, 27), date(2027, 1, 2)), (date(2027, 1, 3), date(2027, 1, 9))),
        ),
        (
            "(20th to 26th Dec ) (27th to 02nd Jan ) (metric tons)",
            "Week 52/2026",
            ((date(2026, 12, 20), date(2026, 12, 26)), (date(2026, 12, 27), date(2027, 1, 2))),
        ),
    ],
)
def test_semana_da_virada_de_ano_pega_o_ano_pela_edicao(rotulos, edicao, esperado):
    ano, semana, periodos = parser._weekly_periods(_linhas(rotulos, "PORT Soybean", edicao), 1.0)
    assert (semana, ano) == tuple(int(parte) for parte in edicao.split()[1].split("/"))
    assert (periodos["last_week"], periodos["current_week"]) == esperado


def test_rotulos_que_nao_formam_semanas_seguidas_saem_nulos():
    linhas = _linhas(
        "(06th to 12th Sep ) (27th to 03rd Oct ) (metric tons)", "PORT", "Week 36/2026"
    )
    assert parser._weekly_periods(linhas, 1.0) == (
        2026,
        36,
        {"last_week": (None, None), "current_week": (None, None)},
    )


@pytest.mark.parametrize(
    "textos",
    [
        ("(06th to 12th Sep ) (metric tons)", "PORT", "Week 36/2026"),
        ("(06th to 12th Sep ) (13th to 19th Sep )", "PORT", "Semana 36"),
        ("(06th to 12th Xyz ) (13th to 19th Sep )", "PORT", "Week 36/2026"),
    ],
)
def test_boletim_sem_rotulos_ou_sem_semana_recusa(textos):
    with levanta_exatamente(ParseError, "rótulos das duas semanas"):
        parser._weekly_periods(_linhas(*textos), 1.0)


async def test_embarques_avisa_quando_as_datas_ficam_nulas():
    artigo = models.ANECArticle(
        id=1,
        cuid="w36",
        title_en="ANEC - 36.2026",
        slug_en="week36",
        created_at=datetime(2026, 9, 16, tzinfo=UTC),
        pdf_url="https://www.anec.com.br/uploads/w36.pdf",
        media_updated_at=datetime(2026, 9, 16, tzinfo=UTC),
    )
    semanal = pd.DataFrame(
        [
            {
                "porto": "SANTOS",
                "produto": "soybean",
                "periodo": "last_week",
                "valor_ton": 1.0,
                "ano": 2026,
                "semana": 36,
                "data_inicio": pd.NaT,
                "data_fim": pd.NaT,
            }
        ]
    )
    report = parser.ParsedReport(semanal, pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), "x")
    aquisicao = client.Aquisicao(
        b"pdf", artigo.pdf_url, False, datetime(2026, 9, 17, tzinfo=UTC), {}
    )
    with (
        patch("agrobr.anec.client._acquire_latest", AsyncMock(return_value=(aquisicao, artigo))),
        patch("agrobr.anec.parser.parse_anec_pdf", return_value=report),
        warnings.catch_warnings(record=True) as avisos,
    ):
        warnings.simplefilter("always")
        _frame, meta = await api.embarques(ano=2026, use_cache=False, return_meta=True)
        _dados, meta_dataset = await datasets.embarques_anec(
            ano=2026, use_cache=False, return_meta=True
        )
    mensagens = [str(aviso.message) for aviso in avisos if "consecutivas" in str(aviso.message)]
    assert [mensagem.split(" não formam")[0] for mensagem in mensagens] == [
        "Os rótulos das duas semanas do boletim 36/2026"
    ] * 2
    assert mensagens[0] in meta.validation_warnings
    assert meta.schema_version == "1.1"
    assert meta_dataset.contract_version == "1.1"


def test_leitura_com_dia_que_nao_existe_no_mes_fica_de_fora():
    linhas = _linhas(
        "(31st to 06th Jun ) (07th to 13th Jun ) (metric tons)", "PORT", "Week 22/2026"
    )
    assert parser._weekly_periods(linhas, 1.0) == (
        2026,
        22,
        {
            "last_week": (date(2026, 5, 31), date(2026, 6, 6)),
            "current_week": (date(2026, 6, 7), date(2026, 6, 13)),
        },
    )
