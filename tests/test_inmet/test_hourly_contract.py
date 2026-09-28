from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from agrobr import datasets, inmet
from agrobr.exceptions import ParseError
from agrobr.inmet import client, parser
from tests.test_inmet.test_api_observation_scope import observation


async def test_clima_horario_aceita_observacoes_oficiais_com_sufixo_utc(monkeypatch):
    path = Path(__file__).parents[1] / "golden_data/inmet/observacoes_sample/response.json"
    rows = json.loads(path.read_text(encoding="utf-8"))
    source = AsyncMock(return_value=rows)
    fallback = AsyncMock(side_effect=AssertionError("Histórico não deve ser necessário"))
    monkeypatch.setattr(client, "fetch_dados_estacao", source)
    monkeypatch.setattr(inmet, "historico_periodo", fallback)
    frame = await datasets.clima(
        estacao=rows[0]["CD_ESTACAO"],
        inicio=min(row["DT_MEDICAO"] for row in rows),
        fim=max(row["DT_MEDICAO"] for row in rows),
        agregacao="horario",
    )
    assert frame.hora_utc.tolist() == ["0000", "0100", "0200"]
    source.assert_awaited_once()
    fallback.assert_not_awaited()


@pytest.mark.parametrize("hour", ["2400 UTC", "1230 UTC", None, 1200, "inválida"])
def test_observacao_com_hora_invalida_falha_no_parser(hour):
    with pytest.raises(ParseError, match="Hora UTC inválida"):
        parser.parse_observacoes([observation(HR_MEDICAO=hour)])
