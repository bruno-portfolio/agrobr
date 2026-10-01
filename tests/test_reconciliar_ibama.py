from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pandas as pd
import pytest

pytest.importorskip("shapely")
reconciliacao = pytest.importorskip("scripts.reconciliar_ibama")


@pytest.mark.parametrize(
    "coluna,origem", [("data_embargo", "DAT_EMBARGO"), ("data_desembargo", "DAT_DESEMBARGO")]
)
@pytest.mark.parametrize(
    ("bruto", "publicado", "status"),
    [
        ("2925-09-25 15:10:00", None, "ok"),
        ("2090-03-16 18:10:00", None, "ok"),
        ("2080-01-10 09:52:00", None, "ok"),
        ("2063-06-20 01:17:02", None, "ok"),
        ("0026-03-11 00:00:00", None, "ok"),
        ("1667-05-31 11:10:00", None, "ok"),
        ("2026-09-30 23:59:59", "2026-09-30 23:59:59", "ok"),
        ("2026-10-01 00:00:00", None, "ok"),
        ("2026-09-29 10:00:00", "2026-09-28 10:00:00", "mismatch"),
        ("2026-09-29 10:00:00", None, "mismatch"),
        ("2026-02-31 00:00:00", None, "ok"),
        ("", None, "ok"),
    ],
)
def test_comparar_datas_pela_edicao_da_fonte(monkeypatch, coluna, origem, bruto, publicado, status):
    linha = {nome: "" for _, nome, _ in reconciliacao.COLUNAS}
    linha.update(ULTIMA_ATUALIZACAO_RELATORIO="2026-09-30 00:26:27")
    linha[origem] = bruto
    registro = dict.fromkeys(nome for nome, _, _ in reconciliacao.COLUNAS)
    registro["cancelado"] = False
    registro[coluna] = pd.Timestamp(publicado) if publicado else pd.NaT
    frame = pd.DataFrame([registro])
    meta = SimpleNamespace(
        source_url=reconciliacao.CSV_URL,
        source_details={"ultima_atualizacao_relatorio": "2099-12-31 00:00:00"},
    )
    monkeypatch.setattr(reconciliacao, "saida_agrobr", mock.AsyncMock(return_value=(frame, meta)))

    resultado = reconciliacao.comparar([linha], False, {})

    assert resultado["status"] == status
    assert resultado["problems"] == ([] if status == "ok" else ["1 linhas divergentes"])
