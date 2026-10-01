from __future__ import annotations

from unittest import mock

import httpx
import pandas as pd
import pytest

pytest.importorskip("shapely")
reconciliacao = pytest.importorskip("scripts.reconciliar_funai")


@pytest.mark.parametrize(
    ("bruto", "publicado", "status"),
    [
        ("05/09/2023", "2023-09-05", "ok"),
        ("05/09/2023", "2023-05-09", "mismatch"),
        ("05/09/2023", None, "mismatch"),
        ("", None, "ok"),
        ("31/02/2023", None, "ok"),
        ("31/12/1899", None, "ok"),
        ("01/01/2100", None, "ok"),
        ("01/01/1900", "1900-01-01", "ok"),
        ("31/12/2099", "2099-12-31", "ok"),
    ],
)
def test_comparar_data_publicada(monkeypatch, bruto, publicado, status):
    linha = dict.fromkeys(reconciliacao.PRINCIPAIS, "")
    linha["data_atualizacao"] = bruto
    registro = dict.fromkeys(reconciliacao.PRINCIPAIS.values())
    registro["data_atualizacao"] = pd.Timestamp(publicado) if publicado else pd.NaT
    registro["feature_id"] = "tis_poligonais.101"
    frame = pd.DataFrame([registro])
    monkeypatch.setattr(reconciliacao, "atributos", lambda _cliente: (list(linha), "geom"))
    monkeypatch.setattr(reconciliacao, "linhas_oficiais", lambda *_args: [linha])
    monkeypatch.setattr(reconciliacao, "saida_agrobr", mock.AsyncMock(return_value=(frame, [])))

    with httpx.Client() as cliente:
        resultado = reconciliacao.comparar(cliente, {})

    assert resultado["status"] == status
    assert resultado["linhas_publicadas"] == 1
    assert resultado["problems"] == ([] if status == "ok" else ["1 linhas divergentes"])
