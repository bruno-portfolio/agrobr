from __future__ import annotations

import json
import warnings
from typing import Any
from unittest.mock import AsyncMock

import pytest

from agrobr.alt.sicar import api, client

from .test_api import URL, geo_capture

AVISO = "sicar: o resultado parou em max_registros="
DF = {"uf": "DF", "municipio": "Brasília"}
MS = {"uf": "MS", "municipio": 5007901}


def _sem_total(nome: str) -> bytes:
    documento = json.loads(geo_capture(nome))
    documento.pop("numberMatched")
    return json.dumps(documento).encode()


async def _consultar(
    monkeypatch: pytest.MonkeyPatch,
    pagina: bytes,
    consulta: dict[str, Any],
    max_features: int | None,
):
    monkeypatch.setattr(client, "fetch_imoveis_geo", AsyncMock(return_value=([pagina], URL)))
    with warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        frame, meta = await api.imoveis_geo(
            **consulta, max_registros=max_features, return_meta=True
        )
    return (
        frame,
        meta,
        [str(aviso.message) for aviso in emitidos if str(aviso.message).startswith(AVISO)],
    )


@pytest.mark.parametrize(
    ("pagina", "total", "trecho"),
    [
        (
            lambda: geo_capture("df_geo_srs4326_count3.json"),
            21011,
            "a fonte tem 21011 imóveis nesta consulta",
        ),
        (
            lambda: _sem_total("df_geo_srs4326_count3.json"),
            None,
            "a fonte não informou o total, e pode haver mais imóveis",
        ),
    ],
    ids=["total_publicado", "sem_total"],
)
async def test_corte_em_max_features_avisa_e_marca_o_meta(monkeypatch, pagina, total, trecho):
    frame, meta, emitidos = await _consultar(monkeypatch, pagina(), DF, 3)

    esperado = (
        f"{AVISO}3; {trecho}. Use max_registros=None ou filtre por município para trazer todos."
    )
    detalhes = meta.source_details["sicar"]
    assert len(frame) == 3
    assert [detalhes.get(chave) for chave in ("truncado", "max_registros", "total_fonte")] == [
        True,
        3,
        total,
    ]
    assert esperado in meta.validation_warnings
    assert emitidos == [esperado]


@pytest.mark.parametrize(
    ("nome", "consulta", "max_features"),
    [
        ("df_geo_srs4326_count3.json", DF, 4),
        ("df_geo_srs4326_count3.json", DF, None),
        ("ms_5007901_geo.json", MS, 6),
    ],
    ids=["abaixo_do_limite", "sem_limite", "total_igual_ao_limite"],
)
async def test_resultado_completo_nao_recebe_marca(monkeypatch, nome, consulta, max_features):
    frame, meta, emitidos = await _consultar(monkeypatch, geo_capture(nome), consulta, max_features)

    assert not frame.empty
    assert meta.source_details["sicar"].get("truncado") is None
    assert not [aviso for aviso in meta.validation_warnings if aviso.startswith(AVISO)]
    assert emitidos == []
