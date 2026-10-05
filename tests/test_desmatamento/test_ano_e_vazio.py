from __future__ import annotations

import warnings
from unittest.mock import AsyncMock

from agrobr.desmatamento import api, client
from agrobr.exceptions import InvalidParameterError
from agrobr.utils import time as time_utils
from tests.helpers import (
    desmatamento_features,
    install_desmatamento_wfs,
    levanta_exatamente,
    sem_excecao,
)

AVISO = (
    "PRODES sem feição no WFS para Cerrado/2024 (ano ainda não publicado ou sem desmatamento no recorte); "
    "o resultado vem vazio"
)


async def test_prodes_recusa_ano_posterior_ao_corrente_antes_da_rede(monkeypatch):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_acquisition", fetch)
    ano = time_utils.hoje().year + 1
    with levanta_exatamente(InvalidParameterError, match=f"Ano {ano} posterior ao corrente"):
        await api.prodes(bioma="Cerrado", ano=ano)
    fetch.assert_not_awaited()


async def test_prodes_aceita_o_ano_corrente(monkeypatch):
    calls = install_desmatamento_wfs(monkeypatch, [])
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame = await api.prodes(bioma="Cerrado", ano=time_utils.hoje().year)
    assert frame.empty and calls


async def test_prodes_vazio_para_ano_valido_avisa(monkeypatch):
    install_desmatamento_wfs(monkeypatch, [])
    with sem_excecao(), warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        frame, meta = await api.prodes(bioma="Cerrado", ano=2024, return_meta=True)
    assert frame.empty
    assert AVISO in meta.validation_warnings
    assert AVISO in [str(aviso.message) for aviso in emitidos if aviso.category is UserWarning]


async def test_prodes_com_feicao_nao_avisa_vazio(monkeypatch):
    install_desmatamento_wfs(
        monkeypatch, [f for f in desmatamento_features() if f["properties"]["year"] == 2025]
    )
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame, meta = await api.prodes(bioma="Amazônia", ano=2025, return_meta=True)
    assert not frame.empty
    assert not [
        aviso for aviso in meta.validation_warnings if aviso.startswith("PRODES sem feição")
    ]
