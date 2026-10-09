from typing import Any
from unittest.mock import AsyncMock

import pytest

from agrobr import bcb
from agrobr.bcb import client
from tests.helpers import sem_excecao

ANO, MES = "2024.0", "9.0"


def _regiao_uf() -> dict[str, Any]:
    return {
        "nomeUF": "MT",
        "AnoEmissao": ANO,
        "MesEmissao": MES,
        "cdPrograma": "0001",
        "QtdCusteio": 4,
        "VlCusteio": 1000.0,
        "QtdInvestimento": 0,
        "VlInvestimento": 0.0,
        "QtdComercializacao": 0,
        "VlComercializacao": 0.0,
        "QtdIndustrializacao": 0,
        "VlIndustrializacao": 0.0,
    }


def _custeio_produto() -> dict[str, Any]:
    return {
        "nomeProduto": '"SOJA"',
        "nomeUF": "MT",
        "AnoEmissao": ANO,
        "MesEmissao": MES,
        "QtdCusteio": 4,
        "VlCusteio": 1000.0,
        "cdPrograma": "0001",
    }


@pytest.mark.parametrize("safra", [None, "2024/25"])
async def test_total_com_ano_e_mes_inteiros_em_float_monta_os_meses(monkeypatch, safra):
    monkeypatch.setattr(client, "_fetch_odata", AsyncMock(return_value={"value": [_regiao_uf()]}))

    with sem_excecao():
        frame, meta = await bcb.credito_rural_total(safra=safra, return_meta=True)
        sem_meta = await bcb.credito_rural_total(safra=safra)

    assert frame[["safra", "qtd_contratos", "valor"]].values.tolist() == [["2024/25", 4, 1000.0]]
    assert sem_meta.equals(frame)
    assert meta.source_details["meses"] == {
        "primeiro": "2024-09",
        "ultimo": "2024-09",
        "quantidade": 1,
    }


async def test_credito_rural_com_safra_e_ano_e_mes_inteiros_em_float(monkeypatch):
    monkeypatch.setattr(
        client, "_fetch_odata", AsyncMock(return_value={"value": [_custeio_produto()]})
    )

    with sem_excecao():
        frame = await bcb.credito_rural("soja", safra="2024/25", agregacao="registro")

    assert frame[["safra", "ano_emissao", "mes_emissao", "qtd_contratos"]].values.tolist() == [
        ["2024/25", 2024, 9, 4]
    ]
