from __future__ import annotations

import warnings
from typing import Any

import pytest

from agrobr import datasets
from agrobr.alt.mapa_psr import api, client
from tests.helpers import binary_stream, sem_excecao

HEADER = (
    "ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL;NR_APOLICE;"
    "NM_MUNICIPIO_PROPRIEDADE;NR_AREA_TOTAL;DT_APOLICE\n"
)
AVISO_2025 = (
    "PSR: o arquivo publicado pelo MAPA tem apólices até 21/08/2025; 2025 pode estar incompleto"
)


def _servir(monkeypatch: pytest.MonkeyPatch, periodo: str, linhas: list[tuple[int, str]]) -> None:
    corpo = HEADER + "".join(
        f"{ano};MT;MILHO 2ª SAFRA;A{i};SORRISO;20;{data}\n" for i, (ano, data) in enumerate(linhas)
    )

    def abrir(nome: str):
        assert nome == periodo
        return binary_stream(corpo.encode())

    monkeypatch.setattr(client, "open_periodo", abrir)


async def _chamar(chamada: Any) -> tuple[Any, list[str]]:
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        resultado = await chamada
    return resultado, [
        str(aviso.message) for aviso in avisos if "pode estar incompleto" in str(aviso.message)
    ]


async def test_ano_publicado_pela_metade_avisa_com_a_ultima_apolice(monkeypatch):
    _servir(
        monkeypatch,
        "2025",
        [(2025, "02/01/2025"), (2025, "-"), (2025, "31/12/202"), (2025, "21/08/2025")],
    )
    (frame, meta), avisos = await _chamar(api.apolices(produto="soja", ano=2025, return_meta=True))
    assert frame.empty
    assert meta.validation_warnings == avisos == [AVISO_2025]
    assert meta.source_details["corpos"][0]["ultima_apolice"] == "2025-08-21"
    (_, meta_dataset), avisos = await _chamar(
        datasets.seguro_rural("soja", ano=2025, return_meta=True)
    )
    assert avisos == [AVISO_2025]
    assert AVISO_2025 in meta_dataset.validation_warnings


async def test_ultima_apolice_em_setembro_avisa(monkeypatch):
    _servir(monkeypatch, "2016-2024", [(2023, "28/12/2023"), (2024, "30/09/2024")])
    (_, meta), avisos = await _chamar(api.apolices(ano_inicio=2023, ano_fim=2024, return_meta=True))
    esperado = (
        "PSR: o arquivo publicado pelo MAPA tem apólices até 30/09/2024; 2024 pode estar incompleto"
    )
    assert meta.validation_warnings == avisos == [esperado]


@pytest.mark.parametrize(
    "periodo,linhas,ano",
    [
        ("2006-2015", [(2015, "05/01/2015"), (2015, "26/10/2015")], 2015),
        ("2016-2024", [(2024, "01/10/2024")], 2024),
        ("2016-2024", [(2023, "05/09/2023"), (2024, "03/05/2024")], 2023),
        ("2016-2024", [(2024, "31/02/2024"), (2024, "-"), (2024, "")], 2024),
    ],
    ids=["fecha_em_outubro", "primeiro_de_outubro", "ano_do_meio", "sem_data_legivel"],
)
async def test_ano_completo_ou_fora_do_pedido_nao_avisa(monkeypatch, periodo, linhas, ano):
    _servir(monkeypatch, periodo, linhas)
    (_, meta), avisos = await _chamar(api.apolices(ano=ano, return_meta=True))
    assert [a for a in meta.validation_warnings if "pode estar incompleto" in a] == avisos == []
