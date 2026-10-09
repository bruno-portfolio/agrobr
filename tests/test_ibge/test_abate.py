from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import contracts, datasets, ibge
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import agregados, client
from tests.helpers import (
    assert_replay_samples,
    assert_replay_served,
    install_replay_http,
    levanta_exatamente,
    sem_excecao,
)
from tests.test_ibge import test_agregados as replay

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/ibge/abate_bovino_categorias_20261008"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
ROTULOS = {
    "992": "total",
    "55": "bois",
    "56": "vacas",
    "111734": "novilhos",
    "111735": "novilhas",
    "57": "vitelos",
}
COLUNAS_DE_ANTES = [
    "trimestre",
    "localidade",
    "localidade_cod",
    "especie",
    "animais_abatidos",
    "peso_carcacas",
    "fonte",
]


class TestAbateValidation:
    async def test_erro_lista_especies(self):
        from agrobr.ibge.api import abate

        with pytest.raises(ValueError, match="bovino"):
            await abate("lagosta")


def _servir(monkeypatch, corpo: str) -> dict:
    pedido = {
        "match": {"path": MANIFESTO["corpos"][corpo]["requested_url"], "params": {}, "skip": 0},
        "file": f"{corpo}.json",
        "content_type": "application/json",
    }
    return install_replay_http(monkeypatch, {"requests": [pedido]}, GOLDEN)


def _celulas(corpo: str) -> dict[tuple[str, str, str], int | None]:
    registros = json.loads((GOLDEN / f"{corpo}.json").read_text(encoding="utf-8"))
    legenda = {"-": 0, "X": None, "...": None}
    return {
        (r["D1C"], ROTULOS[r["D6C"]], r["D3C"]): legenda[r["V"]]
        if r["V"] in legenda
        else int(r["V"])
        for r in registros
    }


def _publicado(frame: pd.DataFrame) -> dict[tuple[str, str, str], int | None]:
    return {
        (str(linha.localidade_cod), linha.categoria, variavel): None
        if pd.isna(getattr(linha, coluna))
        else int(getattr(linha, coluna))
        for linha in frame.itertuples()
        for variavel, coluna in (("284", "animais_abatidos"), ("285", "peso_carcacas"))
    }


@pytest.mark.parametrize(("corpo", "uf"), [("UF", None), ("MT", "MT")])
async def test_bovino_todas_as_categorias_com_os_valores_publicados(monkeypatch, corpo, uf):
    visto = _servir(monkeypatch, corpo)

    with sem_excecao():
        frame, meta = await ibge.abate(
            "bovino", trimestre="202303", uf=uf, categoria="todas", return_meta=True
        )

    assert_replay_served(visto)
    por_localidade = frame.groupby("localidade_cod")["categoria"].apply(sorted)
    assert por_localidade.tolist() == [sorted(ROTULOS.values())] * len(por_localidade)
    assert _publicado(frame) == _celulas(corpo)
    assert set(frame["trimestre"]) == {"202303"} and set(frame["especie"]) == {"bovino"}
    assert meta.schema_version == contracts.get_contract("abate_trimestral").version
    assert contracts.get_contract("abate_trimestral").validate(frame) == (True, [])


async def test_bovino_uma_categoria_pede_so_o_codigo_dela(monkeypatch):
    registros = json.loads((GOLDEN / "MT.json").read_text(encoding="utf-8"))
    vacas = pd.DataFrame([r for r in registros if r["D6C"] == "56"])
    fetch = AsyncMock(return_value=vacas)
    monkeypatch.setattr(client, "fetch_sidra", fetch)

    frame = await ibge.abate("bovino", trimestre="202303", uf="MT", categoria="Vacas")

    assert fetch.await_args.kwargs["classifications"] == {
        "12716": "115236",
        "12529": "118225",
        "18": "56",
    }
    assert frame[["categoria", "animais_abatidos", "peso_carcacas"]].values.tolist() == [
        ["vacas", 365311, 84300962.0]
    ]


async def test_padrao_sem_categoria_e_a_saida_de_hoje(monkeypatch):
    monkeypatch.setattr(agregados, "_periodos_cache", {})
    visto = replay._fallback_frame("abate_bovino_2024T1", monkeypatch)
    fetch = AsyncMock(wraps=client.fetch_sidra)
    monkeypatch.setattr(client, "fetch_sidra", fetch)

    frame = await ibge.abate("bovino", trimestre="2024T1")

    assert_replay_served(visto)
    assert fetch.await_args.kwargs["classifications"] == {
        "12716": "115236",
        "12529": "118225",
        "18": "992",
    }
    caso = replay.ORACLE_CASES["abate_bovino_2024T1"]
    assert len(frame) == caso["rows"]
    assert_replay_samples(frame, caso)
    assert frame.drop(columns="categoria").columns.tolist() == COLUNAS_DE_ANTES
    assert set(frame["categoria"]) == {"total"}


@pytest.mark.parametrize(
    ("especie", "categoria", "mensagem"),
    [
        ("suino", "vacas", "só existe no abate bovino"),
        ("frango", "todas", "só existe no abate bovino"),
        ("bovino", "bezerros", r"Tipo de rebanho inválido: 'bezerros'.*vitelos.*todas"),
    ],
)
async def test_categoria_fora_do_bovino_ou_desconhecida_falha_sem_rede(
    monkeypatch, especie, categoria, mensagem
):
    fetch = AsyncMock()
    monkeypatch.setattr(client, "fetch_sidra", fetch)

    with levanta_exatamente(InvalidParameterError, mensagem):
        await ibge.abate(especie, trimestre="202303", categoria=categoria)

    fetch.assert_not_awaited()


async def test_suino_sai_com_categoria_total(monkeypatch):
    registros = json.loads((GOLDEN / "MT.json").read_text(encoding="utf-8"))
    sem_rebanho = [
        {k: v for k, v in r.items() if k[:2] != "D6"} for r in registros if r["D6C"] == "992"
    ]
    monkeypatch.setattr(client, "fetch_sidra", AsyncMock(return_value=pd.DataFrame(sem_rebanho)))

    frame = await ibge.abate("suino", trimestre="202303", uf="MT")

    assert frame["categoria"].tolist() == ["total"]


async def test_dataset_repassa_a_categoria(monkeypatch):
    visto = _servir(monkeypatch, "MT")

    frame, meta = await datasets.abate_trimestral(
        "bovino", "202303", uf="MT", categoria="todas", return_meta=True
    )

    assert_replay_served(visto)
    assert _publicado(frame) == _celulas("MT")
    assert meta.contract_version == contracts.get_contract("abate_trimestral").version
