import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.ibge import client
from tests import helpers

DESPESAS_2017 = (
    Path(__file__).resolve().parents[1]
    / "golden_data/ibge/censo_2017_despesa_adubos_20260923/sidra_6899_uf.json"
)
URL_6899 = (
    "https://apisidra.ibge.gov.br/values/t/6899/n3/all/h/n/p/all/v/2,1996"
    "/c829/46302/c210/45953/c218/46502/c12517/113601"
)


@pytest.fixture(autouse=True)
def sem_periodos_ibge(monkeypatch: pytest.MonkeyPatch) -> None:
    """As capturas deste módulo não trazem o `/periodos`."""

    async def sem_metadado(_table_code: str, _df: object) -> dict[str, object]:
        return {}

    monkeypatch.setattr(client, "_periodos_modificacao", sem_metadado)


def _mock_df():
    return pd.DataFrame(
        [
            {
                "ano": 2017,
                "localidade": "Brasil",
                "localidade_cod": 1,
                "tema": "efetivo_rebanho",
                "categoria": "bovino",
                "variavel": "numero_estabelecimentos",
                "valor": 2500000.0,
                "unidade": "Estabelecimentos",
                "fonte": "ibge_censo_agro",
            },
        ]
    )


def test_dataset_do_censo_oferece_todos_os_temas_da_fonte():
    assert set(datasets.info("censo_agropecuario")["products"]) == set(client.TEMAS_CENSO_AGRO)


async def test_despesa_com_adubos_confere_a_tabela_oficial_6899(monkeypatch):
    oficial = json.loads(DESPESAS_2017.read_text(encoding="utf-8"))
    path, params, skip = helpers.replay_signature(URL_6899)
    pedido = {"match": {"path": path, "params": dict(params), "skip": skip}}
    pedido |= {"file": DESPESAS_2017.name, "content_type": "application/json"}
    seen = helpers.install_replay_http(monkeypatch, {"requests": [pedido]}, DESPESAS_2017.parent)
    try:
        frame = await datasets.censo_agropecuario("despesa_adubos", ano=2017)
    finally:
        helpers.assert_replay_served(seen)
    assert {(linha["D5N"], linha["D4N"], linha["D6N"], linha["D7N"]) for linha in oficial} == {
        ("Adubos e corretivos", "Total", "Total", "Total")
    }
    esperado = {
        (
            linha["D1N"],
            int(linha["D1C"]),
            "estabelecimentos" if linha["D3C"] == "2" else "valor_mil_reais",
        ): (float(linha["V"]), linha["MN"].lower())
        for linha in oficial
    }
    observado = {
        (r.localidade, r.localidade_cod, r.variavel): (r.valor, r.unidade)
        for r in frame.itertuples()
    }
    assert len(frame) == len(oficial) == 54
    assert observado == esperado
    assert observado[("São Paulo", 35, "valor_mil_reais")] == (5590636.0, "mil reais")
    assert set(zip(frame["ano"], frame["tema"], strict=True)) == {(2017, "despesa_adubos")}
