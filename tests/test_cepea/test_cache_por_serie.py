from __future__ import annotations

from datetime import date, datetime

import pytest

from agrobr import constants, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api
from agrobr.datasets.preco_diario import PRECO_DIARIO_INFO
from agrobr.utils.time import utcnow
from tests.helpers import sem_excecao

DIAS = [date(2026, 9, 21), date(2026, 9, 22), date(2026, 9, 23)]
PERIODO = {"inicio": "2026-09-21", "fim": "2026-09-23"}
PRACA = "São Paulo/SP"


@pytest.fixture
def store(tmp_path, monkeypatch):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: store)
    yield store
    store.close()


def gravar_como_antes(store, produto: str, valores: list[float], coleta: datetime) -> None:
    with store._conexao() as conn:
        conn.executemany(
            "INSERT INTO indicadores (produto, praca, data, valor, unidade, fonte, collected_at, "
            "parser_version, anomalies) VALUES (?, ?, ?, ?, 'BRL/@', 'cepea', ?, 101, '[]')",
            [
                (produto, PRACA, dia, valor, coleta)
                for dia, valor in zip(DIAS, valores, strict=True)
            ],
        )


def test_canonico_e_o_nome_do_preco_diario():
    apelidos = {
        nome: serie for nome, serie in constants.CEPEA_SERIE_CANONICA.items() if nome != serie
    }

    assert apelidos == {"boi_gordo": "boi", "cafe_arabica": "cafe"}
    assert set(apelidos.values()) <= set(PRECO_DIARIO_INFO.products)


@pytest.mark.parametrize(("apelido", "canonico"), [("boi_gordo", "boi"), ("cafe_arabica", "cafe")])
async def test_cache_gravado_pelo_apelido_serve_o_canonico(store, apelido, canonico):
    gravar_como_antes(store, apelido, [300.0, 301.5, 302.0], datetime(2026, 9, 23, 20))

    with sem_excecao():
        offline = await api.indicador(canonico, **PERIODO, offline=True)
        do_dataset = await datasets.preco_diario(canonico, **PERIODO, offline=True)
        async with datasets.deterministic("2026-09-30"):
            congelado = await datasets.preco_diario(canonico, **PERIODO)

    for frame in (offline, do_dataset, congelado):
        assert frame.sort_values("data")["valor"].tolist() == [300.0, 301.5, 302.0]
        assert set(frame["produto"]) == {canonico}


async def test_as_duas_chaves_no_mesmo_dia_dao_uma_linha_a_coleta_mais_nova(store):
    gravar_como_antes(store, "boi_gordo", [300.0, 301.5, 302.0], datetime(2026, 9, 23, 20))
    gravar_como_antes(store, "boi", [310.0, 311.5, 312.0], datetime(2026, 9, 24, 20))

    with sem_excecao():
        frame = await api.indicador("boi_gordo", **PERIODO, offline=True)

    assert frame["valor"].tolist() == [310.0, 311.5, 312.0]
    assert set(frame["produto"]) == {"boi_gordo"}


def test_gravacao_nova_vai_para_o_canonico(store):
    store.indicadores_upsert(
        [
            {
                "produto": "cafe_arabica",
                "praca": PRACA,
                "data": DIAS[0],
                "valor": 1500.0,
                "unidade": "BRL/sc60kg",
                "fonte": "cepea",
            }
        ]
    )
    store.serie_registrar("boi_gordo", DIAS[-1], utcnow())

    with store._conexao() as conn:
        gravados = conn.execute("SELECT produto FROM indicadores").fetchall()
        series = conn.execute("SELECT produto FROM cepea_series").fetchall()

    assert gravados == [("cafe",)] and series == [("boi",)]


def test_apelido_nao_baixa_de_novo_a_serie_que_o_canonico_tem(store):
    store.serie_registrar("boi", DIAS[-1], utcnow())

    assert store.serie_cobertura("boi_gordo") == store.serie_cobertura("boi")
    assert api._precisa_da_serie(store, "boi_gordo", date(2020, 1, 1), DIAS[-1], False) is False
