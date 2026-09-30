from __future__ import annotations

import os
import re
import subprocess
import sys
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest import mock

import duckdb
import pytest

from agrobr.cache import duckdb_store
from agrobr.cache.duckdb_store import DuckDBStore
from agrobr.constants import CacheSettings
from tests.helpers import sem_excecao

SOJA_248 = [
    {
        "produto": "soja",
        "praca": "paranagua",
        "data": datetime(2026, 1, 1) + timedelta(days=dia),
        "valor": 100.0 + dia,
        "unidade": "BRL/sc",
        "fonte": "cepea",
    }
    for dia in range(248)
]

LER_EM_OUTRO_PROCESSO = """
import sys
from agrobr import constants
from agrobr.cache import duckdb_store
store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=sys.argv[1], db_name=sys.argv[2]))
print('linhas=%d' % len(store.indicadores_query('soja')))
"""

MORRER_NO_2O_BLOCO = """
import os
import sys
from datetime import datetime, timedelta
from agrobr import constants
from agrobr.cache import duckdb_store
conectar = duckdb_store.duckdb.connect
class MorreNo2oBloco:
    def __init__(self, conn):
        self.conn, self.blocos = conn, 0
    def executemany(self, sql, params):
        self.blocos += 1
        if self.blocos == 2:
            os._exit(3)
        return self.conn.executemany(sql, params)
    def __getattr__(self, nome):
        return getattr(self.conn, nome)
duckdb_store.duckdb.connect = lambda *a, **k: MorreNo2oBloco(conectar(*a, **k))
duckdb_store.UPSERT_CHUNK_SIZE = 100
store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=sys.argv[1], db_name=sys.argv[2]))
store.indicadores_upsert([
    {'produto': 'soja', 'praca': 'paranagua', 'data': datetime(2026, 1, 1) + timedelta(days=dia),
     'valor': 100.0 + dia, 'unidade': 'BRL/sc', 'fonte': 'cepea'}
    for dia in range(248)
])
"""


def outro_processo(codigo: str, store: DuckDBStore) -> subprocess.CompletedProcess[str]:
    raiz = Path(duckdb_store.__file__).parents[2]
    return subprocess.run(
        [sys.executable, "-c", codigo, str(store.settings.cache_dir), store.settings.db_name],
        cwd=raiz,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        env={**os.environ, "PYTHONPATH": str(raiz), "PYTHONIOENCODING": "utf-8"},
        check=False,
    )


@pytest.fixture()
def tmp_store(tmp_path: Path) -> DuckDBStore:
    settings = CacheSettings(cache_dir=tmp_path, db_name="test.duckdb")
    store = DuckDBStore(settings)
    yield store
    store.close()


class TestIndicadores:
    @pytest.mark.parametrize("null_field", ["produto", "data", "valor", "unidade", "fonte"])
    def test_null_required_field_preserves_valid_rows(self, tmp_store, null_field):
        valid = {
            "produto": "soja",
            "praca": "paranagua",
            "data": datetime(2024, 6, 15),
            "valor": 135.50,
            "unidade": "BRL/sc60kg",
            "fonte": "cepea",
        }
        invalid = {**valid, "praca": "invalid", null_field: None}
        assert tmp_store.indicadores_upsert([valid, invalid]) == 1
        assert tmp_store.indicadores_upsert([invalid, {**valid, "valor": 140.0}]) == 1
        stored = tmp_store.indicadores_query("soja")
        assert len(stored) == 1
        assert float(stored[0]["valor"]) == 140.0
        assert stored[0]["praca"] == "paranagua"

    def test_upsert_and_query(self, tmp_store: DuckDBStore):
        indicadores = [
            {
                "produto": "soja",
                "praca": "paranagua",
                "data": datetime(2024, 6, 15),
                "valor": 135.50,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            }
        ]
        count = tmp_store.indicadores_upsert(indicadores)
        assert count == 1

        results = tmp_store.indicadores_query("soja")
        assert len(results) == 1
        assert results[0]["praca"] == "paranagua"

    def test_upsert_empty_list(self, tmp_store: DuckDBStore):
        assert tmp_store.indicadores_upsert([]) == 0

    def test_query_with_date_range(self, tmp_store: DuckDBStore):
        for month in range(1, 7):
            tmp_store.indicadores_upsert(
                [
                    {
                        "produto": "soja",
                        "praca": "paranagua",
                        "data": datetime(2024, month, 15),
                        "valor": 130.0 + month,
                        "unidade": "BRL/sc",
                        "fonte": "cepea",
                    }
                ]
            )

        results = tmp_store.indicadores_query(
            "soja",
            inicio=datetime(2024, 3, 1),
            fim=datetime(2024, 4, 30),
        )
        assert len(results) == 2

    @pytest.mark.parametrize("praca", ["paranagua", "Paranaguá/PR"])
    def test_query_praca_normalizada(self, tmp_store: DuckDBStore, praca: str):
        tmp_store.indicadores_upsert(
            [
                {
                    "produto": "soja",
                    "praca": "Paranaguá/PR",
                    "data": datetime(2024, 6, 15),
                    "valor": 135.50,
                    "unidade": "BRL/sc",
                    "fonte": "cepea",
                },
                {
                    "produto": "soja",
                    "praca": "Paraná",
                    "data": datetime(2024, 6, 15),
                    "valor": 130.00,
                    "unidade": "BRL/sc",
                    "fonte": "cepea",
                },
            ]
        )

        results = tmp_store.indicadores_query("soja", praca=praca)

        assert len(results) == 1
        assert results[0]["praca"] == "Paranaguá/PR"


class TestIndicadoresUpsertChunkError:
    def test_chunk_error_fallback_row_by_row(self, tmp_store: DuckDBStore):
        indicadores = [
            {
                "produto": "soja",
                "praca": f"p_{i}",
                "data": datetime(2024, 1, 1) + timedelta(days=i),
                "valor": 100.0 + i,
                "unidade": "BRL/sc",
                "fonte": "cepea",
            }
            for i in range(3)
        ]

        real_conn = tmp_store._get_conn()
        call_count = [0]

        class ConnWrapper:
            def __getattr__(self, name):
                if name == "executemany":

                    def patched_executemany(sql, params):
                        call_count[0] += 1
                        if call_count[0] == 1 and "_ind_staging" in sql:
                            import duckdb

                            raise duckdb.Error("simulated chunk error")
                        return real_conn.executemany(sql, params)

                    return patched_executemany
                return getattr(real_conn, name)

        with mock.patch.object(tmp_store, "_get_conn", return_value=ConnWrapper()):
            count = tmp_store.indicadores_upsert(indicadores)

        assert call_count[0] == 1
        assert count == 3
        assert sorted(item["praca"] for item in tmp_store.indicadores_query("soja")) == [
            "p_0",
            "p_1",
            "p_2",
        ]

    def test_falha_no_merge_nao_grava_nada(self, tmp_store: DuckDBStore):
        real_conn = tmp_store._get_conn()

        class FalhaNoMerge:
            def execute(self, sql, *args):
                if sql is duckdb_store._MERGE_SQL:
                    raise duckdb.Error("merge simulado")
                return real_conn.execute(sql, *args)

            def __getattr__(self, nome):
                return getattr(real_conn, nome)

        with mock.patch.object(tmp_store, "_get_conn", return_value=FalhaNoMerge()):
            assert tmp_store.indicadores_upsert(SOJA_248[:3]) == 0

        assert tmp_store.indicadores_query("soja") == []


class TestCacheDegradado:
    def test_connect_falha_degrada_para_no_op(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="locked.duckdb")
        store = DuckDBStore(settings)
        indicador = {
            "produto": "soja",
            "praca": "paranagua",
            "data": datetime(2024, 6, 15),
            "valor": 100.0,
            "unidade": "BRL/sc",
            "fonte": "cepea",
        }

        with mock.patch(
            "agrobr.cache.duckdb_store.duckdb.connect",
            side_effect=duckdb.IOException("File is already open in another process"),
        ) as mock_connect:
            assert store.indicadores_query("soja") == []
            assert store.indicadores_upsert([indicador]) == 0
            assert store.indicadores_query("soja") == []

        assert mock_connect.call_count == 3
        assert store._degraded is True
        store.close()

    def test_oserror_tambem_degrada(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="sem_permissao.duckdb")
        store = DuckDBStore(settings)

        with mock.patch(
            "agrobr.cache.duckdb_store.duckdb.connect",
            side_effect=OSError("permission denied"),
        ):
            assert store.indicadores_query("soja") == []
            assert store.indicadores_ultima_coleta("soja") is None

        assert store._degraded is True

    def test_falha_no_schema_fecha_conexao_parcial(self, tmp_path: Path):
        settings = CacheSettings(cache_dir=tmp_path, db_name="schema_quebrado.duckdb")
        store = DuckDBStore(settings)
        abertas: list[duckdb.DuckDBPyConnection] = []
        conectar = duckdb.connect

        def espiar(*args, **kwargs):
            abertas.append(conectar(*args, **kwargs))
            return abertas[-1]

        with (
            mock.patch("agrobr.cache.duckdb_store.duckdb.connect", side_effect=espiar),
            mock.patch(
                "agrobr.cache.migrations.migrate",
                side_effect=duckdb.Error("migration boom"),
            ),
        ):
            assert store.indicadores_query("soja") == []

        assert store._degraded is True
        assert len(abertas) == 1
        with pytest.raises(duckdb.ConnectionException):
            abertas[0].execute("SELECT 1")
        assert store.indicadores_query("soja") == []

    def test_cache_degradado_avisa_uma_vez_com_caminho_motivo_e_dica(self, tmp_path: Path):
        arquivo = tmp_path / "nao_e_pasta"
        arquivo.write_text("x", encoding="utf-8")
        store = DuckDBStore(CacheSettings(cache_dir=arquivo / "cache"))

        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always")
            assert store.indicadores_query("soja") == []
            assert store.indicadores_upsert(SOJA_248[:1]) == 0

        mensagens = [str(aviso.message) for aviso in avisos if aviso.category is UserWarning]
        assert len(mensagens) == 1
        assert str(store.db_path) in mensagens[0]
        assert re.search(r"\((\w+Error): ", mensagens[0])
        assert "AGROBR_CACHE_DIR" in mensagens[0]

    @pytest.mark.parametrize(
        "mensagem",
        [
            'IO Error: Cannot open file "agrobr.duckdb": O arquivo já está sendo usado por outro processo.',
            'IO Error: Could not write file "agrobr.duckdb": No space left on device',
        ],
        ids=["em_uso", "disco_cheio"],
    )
    def test_arquivo_em_uso_ou_disco_cheio_fica_onde_esta(self, tmp_path: Path, mensagem: str):
        store = DuckDBStore(CacheSettings(cache_dir=tmp_path))
        store.indicadores_upsert(SOJA_248[:3])

        with mock.patch(
            "agrobr.cache.duckdb_store.duckdb.connect", side_effect=duckdb.IOException(mensagem)
        ):
            assert store.indicadores_query("soja") == []

        assert [arquivo.name for arquivo in tmp_path.iterdir()] == ["agrobr.duckdb"]
        assert len(store.indicadores_query("soja")) == 3

    def test_banco_ilegivel_vai_para_o_lado_com_o_wal(self, tmp_path: Path):
        store = DuckDBStore(CacheSettings(cache_dir=tmp_path))
        store.db_path.write_bytes(b"lixo" * 1000)
        wal = tmp_path / "agrobr.duckdb.wal"
        wal.write_bytes(b"wal velho")

        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always")
            assert store.indicadores_query("soja") == []

        movidos = sorted(tmp_path.glob("*.corrompido-*"))
        assert [arquivo.name.split(".corrompido-")[0] for arquivo in movidos] == [
            "agrobr.duckdb",
            "agrobr.duckdb.wal",
        ]
        assert movidos[1].read_bytes() == b"wal velho"
        [mensagem] = [str(aviso.message) for aviso in avisos]
        assert f"o cache em {store.db_path} está danificado" in mensagem
        assert f"movido para {movidos[0]}" in mensagem
        assert store.indicadores_upsert(SOJA_248[:3]) == 3
        assert len(store.indicadores_query("soja")) == 3

    def test_banco_ilegivel_que_nao_sai_do_lugar_segue_sem_cache(self, tmp_path: Path):
        store = DuckDBStore(CacheSettings(cache_dir=tmp_path))
        store.db_path.write_bytes(b"lixo" * 1000)

        with (
            warnings.catch_warnings(record=True) as avisos,
            mock.patch.object(Path, "rename", side_effect=PermissionError("arquivo em uso")),
        ):
            warnings.simplefilter("always")
            assert store.indicadores_query("soja") == []

        assert [arquivo.name for arquivo in tmp_path.iterdir()] == ["agrobr.duckdb"]
        [mensagem] = [str(aviso.message) for aviso in avisos]
        assert mensagem.startswith(f"agrobr: cache indisponível em {store.db_path}")

    @pytest.mark.parametrize(
        ("operacao", "sem_cache"),
        [
            (lambda store: store.indicadores_query("soja"), []),
            (lambda store: store.indicadores_ultima_coleta("soja"), None),
            (lambda store: store.serie_cobertura("soja"), None),
            (
                lambda store: store.serie_registrar(
                    "soja", date(2026, 9, 25), datetime(2026, 9, 26)
                ),
                None,
            ),
            (lambda store: store.indicadores_upsert(SOJA_248[3:6]), 0),
        ],
        ids=["query", "ultima_coleta", "serie_cobertura", "serie_registrar", "upsert"],
    )
    def test_dano_visto_na_operacao_segue_sem_cache(
        self, tmp_store: DuckDBStore, operacao, sem_cache
    ):
        tmp_store.indicadores_upsert(SOJA_248[:3])
        real_conn = tmp_store._get_conn()

        class LeituraDanificada:
            def execute(self, *_argumentos):
                raise duckdb.IOException(
                    'IO Error: Could not read all bytes from file "test.duckdb": '
                    "wanted=262144 read=77414"
                )

            def __getattr__(self, nome):
                return getattr(real_conn, nome)

        with (
            sem_excecao(),
            mock.patch.object(tmp_store, "_get_conn", return_value=LeituraDanificada()),
        ):
            assert operacao(tmp_store) == sem_cache

        assert len(list(tmp_store.db_path.parent.glob("test.duckdb.corrompido-*"))) == 1

    def test_dano_visto_so_no_merge_vai_para_o_lado(self, tmp_store: DuckDBStore):
        tmp_store.indicadores_upsert(SOJA_248[:3])
        real_conn = tmp_store._get_conn()

        class MergeLeArquivoDanificado:
            def execute(self, sql, *args):
                if sql is duckdb_store._MERGE_SQL:
                    raise duckdb.TransactionException(
                        "TransactionContext Error: Failed to commit: Could not read all bytes "
                        'from file "test.duckdb": wanted=262144 read=169779'
                    )
                return real_conn.execute(sql, *args)

            def __getattr__(self, nome):
                return getattr(real_conn, nome)

        with mock.patch.object(tmp_store, "_get_conn", return_value=MergeLeArquivoDanificado()):
            assert tmp_store.indicadores_upsert(SOJA_248[3:6]) == 0

        assert len(list(tmp_store.db_path.parent.glob("test.duckdb.corrompido-*"))) == 1
        assert tmp_store.indicadores_query("soja") == []


class TestGetStore:
    def test_singleton_pattern(self):
        import agrobr.cache.duckdb_store as store_mod

        store_mod._store = None
        with mock.patch.object(store_mod, "DuckDBStore") as mock_cls:
            mock_instance = mock.MagicMock()
            mock_cls.return_value = mock_instance
            s1 = store_mod.get_store()
            s2 = store_mod.get_store()
            assert s1 is s2
            mock_cls.assert_called_once()
        store_mod._store = None


def test_operacao_fecha_a_conexao_ao_fim(tmp_store):
    abertas: list[duckdb.DuckDBPyConnection] = []
    conectar = duckdb.connect

    def espiar(*args, **kwargs):
        abertas.append(conectar(*args, **kwargs))
        return abertas[-1]

    with mock.patch("agrobr.cache.duckdb_store.duckdb.connect", side_effect=espiar):
        assert tmp_store.indicadores_upsert(SOJA_248[:2]) == 2
        assert len(tmp_store.indicadores_query("soja")) == 2

    assert len(abertas) == 2
    for conexao in abertas:
        with pytest.raises(duckdb.ConnectionException):
            conexao.execute("SELECT 1")


def test_segundo_processo_usa_o_cache_com_o_primeiro_vivo(tmp_store):
    assert tmp_store.indicadores_upsert(SOJA_248) == 248
    assert len(tmp_store.indicadores_query("soja")) == 248

    filho = outro_processo(LER_EM_OUTRO_PROCESSO, tmp_store)

    assert filho.returncode == 0, filho.stderr[-2000:]
    assert re.findall(r"linhas=(\d+)", filho.stdout) == ["248"]


def test_processo_morto_no_meio_do_upsert_nao_deixa_linha_parcial(tmp_store):
    filho = outro_processo(MORRER_NO_2O_BLOCO, tmp_store)

    assert filho.returncode == 3, filho.stderr[-2000:]
    assert tmp_store.indicadores_query("soja") == []
    assert tmp_store.indicadores_upsert(SOJA_248) == 248
    assert len(tmp_store.indicadores_query("soja")) == 248
