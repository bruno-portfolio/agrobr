"""Tests for snapshots module."""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
from datetime import date, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from agrobr.snapshots import (
    SnapshotManifest,
    _collect_snapshot,
    _snapshot_cepea,
    _snapshot_conab,
    _snapshot_ibge,
    _validate_path_component,
    create_snapshot,
    delete_snapshot,
    get_snapshot,
    get_snapshots_dir,
    list_snapshots,
    load_from_snapshot,
)
from tests.helpers import capturar_logs, levanta_exatamente, make_snapshot_source, sem_excecao

try:
    import pyarrow  # noqa: F401

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

requires_pyarrow = pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")


def _make_manifest(
    name: str = "test",
    sources: list[str] | None = None,
) -> SnapshotManifest:
    return SnapshotManifest(
        name=name,
        created_at=datetime(2025, 1, 15, 10, 0),
        agrobr_version="0.3.0",
        sources=sources or [],
    )


def _write_manifest(path, manifest: SnapshotManifest) -> None:
    with open(path / "manifest.json", "w") as f:
        json.dump(manifest.to_dict(), f)


class TestSnapshotManifest:
    def test_manifest_from_dict(self):
        data = {
            "name": "2025-01-15",
            "created_at": "2025-01-15T10:30:00",
            "agrobr_version": "0.3.0",
            "sources": ["cepea"],
            "files": {},
            "metadata": {},
        }
        manifest = SnapshotManifest.from_dict(data)
        assert manifest.name == "2025-01-15"
        assert manifest.created_at == datetime(2025, 1, 15, 10, 30)

    def test_manifest_from_dict_datetime_already_parsed(self):
        dt = datetime(2025, 6, 1, 12, 0)
        data = {
            "name": "test",
            "created_at": dt,
            "agrobr_version": "0.3.0",
            "sources": [],
            "files": {},
            "metadata": {},
        }
        manifest = SnapshotManifest.from_dict(data)
        assert manifest.created_at is dt


class TestGetSnapshotsDir:
    def test_returns_config_snapshot_dir(self, tmp_path):
        mock_config = MagicMock()
        mock_config.get_snapshot_dir.return_value = tmp_path / "snaps"
        with patch("agrobr.snapshots.get_config", return_value=mock_config):
            result = get_snapshots_dir()
            assert result == tmp_path / "snaps"
            mock_config.get_snapshot_dir.assert_called_once()


class TestSnapshotOperations:
    def test_list_snapshots_nonexistent_dir(self, tmp_path):
        missing_dir = tmp_path / "does_not_exist"
        with patch("agrobr.snapshots.get_snapshots_dir", return_value=missing_dir):
            snapshots = list_snapshots()
            assert snapshots == []

    def test_list_snapshots_corrupt_manifest(self, tmp_path):
        snapshot_dir = tmp_path / "corrupt"
        snapshot_dir.mkdir()
        (snapshot_dir / "manifest.json").write_text("{invalid json")

        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            snapshots = list_snapshots()
            assert snapshots == []

    def test_get_snapshot_found(self, tmp_path):
        snapshot_dir = tmp_path / "test-snapshot"
        snapshot_dir.mkdir()
        _write_manifest(snapshot_dir, _make_manifest("test-snapshot"))

        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            snapshot = get_snapshot("test-snapshot")
            assert snapshot is not None
            assert snapshot.name == "test-snapshot"

    def test_get_snapshot_not_found(self, tmp_path):
        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            snapshot = get_snapshot("nonexistent")
            assert snapshot is None

    def test_delete_snapshot_success(self, tmp_path):
        snapshot_dir = tmp_path / "to-delete"
        snapshot_dir.mkdir()
        _write_manifest(snapshot_dir, _make_manifest("to-delete"))

        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            result = delete_snapshot("to-delete")
            assert result is True
            assert not snapshot_dir.exists()

    def test_delete_snapshot_not_found(self, tmp_path):
        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            result = delete_snapshot("nonexistent")
            assert result is False


class TestLoadFromSnapshot:
    def test_load_file_not_found(self, tmp_path):
        snapshot_dir = tmp_path / "2025-01-15" / "cepea"
        snapshot_dir.mkdir(parents=True)

        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch("agrobr.config.get_config") as mock_config,
        ):
            mock_config.return_value.snapshot_date = None
            loaded = load_from_snapshot("cepea", "trigo", snapshot_name="2025-01-15")
            assert loaded is None

    def test_load_no_snapshot_specified(self, tmp_path):
        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch("agrobr.config.get_config") as mock_config,
        ):
            mock_config.return_value.snapshot_date = None
            with pytest.raises(ValueError, match="No snapshot specified"):
                load_from_snapshot("cepea", "soja")

    @requires_pyarrow
    def test_load_uses_config_snapshot_date(self, tmp_path):
        snapshot_dir = tmp_path / "2025-06-01" / "conab"
        snapshot_dir.mkdir(parents=True)

        df = pd.DataFrame({"safra": ["2024/25"], "valor": [100.0]})
        df.to_parquet(snapshot_dir / "safras.parquet", index=False)

        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch("agrobr.snapshots.get_config") as mock_config,
        ):
            mock_config.return_value.snapshot_date = date(2025, 6, 1)
            loaded = load_from_snapshot("conab", "safras")
            assert loaded is not None
            assert len(loaded) == 1


class TestPathTraversalProtection:
    TRAVERSAL_PAYLOADS = [
        "../../etc/passwd",
        "..\\..\\windows",
        "../..",
        "foo/bar",
        "foo\\bar",
        "",
        ".hidden",
    ]

    def test_delete_snapshot_rejects_traversal(self, tmp_path):
        with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path):
            for payload in self.TRAVERSAL_PAYLOADS:
                with pytest.raises(ValueError):
                    delete_snapshot(payload)

    def test_load_from_snapshot_rejects_traversal(self, tmp_path):
        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch("agrobr.config.get_config") as mock_config,
        ):
            mock_config.return_value.snapshot_date = None
            with pytest.raises(ValueError):
                load_from_snapshot("../../etc", "passwd", snapshot_name="2025-01-15")
            with pytest.raises(ValueError):
                load_from_snapshot("cepea", "../../etc", snapshot_name="2025-01-15")
            with pytest.raises(ValueError):
                load_from_snapshot("cepea", "soja", snapshot_name="../../etc")


class TestCreateSnapshot:
    @pytest.fixture(autouse=True)
    def _available_engine(self, monkeypatch):
        monkeypatch.setattr("agrobr.snapshots.importlib.util.find_spec", lambda _name: object())

    @pytest.mark.asyncio
    async def test_create_snapshot_default_name(self, tmp_path):
        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch(
                "agrobr.snapshots._snapshot_cepea",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ),
            patch(
                "agrobr.snapshots._snapshot_conab",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ),
            patch(
                "agrobr.snapshots._snapshot_ibge",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ),
        ):
            result = await create_snapshot()
            assert result is not None
            assert result.name == date.today().isoformat()

    @pytest.mark.asyncio
    async def test_create_snapshot_custom_name(self, tmp_path):
        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch(
                "agrobr.snapshots._snapshot_cepea",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ),
            patch(
                "agrobr.snapshots._snapshot_conab",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ),
            patch(
                "agrobr.snapshots._snapshot_ibge",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ),
        ):
            result = await create_snapshot(name="my-snap")
            assert result is not None
            assert result.name == "my-snap"

    @pytest.mark.asyncio
    async def test_create_snapshot_already_exists(self, tmp_path):
        existing = tmp_path / "existing"
        existing.mkdir()

        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            pytest.raises(ValueError, match="already exists"),
        ):
            await create_snapshot(name="existing")

    @pytest.mark.asyncio
    async def test_create_snapshot_source_error_continues(self, tmp_path):
        with (
            patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
            patch(
                "agrobr.snapshots._snapshot_cepea",
                new_callable=AsyncMock,
                side_effect=RuntimeError("cepea broke"),
            ),
            patch(
                "agrobr.snapshots._snapshot_conab",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ) as mock_conab,
            patch(
                "agrobr.snapshots._snapshot_ibge",
                new_callable=AsyncMock,
                side_effect=make_snapshot_source,
            ) as mock_ibge,
        ):
            result = await create_snapshot(name="error-resilient")
            assert result is not None
            mock_conab.assert_awaited_once()
            mock_ibge.assert_awaited_once()


class TestSnapshotCepea:
    @pytest.mark.asyncio
    async def test_snapshot_cepea_empty_df(self, tmp_path):
        from agrobr.snapshots import _snapshot_cepea

        manifest = _make_manifest("test")
        empty_df = pd.DataFrame()

        with (
            patch("agrobr.cepea.produtos", new_callable=AsyncMock, return_value=["soja"]),
            patch("agrobr.cepea.indicador", new_callable=AsyncMock, return_value=empty_df),
        ):
            await _snapshot_cepea(tmp_path, manifest)

        assert not (tmp_path / "soja.parquet").exists()
        assert "cepea/soja.parquet" not in manifest.files

    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_cepea_produto_error(self, tmp_path):
        from agrobr.snapshots import _snapshot_cepea

        manifest = _make_manifest("test")
        df = pd.DataFrame({"data": ["2025-01-01"], "valor": [100.0]})

        with (
            patch(
                "agrobr.cepea.produtos",
                new_callable=AsyncMock,
                return_value=["soja", "milho"],
            ),
            patch(
                "agrobr.cepea.indicador",
                new_callable=AsyncMock,
                side_effect=[RuntimeError("fail"), df],
            ),
        ):
            await _snapshot_cepea(tmp_path, manifest)

        assert not (tmp_path / "soja.parquet").exists()
        assert (tmp_path / "milho.parquet").exists()
        assert "cepea/milho.parquet" in manifest.files


class TestSnapshotConab:
    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_conab_success(self, tmp_path):
        from agrobr.snapshots import _snapshot_conab

        manifest = _make_manifest("test")
        df_safras = pd.DataFrame({"safra": ["2024/25"], "producao": [100.0]})
        df_balanco = pd.DataFrame({"item": ["oferta"], "valor": [200.0]})

        with (
            patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=df_safras),
            patch("agrobr.conab.balanco", new_callable=AsyncMock, return_value=df_balanco),
        ):
            await _snapshot_conab(tmp_path, manifest)

        assert (tmp_path / "safras.parquet").exists()
        assert (tmp_path / "balanco.parquet").exists()
        assert manifest.files["conab/safras.parquet"]["rows"] == 1
        assert manifest.files["conab/balanco.parquet"]["rows"] == 1

    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_conab_empty_safras(self, tmp_path):
        from agrobr.snapshots import _snapshot_conab

        manifest = _make_manifest("test")
        df_balanco = pd.DataFrame({"item": ["oferta"], "valor": [200.0]})

        with (
            patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=pd.DataFrame()),
            patch("agrobr.conab.balanco", new_callable=AsyncMock, return_value=df_balanco),
        ):
            await _snapshot_conab(tmp_path, manifest)

        assert not (tmp_path / "safras.parquet").exists()
        assert (tmp_path / "balanco.parquet").exists()

    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_conab_safras_error(self, tmp_path):
        from agrobr.snapshots import _snapshot_conab

        manifest = _make_manifest("test")
        df_balanco = pd.DataFrame({"item": ["oferta"], "valor": [200.0]})

        with (
            patch(
                "agrobr.conab.safras",
                new_callable=AsyncMock,
                side_effect=RuntimeError("fail"),
            ),
            patch("agrobr.conab.balanco", new_callable=AsyncMock, return_value=df_balanco),
        ):
            await _snapshot_conab(tmp_path, manifest)

        assert not (tmp_path / "safras.parquet").exists()
        assert (tmp_path / "balanco.parquet").exists()

    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_conab_balanco_error(self, tmp_path):
        from agrobr.snapshots import _snapshot_conab

        manifest = _make_manifest("test")
        df_safras = pd.DataFrame({"safra": ["2024/25"], "producao": [100.0]})

        with (
            patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=df_safras),
            patch(
                "agrobr.conab.balanco",
                new_callable=AsyncMock,
                side_effect=RuntimeError("fail"),
            ),
        ):
            await _snapshot_conab(tmp_path, manifest)

        assert (tmp_path / "safras.parquet").exists()
        assert not (tmp_path / "balanco.parquet").exists()


class TestSnapshotIbge:
    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_ibge_empty_pam(self, tmp_path):
        from agrobr.snapshots import _snapshot_ibge

        manifest = _make_manifest("test")
        df_lspa = pd.DataFrame({"produto": ["soja"], "previsao": [95.0]})

        with (
            patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=pd.DataFrame()),
            patch("agrobr.ibge.lspa", new_callable=AsyncMock, return_value=df_lspa),
        ):
            await _snapshot_ibge(tmp_path, manifest)

        assert not (tmp_path / "pam.parquet").exists()
        assert (tmp_path / "lspa.parquet").exists()

    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_ibge_pam_error(self, tmp_path):
        from agrobr.snapshots import _snapshot_ibge

        manifest = _make_manifest("test")
        df_lspa = pd.DataFrame({"produto": ["soja"], "previsao": [95.0]})

        with (
            patch(
                "agrobr.ibge.pam",
                new_callable=AsyncMock,
                side_effect=RuntimeError("fail"),
            ),
            patch("agrobr.ibge.lspa", new_callable=AsyncMock, return_value=df_lspa),
        ):
            await _snapshot_ibge(tmp_path, manifest)

        assert not (tmp_path / "pam.parquet").exists()
        assert (tmp_path / "lspa.parquet").exists()

    @requires_pyarrow
    @pytest.mark.asyncio
    async def test_snapshot_ibge_lspa_error(self, tmp_path):
        from agrobr.snapshots import _snapshot_ibge

        manifest = _make_manifest("test")
        df_pam = pd.DataFrame({"produto": ["soja"], "producao": [100.0]})

        with (
            patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=df_pam),
            patch(
                "agrobr.ibge.lspa",
                new_callable=AsyncMock,
                side_effect=RuntimeError("fail"),
            ),
        ):
            await _snapshot_ibge(tmp_path, manifest)

        assert (tmp_path / "pam.parquet").exists()
        assert not (tmp_path / "lspa.parquet").exists()


def test_list_snapshots_ignora_diretorio_oculto_e_nao_avisa(tmp_path):
    oculto = tmp_path / ".staging-snap"
    oculto.mkdir()
    _write_manifest(oculto, _make_manifest("staging"))
    (tmp_path / "sem-manifesto").mkdir()
    with (
        patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
        capturar_logs() as registros,
    ):
        assert list_snapshots() == []
    assert [r["event"] for r in registros if r["log_level"] == "warning"] == []


async def test_collect_snapshot_so_registra_erro_da_fonte_sem_dados(tmp_path):
    manifest = _make_manifest("teste", sources=["cepea", "conab"])

    async def com_dados(_path, alvo):
        alvo.files["cepea/soja.parquet"] = {"rows": 1, "columns": ["valor"]}

    with (
        patch("agrobr.snapshots._snapshot_cepea", side_effect=com_dados),
        patch("agrobr.snapshots._snapshot_conab", new_callable=AsyncMock),
    ):
        await _collect_snapshot(tmp_path, manifest)
    assert manifest.metadata.get("errors") == {"conab": ["Nenhum conjunto de dados disponível"]}


async def test_snapshot_cepea_sem_dado_nao_registra_erro(tmp_path):
    manifest = _make_manifest("teste")
    with (
        patch("agrobr.cepea.produtos", new_callable=AsyncMock, return_value=["soja"]),
        patch("agrobr.cepea.indicador", new_callable=AsyncMock, return_value=None),
    ):
        await _snapshot_cepea(tmp_path, manifest)
    assert manifest.metadata.get("errors") is None
    assert manifest.files == {}


@pytest.mark.parametrize(
    ("coletar", "alvos"),
    [
        (_snapshot_conab, ("agrobr.conab.safras", "agrobr.conab.balanco")),
        (_snapshot_ibge, ("agrobr.ibge.pam", "agrobr.ibge.lspa")),
    ],
)
async def test_fonte_sem_dados_nao_grava_arquivo_nem_erro(tmp_path, coletar, alvos):
    manifest = _make_manifest("teste")
    with (
        patch(alvos[0], new_callable=AsyncMock, return_value=None),
        patch(alvos[1], new_callable=AsyncMock, return_value=None),
    ):
        await coletar(tmp_path, manifest)
    assert manifest.files == {}
    assert manifest.metadata.get("errors") is None


@pytest.mark.parametrize(
    ("chamada", "mensagem"),
    [
        (
            lambda: load_from_snapshot("cepea", "soja", snapshot_name="bad!"),
            "Invalid snapshot name",
        ),
        (lambda: load_from_snapshot("bad!", "soja", snapshot_name="2025-01-15"), "Invalid source"),
        (lambda: create_snapshot(name="bad!", sources=["cepea"]), "Invalid snapshot name"),
    ],
    ids=["load_nome", "load_fonte", "create_nome"],
)
async def test_nome_fora_do_padrao_e_recusado_antes_do_disco(tmp_path, chamada, mensagem):
    with (
        patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
        patch("agrobr.snapshots._collect_snapshot", new_callable=AsyncMock) as coletar,
        levanta_exatamente(ValueError, match=f"^{mensagem}: 'bad!'$"),
    ):
        retorno = chamada()
        if asyncio.iscoroutine(retorno):
            await retorno
    coletar.assert_not_awaited()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "nome", ["NUL", "con", "Aux", "PRN.csv", "com1", "LPT9.tar.gz", "2025-01-15.", "a.."]
)
async def test_nome_reservado_do_windows_e_recusado_em_todo_so(tmp_path, nome):
    with (
        patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
        patch("agrobr.snapshots._collect_snapshot", new_callable=AsyncMock) as coletar,
        levanta_exatamente(ValueError, match="nome reservado do Windows ou terminado em ponto"),
    ):
        await create_snapshot(name=nome, sources=["cepea"])
    coletar.assert_not_awaited()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("nome", ["conab", "console", "COM10", "LPT0", "nul_2025", "a.b"])
def test_nome_parecido_com_reservado_segue_valido(nome):
    with sem_excecao():
        _validate_path_component(nome, "snapshot name")


def _snapshot_com_safras(raiz, monkeypatch, versao: str) -> None:
    pasta = raiz / "2025-06-01" / "conab"
    pasta.mkdir(parents=True)
    pd.DataFrame({"safra": ["2024/25"], "valor": [100.0]}).to_parquet(
        pasta / "safras.parquet", index=False
    )
    original = importlib.metadata.version
    monkeypatch.setattr(
        importlib.metadata,
        "version",
        lambda nome: versao if nome == "pyarrow" else original(nome),
    )


@requires_pyarrow
@pytest.mark.parametrize("versao", ["0.17.1", "13.0.0", "14.0.0"])
def test_load_recusa_pyarrow_anterior_a_14_0_1(tmp_path, monkeypatch, versao):
    _snapshot_com_safras(tmp_path, monkeypatch, versao)

    with (
        patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path),
        patch("agrobr.snapshots.pd.read_parquet") as ler,
        levanta_exatamente(
            ImportError, rf"pyarrow {versao} .*CVE-2023-47248.*pip install \"pyarrow>=14\.0\.1\""
        ),
    ):
        load_from_snapshot("conab", "safras", snapshot_name="2025-06-01")
    ler.assert_not_called()


@requires_pyarrow
@pytest.mark.parametrize("versao", ["14.0.1", "15.0.0.dev123"])
def test_load_aceita_pyarrow_a_partir_de_14_0_1(tmp_path, monkeypatch, versao):
    _snapshot_com_safras(tmp_path, monkeypatch, versao)

    with patch("agrobr.snapshots.get_snapshots_dir", return_value=tmp_path), sem_excecao():
        carregado = load_from_snapshot("conab", "safras", snapshot_name="2025-06-01")

    assert carregado is not None
    assert carregado["safra"].tolist() == ["2024/25"]
