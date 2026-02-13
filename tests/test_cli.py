"""Tests for agrobr CLI module."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
from typer.testing import CliRunner

from agrobr.cli import app

runner = CliRunner()


class TestMainApp:
    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "agrobr version" in result.output

    def test_version_short_flag(self):
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert "agrobr version" in result.output

    def test_help(self):
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "agrobr" in result.output


class TestHealthCommand:
    def test_health_default(self):
        result = runner.invoke(app, ["health"])
        assert result.exit_code == 0
        assert "Health check" in result.output

    def test_health_json_output(self):
        result = runner.invoke(app, ["health", "--output", "json"])
        assert result.exit_code == 0
        output = result.output
        json_start = output.index("{")
        data = json.loads(output[json_start:])
        assert data["status"] == "ok"
        assert "checks" in data


class TestDoctorCommand:
    def test_doctor_success(self):
        mock_result = MagicMock()
        mock_result.to_rich.return_value = "agrobr diagnostics v0.9.0\nAll OK"

        with patch(
            "agrobr.health.doctor.run_diagnostics", new_callable=AsyncMock, return_value=mock_result
        ):
            result = runner.invoke(app, ["doctor"])

        assert result.exit_code == 0
        assert "agrobr diagnostics" in result.output

    def test_doctor_json_output(self):
        mock_result = MagicMock()
        mock_result.to_dict.return_value = {"version": "0.9.0", "status": "healthy"}

        with patch(
            "agrobr.health.doctor.run_diagnostics", new_callable=AsyncMock, return_value=mock_result
        ):
            result = runner.invoke(app, ["doctor", "--json"])

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["version"] == "0.9.0"

    def test_doctor_error(self):
        with patch(
            "agrobr.health.doctor.run_diagnostics",
            new_callable=AsyncMock,
            side_effect=RuntimeError("conn failed"),
        ):
            result = runner.invoke(app, ["doctor"])

        assert result.exit_code == 1
        assert "Erro" in result.output


class TestCepeaCommands:
    def test_cepea_indicador(self):
        result = runner.invoke(app, ["cepea", "indicador", "soja"])
        assert result.exit_code == 0
        assert "soja" in result.output


class TestCacheCommands:
    def test_cache_status(self):
        result = runner.invoke(app, ["cache", "status"])
        assert result.exit_code == 0

    def test_cache_clear(self):
        result = runner.invoke(app, ["cache", "clear"])
        assert result.exit_code == 0


class TestConabCommands:
    def test_conab_safras_success(self):
        df = pd.DataFrame({"safra": ["2025/26"], "produto": ["soja"], "area": [1000]})
        with patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["conab", "safras", "soja"])
        assert result.exit_code == 0
        assert "soja" in result.output

    def test_conab_safras_empty(self):
        df = pd.DataFrame()
        with patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["conab", "safras", "quinoa"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

    def test_conab_safras_json(self):
        df = pd.DataFrame({"safra": ["2025/26"], "produto": ["soja"], "area": [1000]})
        with patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["conab", "safras", "soja", "--formato", "json"])
        assert result.exit_code == 0

    def test_conab_safras_csv(self):
        df = pd.DataFrame({"safra": ["2025/26"], "produto": ["soja"], "area": [1000]})
        with patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["conab", "safras", "soja", "--formato", "csv"])
        assert result.exit_code == 0
        assert "safra" in result.output

    def test_conab_safras_error(self):
        with patch(
            "agrobr.conab.safras", new_callable=AsyncMock, side_effect=RuntimeError("API down")
        ):
            result = runner.invoke(app, ["conab", "safras", "soja"])
        assert result.exit_code == 1

    def test_conab_balanco_success(self):
        df = pd.DataFrame({"produto": ["soja"], "oferta": [100], "demanda": [90]})
        with patch("agrobr.conab.balanco", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["conab", "balanco", "soja"])
        assert result.exit_code == 0

    def test_conab_balanco_empty(self):
        with patch("agrobr.conab.balanco", new_callable=AsyncMock, return_value=pd.DataFrame()):
            result = runner.invoke(app, ["conab", "balanco"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

    def test_conab_balanco_json(self):
        df = pd.DataFrame({"produto": ["soja"], "oferta": [100]})
        with patch("agrobr.conab.balanco", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["conab", "balanco", "soja", "--formato", "json"])
        assert result.exit_code == 0

    def test_conab_balanco_csv(self):
        df = pd.DataFrame({"produto": ["soja"], "oferta": [100]})
        with patch("agrobr.conab.balanco", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["conab", "balanco", "soja", "--formato", "csv"])
        assert result.exit_code == 0

    def test_conab_balanco_error(self):
        with patch(
            "agrobr.conab.balanco", new_callable=AsyncMock, side_effect=RuntimeError("fail")
        ):
            result = runner.invoke(app, ["conab", "balanco"])
        assert result.exit_code == 1

    def test_conab_levantamentos_success(self):
        levs = [{"safra": "2025/26", "levantamento": i} for i in range(1, 13)]
        with patch("agrobr.conab.levantamentos", new_callable=AsyncMock, return_value=levs):
            result = runner.invoke(app, ["conab", "levantamentos"])
        assert result.exit_code == 0
        assert "... e mais 2 levantamentos" in result.output

    def test_conab_levantamentos_error(self):
        with patch(
            "agrobr.conab.levantamentos", new_callable=AsyncMock, side_effect=RuntimeError("fail")
        ):
            result = runner.invoke(app, ["conab", "levantamentos"])
        assert result.exit_code == 1

    def test_conab_produtos(self):
        with patch("agrobr.conab.produtos", new_callable=AsyncMock, return_value=["soja", "milho"]):
            result = runner.invoke(app, ["conab", "produtos"])
        assert result.exit_code == 0
        assert "soja" in result.output
        assert "milho" in result.output


class TestIbgeCommands:
    def test_ibge_pam_success(self):
        df = pd.DataFrame({"produto": ["soja"], "ano": [2023], "valor": [1000]})
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "pam", "soja"])
        assert result.exit_code == 0

    def test_ibge_pam_with_ano(self):
        df = pd.DataFrame({"produto": ["soja"], "ano": [2023], "valor": [1000]})
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "pam", "soja", "--ano", "2023"])
        assert result.exit_code == 0

    def test_ibge_pam_with_multiple_anos(self):
        df = pd.DataFrame({"produto": ["soja", "soja"], "ano": [2022, 2023]})
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "pam", "soja", "--ano", "2022,2023"])
        assert result.exit_code == 0

    def test_ibge_pam_empty(self):
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=pd.DataFrame()):
            result = runner.invoke(app, ["ibge", "pam", "quinoa"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

    def test_ibge_pam_json(self):
        df = pd.DataFrame({"produto": ["soja"], "ano": [2023]})
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "pam", "soja", "--formato", "json"])
        assert result.exit_code == 0

    def test_ibge_pam_csv(self):
        df = pd.DataFrame({"produto": ["soja"], "ano": [2023]})
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "pam", "soja", "--formato", "csv"])
        assert result.exit_code == 0
        assert "produto" in result.output

    def test_ibge_pam_error(self):
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = runner.invoke(app, ["ibge", "pam", "soja"])
        assert result.exit_code == 1

    def test_ibge_lspa_success(self):
        df = pd.DataFrame({"produto": ["soja"], "mes": [1]})
        with patch("agrobr.ibge.lspa", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "lspa", "soja"])
        assert result.exit_code == 0

    def test_ibge_lspa_empty(self):
        with patch("agrobr.ibge.lspa", new_callable=AsyncMock, return_value=pd.DataFrame()):
            result = runner.invoke(app, ["ibge", "lspa", "soja"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

    def test_ibge_lspa_json(self):
        df = pd.DataFrame({"produto": ["soja"], "mes": [1]})
        with patch("agrobr.ibge.lspa", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "lspa", "soja", "--formato", "json"])
        assert result.exit_code == 0

    def test_ibge_lspa_error(self):
        with patch("agrobr.ibge.lspa", new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = runner.invoke(app, ["ibge", "lspa", "soja"])
        assert result.exit_code == 1

    def test_ibge_produtos_pam(self):
        with patch(
            "agrobr.ibge.produtos_pam", new_callable=AsyncMock, return_value=["soja", "milho"]
        ):
            result = runner.invoke(app, ["ibge", "produtos"])
        assert result.exit_code == 0
        assert "PAM" in result.output

    def test_ibge_produtos_lspa(self):
        with patch("agrobr.ibge.produtos_lspa", new_callable=AsyncMock, return_value=["soja"]):
            result = runner.invoke(app, ["ibge", "produtos", "--pesquisa", "lspa"])
        assert result.exit_code == 0
        assert "LSPA" in result.output


class TestConfigCommands:
    def test_config_show(self):
        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        assert "Cache Settings" in result.output
        assert "HTTP Settings" in result.output
        assert "Alert Settings" in result.output


class TestSnapshotCommands:
    def test_snapshot_list_empty(self):
        with patch("agrobr.snapshots.list_snapshots", return_value=[]):
            result = runner.invoke(app, ["snapshot", "list"])
        assert result.exit_code == 0
        assert "Nenhum snapshot" in result.output

    def test_snapshot_list_with_data(self):
        mock_snap = MagicMock()
        mock_snap.name = "snap_2024"
        mock_snap.created_at = datetime(2024, 1, 1, 12, 0)
        mock_snap.size_bytes = 2 * 1024 * 1024
        mock_snap.sources = ["cepea", "conab"]
        mock_snap.file_count = 10

        with patch("agrobr.snapshots.list_snapshots", return_value=[mock_snap]):
            result = runner.invoke(app, ["snapshot", "list"])
        assert result.exit_code == 0
        assert "snap_2024" in result.output

    def test_snapshot_list_json(self):
        mock_snap = MagicMock()
        mock_snap.name = "snap_2024"
        mock_snap.created_at = datetime(2024, 1, 1, 12, 0)
        mock_snap.size_bytes = 1024 * 1024
        mock_snap.sources = ["cepea"]
        mock_snap.file_count = 5

        with patch("agrobr.snapshots.list_snapshots", return_value=[mock_snap]):
            result = runner.invoke(app, ["snapshot", "list", "--json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data[0]["name"] == "snap_2024"

    def test_snapshot_create_success(self):
        mock_info = MagicMock()
        mock_info.name = "test_snap"
        mock_info.path = "/tmp/test_snap"
        mock_info.file_count = 3

        with patch(
            "agrobr.snapshots.create_snapshot", new_callable=AsyncMock, return_value=mock_info
        ):
            result = runner.invoke(app, ["snapshot", "create", "test_snap"])
        assert result.exit_code == 0
        assert "sucesso" in result.output

    def test_snapshot_create_with_sources(self):
        mock_info = MagicMock()
        mock_info.name = "test_snap"
        mock_info.path = "/tmp/test_snap"
        mock_info.file_count = 2

        with patch(
            "agrobr.snapshots.create_snapshot", new_callable=AsyncMock, return_value=mock_info
        ):
            result = runner.invoke(
                app, ["snapshot", "create", "test_snap", "--sources", "cepea,conab"]
            )
        assert result.exit_code == 0

    def test_snapshot_create_value_error(self):
        with patch(
            "agrobr.snapshots.create_snapshot",
            new_callable=AsyncMock,
            side_effect=ValueError("bad name"),
        ):
            result = runner.invoke(app, ["snapshot", "create", "bad!"])
        assert result.exit_code == 1

    def test_snapshot_create_generic_error(self):
        with patch(
            "agrobr.snapshots.create_snapshot",
            new_callable=AsyncMock,
            side_effect=RuntimeError("disk full"),
        ):
            result = runner.invoke(app, ["snapshot", "create", "snap"])
        assert result.exit_code == 1

    def test_snapshot_delete_success(self):
        mock_snap = MagicMock()
        with (
            patch("agrobr.snapshots.get_snapshot", return_value=mock_snap),
            patch("agrobr.snapshots.delete_snapshot", return_value=True),
        ):
            result = runner.invoke(app, ["snapshot", "delete", "old_snap", "--force"])
        assert result.exit_code == 0
        assert "removido" in result.output

    def test_snapshot_delete_not_found(self):
        with patch("agrobr.snapshots.get_snapshot", return_value=None):
            result = runner.invoke(app, ["snapshot", "delete", "nope", "--force"])
        assert result.exit_code == 1

    def test_snapshot_delete_cancelled(self):
        mock_snap = MagicMock()
        with patch("agrobr.snapshots.get_snapshot", return_value=mock_snap):
            result = runner.invoke(app, ["snapshot", "delete", "snap"], input="n\n")
        assert result.exit_code == 0
        assert "cancelada" in result.output

    def test_snapshot_delete_failed(self):
        mock_snap = MagicMock()
        with (
            patch("agrobr.snapshots.get_snapshot", return_value=mock_snap),
            patch("agrobr.snapshots.delete_snapshot", return_value=False),
        ):
            result = runner.invoke(app, ["snapshot", "delete", "snap", "--force"])
        assert result.exit_code == 1

    def test_snapshot_use_success(self):
        mock_snap = MagicMock()
        with (
            patch("agrobr.snapshots.get_snapshot", return_value=mock_snap),
            patch("agrobr.config.set_mode"),
        ):
            result = runner.invoke(app, ["snapshot", "use", "my_snap"])
        assert result.exit_code == 0
        assert "deterministico" in result.output

    def test_snapshot_use_not_found(self):
        with patch("agrobr.snapshots.get_snapshot", return_value=None):
            result = runner.invoke(app, ["snapshot", "use", "nope"])
        assert result.exit_code == 1
        assert "nao encontrado" in result.output
