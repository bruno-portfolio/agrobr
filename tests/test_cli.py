"""Tests for agrobr CLI module."""

from __future__ import annotations

import io
import json
import sys
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
from typer.testing import CliRunner

from agrobr import cli
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
    def _mock_check_result(self, source="cepea"):
        from agrobr.constants import Fonte
        from agrobr.health.checker import CheckResult, CheckStatus

        return CheckResult(
            source=Fonte(source),
            status=CheckStatus.OK,
            latency_ms=100.0,
            message="OK",
            details={},
            timestamp=datetime(2024, 1, 1),
        )

    def test_health_default(self):
        with patch(
            "agrobr.health.checker.run_all_checks",
            new_callable=AsyncMock,
            return_value=[self._mock_check_result()],
        ):
            result = runner.invoke(app, ["health"])
        assert result.exit_code == 0
        assert "Health Check Results" in result.output

    def test_health_json_output(self):
        with patch(
            "agrobr.health.checker.run_all_checks",
            new_callable=AsyncMock,
            return_value=[self._mock_check_result()],
        ):
            result = runner.invoke(app, ["health", "--output", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "summary" in data
        assert "checks" in data

    def test_health_unknown_source(self):
        result = runner.invoke(app, ["health", "--source", "nonexistent"])
        assert result.exit_code == 1
        assert "Fonte desconhecida" in result.output

    def test_health_exit_code_1_on_failure(self):
        from agrobr.constants import Fonte
        from agrobr.health.checker import CheckResult, CheckStatus

        failed = CheckResult(
            source=Fonte.CEPEA,
            status=CheckStatus.FAILED,
            latency_ms=0,
            message="down",
            details={},
            timestamp=datetime(2024, 1, 1),
        )
        with patch(
            "agrobr.health.checker.run_all_checks",
            new_callable=AsyncMock,
            return_value=[failed],
        ):
            result = runner.invoke(app, ["health"])
        assert result.exit_code == 1

    def test_health_cp1252_stdout_does_not_raise(self):
        stdout_buffer = io.BytesIO()
        stderr_buffer = io.BytesIO()
        stdout = io.TextIOWrapper(stdout_buffer, encoding="cp1252")
        stderr = io.TextIOWrapper(stderr_buffer, encoding="cp1252")

        with (
            patch.object(sys, "stdout", stdout),
            patch.object(sys, "stderr", stderr),
            patch("agrobr.cli._configure_cli_logging"),
            patch(
                "agrobr.health.checker.run_all_checks",
                new_callable=AsyncMock,
                return_value=[self._mock_check_result()],
            ),
        ):
            cli.main(_version=False, verbose=False)
            cli.health(source=None, deep=False, output="text")
            stdout.flush()
            output = stdout_buffer.getvalue().decode("cp1252")

        assert stdout.errors == "replace"
        assert stderr.errors == "replace"
        assert "? CEPEA: ok" in output


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
        df = pd.DataFrame({"data": ["2025-01-01"], "produto": ["soja"], "valor": [150.0]})
        with patch("agrobr.cepea.indicador", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["cepea", "indicador", "soja"])
        assert result.exit_code == 0
        assert "soja" in result.output

    def test_cepea_indicador_ultimo(self):
        df = pd.DataFrame(
            {
                "data": ["2025-01-01", "2025-01-02"],
                "produto": ["soja", "soja"],
                "valor": [150.0, 151.0],
            }
        )
        with patch("agrobr.cepea.indicador", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["cepea", "indicador", "soja", "--ultimo"])
        assert result.exit_code == 0
        assert "151" in result.output

    def test_cepea_indicador_empty(self):
        df = pd.DataFrame()
        with patch("agrobr.cepea.indicador", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["cepea", "indicador", "soja"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

    def test_cepea_indicador_error(self):
        with patch("agrobr.cepea.indicador", new_callable=AsyncMock, side_effect=Exception("fail")):
            result = runner.invoke(app, ["cepea", "indicador", "soja"])
        assert result.exit_code == 1


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


class TestIbgeCensoHistoricoCommands:
    def test_censo_historico_success(self):
        df = pd.DataFrame(
            {
                "ano": pd.array([1985], dtype="Int64"),
                "localidade": ["São Paulo"],
                "localidade_cod": pd.array([35], dtype="Int64"),
                "tema": ["estabelecimentos_area"],
                "categoria": ["total"],
                "variavel": ["estabelecimentos"],
                "valor": [5801809.0],
                "unidade": ["Unidades"],
                "fonte": ["ibge_censo_agro_historico"],
            }
        )
        with patch("agrobr.ibge.censo_agro_historico", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "censo-historico", "estabelecimentos_area"])
        assert result.exit_code == 0
        assert "estabelecimentos_area" in result.output

    def test_censo_historico_with_ano(self):
        df = pd.DataFrame(
            {
                "ano": pd.array([1985], dtype="Int64"),
                "localidade": ["Brasil"],
                "localidade_cod": pd.array([1], dtype="Int64"),
                "tema": ["uso_terra"],
                "categoria": ["total"],
                "variavel": ["area"],
                "valor": [100000.0],
                "unidade": ["Hectares"],
                "fonte": ["ibge_censo_agro_historico"],
            }
        )
        with patch("agrobr.ibge.censo_agro_historico", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(app, ["ibge", "censo-historico", "uso_terra", "--ano", "1985"])
        assert result.exit_code == 0

    def test_censo_historico_with_multiple_anos(self):
        df = pd.DataFrame(
            {
                "ano": pd.array([1985, 2006], dtype="Int64"),
                "localidade": ["Brasil", "Brasil"],
                "localidade_cod": pd.array([1, 1], dtype="Int64"),
                "tema": ["uso_terra", "uso_terra"],
                "categoria": ["total", "total"],
                "variavel": ["area", "area"],
                "valor": [100000.0, 200000.0],
                "unidade": ["Hectares", "Hectares"],
                "fonte": ["ibge_censo_agro_historico", "ibge_censo_agro_historico"],
            }
        )
        with patch("agrobr.ibge.censo_agro_historico", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(
                app, ["ibge", "censo-historico", "uso_terra", "--ano", "1985,2006"]
            )
        assert result.exit_code == 0

    def test_censo_historico_empty(self):
        with patch(
            "agrobr.ibge.censo_agro_historico", new_callable=AsyncMock, return_value=pd.DataFrame()
        ):
            result = runner.invoke(app, ["ibge", "censo-historico", "uso_terra"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

    def test_censo_historico_json(self):
        df = pd.DataFrame(
            {
                "ano": pd.array([1985], dtype="Int64"),
                "localidade": ["São Paulo"],
                "localidade_cod": pd.array([35], dtype="Int64"),
                "tema": ["pessoal_tratores"],
                "categoria": ["total"],
                "variavel": ["pessoal_ocupado"],
                "valor": [100000.0],
                "unidade": ["Pessoas"],
                "fonte": ["ibge_censo_agro_historico"],
            }
        )
        with patch("agrobr.ibge.censo_agro_historico", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(
                app, ["ibge", "censo-historico", "pessoal_tratores", "--formato", "json"]
            )
        assert result.exit_code == 0

    def test_censo_historico_csv(self):
        df = pd.DataFrame(
            {
                "ano": pd.array([1985], dtype="Int64"),
                "localidade": ["São Paulo"],
                "localidade_cod": pd.array([35], dtype="Int64"),
                "tema": ["pessoal_tratores"],
                "categoria": ["total"],
                "variavel": ["pessoal_ocupado"],
                "valor": [100000.0],
                "unidade": ["Pessoas"],
                "fonte": ["ibge_censo_agro_historico"],
            }
        )
        with patch("agrobr.ibge.censo_agro_historico", new_callable=AsyncMock, return_value=df):
            result = runner.invoke(
                app, ["ibge", "censo-historico", "pessoal_tratores", "--formato", "csv"]
            )
        assert result.exit_code == 0
        assert "ano" in result.output

    def test_censo_historico_error(self):
        with patch(
            "agrobr.ibge.censo_agro_historico",
            new_callable=AsyncMock,
            side_effect=ValueError("Tema não suportado"),
        ):
            result = runner.invoke(app, ["ibge", "censo-historico", "inexistente"])
        assert result.exit_code == 1

    def test_temas_historico(self):
        temas = [
            "estabelecimentos_area",
            "uso_terra",
            "pessoal_tratores",
            "condicao_produtor",
            "efetivo_animais",
            "producao_animal",
            "producao_vegetal",
            "lavoura_permanente",
            "lavoura_temporaria",
        ]
        with patch(
            "agrobr.ibge.temas_censo_agro_historico", new_callable=AsyncMock, return_value=temas
        ):
            result = runner.invoke(app, ["ibge", "temas-historico"])
        assert result.exit_code == 0
        assert "Censo Agropecuario Historico" in result.output
        assert "estabelecimentos_area" in result.output
        assert "lavoura_temporaria" in result.output


class TestConfigCommands:
    def test_config_show(self):
        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code == 0
        assert "Cache Settings" in result.output
        assert "HTTP Settings" in result.output


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
