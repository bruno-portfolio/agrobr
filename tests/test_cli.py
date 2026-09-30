"""Tests for agrobr CLI module."""

from __future__ import annotations

import asyncio
import io
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
import typer
from typer.testing import CliRunner

from agrobr import cli, constants
from agrobr.cache import duckdb_store
from agrobr.cepea import api as cepea_api
from agrobr.cli import app
from agrobr.models import Indicador
from agrobr.normalize import regions
from agrobr.utils import time as time_utils

runner = CliRunner()


class TestMainApp:
    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "agrobr version" in result.output


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
            output = stdout_buffer.getvalue().decode("utf-8")

        assert (stdout.encoding, stdout.errors) == ("utf-8", "replace")
        assert (stderr.encoding, stderr.errors) == ("cp1252", "replace")
        assert "✓ CEPEA: ok" in output


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


def _indicador(produto: str, praca: str, dia: date, valor: str) -> Indicador:
    return Indicador(
        fonte=constants.Fonte.CEPEA,
        produto=produto,
        praca=praca,
        data=dia,
        valor=Decimal(valor),
        unidade="BRL/kg",
        parser_version=2,
    )


class TestCepeaCommands:
    def test_cepea_indicador_ultimo(self):
        recente = _indicador("soja", "Paranaguá/PR", date(2025, 1, 2), "151")
        with patch("agrobr.cepea.ultimo", new_callable=AsyncMock, return_value=recente):
            result = runner.invoke(app, ["cepea", "indicador", "soja", "--ultimo"])
        assert result.exit_code == 0
        assert "151" in result.output


class TestConabCommands:
    def test_conab_levantamentos_success(self):
        levs = [{"safra": "2025/26", "levantamento": i} for i in range(1, 13)]
        with patch("agrobr.conab.levantamentos", new_callable=AsyncMock, return_value=levs):
            result = runner.invoke(app, ["conab", "levantamentos"])
        assert result.exit_code == 0
        assert "... e mais 2 levantamentos" in result.output

    def test_conab_produtos(self):
        with patch("agrobr.conab.produtos", new_callable=AsyncMock, return_value=["soja", "milho"]):
            result = runner.invoke(app, ["conab", "produtos"])
        assert result.exit_code == 0
        assert "soja" in result.output
        assert "milho" in result.output


class TestIbgeCommands:
    def test_ibge_pam_empty(self):
        with patch("agrobr.ibge.pam", new_callable=AsyncMock, return_value=pd.DataFrame()):
            result = runner.invoke(app, ["ibge", "pam", "quinoa"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

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
    def test_censo_historico_empty(self):
        with patch(
            "agrobr.ibge.censo_agro_historico", new_callable=AsyncMock, return_value=pd.DataFrame()
        ):
            result = runner.invoke(app, ["ibge", "censo-historico", "uso_terra"])
        assert result.exit_code == 0
        assert "Nenhum dado" in result.output

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

    def test_snapshot_delete_success(self):
        mock_snap = MagicMock()
        with (
            patch("agrobr.snapshots.get_snapshot", return_value=mock_snap),
            patch("agrobr.snapshots.delete_snapshot", return_value=True),
        ):
            result = runner.invoke(app, ["snapshot", "delete", "old_snap", "--force"])
        assert result.exit_code == 0
        assert "removido" in result.output

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


@pytest.mark.parametrize(
    ("alvo", "argv", "mensagem"),
    [
        ("agrobr.cepea.indicador", ["cepea", "indicador", "soja"], "Erro: falhou"),
        ("agrobr.conab.safras", ["conab", "safras", "soja"], "Erro: falhou"),
        ("agrobr.conab.balanco", ["conab", "balanco", "soja"], "Erro: falhou"),
        ("agrobr.conab.levantamentos", ["conab", "levantamentos"], "Erro: falhou"),
        ("agrobr.ibge.pam", ["ibge", "pam", "soja"], "Erro: falhou"),
        ("agrobr.ibge.lspa", ["ibge", "lspa", "soja"], "Erro: falhou"),
        (
            "agrobr.ibge.censo_agro_historico",
            ["ibge", "censo-historico", "uso_terra"],
            "Erro: falhou",
        ),
        (
            "agrobr.snapshots.create_snapshot",
            ["snapshot", "create", "teste"],
            "Erro ao criar snapshot: falhou",
        ),
    ],
)
def test_erro_da_fonte_vira_mensagem_e_codigo_um(alvo, argv, mensagem):
    with patch(alvo, new_callable=AsyncMock, side_effect=RuntimeError("falhou")):
        result = runner.invoke(app, argv)
    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert mensagem in result.output


def test_snapshot_create_recusa_nome_com_a_mensagem_da_validacao():
    with patch(
        "agrobr.snapshots.create_snapshot",
        new_callable=AsyncMock,
        side_effect=ValueError("nome inválido"),
    ):
        result = runner.invoke(app, ["snapshot", "create", "bad!"])
    assert result.exit_code == 1
    assert "Erro: nome inválido" in result.output
    assert "Erro ao criar snapshot" not in result.output


@pytest.mark.parametrize(
    ("extra", "presentes", "ausentes"),
    [([], ["150.25", "151.75"], []), (["--ultimo"], ["151.75"], ["150.25"])],
)
def test_cepea_ultimo_mostra_so_a_linha_mais_recente(extra, presentes, ausentes):
    df = pd.DataFrame(
        {
            "data": ["2025-01-01", "2025-01-02"],
            "produto": ["soja", "soja"],
            "valor": [150.25, 151.75],
        }
    )
    recente = _indicador("soja", "Paranaguá/PR", date(2025, 1, 2), "151.75")
    with (
        patch("agrobr.cepea.indicador", new_callable=AsyncMock, return_value=df),
        patch("agrobr.cepea.ultimo", new_callable=AsyncMock, return_value=recente),
    ):
        result = runner.invoke(app, ["cepea", "indicador", "soja", *extra])
    assert result.exit_code == 0
    assert all(valor in result.output for valor in presentes)
    assert not any(valor in result.output for valor in ausentes)


@pytest.mark.parametrize(
    ("produto", "pracas"),
    [
        ("suino", ["MG - posto", "SP - posto"]),
        ("leite", ["BRASIL", "MG", "SP"]),
    ],
)
def test_cepea_ultimo_da_cli_escolhe_a_praca_do_cepea_ultimo(monkeypatch, produto, pracas):
    dia = time_utils.hoje().replace(day=1) if produto == "leite" else time_utils.hoje()
    linhas = [
        {**cepea_api._indicadores_to_dicts([_indicador(produto, praca, dia, f"{100 + i}")])[0]}
        for i, praca in enumerate(pracas)
    ]
    for linha in linhas:
        linha["collected_at"] = datetime.now()
    store = MagicMock()
    store.indicadores_query.side_effect = lambda **filtro: [
        linha
        for linha in linhas
        if filtro.get("praca") is None
        or regions.slugificar_praca(linha["praca"]) == regions.slugificar_praca(filtro["praca"])
    ]
    store.indicadores_ultima_coleta.return_value = datetime.now()
    monkeypatch.setattr(cepea_api, "get_store", lambda: store)
    monkeypatch.setattr(cepea_api, "_vencido", lambda _ultima_coleta: False)
    monkeypatch.setattr(
        cepea_api, "_fetch_and_parse", AsyncMock(side_effect=AssertionError("sem rede"))
    )
    esperado = asyncio.run(cepea_api.ultimo(produto))

    result = runner.invoke(app, ["cepea", "indicador", produto, "--ultimo", "--formato", "json"])

    assert result.exit_code == 0, result.output
    linhas_saida = json.loads(result.output[result.output.index("[") :])
    assert [(linha["praca"], float(linha["valor"])) for linha in linhas_saida] == [
        (esperado.praca, float(esperado.valor))
    ]
    assert esperado.praca != pracas[-1]


def test_cepea_ultimo_nao_combina_com_periodo():
    with patch("agrobr.cepea.ultimo", new_callable=AsyncMock) as ultimo:
        result = runner.invoke(
            app, ["cepea", "indicador", "soja", "--ultimo", "--inicio", "2025-01-01"]
        )
    assert result.exit_code == 1
    assert "--ultimo não combina com --inicio/--fim" in result.output
    ultimo.assert_not_awaited()


@pytest.mark.parametrize(
    ("argv", "alvo", "ano"),
    [
        (["ibge", "pam", "soja", "--ano", "2023"], "agrobr.ibge.pam", 2023),
        (["ibge", "pam", "soja", "--ano", "2022,2023"], "agrobr.ibge.pam", [2022, 2023]),
        (
            ["ibge", "censo-historico", "uso_terra", "--ano", "1985"],
            "agrobr.ibge.censo_agro_historico",
            1985,
        ),
        (
            ["ibge", "censo-historico", "uso_terra", "--ano", "1970,1985"],
            "agrobr.ibge.censo_agro_historico",
            [1970, 1985],
        ),
    ],
)
def test_ano_da_linha_de_comando_chega_a_fonte(argv, alvo, ano):
    with patch(alvo, new_callable=AsyncMock, return_value=pd.DataFrame({"ano": [1]})) as fonte:
        result = runner.invoke(app, argv)
    assert result.exit_code == 0
    assert fonte.await_args.kwargs["ano"] == ano


def test_snapshot_list_mostra_tabela_legivel():
    snap = MagicMock()
    snap.name = "snap_2024"
    snap.created_at = datetime(2024, 1, 1, 12, 0)
    snap.size_bytes = 2 * 1024 * 1024
    snap.sources = ["cepea", "conab"]
    snap.file_count = 10
    with patch("agrobr.snapshots.list_snapshots", return_value=[snap]):
        result = runner.invoke(app, ["snapshot", "list"])
    assert result.exit_code == 0
    linhas = result.output.splitlines()
    assert "Snapshots disponiveis:" in linhas
    assert "    Criado em: 2024-01-01 12:00" in linhas
    assert "    Tamanho: 2.00 MB" in linhas
    assert "    Fontes: cepea, conab" in linhas


def test_snapshot_delete_inexistente_avisa_e_nao_apaga():
    with (
        patch("agrobr.snapshots.get_snapshot", return_value=None),
        patch("agrobr.snapshots.delete_snapshot") as apagar,
    ):
        result = runner.invoke(app, ["snapshot", "delete", "nope", "--force"])
    assert result.exit_code == 1
    assert "Snapshot 'nope' nao encontrado." in result.output
    apagar.assert_not_called()


@pytest.fixture
def cache_cepea(tmp_path, monkeypatch):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    hoje = time_utils.hoje()
    store.indicadores_upsert(
        cepea_api._indicadores_to_dicts(
            [
                _indicador("soja", "Paranaguá/PR", hoje - timedelta(days=1), "150.25"),
                _indicador("soja", "Paranaguá/PR", hoje, "151.75"),
                _indicador("soja", "Paraná", hoje, "140.5"),
            ]
        )
    )
    monkeypatch.setattr(cepea_api, "get_store", lambda: store)
    monkeypatch.setattr(cepea_api, "_vencido", lambda _ultima_coleta: False)
    monkeypatch.setattr(
        cepea_api, "_fetch_and_parse", AsyncMock(side_effect=AssertionError("sem rede"))
    )
    yield hoje
    store.close()


@pytest.mark.usefixtures("cache_cepea")
@pytest.mark.parametrize("formato", ["json", "csv"])
def test_cepea_ultimo_sai_com_as_colunas_e_os_tipos_do_indicador(formato):
    tabela = runner.invoke(app, ["cepea", "indicador", "soja", "--formato", formato])
    recente = runner.invoke(app, ["cepea", "indicador", "soja", "--ultimo", "--formato", formato])

    assert (tabela.exit_code, recente.exit_code) == (0, 0), tabela.output + recente.output
    if formato == "json":
        linhas, saida = json.loads(tabela.stdout), json.loads(recente.stdout)
        assert len(saida) == 1
        linha = saida[0]
        assert list(linha) == list(linhas[0])
        assert isinstance(linha["valor"], float)
    else:
        linhas, saida = tabela.stdout.splitlines(), recente.stdout.splitlines()
        assert saida[0] == linhas[0]
        assert len(saida) == 2
        linha = saida[1]
    assert linha in linhas


def test_json_da_cli_sai_com_a_data_em_iso(cache_cepea):
    result = runner.invoke(app, ["cepea", "indicador", "soja", "--formato", "json"])

    assert result.exit_code == 0, result.output
    datas = sorted({linha["data"] for linha in json.loads(result.stdout)})
    assert datas == [
        f"{(cache_cepea - timedelta(days=dias)).isoformat()}T00:00:00.000" for dias in (1, 0)
    ]


@pytest.mark.parametrize(
    ("argv", "opcoes", "na_ajuda"),
    [
        (["cepea", "indicador", "soja"], "'table', 'csv', 'json'", "<table|csv|json>"),
        (["conab", "safras", "soja"], "'table', 'csv', 'json'", "<table|csv|json>"),
        (["ibge", "pam", "soja"], "'table', 'csv', 'json'", "<table|csv|json>"),
        (["health"], "'text', 'json'", "<text|json>"),
    ],
    ids=["cepea", "conab", "ibge", "health"],
)
def test_formato_invalido_sai_com_2_e_sem_saida_padrao(argv, opcoes, na_ajuda):
    resultado = runner.invoke(app, [*argv, "-o", "xml"])
    ajuda = runner.invoke(app, [*argv, "--help"])

    assert resultado.exit_code == 2
    assert resultado.stdout == ""
    assert f"'xml' is not one of {opcoes}" in " ".join(resultado.stderr.replace("│", " ").split())
    assert na_ajuda in ajuda.stdout


def test_snapshot_use_inexistente_escreve_so_no_stderr():
    with patch("agrobr.snapshots.get_snapshot", return_value=None):
        resultado = runner.invoke(app, ["snapshot", "use", "nope"])

    assert resultado.exit_code == 1
    assert resultado.stdout == ""
    assert "Use 'agrobr snapshot list' para ver snapshots disponiveis." in resultado.stderr


def _comandos(grupo, caminho=()):
    for nome, comando in grupo.commands.items():
        if hasattr(comando, "commands"):
            yield from _comandos(comando, (*caminho, nome))
        else:
            yield (*caminho, nome), comando


def test_todo_formato_da_cli_recusa_valor_fora_da_lista():
    recusas = []
    for caminho, comando in _comandos(typer.main.get_command(app)):
        if not any({"--formato", "--output"} & set(opcao.opts) for opcao in comando.params):
            continue
        argumentos = ["x" for p in comando.params if p.param_type_name == "argument" and p.required]
        resultado = runner.invoke(app, [*caminho, *argumentos, "-o", "xml"])
        erro = " ".join(resultado.stderr.replace("│", " ").split())
        recusas.append(
            (
                " ".join(caminho),
                resultado.exit_code,
                resultado.stdout,
                "'xml' is not one of" in erro,
            )
        )

    assert len(recusas) == 8
    assert recusas == [(nome, 2, "", True) for nome, *_ in recusas]


def test_todo_comando_tem_a_linha_de_descricao_no_help():
    comandos = list(_comandos(typer.main.get_command(app)))
    sem_descricao = []
    for caminho, comando in comandos:
        ajuda = runner.invoke(app, [*caminho, "--help"])
        texto = " ".join(ajuda.stdout.replace("│", " ").split())
        if not comando.help or " ".join(comando.help.split()) not in texto:
            sem_descricao.append(" ".join(caminho))

    assert len(comandos) == 19
    assert sem_descricao == []


@pytest.mark.usefixtures("cache_cepea")
def test_csv_da_cli_sai_com_uma_quebra_por_linha(monkeypatch):
    monkeypatch.setattr(os, "linesep", "\r\n")

    resultado = runner.invoke(app, ["cepea", "indicador", "soja", "-o", "csv"])

    assert resultado.exit_code == 0, resultado.output
    assert len(resultado.stdout_bytes.splitlines()) == 4
    assert set(pd.read_csv(io.BytesIO(resultado.stdout_bytes))["praca"]) == {
        "Paranaguá/PR",
        "Paraná",
    }


@pytest.mark.usefixtures("cache_cepea")
def test_csv_redirecionado_em_cp1252_sai_em_utf8(monkeypatch):
    monkeypatch.setattr(os, "linesep", "\r\n")
    buffer = io.BytesIO()
    saida = io.TextIOWrapper(buffer, encoding="cp1252", newline="\r\n")

    with patch.object(sys, "stdout", saida):
        cli.main(_version=False, verbose=False)
        cli.cepea_indicador(
            produto="soja", inicio=None, fim=None, ultimo=False, formato=cli.Formato.CSV
        )
        saida.flush()

    corpo = buffer.getvalue()
    assert b"\r\r\n" not in corpo
    assert corpo.count(b"\r\n") == corpo.count(b"\n") == 4
    assert "Paranaguá/PR".encode() in corpo
    assert set(pd.read_csv(io.BytesIO(corpo))["praca"]) == {"Paranaguá/PR", "Paraná"}


CLI_COM_CEPEA_SERVIDO = """
from unittest.mock import AsyncMock, patch

import pandas as pd

from agrobr import cli

frame = pd.DataFrame({"data": ["2026-09-25", "2026-09-26"], "praca": ["Paranaguá/PR", "Paraná"]})
with patch("agrobr.cepea.indicador", AsyncMock(return_value=frame)):
    cli.app(["cepea", "indicador", "soja", "-o", "csv"])
"""


@pytest.mark.skipif(sys.platform != "win32", reason="a saida redirecionada em cp1252 e do Windows")
def test_csv_da_cli_num_processo_real_do_windows():
    ambiente = {
        chave: valor
        for chave, valor in os.environ.items()
        if chave not in {"PYTHONIOENCODING", "PYTHONUTF8"}
    }

    processo = subprocess.run(
        [sys.executable, "-c", CLI_COM_CEPEA_SERVIDO],
        capture_output=True,
        env=ambiente,
        timeout=120,
        check=False,
    )

    assert processo.returncode == 0, processo.stderr.decode("utf-8", "replace")
    assert (
        processo.stdout == "data,praca\r\n2026-09-25,Paranaguá/PR\r\n2026-09-26,Paraná\r\n".encode()
    )
    assert pd.read_csv(io.BytesIO(processo.stdout))["praca"].tolist() == ["Paranaguá/PR", "Paraná"]
