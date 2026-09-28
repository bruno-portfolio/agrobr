from __future__ import annotations

import logging
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import structlog

import agrobr
from agrobr import cli
from tests.helpers import sem_excecao

RAIZ = Path(__file__).resolve().parents[1]
PAGINA = RAIZ / "tests" / "golden_data" / "cepea" / "cache_ttl_20260923" / "soja_20260923.html"
PREPARO = {
    "padrao": "",
    "debug": "import logging\nlogging.basicConfig(level=logging.DEBUG)",
    "so_agrobr": (
        "import logging\nlogging.basicConfig(level=logging.WARNING)\n"
        'logging.getLogger("agrobr").setLevel(logging.INFO)\n'
        'logging.getLogger("outro").info("log de outro pacote")'
    ),
    "usuario": (
        "import sys, structlog\n"
        "structlog.configure(\n"
        '    processors=[lambda _l, _m, evento: {"marca": "usuario", **evento},'
        " structlog.processors.JSONRenderer()],\n"
        "    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),\n"
        ")"
    ),
}
BIBLIOTECA = """
import agrobr
from pathlib import Path
from agrobr.cepea.parsers.v1 import CepeaParserV1

html = Path({pagina!r}).read_text(encoding="utf-8")
print(CepeaParserV1().parse(html, "soja")[0].valor)
"""
CLI = """
import sys
from unittest.mock import AsyncMock
from pathlib import Path
from agrobr.cepea import client
from agrobr.cli import app

pagina = client.FetchResult(Path({pagina!r}).read_text(encoding="utf-8"), "cepea")
client.fetch_indicador_page = AsyncMock(return_value=pagina)
app(sys.argv[1:])
"""


def _rodar(codigo: str, *argumentos: str, cache: Path) -> subprocess.CompletedProcess[str]:
    """Roda num processo novo: o structlog é global, e o do pytest não pode ser tocado."""
    ambiente = {
        **os.environ,
        "PYTHONPATH": str(RAIZ),
        "PYTHONIOENCODING": "utf-8",
        "AGROBR_CACHE_CACHE_DIR": str(cache),
    }
    return subprocess.run(
        [sys.executable, "-c", codigo, *argumentos],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=ambiente,
        cwd=RAIZ,
        timeout=300,
        check=False,
    )


@pytest.mark.parametrize("modo", list(PREPARO))
def test_como_biblioteca_o_log_nunca_sai_na_saida_padrao(tmp_path, modo):
    codigo = PREPARO[modo] + "\n" + textwrap.dedent(BIBLIOTECA.format(pagina=str(PAGINA)))

    processo = _rodar(codigo, cache=tmp_path)

    assert processo.returncode == 0, processo.stderr
    assert processo.stdout == "161.65\n"
    esperado = {
        "padrao": "",
        "debug": "parse_success",
        "so_agrobr": "parse_success",
        "usuario": '"marca": "usuario"',
    }[modo]
    assert esperado in processo.stderr
    assert (processo.stderr == "") is (modo == "padrao")
    assert "log de outro pacote" not in processo.stderr


@pytest.mark.parametrize("verbose", [True, False])
def test_cli_com_verbose_loga_na_saida_de_erro(tmp_path, verbose):
    argumentos = [*(["--verbose"] if verbose else []), "cepea", "indicador", "soja", "--ultimo"]
    codigo = textwrap.dedent(CLI.format(pagina=str(PAGINA)))

    processo = _rodar(codigo, *argumentos, "--formato", "csv", cache=tmp_path)

    assert processo.returncode == 0, processo.stderr
    assert processo.stdout.splitlines()[0].startswith("data,")
    assert "161.65" in processo.stdout
    assert ("parse_success" in processo.stderr) is verbose
    assert "parse_success" not in processo.stdout


def test_configuracao_padrao_roteia_pelo_logging(capsys, caplog):
    structlog.reset_defaults()
    agrobr._configurar_logs()
    caplog.set_level(logging.INFO, logger="agrobr")

    structlog.get_logger("agrobr.teste").info("evento_de_teste")
    structlog.get_logger("agrobr.teste").debug("evento_filtrado")

    assert capsys.readouterr().out == ""
    mensagens = [registro.getMessage() for registro in caplog.records]
    assert [m for m in mensagens if "evento_de_teste" in m] and not [
        m for m in mensagens if "evento_filtrado" in m
    ]


def test_configuracao_do_usuario_prevalece():
    structlog.reset_defaults()
    fabrica = structlog.PrintLoggerFactory(file=sys.stderr)
    structlog.configure(logger_factory=fabrica)

    agrobr._configurar_logs()

    assert structlog.get_config()["logger_factory"] is fabrica


def test_cli_troca_a_configuracao_da_biblioteca(capsys):
    structlog.reset_defaults()
    agrobr._configurar_logs()
    cli._configure_cli_logging(True)

    with sem_excecao():
        structlog.get_logger("agrobr.teste").info("evento_da_cli")

    saida = capsys.readouterr()
    assert "evento_da_cli" in saida.err
    assert saida.out == ""
