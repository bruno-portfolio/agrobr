from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import ANY

import pytest
import structlog

from agrobr import _log

RAIZ = Path(__file__).resolve().parents[1]
PAGINA = RAIZ / "tests" / "golden_data" / "cepea" / "cache_ttl_20260923" / "soja_20260923.html"
LOGGING = {
    "padrao": "",
    "debug": "import logging\nlogging.basicConfig(level=logging.DEBUG)",
    "so_agrobr": (
        "import logging\nlogging.basicConfig(level=logging.WARNING)\n"
        'logging.getLogger("agrobr").setLevel(logging.INFO)\n'
        'logging.getLogger("outro").info("log de outro pacote")'
    ),
}
STRUCTLOG_DO_APP = {
    "configurado_antes": (
        "import sys, structlog\n"
        "structlog.configure(\n"
        '    processors=[lambda _l, _m, evento: {"marca": "usuario", **evento},'
        " structlog.processors.JSONRenderer()],\n"
        "    logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),\n"
        ")",
        "",
    ),
    "so_fabrica_depois": (
        "",
        "import sys, structlog\n"
        "structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=sys.stderr))",
    ),
    "sem_configurar": ("", ""),
}
BIBLIOTECA = """
{antes}
import agrobr
{depois}
from pathlib import Path
from agrobr.cepea.parsers.v1 import CepeaParserV1

html = Path({pagina!r}).read_text(encoding="utf-8")
print(CepeaParserV1().parse(html, "soja")[0].valor)
{app}
"""
IMPORT = """
import logging, structlog

antes = structlog.get_config()
import agrobr, agrobr.cli
print(structlog.is_configured(), structlog.get_config() == antes)
print(logging.getLogger("agrobr").handlers, logging.getLogger().handlers)
"""
CLI = """
import sys
from pathlib import Path
from agrobr import _log
from agrobr.cepea import client
from agrobr.cli import app

pagina = client.FetchResult(Path({pagina!r}).read_text(encoding="utf-8"), "cepea")


async def buscar(*_args, **_kwargs):
    _log.get_logger("agrobr.teste").warning("aviso_da_cli")
    return pagina


client.fetch_indicador_page = buscar
app(sys.argv[1:])
"""


def _rodar(codigo: str, *argumentos: str, cache: Path) -> subprocess.CompletedProcess[str]:
    """Roda num processo novo: o structlog e o ``logging`` são globais, e os do pytest não podem ser tocados."""
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


def _biblioteca(antes: str = "", depois: str = "", app: str = "") -> str:
    return BIBLIOTECA.format(antes=antes, depois=depois, app=app, pagina=str(PAGINA))


def _logs_do_parser(saida_de_erro: str) -> list[dict[str, object]]:
    prefixo = "INFO:agrobr.cepea.parsers.v1:"
    return [
        json.loads(linha.removeprefix(prefixo))
        for linha in saida_de_erro.splitlines()
        if linha.startswith(prefixo)
    ]


def test_import_nao_configura_o_structlog_nem_o_logging(tmp_path):
    processo = _rodar(IMPORT, cache=tmp_path)

    assert processo.returncode == 0, processo.stderr
    assert processo.stdout == "False True\n[] []\n"


@pytest.mark.parametrize("modo", list(LOGGING))
def test_como_biblioteca_o_log_nunca_sai_na_saida_padrao(tmp_path, modo):
    processo = _rodar(_biblioteca(antes=LOGGING[modo]), cache=tmp_path)

    assert processo.returncode == 0, processo.stderr
    assert processo.stdout == "161.65\n"
    eventos = [log["event"] for log in _logs_do_parser(processo.stderr)]
    assert ("parse_success" in eventos) is (modo != "padrao")
    assert (processo.stderr == "") is (modo == "padrao")
    assert "log de outro pacote" not in processo.stderr


@pytest.mark.parametrize("cenario", list(STRUCTLOG_DO_APP))
def test_structlog_do_app_segue_o_dele_e_o_agrobr_segue_no_logging(tmp_path, cenario):
    antes, depois = STRUCTLOG_DO_APP[cenario]
    codigo = _biblioteca(
        antes=f"{antes}\nimport logging\nlogging.basicConfig(level=logging.INFO)",
        depois=depois,
        app='import structlog\nstructlog.get_logger().info("log_do_app")',
    )

    processo = _rodar(codigo, cache=tmp_path)

    assert processo.returncode == 0, processo.stderr
    assert processo.stdout.splitlines()[0] == "161.65"
    logs = _logs_do_parser(processo.stderr)
    assert "parse_success" in [log["event"] for log in logs]
    assert all(log["logger"] == "agrobr.cepea.parsers.v1" and "marca" not in log for log in logs)
    assert "parse_success" not in processo.stdout
    do_app = {
        "configurado_antes": "stderr",
        "so_fabrica_depois": "stderr",
        "sem_configurar": "stdout",
    }
    assert "log_do_app" in getattr(processo, do_app[cenario])
    assert ('{"marca": "usuario", "event": "log_do_app"}' in processo.stderr) is (
        cenario == "configurado_antes"
    )


@pytest.mark.parametrize("verbose", [True, False])
def test_cli_com_verbose_loga_na_saida_de_erro(tmp_path, verbose):
    argumentos = [*(["--verbose"] if verbose else []), "cepea", "indicador", "soja", "--ultimo"]
    codigo = CLI.format(pagina=str(PAGINA))

    processo = _rodar(codigo, *argumentos, "--formato", "csv", cache=tmp_path)

    assert processo.returncode == 0, processo.stderr
    assert processo.stdout.splitlines()[0].startswith("data,")
    assert "161.65" in processo.stdout
    assert ("parse_success" in processo.stderr) is verbose
    assert "aviso_da_cli" in processo.stderr
    assert "parse_success" not in processo.stdout
    assert "aviso_da_cli" not in processo.stdout


def test_logger_do_agrobr_ignora_a_configuracao_global_do_structlog(capsys, caplog):
    structlog.configure(
        processors=[structlog.processors.JSONRenderer()],
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
    )
    caplog.set_level(logging.INFO, logger="agrobr")
    logger = _log.get_logger("agrobr.teste")

    logger.info("evento_de_teste", valor=1)
    logger.debug("evento_filtrado")

    assert capsys.readouterr().out == ""
    [registro] = [r for r in caplog.records if r.name.startswith("agrobr")]
    assert (registro.name, registro.levelno) == ("agrobr.teste", logging.INFO)
    assert json.loads(registro.getMessage()) == {
        "event": "evento_de_teste",
        "valor": 1,
        "logger": "agrobr.teste",
        "level": "info",
        "timestamp": ANY,
    }
