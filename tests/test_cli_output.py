from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from agrobr import _log
from agrobr.cli import app


@pytest.mark.parametrize(
    "command, target",
    [
        (["cepea", "indicador", "soja"], "agrobr.cepea.indicador"),
        (["conab", "safras", "soja"], "agrobr.conab.safras"),
        (["conab", "balanco"], "agrobr.conab.balanco"),
        (["ibge", "pam", "soja"], "agrobr.ibge.pam"),
        (["ibge", "lspa", "soja"], "agrobr.ibge.lspa"),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("formato", ["json", "csv"])
def test_machine_output_round_trip(command, target, empty, formato):
    df = pd.DataFrame({"produto": ["feijão"], "valor": [12.5]})
    if empty:
        df = df.iloc[:0]
    with patch(target, new_callable=AsyncMock, return_value=df):
        result = CliRunner().invoke(app, [*command, "--formato", formato])
    assert result.exit_code == 0, result.output
    assert "Consultando" not in result.stdout
    assert "Consultando" in result.stderr
    if formato == "json":
        assert json.loads(result.stdout) == df.to_dict(orient="records")
    else:
        reader = csv.DictReader(io.StringIO(result.stdout))
        assert reader.fieldnames == ["produto", "valor"]
        assert list(reader) == ([] if empty else [{"produto": "feijão", "valor": "12.5"}])


def test_empty_snapshot_list_json():
    with patch("agrobr.snapshots.list_snapshots", return_value=[]):
        result = CliRunner().invoke(app, ["snapshot", "list", "--formato", "json"])
    assert result.exit_code == 0
    assert json.loads(result.stdout) == []


@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("falha", [False, True])
def test_logs_legiveis_no_stderr_e_configuracao_restaurada(verbose, falha):
    logger = _log.get_logger("agrobr.cli.teste")
    stdlib = logging.getLogger("agrobr")
    original = (stdlib.handlers[:], stdlib.level, stdlib.propagate, _log.PROCESSADORES[:])

    async def consultar(*_args, **_kwargs):
        logger.info("coleta_iniciada", tentativa=1)
        logger.warning("fonte_indisponivel", tentativa=2)
        if falha:
            raise RuntimeError("coleta interrompida")
        return pd.DataFrame({"produto": ["soja"], "valor": [12.5]})

    argumentos = ["--verbose"] if verbose else []
    argumentos += ["ibge", "pam", "soja", "--formato", "json"]
    with patch("agrobr.ibge.pam", side_effect=consultar):
        resultado = CliRunner().invoke(app, argumentos)

    assert resultado.exit_code == (1 if falha else 0), resultado.output
    assert "fonte_indisponivel" in resultado.stderr
    assert ("coleta_iniciada" in resultado.stderr) is verbose
    assert '"event"' not in resultado.stderr
    assert "fonte_indisponivel" not in resultado.stdout
    if falha:
        assert resultado.stdout == ""
        assert "Erro: coleta interrompida" in resultado.stderr
    else:
        assert json.loads(resultado.stdout) == [{"produto": "soja", "valor": 12.5}]
    assert (stdlib.handlers, stdlib.level, stdlib.propagate, _log.PROCESSADORES) == original


@pytest.mark.parametrize(
    "argumentos",
    [["health", "--output", "json"], ["doctor", "--json"], ["snapshot", "list", "--json"]],
)
def test_opcoes_antigas_de_formato_sao_recusadas(argumentos):
    resultado = CliRunner().invoke(app, argumentos)
    assert resultado.exit_code == 2
    assert resultado.stdout == ""
    assert "No such option" in resultado.stderr


@pytest.mark.parametrize(
    ("argumentos", "alvo"),
    [
        (["ibge", "pam", "soja"], "agrobr.ibge.pam"),
        (["ibge", "censo-historico", "uso_terra"], "agrobr.ibge.censo_agro_historico"),
    ],
)
@pytest.mark.parametrize("ano", ["abc", "2020,abc", "2020,", ""])
def test_ano_invalido_recusado_antes_da_consulta(argumentos, alvo, ano):
    with patch(alvo, new_callable=AsyncMock) as consulta:
        resultado = CliRunner().invoke(app, [*argumentos, "--ano", ano])
    assert resultado.exit_code == 2
    assert resultado.stdout == ""
    assert "ano inválido" in resultado.stderr
    assert "invalid literal" not in resultado.stderr
    consulta.assert_not_awaited()


@pytest.mark.parametrize("pesquisa", ["pam", "PAM", "lspa", "LSPA", "inexistente"])
def test_pesquisa_escolhe_o_catalogo_sem_fallback_silencioso(pesquisa):
    with (
        patch("agrobr.ibge.produtos_pam", new_callable=AsyncMock, return_value=["soja"]) as pam,
        patch(
            "agrobr.ibge.produtos_lspa", new_callable=AsyncMock, return_value=["milho_1"]
        ) as lspa,
    ):
        resultado = CliRunner().invoke(app, ["ibge", "produtos", "--pesquisa", pesquisa])
    if pesquisa == "inexistente":
        assert resultado.exit_code == 2
        assert resultado.stdout == ""
        pam.assert_not_awaited()
        lspa.assert_not_awaited()
    else:
        assert resultado.exit_code == 0
        esperado, outro = (pam, lspa) if pesquisa.lower() == "pam" else (lspa, pam)
        esperado.assert_awaited_once()
        outro.assert_not_awaited()
        assert ("soja" if pesquisa.lower() == "pam" else "milho_1") in resultado.stdout


@pytest.mark.parametrize(
    ("argumentos", "alvo", "filtros"),
    [
        (
            ["cepea", "indicador", "soja", "--praca", "Paranaguá/PR"],
            "agrobr.cepea.indicador",
            {"praca": "Paranaguá/PR"},
        ),
        (
            ["conab", "safras", "soja", "--levantamento", "3"],
            "agrobr.conab.safras",
            {"levantamento": 3},
        ),
        (
            ["conab", "balanco", "soja", "--safra", "2024/25", "--levantamento", "3"],
            "agrobr.conab.balanco",
            {"safra": "2024/25", "levantamento": 3},
        ),
    ],
)
def test_filtros_da_cli_chegam_a_api(argumentos, alvo, filtros):
    with patch(alvo, new_callable=AsyncMock, return_value=pd.DataFrame()) as consulta:
        resultado = CliRunner().invoke(app, argumentos)
    assert resultado.exit_code == 0, resultado.output
    consulta.assert_awaited_once()
    assert {chave: consulta.await_args.kwargs[chave] for chave in filtros} == filtros


LEVANTAMENTOS = [
    {
        "url": f"https://www.gov.br/conab/{n}o-levantamento-safra-2025-26/tabela.xlsx",
        "levantamento": n,
        "safra": "2025/26",
        "ano_inicio": 2025,
        "ano_fim": 26,
        "data_publicacao": date(2026, n % 12 + 1, 10) if n > 1 else None,
    }
    for n in range(12, 0, -1)
]


@pytest.mark.parametrize("formato", ["table", "csv", "json"])
def test_levantamentos_saem_inteiros_no_formato_pedido(formato):
    with patch("agrobr.conab.levantamentos", new_callable=AsyncMock, return_value=LEVANTAMENTOS):
        resultado = CliRunner().invoke(app, ["conab", "levantamentos", "--formato", formato])

    assert resultado.exit_code == 0, resultado.output
    assert "Listando levantamentos" in resultado.stderr
    assert "Listando" not in resultado.stdout
    if formato == "json":
        esperado = [
            {
                **lev,
                "data_publicacao": lev["data_publicacao"]
                and f"{lev['data_publicacao']}T00:00:00.000",
            }
            for lev in LEVANTAMENTOS
        ]
        assert json.loads(resultado.stdout) == esperado
    elif formato == "csv":
        linhas = list(csv.DictReader(io.StringIO(resultado.stdout)))
        assert [int(linha["levantamento"]) for linha in linhas] == list(range(12, 0, -1))
        assert list(linhas[0]) == list(LEVANTAMENTOS[0])
        assert (linhas[0]["data_publicacao"], linhas[-1]["data_publicacao"]) == ("2026-01-10", "")
    else:
        linhas = resultado.stdout.splitlines()
        assert len(linhas) == 13
        assert all(lev["url"] in resultado.stdout for lev in LEVANTAMENTOS)
        assert "e mais" not in resultado.stdout
