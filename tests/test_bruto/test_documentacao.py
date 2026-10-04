from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from agrobr import bruto
from agrobr.bruto import models, registry

DOCS = Path(__file__).parents[2] / "docs"
CONTRATOS = [DOCS / "contracts" / "bruto.md", DOCS / "contracts" / "bruto.en.md"]
APIS = [DOCS / "api" / "bruto.md", DOCS / "api" / "bruto.en.md"]


def _primeira_coluna(caminho: Path) -> list[str]:
    return re.findall(r"^\| `([^`]+)` \|", caminho.read_text("utf-8"), flags=re.M)


def _exemplos(caminho: Path) -> list[str]:
    return [
        linha
        for linha in caminho.read_text("utf-8").splitlines()
        if linha.startswith('{"agrobr_version"')
    ]


def test_contrato_pt_e_en_documentam_os_mesmos_campos_e_todos_os_do_manifesto():
    pt, en = (_primeira_coluna(caminho) for caminho in CONTRATOS)

    assert pt == en
    assert set(models.RecursoBruto.model_fields) <= set(pt)
    assert {f"selecao.{campo}" for campo in models.Selecao.model_fields} <= set(pt)
    assert {f"cobertura.{campo}" for campo in models.Cobertura.model_fields} <= set(pt)


def test_os_dez_exemplos_sao_iguais_nas_duas_linguas_e_validos_no_modelo():
    pt, en = (_exemplos(caminho) for caminho in CONTRATOS)

    assert pt == en and len(pt) == 10
    for linha in pt:
        assert models.RecursoBruto.model_validate_json(linha).linha() == linha


@pytest.mark.parametrize("caminho", CONTRATOS, ids=lambda caminho: caminho.name)
def test_tabela_de_recursos_do_contrato_e_a_do_registro(caminho):
    secao = caminho.read_text("utf-8").split("\n## ")[1]
    linhas = [linha for linha in secao.splitlines() if linha.count("|") == 5]
    pares = {
        tuple(m.groups()) for linha in linhas if (m := re.match(r"\| `(\w+)` \| `(\w+)` \|", linha))
    }

    assert pares == set(registry.RECURSOS)


@pytest.mark.parametrize("caminho", APIS, ids=lambda caminho: caminho.name)
def test_pagina_da_api_documenta_os_parametros_do_coletar(caminho):
    documentados = re.findall(r"^\| ([a-z_]+) \| ", caminho.read_text("utf-8"), flags=re.M)

    parametros = list(inspect.signature(bruto.coletar).parameters)
    assert documentados[: len(parametros)] == parametros
