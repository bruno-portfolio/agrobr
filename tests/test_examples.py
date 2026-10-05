from __future__ import annotations

import ast
import importlib
import importlib.util
import inspect
from pathlib import Path
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import cepea, conab, ibge

EXEMPLOS = sorted((Path(__file__).resolve().parents[1] / "examples").glob("*.py"))


def chamadas_do_agrobr(arvore: ast.Module) -> list[tuple[str, str, ast.Call]]:
    modulos = {
        alias.asname or alias.name: f"{no.module}.{alias.name}"
        for no in ast.walk(arvore)
        if isinstance(no, ast.ImportFrom) and (no.module or "").startswith("agrobr")
        for alias in no.names
    }
    return [
        (modulos[no.func.value.id], no.func.attr, no)
        for no in ast.walk(arvore)
        if isinstance(no, ast.Call)
        and isinstance(no.func, ast.Attribute)
        and isinstance(no.func.value, ast.Name)
        and no.func.value.id in modulos
    ]


@pytest.mark.parametrize("exemplo", EXEMPLOS, ids=lambda caminho: caminho.name)
def test_exemplo_chama_a_api_publica_com_argumentos_validos(exemplo: Path):
    chamadas = chamadas_do_agrobr(ast.parse(exemplo.read_text(encoding="utf-8")))

    erros = []
    for modulo, nome, chamada in chamadas:
        onde = f"{exemplo.name}:{chamada.lineno} {modulo}.{nome}"
        funcao = getattr(importlib.import_module(modulo), nome, None)
        if funcao is None:
            erros.append(f"{onde} não existe")
            continue
        posicionais = [None] * sum(not isinstance(arg, ast.Starred) for arg in chamada.args)
        nomeados = {kw.arg: None for kw in chamada.keywords if kw.arg is not None}
        try:
            inspect.signature(funcao).bind(*posicionais, **nomeados)
        except TypeError as erro:
            erros.append(f"{onde}: {erro}")

    assert chamadas
    assert erros == []


async def test_pipeline_async_exporta_safras_e_pam_mesmo_sem_nenhum_preco(monkeypatch, tmp_path):
    caminho = next(exemplo for exemplo in EXEMPLOS if exemplo.name == "pipeline_async.py")
    spec = importlib.util.spec_from_file_location("exemplo_pipeline_async", caminho)
    assert spec is not None and spec.loader is not None
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    tabela = pd.DataFrame({"produto": ["soja"], "valor": [1.0]})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cepea, "indicador", AsyncMock(side_effect=RuntimeError("simulada")))
    monkeypatch.setattr(conab, "safras", AsyncMock(return_value=tabela))
    monkeypatch.setattr(ibge, "pam", AsyncMock(return_value=tabela))

    await modulo.main()

    assert sorted(p.name for p in (tmp_path / "output").iterdir()) == ["pam.csv", "safras.csv"]
