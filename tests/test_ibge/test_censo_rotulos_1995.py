from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import ibge
from tests.helpers import sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "ibge" / "censo_rotulos_1995_20260927"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))["tabelas"]
NOME_PELO_ROTULO = {
    "Número de informantes": "informantes",
    "Número de estabelecimentos agropecuários": "estabelecimentos",
    "Efetivo dos rebanhos": "cabecas",
    "Área dos estabelecimentos agropecuários": "area",
    "Quantidade produzida": "producao",
    "Área colhida": "area_colhida",
}
TABELAS = {
    "efetivo_rebanho": ["323"],
    "uso_terra": ["316", "311"],
    "lavoura_temporaria": ["497", "492", "503"],
    "lavoura_permanente": ["509", "504", "510"],
}


def _corpo(tabela: str) -> bytes:
    corpo = (GOLDEN / f"{tabela}.json").read_bytes()
    digest = hashlib.sha256(corpo).hexdigest()
    assert digest == MANIFESTO[tabela]["sha256"]
    return corpo


def _rotulos(tabela: str) -> set[str]:
    codigo = re.search(r"/v/(\d+)/", MANIFESTO[tabela]["url"]).group(1)
    return {
        linha[chave[:-1] + "N"]
        for linha in json.loads(_corpo(tabela))
        for chave, valor in linha.items()
        if re.fullmatch(r"D\dC", chave) and valor == codigo
    }


@pytest.fixture
def sidra_gravada(monkeypatch: pytest.MonkeyPatch) -> None:
    async def send(
        _client: httpx.AsyncClient, request: httpx.Request, **_kwargs: Any
    ) -> httpx.Response:
        if request.url.host != "apisidra.ibge.gov.br":
            return httpx.Response(404, request=request)
        tabela = re.search(r"/t/(\d+)/", request.url.path).group(1)
        assert str(request.url) == MANIFESTO[tabela]["url"]
        return httpx.Response(
            200,
            content=_corpo(tabela),
            headers={"content-type": "application/json; charset=utf-8"},
            request=request,
        )

    monkeypatch.setattr(httpx.AsyncClient, "send", send)


@pytest.mark.usefixtures("sidra_gravada")
@pytest.mark.parametrize("tema", list(TABELAS))
async def test_nome_publicado_segue_o_rotulo_da_sidra_em_1995(tema):
    rotulos = {rotulo for tabela in TABELAS[tema] for rotulo in _rotulos(tabela)}

    with sem_excecao():
        frame = await ibge.censo_agro(tema, ano=1995, nivel="brasil")

    assert len(rotulos) == len(TABELAS[tema])
    assert set(frame["variavel"]) == {NOME_PELO_ROTULO.get(rotulo) for rotulo in rotulos}
