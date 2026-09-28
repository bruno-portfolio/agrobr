from __future__ import annotations

import json
import re
from pathlib import Path

import httpx
import pytest

from agrobr import comtrade
from agrobr.comtrade import api, models
from agrobr.exceptions import InvalidParameterError

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/comtrade/aliases_20260925"
REVISOES = ("H0", "H1", "H2", "H3", "H4", "H5", "H6")
SUBPOSICOES = {
    "soja": ("1201", r"SOYA BEANS", r"; SEED"),
    "cafe": ("0901", r"COFFEE", r"HUSKS|SUBSTITUTES"),
    "carne_frango": ("0207", r"GALLUS DOMESTICUS|^FOWLS?\b|^FOWL CUTS", None),
    "suco_laranja": ("2009", r"ORANGE", r"EXCLUDING ORANGE"),
}
POSICOES = {
    "farelo_soja": (("2304",), r"SOYA-BEAN OIL", None),
    "oleo_soja": (("1507",), r"^SOYA-BEAN OIL", None),
    "milho": (("1005",), r"MAIZE", None),
    "arroz": (("1006",), r"^RICE$", None),
    "trigo": (("1001",), r"WHEAT", None),
    "acucar": (("1701",), r"SUGAR", None),
    "etanol": (("2207",), r"ETHYL ALCOHOL", None),
    "algodao": (("5201", "5202", "5203"), r"^COTTON", r"WASTE"),
    "carne_bovina": (("0201", "0202"), r"BOVINE", None),
    "carne_suina": (("0203",), r"SWINE", None),
    "celulose": (("4701", "4702", "4703", "4704", "4705", "4706"), r"SODA OR SULPHATE", None),
    "tabaco": (("2401", "2402", "2403", "2404"), r"UNMANUFACTURED", None),
}


def _textos(revisao: str) -> dict[str, str]:
    dados = json.loads((GOLDEN / "referencia" / f"{revisao}.json").read_text(encoding="utf-8"))
    return {
        str(e["id"]): str(e["text"]).split(" - ", 1)[1].strip().upper() for e in dados["results"]
    }


def _produto(textos: dict[str, str], candidatos, exige: str, exclui: str | None) -> set[str]:
    return {
        codigo
        for codigo in candidatos
        if codigo in textos
        and re.search(exige, textos[codigo])
        and not (exclui and re.search(exclui, textos[codigo]))
    }


@pytest.fixture(scope="module")
def referencia():
    return {revisao: _textos(revisao) for revisao in REVISOES}


@pytest.mark.parametrize("alias", sorted(SUBPOSICOES))
def test_alias_sao_as_subposicoes_oficiais_do_produto(alias, referencia):
    posicao, exige, exclui = SUBPOSICOES[alias]
    codigos = set(models.resolve_hs(alias))
    for revisao in ("H0", "H6"):
        textos = referencia[revisao]
        filhas = [c for c in textos if len(c) == 6 and c.startswith(posicao)]
        produto = _produto(textos, filhas, exige, exclui)
        assert produto, (alias, revisao)
        if alias == "carne_frango" and revisao == "H0":
            assert not codigos & set(textos)
            assert "DUCKS" in referencia["H6"]["020741"] and "020741" in produto
            continue
        assert codigos & set(textos) == produto, (alias, revisao)
    vigentes = [r for r in REVISOES if codigos & set(referencia[r])]
    esperadas = REVISOES[1:] if alias == "carne_frango" else REVISOES
    assert tuple(vigentes) == esperadas, alias


@pytest.mark.parametrize("alias", sorted(POSICOES))
def test_alias_de_posicao_segue_a_descricao_oficial(alias, referencia):
    candidatas, exige, exclui = POSICOES[alias]
    textos = referencia["H6"]
    assert set(models.resolve_hs(alias)) == _produto(textos, candidatas, exige, exclui)
    assert all(set(models.resolve_hs(alias)) <= set(referencia[r]) for r in REVISOES)


def test_complexo_soja_e_a_uniao_das_partes():
    partes = {c for a in ("soja", "farelo_soja", "oleo_soja") for c in models.resolve_hs(a)}
    assert set(models.resolve_hs("complexo_soja")) == partes


def _servidor(monkeypatch, linhas: list[dict]) -> list[httpx.Request]:
    pedidos: list[httpx.Request] = []
    original = httpx.AsyncClient

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        codigos = request.url.params["cmdCode"].split(",")
        dados = [linha for linha in linhas if linha["cmdCode"] in codigos]
        if request.url.params.get("countOnly") == "true":
            return httpx.Response(200, json={"count": len(dados), "data": {}, "error": ""})
        return httpx.Response(200, json={"count": len(dados), "data": dados, "error": ""})

    def cliente(*args, **kwargs):
        return original(*args, transport=httpx.MockTransport(responder), **kwargs)

    monkeypatch.setenv("AGROBR_COMTRADE_API_KEY", "")
    monkeypatch.setattr(httpx, "AsyncClient", cliente)
    return pedidos


@pytest.mark.parametrize(
    "periodo,freq", [(1995, "A"), ("1990-2000", "A"), ("199512", "M"), ("1995,2020", "A")]
)
async def test_carne_frango_antes_de_1996_recusada_antes_da_rede(periodo, freq, monkeypatch):
    pedidos = _servidor(monkeypatch, [])

    with pytest.raises(InvalidParameterError, match="020741"):
        await comtrade.comercio("carne_frango", periodo=periodo, freq=freq)

    assert pedidos == []
    assert api.prepare_query(
        "carne_frango", reporter="US", periodo=1996
    ).hs_codes == models.resolve_hs("carne_frango")
