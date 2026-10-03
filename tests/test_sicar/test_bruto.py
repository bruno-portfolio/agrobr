from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import bruto
from agrobr.alt.sicar import bruto as sicar_bruto
from agrobr.alt.sicar import client
from agrobr.bruto import registry
from agrobr.exceptions import InvalidParameterError, ParseError
from tests.test_bruto.conftest import manifesto, resposta

GOLDEN = Path(__file__).parents[1] / "golden_data" / "sicar"
PASTA = GOLDEN / "bruto_imoveis_4674_20261003"
METADATA = json.loads((PASTA / "metadata.json").read_text("utf-8"))
ESPERADO = json.loads((PASTA / "expected.json").read_text("utf-8"))
SELECAO = {k: v for k, v in METADATA["selecao"].items() if k != "bbox"} | {
    "bbox": tuple(METADATA["selecao"]["bbox"])
}
CRS_4674 = {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4674"}}


def _hits(total: int) -> bytes:
    return (
        b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs/2.0" '
        b'numberMatched="%d" numberReturned="0"/>' % total
    )


class Sicar:
    """Servidor falso: ``paginas`` por ``startIndex`` (corpo oficial) ou ``feicoes`` reembaladas em páginas sintéticas."""

    def __init__(self) -> None:
        self.hits = [(PASTA / "hits.xml").read_bytes(), (PASTA / "hits_depois.xml").read_bytes()]
        self.paginas = {
            info["start_index"]: (PASTA / nome).read_bytes()
            for nome, info in METADATA["files"].items()
            if info["papel"] == "pagina"
        }
        self.feicoes: list[dict[str, Any]] | None = None
        self.pedidos: list[dict[str, str]] = []

    def reembalar(self, feicoes: list[dict[str, Any]]) -> None:
        self.feicoes = feicoes
        self.hits = [_hits(len(feicoes))]

    def responder(self, request: httpx.Request) -> httpx.Response:
        parametros = dict(request.url.params)
        self.pedidos.append(parametros)
        if parametros.get("resultType") == "hits":
            corpo = self.hits[0] if len(self.hits) == 1 else self.hits.pop(0)
            return resposta(200, corpo, {"Content-Type": "text/xml"})
        inicio = int(parametros["startIndex"])
        if self.feicoes is None:
            corpo = self.paginas[inicio]
        else:
            fatia = self.feicoes[inicio : inicio + int(parametros["count"])]
            envelope = {
                "type": "FeatureCollection",
                "features": fatia,
                "numberMatched": len(self.feicoes),
                "numberReturned": len(fatia),
                "crs": CRS_4674,
            }
            corpo = json.dumps(envelope).encode()
        return resposta(200, corpo, {"Content-Type": "application/json;charset=utf-8"})


@pytest.fixture
def sicar(monkeypatch: pytest.MonkeyPatch) -> Sicar:
    servidor = Sicar()

    def make_session() -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(servidor.responder))

    monkeypatch.setattr(client, "make_session", make_session)
    chave = ("sicar", "imoveis")
    monkeypatch.setitem(
        registry.RECURSOS, chave, dataclasses.replace(registry.RECURSOS[chave], habilitado=True)
    )
    return servidor


def _versoes() -> list[dict[str, Any]]:
    """As 2 versões reais de um imóvel de GO (golden de 16/09), com ``dat_criacao`` distintos."""
    colecao = json.loads((GOLDEN / "versoes_20260916" / "go_duplicates.json").read_text("utf-8"))
    return copy.deepcopy(colecao["features"])


async def _coletar(destino: Path, **kwargs: Any) -> Any:
    argumentos = {"tamanho_pagina": 5, "compactar": False, **SELECAO} | kwargs
    return await bruto.coletar("sicar", "imoveis", destino=destino, **argumentos)


async def test_corpos_oficiais_com_feature_id_e_ordem_pelo_par_conferidos(sicar, tmp_path):
    entrada = (await _coletar(tmp_path)).entrada

    assert (entrada.status, entrada.crs, entrada.formato) == ("ok", "EPSG:4674", "geojson")
    assert entrada.cobertura.estado == "conferida" and entrada.cobertura.campo_id == "feature.id"
    assert entrada.cobertura.recebidas == ESPERADO["total_antes"] == len(ESPERADO["ids"])
    assert entrada.parametros == METADATA["parametros"]
    assert entrada.parametros["sortBy"] == "cod_imovel A,dat_criacao A"
    assert entrada.parametros["CQL_FILTER"].startswith("BBOX(geo_area_imovel,")
    assert entrada.selecao.camada == "sicar:sicar_imoveis_df"
    paginas = [(n, i) for n, i in METADATA["files"].items() if i["papel"] == "pagina"]
    for pagina, (nome, info) in zip(entrada.paginas, paginas, strict=True):
        assert (pagina.sha256, pagina.bytes) == (info["sha256"], info["bytes"])
        assert (tmp_path / pagina.arquivo).read_bytes() == (PASTA / nome).read_bytes()
    assert all("propertyName" not in p and "srsName" not in p for p in sicar.pedidos)
    assert [p.get("resultType") for p in sicar.pedidos].count("hits") == 2


async def test_duas_versoes_do_mesmo_imovel_atravessando_paginas_sao_guardadas(sicar, tmp_path):
    versoes = _versoes()
    sicar.reembalar(versoes)

    entrada = (await _coletar(tmp_path, tamanho_pagina=1)).entrada

    assert entrada.status == "ok" and [p.feicoes_recebidas for p in entrada.paginas] == [1, 1]
    assert entrada.cobertura.ids_distintos == 2
    guardados = [
        json.loads((tmp_path / p.arquivo).read_bytes())["features"][0]["id"]
        for p in entrada.paginas
    ]
    assert guardados == [f["id"] for f in versoes]
    assert len({f["properties"]["cod_imovel"] for f in versoes}) == 1


def _empate(feicoes: list[dict[str, Any]]) -> None:
    feicoes[1]["properties"]["dat_criacao"] = feicoes[0]["properties"]["dat_criacao"]


def _invertidas(feicoes: list[dict[str, Any]]) -> None:
    feicoes.reverse()


def _sem_id(feicoes: list[dict[str, Any]]) -> None:
    del feicoes[1]["id"]


def _id_repetido(feicoes: list[dict[str, Any]]) -> None:
    feicoes[1]["id"] = feicoes[0]["id"]


def _data_sem_zona(feicoes: list[dict[str, Any]]) -> None:
    feicoes[1]["properties"]["dat_criacao"] = "2026-09-16T11:40:11.170"


def _data(valor: str) -> Any:
    def aplicar(feicoes: list[dict[str, Any]]) -> None:
        feicoes[1]["properties"]["dat_criacao"] = valor

    return aplicar


NAO_COMPROVADA: set[str] = set()

MUTANTES = {
    "versoes_empatadas": _empate,
    "versoes_fora_de_ordem": _invertidas,
    "feature_id_faltando": _sem_id,
    "feature_id_repetido": _id_repetido,
    "dat_criacao_fora_do_formato": _data_sem_zona,
    "dat_criacao_30_de_fevereiro": _data("2026-02-30T11:40:11.170Z"),
    "dat_criacao_mes_13": _data("2026-13-16T11:40:11.170Z"),
    "dat_criacao_hora_25": _data("2026-09-16T25:40:11.170Z"),
}


@pytest.mark.parametrize("mutante", sorted(MUTANTES))
async def test_versoes_sem_ordem_comprovada_ou_sem_id_viram_erro(sicar, tmp_path, mutante):
    versoes = _versoes()
    MUTANTES[mutante](versoes)
    sicar.reembalar(versoes)

    with pytest.raises(ParseError):
        await _coletar(tmp_path, tamanho_pagina=1)

    (registro,) = manifesto(tmp_path)
    assert registro["status"] == "erro" and registro["cobertura"]["completa"] is False
    esperado = "nao_comprovada" if mutante in NAO_COMPROVADA else "divergente"
    assert registro["cobertura"]["estado"] == esperado


async def test_total_alterado_vira_erro_e_a_retomada_recomeca_da_primeira_pagina(sicar, tmp_path):
    original = sicar.hits[1]
    sicar.hits[1] = _hits(15)

    with pytest.raises(ParseError, match="contagem"):
        await _coletar(tmp_path)
    sicar.hits = [(PASTA / "hits.xml").read_bytes(), original]
    sicar.pedidos.clear()
    retomada = await _coletar(tmp_path, retomar=True)

    assert (retomada.entrada.status, retomada.reutilizado) == ("ok", False)
    assert [p.get("startIndex") for p in sicar.pedidos] == [None, "0", "5", "10", None]
    assert [e["status"] for e in manifesto(tmp_path)] == ["ok"]


async def test_pausa_do_client_a_partir_da_setima_pagina(sicar, tmp_path, monkeypatch):
    pausas: list[float] = []

    async def dormir(segundos: float) -> None:
        pausas.append(segundos)

    monkeypatch.setattr(sicar_bruto.asyncio, "sleep", dormir)
    monkeypatch.setattr(client, "THROTTLE_DELAY", 0.0123)
    feicoes = [
        f
        for p in ESPERADO["paginas"]
        for f in json.loads((PASTA / p["arquivo"]).read_bytes())["features"]
    ]
    sicar.reembalar(feicoes)

    entrada = (await _coletar(tmp_path, tamanho_pagina=2)).entrada

    assert entrada.status == "ok" and len(entrada.paginas) == 7
    assert pausas.count(0.0123) == 1


async def test_uf_obrigatoria_antes_da_rede(sicar, tmp_path):
    with pytest.raises(InvalidParameterError, match="exige uf"):
        await bruto.coletar("sicar", "imoveis", destino=tmp_path)

    assert sicar.pedidos == []
