from __future__ import annotations

import sys
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from agrobr.alt.mapa_psr import api as mapa_psr
from agrobr.alt.sicar import api as sicar
from agrobr.ana import api as ana
from agrobr.antaq import api as antaq
from agrobr.datasets.base import BaseDataset, DatasetInfo
from agrobr.defensivos import api as defensivos
from agrobr.ibama import api as ibama
from agrobr.lista_suja import client as lista_suja
from agrobr.mapbiomas import api as mapbiomas
from agrobr.mapbiomas_alerta import api as alerta
from agrobr.sfb import api as sfb


async def _drenar(gerador: Any) -> None:
    async for _ in gerador:
        pass


GEO = {
    "ana_hidrografia": (
        ana.client,
        "fetch_layer",
        lambda: ana.hidrografia_geo(bbox=(-48.0, -16.0, -47.0, -15.0)),
    ),
    "ana_massas": (ana.client, "fetch_massas_dagua", lambda: ana.massas_dagua_geo(uf="DF")),
    "sfb_cnfp": (sfb.client, "fetch_layer", lambda: sfb.cnfp_geo(uf="DF")),
    "sfb_ifn": (sfb.client, "fetch_ifn", lambda: sfb.ifn_conglomerados_geo(uf="DF")),
    "sicar": (sicar.client, "fetch_imoveis_geo", lambda: sicar.imoveis_geo("DF")),
    "sicar_stream": (
        sicar.client,
        "stream_imoveis_geo",
        lambda: _drenar(sicar.imoveis_geo_stream("DF")),
    ),
    "alerta": (alerta, "_coletar", lambda: alerta.alertas_geo(token="x")),
    "ibama": (ibama._cache, "obter_embargos_csv", lambda: ibama.embargos_geo()),
}

POLARS = {
    "ibama": (ibama._cache, "obter_embargos_csv", lambda: ibama.embargos(as_polars=True)),
    "defensivos": (
        defensivos,
        "_load_snapshot",
        lambda: defensivos.tecnicos(nr_registro="00301", as_polars=True),
    ),
    "sicar": (sicar.client, "fetch_imoveis", lambda: sicar.imoveis("DF", as_polars=True)),
    "antaq": (antaq.client, "fetch_ano_zip", lambda: antaq.movimentacao(2024, as_polars=True)),
    "mapbiomas": (
        mapbiomas.client,
        "fetch_biome_state_bundle",
        lambda: mapbiomas.cobertura(as_polars=True),
    ),
    "mapa_psr": (
        mapa_psr,
        "_urls_dos_periodos",
        lambda: mapa_psr.apolices(ano=2023, as_polars=True),
    ),
}


@pytest.mark.parametrize("nome", GEO)
async def test_geopandas_ausente_falha_antes_da_rede(monkeypatch, nome):
    modulo, atributo, chamar = GEO[nome]
    fonte = Mock(side_effect=AssertionError("a fonte não pode ser chamada"))
    monkeypatch.setattr(modulo, atributo, fonte)
    monkeypatch.setitem(sys.modules, "geopandas", None)

    with pytest.raises(ImportError, match=r"agrobr\[geo\]"):
        await chamar()

    fonte.assert_not_called()


@pytest.mark.parametrize("nome", POLARS)
async def test_polars_ausente_falha_antes_da_rede(monkeypatch, nome):
    modulo, atributo, chamar = POLARS[nome]
    fonte = Mock(side_effect=AssertionError("a fonte não pode ser chamada"))
    monkeypatch.setattr(modulo, atributo, fonte)
    monkeypatch.setitem(sys.modules, "polars", None)

    with pytest.raises(ImportError, match=r"agrobr\[polars\]"):
        await chamar()

    fonte.assert_not_called()


def _datasets_com_espiao() -> tuple[BaseDataset, BaseDataset, Mock]:
    espiao = Mock()

    class NaoNativo(BaseDataset):
        info = DatasetInfo(name="queimadas", description="sem as_polars na assinatura")

        async def fetch(self, _produto: str, return_meta: bool = False, **_kwargs: Any) -> Any:
            espiao(return_meta)

    class Nativo(BaseDataset):
        info = DatasetInfo(name="queimadas", description="as_polars na assinatura")

        async def fetch(
            self, _produto: str, return_meta: bool = False, as_polars: bool = False, **_kwargs: Any
        ) -> Any:
            espiao(return_meta, as_polars)

    return NaoNativo(), Nativo(), espiao


async def test_dataset_com_polars_ausente_falha_antes_da_fonte(monkeypatch):
    nao_nativo, nativo, espiao = _datasets_com_espiao()
    monkeypatch.setitem(sys.modules, "polars", None)

    for dataset in (nao_nativo, nativo):
        with pytest.raises(ImportError, match=r"agrobr\[polars\]"):
            await dataset.fetch("x", as_polars=True)

    espiao.assert_not_called()


async def test_lista_suja_sem_pdfplumber_preserva_a_falha_do_csv(monkeypatch):
    publicacao = Mock(resources={"csv": "https://exemplo/csv", "pdf": "https://exemplo/pdf"})
    pedidos: list[str] = []

    async def buscar(_http: Any, url: str) -> Any:
        pedidos.append(url)
        raise lista_suja.SourceUnavailableError(source="lista_suja", url=url, last_error="HTTP 503")

    monkeypatch.setattr(lista_suja, "_fetch_http", buscar)
    monkeypatch.setattr(lista_suja, "_eligible", lambda _exc: True)
    monkeypatch.setitem(sys.modules, "pdfplumber", None)

    with pytest.raises(ImportError, match=r"agrobr\[pdf\]") as erro:
        await lista_suja._acquire(AsyncMock(), "auto", Mock(url="https://exemplo"), publicacao)

    assert pedidos == ["https://exemplo/csv"]
    assert "HTTP 503" in str(erro.value)


async def test_dataset_nativo_com_as_polars_true_por_padrao_falha_antes_da_fonte(monkeypatch):
    espiao = Mock()

    class NativoPolars(BaseDataset):
        info = DatasetInfo(name="queimadas", description="as_polars=True por padrão")

        async def fetch(self, as_polars: bool = True) -> Any:
            espiao(as_polars)

    monkeypatch.setitem(sys.modules, "polars", None)

    with pytest.raises(ImportError, match=r"agrobr\[polars\]"):
        await NativoPolars().fetch()

    espiao.assert_not_called()
