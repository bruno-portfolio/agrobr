"""Tests for agrobr.health.registry module."""

from __future__ import annotations

import ssl

import httpx

from agrobr.constants import Fonte
from agrobr.health import checker
from agrobr.health.registry import (
    HEALTH_REGISTRY,
    SourceHealthConfig,
    get_affected_datasets,
)


class TestHealthRegistry:
    def test_all_fontes_in_registry(self):
        for fonte in Fonte:
            assert fonte in HEALTH_REGISTRY, f"{fonte} missing from HEALTH_REGISTRY"

    def test_all_urls_are_nonempty_strings(self):
        for fonte, config in HEALTH_REGISTRY.items():
            assert isinstance(config.url, str), f"{fonte} url is not a string"
            assert len(config.url) > 0, f"{fonte} has empty url"

    def test_config_is_frozen_dataclass(self):
        config = HEALTH_REGISTRY[Fonte.CEPEA]
        assert isinstance(config, SourceHealthConfig)


class TestSourceDatasetMap:
    def test_inmet_multiple_fetchers_list_dataset_once(self):
        assert get_affected_datasets(Fonte.INMET) == ["clima"]

    def test_lista_devolvida_e_copia(self):
        get_affected_datasets(Fonte.INMET).append("alterado")
        assert get_affected_datasets(Fonte.INMET) == ["clima"]


def test_sfb_consulta_pontos_ifn_ativos_no_df():
    url = httpx.URL(HEALTH_REGISTRY[Fonte.SFB].url)
    assert url.path.endswith("/DadosAbertos-IFN/dataset_ifn_tb_pontos_lote/FeatureServer/0/query")
    assert dict(url.params) == {
        "where": "no_uf='DF'",
        "outFields": "*",
        "outSR": "4326",
        "f": "json",
        "returnCountOnly": "true",
    }


async def test_sonda_da_funai_completa_a_cadeia_com_o_intermediario_fixado(monkeypatch):
    recebidos = []
    original = httpx.AsyncClient

    def fabrica(**kwargs):
        recebidos.append(kwargs["verify"])
        resposta = httpx.Response(200, text="<wfs:WFS_Capabilities/>")
        return original(transport=httpx.MockTransport(lambda _: resposta), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", fabrica)
    await checker._check_http(HEALTH_REGISTRY[Fonte.FUNAI])

    (contexto,) = recebidos
    assert isinstance(contexto, ssl.SSLContext)
    assert contexto.verify_mode == ssl.CERT_REQUIRED and contexto.check_hostname
    assert "Sectigo Public Server Authentication CA OV R36" in [
        dict(campo[0] for campo in certificado["subject"]).get("commonName")
        for certificado in contexto.get_ca_certs()
    ]
    assert HEALTH_REGISTRY[Fonte.INCRA].verify is True


async def test_sonda_do_comexstat_valida_a_cadeia_com_o_intermediario_fixado(monkeypatch):
    recebidos = []
    original = httpx.AsyncClient

    def fabrica(**kwargs):
        recebidos.append(kwargs["verify"])
        return original(transport=httpx.MockTransport(lambda _: httpx.Response(200)), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", fabrica)
    await checker._check_http(HEALTH_REGISTRY[Fonte.COMEXSTAT])

    (contexto,) = recebidos
    assert isinstance(contexto, ssl.SSLContext)
    assert contexto.verify_mode == ssl.CERT_REQUIRED and contexto.check_hostname
    assert "AC SERPRO AR46 OV TLS CA 2025" in [
        dict(campo[0] for campo in certificado["subject"]).get("commonName")
        for certificado in contexto.get_ca_certs()
    ]
