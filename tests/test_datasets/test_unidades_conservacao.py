from __future__ import annotations

import pytest

from agrobr import contracts, datasets
from agrobr.exceptions import InvalidParameterError, ParseError, ResourceLimitError
from tests.helpers import levanta_exatamente
from tests.test_cnuc.golden import TABULAR, hits, instalar


@pytest.mark.parametrize(
    "kwargs",
    [
        {"uf": "ZZ"},
        {"municipio": "Atlantida"},
        {"esfera": "privada"},
        {"categoria": "Bosque"},
        {"grupo": "XX"},
        {"bioma": "Marte"},
        {"bbox": (1, 2, 0, 3)},
        {"max_registros": -1},
        {"as_polars": 1},
        {"return_meta": "sim"},
    ],
    ids=lambda valor: str(valor),
)
async def test_selecao_invalida_antes_da_rede(monkeypatch, kwargs):
    fetch_count, fetch_ucs = instalar(monkeypatch)
    with levanta_exatamente(InvalidParameterError):
        await datasets.unidades_conservacao(**kwargs)
    fetch_count.assert_not_awaited()
    fetch_ucs.assert_not_awaited()


async def test_deterministic_recusado_antes_da_rede(monkeypatch):
    fetch_count, _ = instalar(monkeypatch)
    async with datasets.deterministic("2026-09-01"):
        with pytest.raises(InvalidParameterError, match="camada é corrente"):
            await datasets.unidades_conservacao()
    fetch_count.assert_not_awaited()


async def test_produto_e_argumento_desconhecido(monkeypatch):
    fetch_count, _ = instalar(monkeypatch)
    dataset = datasets.get_dataset("unidades_conservacao")
    with pytest.raises(InvalidParameterError, match="não aceita produto"):
        await dataset.fetch("soja")
    with pytest.raises(TypeError, match="Argumentos desconhecidos"):
        await dataset.fetch(ano=2020)
    fetch_count.assert_not_awaited()


async def test_contrato_e_proveniencia(monkeypatch):
    instalar(monkeypatch)
    frame, meta = await datasets.unidades_conservacao(uf="SE", return_meta=True)
    contracts.validate_dataset(frame, "unidades_conservacao")
    assert len(frame) == 19
    assert meta.dataset == "unidades_conservacao"
    assert meta.contract_version == "1.0"
    assert meta.source == "datasets.unidades_conservacao/cnuc_wfs"
    assert meta.attempted_sources == ["cnuc_wfs"]
    assert meta.source_details["coverage"]["status"] == "count_reconciled"


async def test_aviso_de_municipio_chega_ao_meta_do_dataset(monkeypatch):
    corpo = TABULAR.replace(
        b"<ms:municipio>GARARU (SE)</ms:municipio>",
        b"<ms:municipio>GARARU-X (SE)</ms:municipio>",
        1,
    )
    instalar(monkeypatch, tabular=corpo)
    with pytest.warns(UserWarning, match="GARARU-X"):
        frame, meta = await datasets.unidades_conservacao(municipio=2802403, return_meta=True)
    assert frame["codigo"].tolist() == []
    assert len(meta.validation_warnings) == 1
    assert "GARARU-X (SE) em 0000.28.5574" in meta.validation_warnings[0]


async def test_vazio_sai_com_os_dtypes_do_contrato(monkeypatch):
    instalar(monkeypatch, contagem=hits(0))
    frame = await datasets.unidades_conservacao(esfera="municipal")
    assert frame.empty
    assert frame.dtypes.equals(contracts.get_contract("unidades_conservacao").empty_frame().dtypes)


async def test_limite_e_contagem_atravessam_o_dataset(monkeypatch):
    instalar(monkeypatch, contagem=hits(10_001))
    with pytest.raises(ResourceLimitError, match="excede o limite"):
        await datasets.unidades_conservacao()
    instalar(monkeypatch, contagem=hits(20))
    with pytest.raises(ParseError, match="Contagem divergente|layout"):
        await datasets.unidades_conservacao(uf="SE")
