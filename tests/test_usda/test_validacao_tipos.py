from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts, datasets, usda
from agrobr.exceptions import InvalidParameterError
from agrobr.usda import models, parser
from tests.test_usda.conftest import registros


@pytest.mark.parametrize("camada", ["fonte", "dataset"])
@pytest.mark.parametrize(
    "opcoes",
    [
        {"market_year": -1},
        {"market_year": 0},
        {"market_year": 1959},
        {"market_year": 9999},
        {"market_year": True},
        {"market_year": "2024"},
        {"country": []},
        {"country": "INVALIDO"},
        {"country": ""},
        {"attributes": 7},
        {"attributes": [None]},
        {"pivot": "sim"},
    ],
)
async def test_psd_entrada_invalida_sem_requisicao(gateway, camada, opcoes):
    consulta = usda.psd
    if camada == "dataset":
        consulta = datasets.oferta_demanda_global
        nomes = {
            "market_year": "ano_comercial",
            "country": "pais",
            "attributes": "atributos",
            "pivot": "pivotar",
        }
        opcoes = {nomes.get(chave, chave): valor for chave, valor in opcoes.items()}
    with pytest.raises(InvalidParameterError):
        await consulta("soja", **opcoes)
    assert gateway.pedidos == []


async def test_psd_produto_nomeado_normaliza_acento_e_preserva_fonte(gateway):
    gateway.servir("cafe_BR_2024.json")
    frame = await usda.psd(produto=" CAFÉ ", market_year=2024)
    assert set(frame["commodity"]) == {"cafe"}
    publicado = next(r for r in registros("cafe_BR_2024.json") if r["attributeId"] == 28)
    assert frame.loc[frame["attribute_id"] == 28, "value"].item() == publicado["value"]
    assert set(frame["unit"]) == {"(1000 60 KG BAGS)"}


def test_psd_cheio_oficial_e_vazio_preservam_dtypes():
    cheio = parser.parse_psd_response(registros("soja_BR_2024.json"))
    vazio = parser.parse_psd_response([])
    pd.testing.assert_frame_equal(cheio.iloc[:0], vazio)
    assert cheio.loc[cheio["attribute_id"] == 4, "value"].item() == 47400.0
    assert str(cheio["value"].dtype) == "float64"
    assert str(cheio["market_year"].dtype) == "Int64"


async def test_dataset_pais_none_usa_brasil_e_nomes_portugueses(gateway):
    gateway.servir("soja_BR_2024.json")
    frame, meta = await datasets.oferta_demanda_global(
        "SOYBEANS", pais=None, ano_comercial=2024, return_meta=True
    )
    assert frame.loc[frame["codigo_atributo"] == 4, ["valor", "unidade"]].values.tolist() == [
        [47400.0, "(1000 HA)"]
    ]
    assert frame["ano_comercial"].eq(2024).all()
    assert frame["mes_atualizacao"].eq(4).all()
    assert meta.contract_version == "2.0"
    assert meta.columns == list(frame)
    assert len(frame.columns) == 13


@pytest.mark.parametrize("polars", [False, True])
async def test_psd_publico_cheio_e_vazio_tipados(gateway, polars):
    if polars:
        pytest.importorskip("polars")
    gateway.servir("soja_BR_2024.json")
    cheio = await usda.psd("soja", market_year=2024, as_polars=polars)
    vazio = await usda.psd("soja", market_year=2024, attributes=["perdas"], as_polars=polars)
    if polars:
        assert vazio.is_empty()
        assert vazio.schema == cheio.schema
    else:
        pd.testing.assert_frame_equal(cheio.iloc[:0], vazio)


def test_catalogo_psd_preserva_o_codigo_publicado_zz():
    assert models.nomes_de_pais()["ZZ"] == "Other"
    assert models.resolve_country_code("zz") == "ZZ"


def test_contrato_psd_historico_e_novo_permanecem_separados():
    from agrobr.contracts import datasets as schemas

    antigo = schemas.OFERTA_DEMANDA_GLOBAL_V1
    atual = contracts.get_contract("oferta_demanda_global")
    assert antigo.version == "1.0"
    assert antigo.primary_key == ["commodity_code", "country_code", "market_year", "attribute"]
    assert atual.to_dict() == schemas.OFERTA_DEMANDA_GLOBAL_V2.to_dict()
    assert atual.version == "2.0"
    assert atual.primary_key == ["codigo_produto", "codigo_pais", "ano_comercial", "atributo"]
