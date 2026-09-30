from __future__ import annotations

import copy

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.datasets.censo_agropecuario import CensoAgropecuarioDataset
from agrobr.datasets.extrativismo_vegetal import ExtrativsmoVegetalDataset
from agrobr.datasets.pecuaria_municipal import PecuariaMunicipalDataset
from agrobr.datasets.producao_anual import ProducaoAnualDataset
from agrobr.datasets.silvicultura import SilviculturaDataset
from agrobr.normalize import regions
from tests import helpers
from tests.test_datasets.conftest import make_source
from tests.test_datasets.test_censo_agropecuario import _mock_df as censo_mock
from tests.test_datasets.test_extrativismo_vegetal import _mock_df as extrativismo_mock
from tests.test_datasets.test_pecuaria_municipal import _mock_df as pecuaria_mock
from tests.test_datasets.test_producao_anual import _mock_df as producao_mock
from tests.test_datasets.test_silvicultura import _mock_df as silvicultura_mock
from tests.test_ibge.test_censo_total import _servir_sidra
from tests.test_sicar.test_reconciliacao_r11 import GOLDEN as SICAR
from tests.test_sicar.test_reconciliacao_r11 import MANIFEST as SICAR_MANIFEST


@pytest.mark.parametrize(
    ("codigos", "esperado"),
    [
        (pd.Series([5300108, 2901403, 4300002], dtype="Int64"), [5300108, 2901403, 4300002]),
        (pd.Series(["1600808", " 5107925 "], dtype=object), [1600808, 5107925]),
        (pd.Series([1, 29, 5300, 53001], dtype="Int64"), [None, None, None, None]),
        (pd.Series(["-", "", None, "abc"], dtype=object), [None, None, None, None]),
        (pd.Series([9900000, 1000000, 1600808.5, 10000000]), [None, None, None, None]),
        (pd.Series([pd.NA], dtype="Int64"), [None]),
    ],
    ids=["inteiro", "texto", "uf_e_brasil", "marcador", "fora_de_uf_ou_nao_inteiro", "nulo"],
)
def test_cod_municipio_e_o_codigo_de_7_digitos_com_prefixo_de_uf(codigos, esperado):
    with helpers.sem_excecao():
        resultado = regions.cod_municipio(codigos)

    assert str(resultado.dtype) == "Int64"
    assert resultado.astype(object).where(resultado.notna(), None).tolist() == esperado


async def test_censo_e_cadastro_cruzam_pelo_cod_municipio_sem_conversao(monkeypatch):
    seen = _servir_sidra(monkeypatch)
    with helpers.sem_excecao():
        censo, _ = await datasets.censo_agropecuario(
            "irrigacao", ano=2017, uf="DF", nivel="municipio", return_meta=True
        )
    assert [url for url in seen["served"] if "/t/6857/" in url]
    caso = next(c for c in SICAR_MANIFEST["cases"] if c["id"] == "sicar_df")
    seen = helpers.install_replay_http(monkeypatch, caso, SICAR)
    with helpers.sem_excecao():
        cadastro = await datasets.cadastro_rural(
            **{("municipio" if k == "cod_municipio" else k): v for k, v in caso["query"].items()}
        )
    helpers.assert_replay_served(seen)

    assert str(censo["cod_municipio"].dtype) == str(cadastro["cod_municipio"].dtype) == "Int64"
    assert (
        censo["cod_municipio"].astype(object).where(censo["cod_municipio"].notna(), None).tolist()
        == censo["localidade_cod"]
        .astype(object)
        .where(censo["localidade_cod"].notna(), None)
        .tolist()
    )
    assert (
        cadastro["cod_municipio"]
        .astype(object)
        .where(cadastro["cod_municipio"].notna(), None)
        .tolist()
        == cadastro["cod_municipio_ibge"]
        .astype(object)
        .where(cadastro["cod_municipio_ibge"].notna(), None)
        .tolist()
    )
    cruzado = censo.merge(cadastro, on="cod_municipio")
    assert len(censo) > 0 and len(cadastro) > 0
    assert len(cruzado) == len(censo) * len(cadastro)
    assert set(cruzado["cod_municipio"]) == {5300108}


IBGE = [
    (ProducaoAnualDataset, producao_mock, ("soja",)),
    (PecuariaMunicipalDataset, pecuaria_mock, ("bovino",)),
    (ExtrativsmoVegetalDataset, extrativismo_mock, ("acai",)),
    (SilviculturaDataset, silvicultura_mock, ("eucalipto_folha",)),
    (CensoAgropecuarioDataset, censo_mock, ("efetivo_rebanho",)),
]


@pytest.mark.parametrize(
    ("classe", "amostra", "argumentos"), IBGE, ids=lambda item: getattr(item, "__name__", "")
)
async def test_datasets_do_ibge_so_trazem_o_codigo_na_linha_de_municipio(
    monkeypatch, classe, amostra, argumentos
):
    linha = amostra().iloc[[0]]
    linhas = pd.concat(
        [
            linha.assign(localidade="Brasil", localidade_cod=1),
            linha.assign(localidade="Mato Grosso", localidade_cod=51),
            linha.assign(localidade="Rondonópolis", localidade_cod=5107602),
        ],
        ignore_index=True,
    )
    monkeypatch.setattr(classe, "info", copy.deepcopy(classe.info))
    dataset = classe()
    dataset.info.sources[0].fetch_fn = make_source(linhas)
    with helpers.sem_excecao():
        frame = await dataset.fetch(*argumentos)

    codigos = dict(
        zip(
            frame["localidade_cod"],
            frame["cod_municipio"]
            .astype(object)
            .where(frame["cod_municipio"].notna(), None)
            .tolist(),
            strict=True,
        )
    )
    assert str(frame["cod_municipio"].dtype) == "Int64"
    assert codigos == {1: None, 51: None, 5107602: 5107602}


async def test_producao_anual_sem_localidade_cod_traz_o_codigo_nulo(monkeypatch):
    monkeypatch.setattr(ProducaoAnualDataset, "info", copy.deepcopy(ProducaoAnualDataset.info))
    dataset = ProducaoAnualDataset()
    dataset.info.sources[0].fetch_fn = make_source(producao_mock())
    with helpers.sem_excecao():
        frame = await dataset.fetch("soja")

    assert "localidade_cod" not in frame
    assert str(frame["cod_municipio"].dtype) == "Int64" and frame["cod_municipio"].isna().all()
