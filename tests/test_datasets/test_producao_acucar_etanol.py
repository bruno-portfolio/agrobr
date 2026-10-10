import warnings
from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr import conab, datasets, sync
from agrobr.contracts import get_contract
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from tests import helpers

MANIFEST = helpers.load_cana_industria_manifest()


async def _consultar(**kwargs):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return await datasets.producao_acucar_etanol(**kwargs)


async def test_dataset_le_a_golden_pelo_contrato_com_valores_do_oraculo(monkeypatch):
    requests = helpers.install_cana_industria_http(monkeypatch)
    frame, meta = await _consultar(return_meta=True)
    contrato = get_contract("producao_acucar_etanol")
    assert requests == [MANIFEST["arquivo"]["url"]]
    assert contrato.validate(frame) == (True, [])
    assert list(frame.columns) == contrato.list_columns()
    assert len(frame) == len(MANIFEST["safras"]) * len(MANIFEST["ufs"])
    for celula in MANIFEST["celulas"]:
        linha = frame[(frame["safra"] == celula["safra"]) & (frame["uf"] == celula["uf"])]
        assert len(linha) == 1, celula
        valor = linha.iloc[0][celula["campo"]]
        if celula["esperado"] is None:
            assert pd.isna(valor), celula
        else:
            assert valor == pytest.approx(celula["esperado"], rel=1e-12, abs=1e-9), celula
    assert (meta.dataset, meta.contract_version, meta.schema_version) == (
        "producao_acucar_etanol",
        contrato.version,
        contrato.version,
    )
    assert meta.source == "datasets.producao_acucar_etanol/conab_cana_industria"
    assert meta.attempted_sources == ["conab_cana_industria"]
    assert meta.raw_content_hash == MANIFEST["arquivo"]["sha256"]
    divergencias = MANIFEST["divergencias"]
    assert sum("etanol total publicado" in a for a in meta.validation_warnings) == len(
        divergencias["total_componentes"]
    )
    assert sum("BRASIL publicado" in a for a in meta.validation_warnings) == len(
        divergencias["brasil_soma_ufs"]
    )
    assert sum("erro do Excel" in a for a in meta.validation_warnings) == len(
        MANIFEST["erros_excel"]
    )


async def test_filtro_inclusivo_pelo_primeiro_ano_da_safra_e_vazio_pelo_contrato(monkeypatch):
    helpers.install_cana_industria_http(monkeypatch)
    frame = await _consultar(ano_inicio=2020, ano_fim=2021, uf="sp")
    assert frame["safra"].tolist() == ["2020/21", "2021/22"]
    assert set(frame["uf"]) == {"SP"}
    vazio, meta = await _consultar(ano_inicio=2030, return_meta=True)
    contrato = get_contract("producao_acucar_etanol")
    assert vazio.empty
    assert contrato.validate(vazio) == (True, [])
    pd.testing.assert_series_equal(vazio.dtypes, contrato.empty_frame().dtypes)
    assert (meta.records_count, meta.validation_warnings) == (0, [])


async def test_vazio_em_polars_tem_o_schema_do_contrato(monkeypatch):
    pl = pytest.importorskip("polars")
    helpers.install_cana_industria_http(monkeypatch)
    frame = await _consultar(ano_inicio=2030, as_polars=True)
    assert frame.is_empty()
    assert frame.columns == get_contract("producao_acucar_etanol").list_columns()
    assert frame.schema["safra"] == pl.String
    assert frame.schema["acucar_mil_ton"] == pl.Float64


async def test_contrato_validado_tambem_sem_meta(monkeypatch):
    repetido = pd.concat([await _golden_frame(monkeypatch)] * 2, ignore_index=True)
    monkeypatch.setattr(conab, "cana_industria", AsyncMock(return_value=(repetido, None)))
    with pytest.raises(ContractViolationError, match="duplicate"):
        await datasets.producao_acucar_etanol(2024, 2024)


async def _golden_frame(monkeypatch) -> pd.DataFrame:
    helpers.install_cana_industria_http(monkeypatch)
    return await _consultar(ano_inicio=2024, ano_fim=2024)


async def test_modo_deterministico_avisa_que_o_dado_e_o_corrente(monkeypatch):
    helpers.install_cana_industria_http(monkeypatch)
    async with datasets.deterministic("2024-06-15"):
        with warnings.catch_warnings(record=True) as capturados:
            warnings.simplefilter("always")
            _, meta = await datasets.producao_acucar_etanol(2023, 2023, return_meta=True)
    aviso = (
        "producao_acucar_etanol: o modo determinístico não se aplica a este dataset; "
        "o dado é o corrente, e não o de 2024-06-15"
    )
    assert aviso in meta.validation_warnings
    assert aviso in [str(item.message) for item in capturados]
    assert meta.snapshot == "2024-06-15"


@pytest.mark.parametrize(
    ("chamada", "erro"),
    [
        (lambda: datasets.producao_acucar_etanol("2020"), InvalidParameterError),
        (lambda: datasets.producao_acucar_etanol(2021, 2020), InvalidParameterError),
        (lambda: datasets.producao_acucar_etanol(uf="XX"), InvalidParameterError),
        (
            lambda: datasets.get_dataset("producao_acucar_etanol").fetch("acucar"),
            InvalidParameterError,
        ),
        (lambda: datasets.producao_acucar_etanol(produto="acucar"), TypeError),
    ],
)
async def test_parametro_invalido_falha_antes_da_rede(chamada, erro, monkeypatch):
    requests = helpers.install_cana_industria_http(monkeypatch)
    with pytest.raises(erro):
        await chamada()
    assert requests == []


def test_registro_e_sync_descobrem_a_fonte_e_o_dataset():
    info = datasets.info("producao_acucar_etanol")
    assert info["products"] == []
    assert info["licenses"] == {"conab_cana_industria": "livre"}
    assert info["source_url"].endswith("/series-historicas/cana-de-acucar/industria")
    assert callable(sync.conab.cana_industria)
    assert callable(sync.datasets.producao_acucar_etanol)
