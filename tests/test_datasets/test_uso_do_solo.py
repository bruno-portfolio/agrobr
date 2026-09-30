import hashlib
from datetime import timedelta

import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente


def _make_cobertura_df(**overrides):
    row = {
        "bioma": "Cerrado",
        "uf": "MT",
        "classe_id": 3,
        "classe": "Formação Florestal",
        "nivel_0": "Floresta",
        "ano": 2022,
        "area_ha": 150000.0,
    }
    row.update(overrides)
    return pd.DataFrame([row])


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize(
    "selectors,states",
    [
        ({"municipio": "2703007"}, {"AL", "PE"}),
        ({"municipio": 2703007, "uf": " Pernambuco "}, {"PE"}),
        ({"municipio": "2703007", "uf": "MT"}, set()),
        ({"municipio": " sOrRiSo "}, {"MT"}),
    ],
)
async def test_public_municipal_replay_contract_and_selectors(
    selectors, states, return_meta, replay_mapbiomas, municipal_capture
):
    requests = replay_mapbiomas()
    try:
        result = await datasets.uso_do_solo(
            nivel="municipio", ano=2025, return_meta=return_meta, **selectors
        )
    except Exception as erro:
        raise AssertionError(f"cobertura municipal publicada falhou: {erro!r}") from erro
    frame = result[0] if return_meta else result
    assert set(frame["uf"]) == states
    contract = contracts.get_contract("mapbiomas_cobertura_municipal")
    assert contract.validate(frame) == (True, [])
    if not states:
        pd.testing.assert_frame_equal(frame, contract.empty_frame())
    if return_meta:
        meta = result[1]
        assert (
            meta.dataset == "uso_do_solo" and meta.contract_version == meta.schema_version == "1.1"
        )
        assert meta.parser_version == 2 and meta.selected_source == "mapbiomas_oficial"
        assert meta.attempted_sources == ["mapbiomas_oficial"]
        assert meta.source == "datasets.uso_do_solo/mapbiomas_oficial"
        assert meta.raw_content_hash == hashlib.sha256(municipal_capture["zip"]).hexdigest()
        assert meta.raw_content_size == len(municipal_capture["zip"])
        assert meta.fetched_at.tzinfo is not None and meta.fetch_timestamp.utcoffset() == timedelta(
            0
        )
        assert meta.snapshot is None and meta.columns == frame.columns.tolist()
        assert meta.source_details["coverage"]["validated_rows"] == 142
        assert meta.source_details["coverage"]["annual_cells"] == 5822
        assert meta.source_details["coverage"]["output_rows"] == len(frame)
    assert len(requests) == 2


def test_registry_dispatches_existing_state_and_new_municipal_contracts():
    dataset = datasets.get_dataset("uso_do_solo")
    assert (
        dataset._contract_name(tipo="cobertura", nivel="municipio")
        == "mapbiomas_cobertura_municipal"
    )
    assert dataset._contract_name(tipo="cobertura", nivel="estado") == "mapbiomas_cobertura"
    assert dataset._contract_name(tipo="transicao", nivel="estado") == "mapbiomas_transicao"


@pytest.mark.parametrize(
    "arguments",
    [
        {"tipo": "transicao", "nivel": "municipio"},
        {"tipo": "transicao", "municipio": "Sorriso"},
        {"tipo": "transicao", "municipio": 5107925},
        {"tipo": "transicao", "ano": 2025},
        {"tipo": "transicao", "classe_id": 3},
        {"tipo": "cobertura", "periodo": "2020-2021"},
        {"tipo": "cobertura", "classe_de_id": 3},
        {"tipo": "cobertura", "classe_para_id": 3},
        {"nivel": "estado", "municipio": "Sorriso"},
        {"nivel": "estado", "municipio": 5107925},
        {"uf": 1},
        {"uf": "Matogrosso"},
        {"bioma": False},
        {"tipo": []},
        {"nivel": []},
        {"nivel": "municipio", "municipio": 1},
        {"nivel": "municipio", "municipio": " "},
        {"nivel": "municipio", "municipio": 510792},
        {"nivel": "municipio", "municipio": "Sorris"},
        {"nivel": "municipio", "municipio": "Bom Jesus"},
        {"ano": True},
        {"ano": 2026},
        {"colecao": 11.0},
        {"classe_id": True},
        {"classe_id": 3.1},
        {"tipo": "transicao", "classe_de_id": False},
        {"tipo": "transicao", "classe_para_id": "3"},
        {"tipo": "transicao", "periodo": "2025-2024"},
        {"as_polars": 1},
        {"return_meta": "false"},
    ],
)
async def test_public_invalid_or_incompatible_options_fail_before_http(arguments, replay_mapbiomas):
    requests = replay_mapbiomas()
    with levanta_exatamente(InvalidParameterError):
        await datasets.uso_do_solo(**arguments)
    assert not requests


@pytest.mark.parametrize(
    "arguments", [{}, {"colecao": 10}, {"nivel": "municipio", "colecao": 11, "ano": 2025}]
)
async def test_deterministic_context_never_labels_an_unselected_revision(
    arguments, replay_mapbiomas
):
    requests = replay_mapbiomas()
    async with deterministic("2024-12-31"):
        with pytest.raises(InvalidParameterError):
            await datasets.uso_do_solo(**arguments)
    assert not requests


@pytest.mark.parametrize("nome", ["estado", "geocodigo"])
async def test_estado_e_geocodigo_nao_sao_mais_parametros(nome, replay_mapbiomas):
    requests = replay_mapbiomas()
    with levanta_exatamente(TypeError, nome):
        await datasets.uso_do_solo(**{nome: "MT" if nome == "estado" else "5107925"})
    assert not requests
