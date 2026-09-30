from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts import funai

GOLDEN = Path(__file__).parents[1] / "golden_data/funai/official_20260907/reference.json"
ALIASES = {
    "codigo": "terrai_codigo",
    "nome": "terrai_nome",
    "etnia": "etnia_nome",
    "municipio": "municipio_nome",
    "uf": "uf_sigla",
    "area_ha": "superficie_perimetro_ha",
    "fase": "fase_ti",
    "modalidade": "modalidade_ti",
    "data_atualizacao": "data_atualizacao",
    "feature_id": None,
    "gid": "gid",
    "reestudo_ti": "reestudo_ti",
    "cr": "cr",
    "faixa_fronteira": "faixa_fronteira",
    "undadm_codigo": "undadm_codigo",
    "undadm_nome": "undadm_nome",
    "undadm_sigla": "undadm_sigla",
    "dominio_uniao": "dominio_uniao",
    "epsg": "epsg",
}
INTEGERS = ("codigo", "gid", "undadm_codigo", "epsg")


@pytest.fixture
def contract():
    return funai.TERRAS_INDIGENAS_V2


@pytest.fixture
def frame():
    features = json.loads(GOLDEN.read_bytes())["features"]
    return pd.DataFrame(
        {
            column: pd.Series(
                [row["id"] if source is None else row["properties"][source] for row in features],
                dtype="Int64"
                if column in INTEGERS
                else "float64"
                if column == "area_ha"
                else pd.StringDtype(storage="python"),
            )
            for column, source in ALIASES.items()
        }
    )


def test_funai_contract_official_cells_and_registry(contract, frame):
    assert contracts.get_contract("funai_terras_indigenas") == contract
    assert contract.name == "funai.terras_indigenas"
    assert contract.version == "2.0" and contract.primary_key == []
    assert [column.name for column in contract.columns] == list(ALIASES)
    assert contract.validate(frame) == (True, [])
    assert frame.loc[0, "undadm_codigo"] == 30202001857
    assert frame.loc[2, "modalidade"] == "Reserva Indígena"
    assert frame.loc[5, "uf"] == "MT, PA"


def test_funai_contract_empty_preserves_complete_schema(contract, frame):
    empty = contract.empty_frame()
    assert list(empty) == list(ALIASES) and empty.empty
    assert empty.dtypes.to_dict() == frame.dtypes.to_dict()
    assert contract.validate(empty) == (True, [])


@pytest.mark.parametrize("value", [None, "", " ", "\t\n"])
def test_funai_contract_feature_id_requires_nonblank_text(contract, frame, value):
    frame.loc[0, "feature_id"] = value
    assert not contract.validate(frame)[0]


@pytest.mark.parametrize(
    "column,dtype",
    [
        ("codigo", "float64"),
        ("gid", "int32"),
        ("undadm_codigo", "object"),
        ("epsg", "int64"),
        ("nome", "object"),
        ("area_ha", "float32"),
        ("area_ha", "Float64"),
    ],
)
def test_funai_contract_coercible_alternative_dtype_rejected(contract, frame, column, dtype):
    frame[column] = frame[column].astype(dtype)
    assert not contract.validate(frame)[0]


@pytest.mark.parametrize("mutation", ["missing", "extra", "reordered", "duplicate"])
def test_funai_contract_complete_ordered_projection_required(contract, frame, mutation):
    if mutation == "missing":
        frame = frame.drop(columns="epsg")
    elif mutation == "extra":
        frame["unpublished"] = None
    elif mutation == "reordered":
        frame = frame.loc[:, list(frame)[::-1]]
    else:
        frame = pd.concat([frame, frame[["codigo"]]], axis=1)
    assert not contract.validate(frame)[0]
