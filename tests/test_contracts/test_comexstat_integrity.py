from __future__ import annotations

import math

import pandas as pd
import pytest

from agrobr import contracts

MONTHLY = ("ano", "mes", "ncm", "uf", "kg_liquido", "valor_fob_usd", "volume_ton")
DETAIL = (
    "ano",
    "mes",
    "ncm",
    "cod_unidade",
    "cod_pais",
    "uf",
    "cod_via",
    "cod_urf",
    "qtd_estatistica",
    "kg_liquido",
    "valor_fob_usd",
)
IMPORT_MONEY = ("valor_frete_usd", "valor_seguro_usd")
LAYOUTS = {
    "exportacao_mensal": MONTHLY,
    "importacao_mensal": (*MONTHLY, *IMPORT_MONEY),
    "exportacao_detalhado": DETAIL,
    "importacao_detalhado": (*DETAIL, *IMPORT_MONEY),
    "dicionario_unidades": ("cod_unidade", "unidade", "sigla_unidade"),
    "dicionario_paises": (
        "cod_pais",
        "cod_pais_iso_numerico",
        "cod_pais_iso_alfa3",
        "pais",
        "pais_ingles",
        "pais_espanhol",
    ),
    "dicionario_vias": ("cod_via", "via"),
    "dicionario_urfs": ("cod_urf", "urf"),
}
INTEGER_COLUMNS = {"ano", "mes", "kg_liquido", "qtd_estatistica"}
VALUES = {
    "ano": 2026,
    "mes": 1,
    "ncm": "00000000",
    "cod_unidade": "00",
    "cod_pais": "000",
    "uf": "MT",
    "cod_via": "00",
    "cod_urf": "0000000",
    "qtd_estatistica": 9007199254740993,
    "kg_liquido": 9007199254740993,
    "valor_fob_usd": 1.25,
    "volume_ton": 9007199254740993 / 1000,
    "valor_frete_usd": 2.5,
    "valor_seguro_usd": 3.75,
}


def dtype(name):
    if name in INTEGER_COLUMNS:
        return "Int64"
    return "float64" if name.startswith("valor_") or name == "volume_ton" else "string[python]"


def frame_for(layout):
    return pd.DataFrame(
        {
            name: pd.Series([VALUES.get(name, " Literal \n")], dtype=dtype(name))
            for name in LAYOUTS[layout]
        }
    )


def contract_for(layout):
    return contracts.get_contract(f"comexstat_{layout}")


@pytest.mark.parametrize("layout", LAYOUTS)
def test_comexstat_eight_schemas_literal_order_version_and_typed_empty(layout):
    contract = contract_for(layout)
    frame = frame_for(layout)
    before = frame.copy(deep=True)
    assert contract.version == ("1.0" if layout.startswith("dicionario_") else "2.0")
    assert contract.list_columns() == list(LAYOUTS[layout])
    assert contract.validate(frame) == (True, [])
    pd.testing.assert_frame_equal(frame, before)
    empty = contract.empty_frame()
    assert contract.validate(empty) == (True, [])
    pd.testing.assert_frame_equal(empty, frame.iloc[:0])
    for name in empty.select_dtypes("string"):
        assert empty[name].dtype.storage == "python"


@pytest.mark.parametrize("layout", LAYOUTS)
def test_comexstat_rejects_reordered_missing_and_extra_columns(layout):
    contract = contract_for(layout)
    frame = frame_for(layout)
    for altered in (frame.iloc[:, ::-1], frame.iloc[:, 1:], frame.assign(extra="unpublished")):
        valid, errors = contract.validate(altered)
        assert not valid and errors


@pytest.mark.parametrize("layout", LAYOUTS)
def test_comexstat_string_objects_are_not_silently_coerced(layout):
    frame = frame_for(layout)
    name = next(name for name in frame if dtype(name) == "string[python]")
    frame[name] = frame[name].astype(object)
    before = frame.copy(deep=True)
    valid, errors = contract_for(layout).validate(frame)
    assert not valid and any(name in error for error in errors)
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("layout", ["exportacao_mensal", "importacao_mensal"])
@pytest.mark.parametrize("kg,tons", [(1000, 999.0), (pd.NA, 1.0), (1000, math.nan), (0, 1.0)])
def test_comexstat_volume_relation_rejects_inconsistency_and_one_sided_null(layout, kg, tons):
    frame = frame_for(layout)
    frame["kg_liquido"] = pd.Series([kg], dtype="Int64")
    frame["volume_ton"] = pd.Series([tons], dtype="float64")
    valid, errors = contract_for(layout).validate(frame)
    assert not valid and any("volume_ton" in error for error in errors)


@pytest.mark.parametrize("layout", ["exportacao_mensal", "importacao_mensal"])
@pytest.mark.parametrize(
    "kg,tons", [(pd.NA, math.nan), (0, 0.0), (0, -0.0), (2**63 - 1, (2**63 - 1) / 1000)]
)
def test_comexstat_volume_relation_preserves_null_zero_and_int64_limit(layout, kg, tons):
    frame = frame_for(layout)
    frame["kg_liquido"] = pd.Series([kg], dtype="Int64")
    frame["volume_ton"] = pd.Series([tons], dtype="float64")
    assert contract_for(layout).validate(frame) == (True, [])
    assert (
        pd.isna(frame["kg_liquido"].iloc[0])
        if pd.isna(kg)
        else int(frame["kg_liquido"].iloc[0]) == kg
    )
    if tons == 0:
        assert math.copysign(1, frame["volume_ton"].iloc[0]) == math.copysign(1, tons)


@pytest.mark.parametrize("name", ["ano", "mes", "kg_liquido", "qtd_estatistica"])
@pytest.mark.parametrize("new_dtype", ["int64", "float64", "object"])
def test_comexstat_integer_dtypes_are_strict_without_float_coercion(name, new_dtype):
    frame = frame_for("exportacao_detalhado")
    frame[name] = frame[name].astype(new_dtype)
    assert not contract_for("exportacao_detalhado").validate(frame)[0]


@pytest.mark.parametrize(
    "name,width", [("ncm", 8), ("cod_unidade", 2), ("cod_pais", 3), ("cod_via", 2), ("cod_urf", 7)]
)
@pytest.mark.parametrize("mutation", ["short", "padded", "unicode", "null"])
def test_comexstat_primary_codes_reject_repair_or_unicode(name, width, mutation):
    frame = frame_for("exportacao_detalhado")
    frame.loc[0, name] = {
        "short": "0",
        "padded": " " + "0" * width,
        "unicode": "\uff10" * width,
        "null": pd.NA,
    }[mutation]
    before = frame.copy(deep=True)
    assert not contract_for("exportacao_detalhado").validate(frame)[0]
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("layout", [name for name in LAYOUTS if name.startswith("dicionario_")])
def test_comexstat_dictionary_descriptions_empty_literal_and_duplicates(layout):
    frame = pd.concat([frame_for(layout)] * 3, ignore_index=True)
    for name in LAYOUTS[layout][1:]:
        frame[name] = pd.Series(["", " NULL \n", "00?"], dtype="string[python]")
    before = frame.copy(deep=True)
    assert contract_for(layout).primary_key == []
    assert contract_for(layout).validate(frame) == (True, [])
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("layout", ["importacao_mensal", "importacao_detalhado"])
@pytest.mark.parametrize("field", IMPORT_MONEY)
def test_comexstat_import_money_separate_nullable_and_float64(layout, field):
    frame = frame_for(layout)
    assert frame["valor_frete_usd"].iloc[0] == 2.5
    assert frame["valor_seguro_usd"].iloc[0] == 3.75
    frame.loc[0, field] = math.nan
    assert contract_for(layout).validate(frame) == (True, [])
    frame[field] = pd.Series([1], dtype="Int64")
    assert not contract_for(layout).validate(frame)[0]
