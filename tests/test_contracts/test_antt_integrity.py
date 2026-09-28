from __future__ import annotations

import pandas as pd
import pytest

from agrobr.contracts import get_contract
from tests.helpers import anttpedagio_fluxo_frame


def test_antt_contrato_vazio_tem_tipos_iguais():
    contract = get_contract("antt_pedagio_fluxo")
    empty = contract.empty_frame()
    assert contract.validate(empty) == (True, [])
    assert empty.dtypes.equals(anttpedagio_fluxo_frame().dtypes)


@pytest.mark.parametrize(
    "category,axles", [("19", 19), ("Categoria 4", 3), ("3 eixos", 4), ("3 eixos", pd.NA)]
)
def test_antt_contrato_rejeita_eixos_sem_evidencia(category, axles):
    frame = anttpedagio_fluxo_frame()
    frame.loc[0, "categoria_eixo"] = category
    frame.loc[0, "n_eixos"] = axles
    valid, errors = get_contract("antt_pedagio_fluxo").validate(frame)
    assert not valid
    assert any("n_eixos" in error for error in errors)


def test_antt_contrato_categoria_inteira_enorme_retorna_violacao():
    frame = anttpedagio_fluxo_frame()
    frame.loc[0, "categoria_eixo"] = "9" * 5000 + " eixos"
    frame.loc[0, "n_eixos"] = pd.NA
    valid, errors = get_contract("antt_pedagio_fluxo").validate(frame)
    assert not valid
    assert any("Int64" in error for error in errors)


def test_antt_contrato_cobrancas_distintas_nao_sao_duplicatas():
    frame = pd.concat([anttpedagio_fluxo_frame()] * 2, ignore_index=True)
    frame.loc[1, "tipo_cobranca"] = "Manual"
    assert get_contract("antt_pedagio_fluxo").validate(frame) == (True, [])


@pytest.mark.parametrize(
    "field,value", [("frequencia", "mensal"), ("uf", "XX"), ("volume", -1), ("praca", " ")]
)
def test_antt_contrato_rejeita_inconsistencia(field, value):
    frame = anttpedagio_fluxo_frame()
    frame.loc[0, field] = value
    assert get_contract("antt_pedagio_fluxo").validate(frame)[0] is False


def test_antt_contrato_rejeita_dtype_textual_inesperado():
    frame = anttpedagio_fluxo_frame()
    frame["tipo_cobranca"] = frame["tipo_cobranca"].astype("object")
    valid, errors = get_contract("antt_pedagio_fluxo").validate(frame)
    assert not valid
    assert any("string[python]" in error for error in errors)
