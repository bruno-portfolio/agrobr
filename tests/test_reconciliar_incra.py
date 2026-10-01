from __future__ import annotations

import pandas as pd
import pytest

from scripts import reconciliar_incra as reconciliacao


def _dados():
    propriedades = dict.fromkeys(reconciliacao.ALIASES)
    propriedades.update(
        dt_publica="2007-10-25",
        dt_public1="2009-08-28",
        dt_titulo="0001-01-01",
        dt_decreto="2009-11-23",
        dt_cadastro="2026-09-30T16:40:02Z",
    )
    registro = dict.fromkeys(reconciliacao.ALIASES.values())
    registro.update(
        feature_id="lim_quilombolas_a.312",
        data_publicacao=pd.Timestamp(2007, 10, 25),
        data_publicacao_2=pd.Timestamp(2009, 8, 28),
        data_titulo=pd.NaT,
        data_decreto=pd.Timestamp(2009, 11, 23),
        data_cadastro=pd.Timestamp("2026-09-30T16:40:02Z"),
    )
    return pd.DataFrame([registro]), [{"id": registro["feature_id"], "properties": propriedades}]


@pytest.mark.parametrize(
    ("bruto", "publicado"),
    [
        ("0001-01-01", None),
        ("0222-11-11", None),
        ("2201-02-15", None),
        ("0205-01-28", None),
        ("1899-12-31", None),
        ("2100-01-01", None),
        ("1900-01-01", "1900-01-01"),
        ("2099-12-31", "2099-12-31"),
        ("2023-02-31", None),
        (None, None),
    ],
)
def test_quilombolas_datas_publicadas_e_descartadas(bruto, publicado):
    frame, features = _dados()
    features[0]["properties"]["dt_titulo"] = bruto
    frame["data_titulo"] = pd.Timestamp(publicado) if publicado else pd.NaT

    resultado = reconciliacao.compare_quilombolas(frame, features)

    assert resultado["status"] == "ok"
    assert resultado["problems"] == []


@pytest.mark.parametrize(
    "coluna",
    ["data_publicacao", "data_publicacao_2", "data_titulo", "data_decreto", "data_cadastro"],
)
def test_quilombolas_data_divergente_segue_mismatch(coluna):
    frame, features = _dados()
    frame[coluna] = pd.Timestamp("2024-01-01", tz="UTC" if coluna == "data_cadastro" else None)

    resultado = reconciliacao.compare_quilombolas(frame, features)

    assert resultado["status"] == "mismatch"
    assert len(resultado["problems"]) == 1
    assert coluna in resultado["problems"][0]


@pytest.mark.parametrize(
    "bruto",
    ["2026-09-30T16:40:02Z", "2026-09-30T13:40:02-03:00", "2026-09-30T16:40:02"],
)
def test_quilombolas_cadastro_compara_o_instante_em_utc(bruto):
    frame, features = _dados()
    features[0]["properties"]["dt_cadastro"] = bruto
    assert reconciliacao.compare_quilombolas(frame, features)["status"] == "ok"
