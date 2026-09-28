from __future__ import annotations

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import httpx
import pandas as pd
import pytest

from agrobr import comtrade
from agrobr.comtrade import api
from agrobr.exceptions import InvalidParameterError


@pytest.mark.parametrize(
    "arguments",
    [
        {"produto": None},
        {"produto": "123"},
        {"produto": "1201,"},
        {"reporter": "world"},
        {"partner": ""},
        {"partner": False},
        {"periodo": True},
        {"periodo": 2023.0},
        {"periodo": "202301-202212", "freq": "M"},
        {"freq": "Q"},
        {"fluxo": "RX"},
        {"require_complete": "yes"},
        {"require_complete": 1},
        {"api_key": 123},
        {"unrecognized_filter": "ignored-before"},
    ],
)
async def test_invalid_arguments_fail_before_http_or_license_warning(arguments, replay_http):
    requests, _ = replay_http()
    args = {"produto": "soja", "periodo": 2023, **arguments}
    with patch.object(api.warnings, "warn_once") as warning, pytest.raises(InvalidParameterError):
        await comtrade.comercio(**args)
    assert requests == []
    warning.assert_not_called()


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize("partner", ["CN", "999"])
async def test_polars_preserves_complete_and_empty_schema(partner, return_meta, replay_http):
    pl = pytest.importorskip("polars")
    replay_http()
    result = await comtrade.comercio(
        "1201", partner=partner, periodo=2023, as_polars=True, return_meta=return_meta
    )
    frame = result[0] if return_meta else result
    assert isinstance(frame, pl.DataFrame)
    assert frame.width == 27
    assert frame.height == (1 if partner == "CN" else 0)
    assert frame["reporter_code"].dtype == frame["mes"].dtype == pl.Int64
    for marca in (
        "classificacao_original",
        "peso_liquido_estimado",
        "peso_bruto_estimado",
        "quantidade_estimada",
    ):
        assert frame[marca].dtype == pl.Boolean
    if return_meta:
        assert result[1].columns == frame.columns
        assert result[1].records_count == frame.height


async def test_replay_mirror_preserves_two_guest_legs_and_classification(replay_http, captures):
    replay_http()
    frame, meta = await comtrade.trade_mirror(
        "1201", reporter="BR", partner="CN", periodo=2023, require_complete=True, return_meta=True
    )
    assert len(frame) == 1 and len(frame.columns) == 24
    assert frame.iloc[0]["ratio_valor"] == pytest.approx(
        captures["oracles"]["mirror"]["ratio_fob_cif"]
    )
    assert frame.iloc[0]["reporter_code"] == 76 and frame.iloc[0]["partner_code"] == 156
    assert meta.schema_version == meta.contract_version == "2.0"
    details = meta.source_details
    assert details["coverage"]["state"] == "complete"
    serialized = json.dumps(details, default=str)
    assert "public/v1/preview" in serialized
    assert hashlib.sha256(captures["bodies"]["soy_br_cn_2023"]).hexdigest() in serialized
    assert hashlib.sha256(captures["bodies"]["soy_cn_br_2023_mirror"]).hexdigest() in serialized
    assert "/data/v1/get" not in serialized
    assert len(details["legs"]) == 2


@pytest.mark.parametrize(
    "arguments",
    [
        {"partner": "world"},
        {"partner": "all"},
        {"partner": "BR"},
        {"reporter": "0"},
        {"periodo": False},
        {"typo_filter": 1},
    ],
)
async def test_mirror_invalid_legs_or_filters_fail_before_http(arguments, replay_http):
    requests, _ = replay_http()
    with pytest.raises(InvalidParameterError):
        await comtrade.trade_mirror("soja", **{"periodo": 2023, **arguments})
    assert requests == []


def test_product_catalog_copies_selection_lists():
    before = comtrade.produtos()["soja"].copy()
    exported = comtrade.produtos()
    exported["soja"].append("9999")
    assert comtrade.produtos()["soja"] == before


async def test_mirror_rejects_hidden_flow_kwarg_before_http_and_warning(replay_http):
    requests, _ = replay_http()
    with patch.object(api.warnings, "warn_once") as warning, pytest.raises(InvalidParameterError):
        await comtrade.trade_mirror("soja", periodo=2023, fluxo="M")
    assert requests == []
    warning.assert_not_called()


R29 = Path(__file__).parents[1] / "golden_data/comtrade/r29_20260926"
AVISO_ESTIMADO = (
    "Comtrade: peso líquido estimado pela ONU (isNetWgtEstimated) em HS 020714/2024; "
    "peso_liquido_kg e volume_ton trazem o valor estimado, não o declarado."
)


async def test_marcas_de_estimativa_da_onu_saem_com_aviso(replay_http, captures):
    recibo = json.loads((R29 / "manifest.json").read_bytes())["arquivos"]["frango_2024.json"]
    corpo = (R29 / "frango_2024.json").read_bytes()
    digest = hashlib.sha256(corpo).hexdigest()
    assert (digest, len(corpo)) == (recibo["sha256"], recibo["bytes"])
    publicado = json.loads(corpo)["data"]
    contagem = json.loads(captures["bodies"]["agro5_2023_count"])["data"]

    def frango(request, _ordem):
        if request.url.params["cmdCode"] != "020711,020712,020713,020714":
            return None
        if request.url.params.get("countOnly") == "true":
            envelope = {"elapsedTime": "0", "count": len(publicado), "data": contagem, "error": ""}
            return httpx.Response(200, json=envelope)
        return httpx.Response(200, content=corpo, headers={"content-type": "application/json"})

    replay_http(frango)
    frame, meta = await comtrade.comercio("carne_frango", periodo=2024, return_meta=True)
    colunas = ["peso_liquido_estimado", "peso_bruto_estimado", "quantidade_estimada"]
    marcas = {
        linha.hs_code: tuple(None if pd.isna(valor) else bool(valor) for valor in linha[colunas])
        for _, linha in frame.iterrows()
    }
    oficial = {
        registro["cmdCode"]: (
            registro["isNetWgtEstimated"],
            registro["isGrossWgtEstimated"],
            registro["isQtyEstimated"],
        )
        for registro in publicado
    }
    assert marcas == oficial
    assert [hs for hs, (peso, _, _) in oficial.items() if peso] == ["020714"]
    assert [aviso for aviso in meta.validation_warnings if "ONU" in aviso] == [AVISO_ESTIMADO]
    assert meta.source_details["peso_liquido_estimado"] == [
        {"periodo": "2024", "hs_code": "020714"}
    ]
    assert meta.schema_version == meta.contract_version == "2.1"


async def test_espelho_lista_a_perna_com_peso_estimado(replay_http, captures):
    replay_http()
    _, meta = await comtrade.trade_mirror(
        "1201", reporter="BR", partner="CN", periodo=2023, require_complete=True, return_meta=True
    )
    pernas = {"reporter": "soy_br_cn_2023", "partner": "soy_cn_br_2023_mirror"}
    esperado = {
        perna: [
            {"periodo": registro["period"], "hs_code": registro["cmdCode"]}
            for registro in json.loads(captures["bodies"][nome])["data"]
            if registro["isNetWgtEstimated"]
        ]
        for perna, nome in pernas.items()
    }
    assert esperado == {"reporter": [], "partner": [{"periodo": "2023", "hs_code": "1201"}]}
    assert meta.source_details["peso_estimado"] == esperado
    assert any("HS 1201/2023" in aviso for aviso in meta.validation_warnings)
