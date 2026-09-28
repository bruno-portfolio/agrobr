from __future__ import annotations

import pandas as pd

from agrobr.queimadas import parser as queimadas_parser


def test_queimadas_legacy_header_preserves_missing_optional_fields():
    raw = (
        "latitude,longitude,data_pas,satelite,pais,estado,municipio,bioma,numero_dias_sem_chuva,precipitacao,risco_fogo,frp\n"
        "-17.7,-57.2,2020-02-06 04:24:00,NPP-375D,Brasil,MATO GROSSO,POCONÉ,Pantanal,3,7.5,-999,4.1\n"
    ).encode()
    frame = queimadas_parser.parse_focos_csv(raw)
    assert frame.loc[0, "lat"] == -17.7
    assert frame.loc[0, "lon"] == -57.2
    assert frame.loc[0, "uf"] == "MT"
    assert f"{frame.loc[0, 'data']:%Y-%m-%d %H:%M}" == "2020-02-06 00:00"
    assert pd.isna(frame.loc[0, "municipio_id"])
    assert pd.isna(frame.loc[0, "risco_fogo"])
