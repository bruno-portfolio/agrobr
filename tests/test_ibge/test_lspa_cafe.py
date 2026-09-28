from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import ibge
from agrobr.contracts import get_contract, validate_dataset
from agrobr.ibge import client

FIXTURE = Path(__file__).parents[1] / "golden_data/ibge/lspa_cafe_202607/capture.json"


async def _fetch_cafe(classifications: dict[str, str], **_kwargs: object) -> pd.DataFrame:
    capture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    return pd.DataFrame(capture[classifications["48"]]["rows"])


@pytest.mark.asyncio
async def test_lspa_cafe_oficial_especies_medidas_e_contrato():
    with patch.object(
        client, "fetch_sidra", new_callable=AsyncMock, side_effect=_fetch_cafe
    ) as fetch:
        frame, meta = await ibge.lspa("cafe", ano=2026, mes=7, return_meta=True)

    assert fetch.await_count == 2
    assert {call.kwargs["classifications"]["48"] for call in fetch.await_args_list} == {
        "39454",
        "39455",
    }
    for call in fetch.await_args_list:
        assert call.kwargs["table_code"] == "6588"
        assert call.kwargs["period"] == "202607"
        assert call.kwargs["territorial_level"] == "1"
        assert call.kwargs["ibge_territorial_code"] == "all"
    assert len(frame) == 8
    assert frame["ano"].unique().tolist() == [2026]
    assert frame["mes"].unique().tolist() == [7]
    assert frame["localidade"].unique().tolist() == ["Brasil"]
    assert frame["localidade_cod"].unique().tolist() == [1]
    assert frame["fonte"].unique().tolist() == ["ibge_lspa"]
    assert set(frame["produto"]) == {"cafe_arabica", "cafe_canephora"}
    assert not frame.duplicated(get_contract("lspa").primary_key).any()
    assert frame.groupby("produto")["variavel_cod"].apply(set).tolist() == [
        {109, 216, 35, 36},
        {109, 216, 35, 36},
    ]
    units = frame.groupby("variavel_cod")["unidade"].unique().to_dict()
    assert {code: values.tolist() for code, values in units.items()} == {
        109: ["Hectares"],
        216: ["Hectares"],
        35: ["Toneladas"],
        36: ["Quilogramas por Hectare"],
    }
    production = frame.loc[frame["variavel_cod"] == 35].set_index("produto")["valor"]
    assert production.to_dict() == {"cafe_arabica": 2663529.0, "cafe_canephora": 1295499.0}
    assert production.sum() == 3959028
    assert frame.loc[frame["variavel_cod"] == 216, "valor"].sum() == 1999738
    yields = frame.loc[frame["variavel_cod"] == 36].set_index("produto")["valor"]
    assert yields.to_dict() == {"cafe_arabica": 1692.0, "cafe_canephora": 3042.0}
    assert str(frame["valor"].dtype) == "float64"
    validate_dataset(frame, "lspa")
    assert meta.source == meta.selected_source == "ibge_lspa"
    assert meta.attempted_sources == ["ibge_lspa"]
    assert meta.dataset == "lspa"
    assert meta.schema_version == meta.contract_version == "2.0"
    assert meta.records_count == len(frame)
    assert meta.columns == frame.columns.tolist()
