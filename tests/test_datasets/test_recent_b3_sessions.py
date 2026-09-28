from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.datasets.futuros_agricolas import _fetch_b3, _fetch_pregao_recente
from agrobr.exceptions import ParseError, SourceUnavailableError
from tests.helpers import collect_failures, isolated_dataset_case

from .conftest import mock_source_meta


async def test_recent_b3_sessions_casos_1():
    with collect_failures() as check:
        case = "test_recent_session_continues_after_unpublished_day"
        with check(case), isolated_dataset_case(case):
            empty = contracts.get_contract("posicoes_abertas").empty_frame()
            published = pd.DataFrame([{"posicoes_abertas": 123}])
            meta = mock_source_meta()
            source = AsyncMock(side_effect=[(empty, meta), (published, meta)])
            with patch(
                "agrobr.datasets.futuros_agricolas._dias_uteis_recentes",
                return_value=["2026-09-04", "2026-09-03"],
            ):
                result, actual_meta = await _fetch_pregao_recente(source, contrato="boi")
            assert result.equals(published)
            assert actual_meta is meta
            assert [call.kwargs["data"] for call in source.await_args_list] == [
                "2026-09-04",
                "2026-09-03",
            ]
        case = "test_recent_window_of_unpublished_days_stays_empty"
        with check(case), isolated_dataset_case(case):
            empty = contracts.get_contract("posicoes_abertas").empty_frame()
            source = AsyncMock(return_value=(empty, mock_source_meta()))
            result, _ = await _fetch_pregao_recente(source)
            assert result.empty
            assert source.await_count == 5


@pytest.mark.asyncio
async def test_recent_all_exceptions_propagates_last_cause():
    error = ParseError(source="b3", reason="layout", parser_version=1)
    source = AsyncMock(side_effect=[SourceUnavailableError(source="b3"), error])
    with (
        patch(
            "agrobr.datasets.futuros_agricolas._dias_uteis_recentes",
            return_value=["2026-09-04", "2026-09-03"],
        ),
        pytest.raises(ParseError) as caught,
    ):
        await _fetch_pregao_recente(source)
    assert caught.value is error


@pytest.mark.asyncio
async def test_explicit_empty_date_does_not_search_another_session():
    empty = contracts.get_contract("posicoes_abertas").empty_frame()
    source = AsyncMock(return_value=(empty, mock_source_meta()))
    with patch("agrobr.b3.posicoes_abertas", source):
        result, _ = await _fetch_b3("boi", tipo="posicoes", data="2026-09-04")
    assert result.empty
    source.assert_awaited_once_with(data="2026-09-04", contrato="boi", return_meta=True)
