from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agrobr.cepea.parsers.consensus import (
    ConsensusResult,
    ConsensusValidator,
    analyze_consensus,
    parse_with_consensus,
    select_best_result,
)
from agrobr.constants import Fonte
from agrobr.exceptions import ParseError
from agrobr.models import Indicador
from tests import helpers


def _make_indicador(valor=Decimal("150.00"), data_val=date(2024, 1, 1)):
    return Indicador(
        fonte=Fonte.CEPEA,
        produto="soja",
        data=data_val,
        valor=valor,
        unidade="BRL/sc60kg",
    )


def _make_parser_cls(
    version: int,
    indicadores: list[Indicador] | None = None,
    *,
    raises: Exception | None = None,
    can_parse_result: tuple[bool, float] = (True, 0.9),
):
    cls = MagicMock()
    instance = MagicMock()
    instance.version = version
    instance.can_parse.return_value = can_parse_result
    if raises:
        instance.parse.side_effect = raises
    else:
        instance.parse.return_value = indicadores if indicadores is not None else []
    cls.return_value = instance
    return cls


class TestAnalyzeConsensus:
    @pytest.mark.parametrize(
        "scenario,parameters",
        [
            ("test_count_mismatch", {}),
            ("test_value_mismatch", {}),
            ("test_date_mismatch", {}),
            ("test_last_date_mismatch", {}),
            ("test_last_value_mismatch", {}),
        ],
        ids=[
            "count_mismatch-0",
            "value_mismatch-0",
            "date_mismatch-0",
            "last_date_mismatch-0",
            "last_value_mismatch-0",
        ],
    )
    def test_divergencias_de_consenso(self, scenario: str, parameters: dict[str, Any]):
        with (
            helpers.collect_failures() as check,
            check((scenario, parameters)),
            helpers.isolated_dataset_case((scenario, parameters)),
        ):
            if scenario == "test_count_mismatch":
                results = {
                    1: [_make_indicador()],
                    2: [_make_indicador(), _make_indicador(data_val=date(2024, 1, 2))],
                }
                divergences, report = analyze_consensus(results, {})
                assert any(d["type"] == "count_mismatch" for d in divergences)
            elif scenario == "test_value_mismatch":
                results = {
                    1: [_make_indicador(valor=Decimal("100.00"))],
                    2: [_make_indicador(valor=Decimal("200.00"))],
                }
                divergences, report = analyze_consensus(results, {})
                assert any("value_mismatch" in d["type"] for d in divergences)
                assert any(d["type"] == "first_value_mismatch" for d in divergences)
            elif scenario == "test_date_mismatch":
                results = {
                    1: [_make_indicador(data_val=date(2024, 1, 1))],
                    2: [_make_indicador(data_val=date(2024, 1, 2))],
                }
                divergences, report = analyze_consensus(results, {})
                assert any("date_mismatch" in d["type"] for d in divergences)
                assert any(d["type"] == "first_date_mismatch" for d in divergences)
            elif scenario == "test_last_date_mismatch":
                results = {
                    1: [
                        _make_indicador(data_val=date(2024, 1, 1)),
                        _make_indicador(data_val=date(2024, 1, 10)),
                    ],
                    2: [
                        _make_indicador(data_val=date(2024, 1, 1)),
                        _make_indicador(data_val=date(2024, 1, 15)),
                    ],
                }
                divergences, report = analyze_consensus(results, {})
                assert any(d["type"] == "last_date_mismatch" for d in divergences)
            elif scenario == "test_last_value_mismatch":
                results = {
                    1: [
                        _make_indicador(data_val=date(2024, 1, 1)),
                        _make_indicador(valor=Decimal("100.00"), data_val=date(2024, 1, 2)),
                    ],
                    2: [
                        _make_indicador(data_val=date(2024, 1, 1)),
                        _make_indicador(valor=Decimal("200.00"), data_val=date(2024, 1, 2)),
                    ],
                }
                divergences, report = analyze_consensus(results, {})
                assert any(d["type"] == "last_value_mismatch" for d in divergences)
                assert report["has_divergence"] is True


class TestParseWithConsensus:
    @pytest.mark.asyncio
    async def test_parser_low_confidence_skipped(self):
        low_conf_cls = _make_parser_cls(1, [_make_indicador()], can_parse_result=(True, 0.3))

        with patch("agrobr.cepea.parsers.consensus.CONSENSUS_PARSERS", [low_conf_cls]):
            result = await parse_with_consensus("<html>test</html>", "soja")

        assert result.all_results == {}
        assert result.indicadores == []

    @pytest.mark.asyncio
    async def test_require_consensus_raises_on_divergence(self):
        ind_v1 = [_make_indicador(valor=Decimal("100.00"))]
        ind_v2 = [_make_indicador(valor=Decimal("200.00"))]
        cls1 = _make_parser_cls(1, ind_v1)
        cls2 = _make_parser_cls(2, ind_v2)

        with (
            patch("agrobr.cepea.parsers.consensus.CONSENSUS_PARSERS", [cls1, cls2]),
            patch("agrobr.alerts.notifier.send_alert", new_callable=AsyncMock),
            pytest.raises(ParseError, match="diverged"),
        ):
            await parse_with_consensus("<html>test</html>", "soja", require_consensus=True)

    @pytest.mark.asyncio
    async def test_selects_latest_version(self):
        ind_v1 = [_make_indicador()]
        ind_v2 = [_make_indicador()]
        cls1 = _make_parser_cls(1, ind_v1)
        cls2 = _make_parser_cls(2, ind_v2)

        with patch("agrobr.cepea.parsers.consensus.CONSENSUS_PARSERS", [cls1, cls2]):
            result = await parse_with_consensus("<html>test</html>", "soja")

        assert result.indicadores == ind_v2

    @pytest.mark.asyncio
    async def test_parser_exception_caught(self):
        failing_cls = _make_parser_cls(1, raises=ValueError("broken"))

        with patch("agrobr.cepea.parsers.consensus.CONSENSUS_PARSERS", [failing_cls]):
            result = await parse_with_consensus("<html>test</html>", "soja")

        assert result.indicadores == []
        assert result.all_results == {}
        assert result.has_consensus is True
        assert result.report["errors"] == {1: "broken"}

    @pytest.mark.asyncio
    async def test_divergence_detected_without_require(self):
        ind_v1 = [_make_indicador(valor=Decimal("100.00"))]
        ind_v2 = [_make_indicador(valor=Decimal("200.00"))]
        cls1 = _make_parser_cls(1, ind_v1)
        cls2 = _make_parser_cls(2, ind_v2)

        with patch("agrobr.cepea.parsers.consensus.CONSENSUS_PARSERS", [cls1, cls2]):
            result = await parse_with_consensus("<html>test</html>", "soja")

        assert result.has_consensus is False
        assert len(result.divergences) > 0
        assert result.indicadores == ind_v2


class TestConsensusValidator:
    @pytest.mark.asyncio
    async def test_divergence_increments_count(self):
        validator = ConsensusValidator()

        with patch(
            "agrobr.cepea.parsers.consensus.parse_with_consensus",
            new_callable=AsyncMock,
        ) as mock_parse:
            from agrobr.cepea.parsers.v1 import CepeaParserV1

            mock_parse.return_value = ConsensusResult(
                indicadores=[],
                parser_used=CepeaParserV1(),
                all_results={},
                has_consensus=False,
                divergences=[{"type": "count_mismatch"}],
                report={},
            )
            await validator.validate("<html>", "soja")

        assert validator.divergence_count == 1
        assert validator.divergence_rate == 1.0

    def test_get_statistics(self):
        validator = ConsensusValidator()
        stats = validator.get_statistics()
        assert stats["total_validations"] == 0
        assert stats["divergence_count"] == 0
        assert stats["divergence_rate"] == 0.0
        assert stats["consensus_rate"] == 1.0

    @pytest.mark.asyncio
    async def test_validate_tracks_history(self):
        validator = ConsensusValidator()

        with patch(
            "agrobr.cepea.parsers.consensus.parse_with_consensus",
            new_callable=AsyncMock,
        ) as mock_parse:
            from agrobr.cepea.parsers.v1 import CepeaParserV1

            mock_parse.return_value = ConsensusResult(
                indicadores=[_make_indicador()],
                parser_used=CepeaParserV1(),
                all_results={1: [_make_indicador()]},
                has_consensus=True,
                divergences=[],
                report={},
            )
            await validator.validate("<html>", "soja")

        assert len(validator.history) == 1
        assert validator.divergence_count == 0
        assert validator.divergence_rate == 0.0


class TestSelectBestResult:
    def test_count_mismatch_selects_most_records(self):
        results = {
            1: [_make_indicador(), _make_indicador(data_val=date(2024, 1, 2))],
            2: [_make_indicador()],
        }
        divergences = [{"type": "count_mismatch"}]
        version, selected = select_best_result(results, divergences)
        assert version == 1
        assert len(selected) == 2

    def test_empty_results(self):
        version, results = select_best_result({}, [])
        assert version == 0
        assert results == []

    def test_selects_highest_version(self):
        results = {
            1: [_make_indicador()],
            2: [_make_indicador()],
        }
        version, _ = select_best_result(results, [])
        assert version == 2
