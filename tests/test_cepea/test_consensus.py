from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.cepea.parsers.consensus import (
    ConsensusValidator,
    analyze_consensus,
    select_best_result,
)
from agrobr.constants import Fonte
from agrobr.models import Indicador


def _make_indicador(valor=Decimal("150.00"), data_val=date(2024, 1, 1)):
    return Indicador(
        fonte=Fonte.CEPEA,
        produto="soja",
        data=data_val,
        valor=valor,
        unidade="BRL/sc60kg",
    )


class TestAnalyzeConsensus:
    def test_single_result_no_divergence(self):
        results = {1: [_make_indicador()]}
        divergences, report = analyze_consensus(results, {})
        assert len(divergences) == 0
        assert 1 in report["successful"]

    def test_empty_results(self):
        divergences, report = analyze_consensus({}, {})
        assert len(divergences) == 0

    def test_count_mismatch(self):
        results = {
            1: [_make_indicador()],
            2: [_make_indicador(), _make_indicador(data_val=date(2024, 1, 2))],
        }
        divergences, report = analyze_consensus(results, {})
        assert any(d["type"] == "count_mismatch" for d in divergences)

    def test_value_mismatch(self):
        results = {
            1: [_make_indicador(valor=Decimal("100.00"))],
            2: [_make_indicador(valor=Decimal("200.00"))],
        }
        divergences, report = analyze_consensus(results, {})
        assert any("value_mismatch" in d["type"] for d in divergences)

    def test_date_mismatch(self):
        results = {
            1: [_make_indicador(data_val=date(2024, 1, 1))],
            2: [_make_indicador(data_val=date(2024, 1, 2))],
        }
        divergences, report = analyze_consensus(results, {})
        assert any("date_mismatch" in d["type"] for d in divergences)

    def test_errors_in_report(self):
        results = {1: [_make_indicador()]}
        errors = {2: "Parse failed"}
        divergences, report = analyze_consensus(results, errors)
        assert 2 in report["failed"]
        assert report["errors"][2] == "Parse failed"

    def test_identical_results_no_divergence(self):
        ind = _make_indicador()
        results = {1: [ind], 2: [ind]}
        divergences, report = analyze_consensus(results, {})
        assert len(divergences) == 0


class TestSelectBestResult:
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

    def test_count_mismatch_selects_most_records(self):
        results = {
            1: [_make_indicador(), _make_indicador(data_val=date(2024, 1, 2))],
            2: [_make_indicador()],
        }
        divergences = [{"type": "count_mismatch"}]
        version, selected = select_best_result(results, divergences)
        assert version == 1
        assert len(selected) == 2


class TestConsensusValidator:
    @pytest.mark.asyncio
    async def test_validate_tracks_history(self):
        validator = ConsensusValidator()

        with patch(
            "agrobr.cepea.parsers.consensus.parse_with_consensus",
            new_callable=AsyncMock,
        ) as mock_parse:
            from agrobr.cepea.parsers.consensus import ConsensusResult
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

    @pytest.mark.asyncio
    async def test_divergence_increments_count(self):
        validator = ConsensusValidator()

        with patch(
            "agrobr.cepea.parsers.consensus.parse_with_consensus",
            new_callable=AsyncMock,
        ) as mock_parse:
            from agrobr.cepea.parsers.consensus import ConsensusResult
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
