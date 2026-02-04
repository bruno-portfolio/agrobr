"""Parsers CEPEA - Versionados para diferentes layouts."""

from __future__ import annotations

from agrobr.cepea.parsers.base import BaseParser
from agrobr.cepea.parsers.consensus import (
    ConsensusResult,
    ConsensusValidator,
    ParserDivergence,
    analyze_consensus,
    parse_with_consensus,
    select_best_result,
)
from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.cepea.parsers.fingerprint import (
    compare_fingerprints,
    extract_fingerprint,
    load_baseline_fingerprint,
    save_baseline_fingerprint,
)
from agrobr.cepea.parsers.v1 import CepeaParserV1

__all__ = [
    "BaseParser",
    "CepeaParserV1",
    "compare_fingerprints",
    "extract_fingerprint",
    "get_parser_with_fallback",
    "load_baseline_fingerprint",
    "save_baseline_fingerprint",
    "ConsensusResult",
    "ConsensusValidator",
    "ParserDivergence",
    "parse_with_consensus",
    "analyze_consensus",
    "select_best_result",
]
