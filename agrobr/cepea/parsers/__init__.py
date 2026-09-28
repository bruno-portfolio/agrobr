"""Parsers CEPEA - Versionados para diferentes layouts."""

from __future__ import annotations

from agrobr.cepea.parsers.base import BaseParser
from agrobr.cepea.parsers.consensus import (
    ConsensusResult,
    ConsensusValidator,
    analyze_consensus,
    parse_with_consensus,
    select_best_result,
)
from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.cepea.parsers.fingerprint import (
    compare_fingerprints,
    extract_fingerprint,
)
from agrobr.cepea.parsers.v1 import CepeaParserV1

__all__ = [
    "BaseParser",
    "CepeaParserV1",
    "compare_fingerprints",
    "extract_fingerprint",
    "get_parser_with_fallback",
    "ConsensusResult",
    "ConsensusValidator",
    "parse_with_consensus",
    "analyze_consensus",
    "select_best_result",
]
