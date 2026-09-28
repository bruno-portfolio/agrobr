from __future__ import annotations

from agrobr.b3.models import (
    parse_vencimento,
)


class TestParseVencimento:
    def test_strips_whitespace(self):
        assert parse_vencimento(" K25 ") == (2025, 5)
