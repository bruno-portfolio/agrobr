from __future__ import annotations

import pytest

from agrobr.cftc.models import (
    CFTC_CONTRACTS,
    resolve_contract_codes,
)


class TestResolveContractCodes:
    def test_none_retorna_todos(self):
        assert resolve_contract_codes(None) == list(CFTC_CONTRACTS)

    def test_codigo_direto(self):
        assert resolve_contract_codes("080732") == ["080732"]

    def test_commodity_sem_contrato_raises(self):
        with pytest.raises(ValueError, match="sem contrato CFTC"):
            resolve_contract_codes("mandioca")
