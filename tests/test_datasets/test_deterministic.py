"""Testes para o context manager determinístico."""

import pytest

from agrobr import datasets, exceptions
from agrobr import deterministic as root_deterministic
from agrobr.datasets.deterministic import (
    deterministic,
    deterministic_decorator,
    get_snapshot,
)
from tests.helpers import collect_failures, isolated_dataset_case


class TestDeterministicContextManager:
    @pytest.mark.parametrize("context", [root_deterministic, datasets.deterministic])
    @pytest.mark.parametrize("snapshot", ["invalid-date", "2025-02-30", None, []])
    async def test_deterministic_invalid_date_raises(self, context, snapshot):
        async with deterministic("2025-12-31"):
            with pytest.raises(exceptions.InvalidParameterError, match="AAAA-MM-DD") as caught:
                async with context(snapshot):
                    pytest.fail("snapshot inválido entrou no contexto")
            assert isinstance(caught.value.__cause__, (ValueError, TypeError))
            assert get_snapshot() == "2025-12-31"

    async def test_deterministic_nested(self):
        async with deterministic("2025-12-31"):
            assert get_snapshot() == "2025-12-31"
            async with deterministic("2024-06-15"):
                assert get_snapshot() == "2024-06-15"
            assert get_snapshot() == "2025-12-31"


class TestDeterministicDecorator:
    def test_decorator_recusa_snapshot_invalido(self):
        with pytest.raises(exceptions.InvalidParameterError, match="AAAA-MM-DD") as caught:
            deterministic_decorator("2025-02-30")
        assert isinstance(caught.value.__cause__, ValueError)

    async def test_deterministic_decorator_casos_1(self):
        with collect_failures() as check:
            case = "test_decorator_sets_snapshot"
            with check(case), isolated_dataset_case(case):

                @deterministic_decorator("2025-01-15")
                async def my_func():
                    return get_snapshot()

                result = await my_func()
                assert result == "2025-01-15"
            case = "test_decorator_resets_after"
            with check(case), isolated_dataset_case(case):

                @deterministic_decorator("2025-01-15")
                async def my_func():
                    return get_snapshot()

                await my_func()
                assert get_snapshot() is None
