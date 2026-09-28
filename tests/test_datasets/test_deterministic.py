"""Testes para o context manager determinístico."""

import pytest

from agrobr.datasets.deterministic import (
    deterministic,
    deterministic_decorator,
    get_snapshot,
)
from tests.helpers import collect_failures, isolated_dataset_case


class TestDeterministicContextManager:
    async def test_deterministic_invalid_date_raises(self):
        with pytest.raises(ValueError):
            async with deterministic("invalid-date"):
                pass

    async def test_deterministic_nested(self):
        async with deterministic("2025-12-31"):
            assert get_snapshot() == "2025-12-31"
            async with deterministic("2024-06-15"):
                assert get_snapshot() == "2024-06-15"
            assert get_snapshot() == "2025-12-31"


class TestDeterministicDecorator:
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
