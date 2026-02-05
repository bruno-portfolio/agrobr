"""Testes para o context manager determinístico."""

import pytest

from agrobr.datasets.deterministic import (
    deterministic,
    deterministic_decorator,
    get_snapshot,
    is_deterministic,
)


class TestDeterministicContextManager:
    async def test_get_snapshot_none_by_default(self):
        assert get_snapshot() is None

    async def test_is_deterministic_false_by_default(self):
        assert is_deterministic() is False

    async def test_deterministic_sets_snapshot(self):
        async with deterministic("2025-12-31"):
            assert get_snapshot() == "2025-12-31"
            assert is_deterministic() is True

    async def test_deterministic_resets_after_exit(self):
        async with deterministic("2025-12-31"):
            pass
        assert get_snapshot() is None
        assert is_deterministic() is False

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
    async def test_decorator_sets_snapshot(self):
        @deterministic_decorator("2025-01-15")
        async def my_func():
            return get_snapshot()

        result = await my_func()
        assert result == "2025-01-15"

    async def test_decorator_resets_after(self):
        @deterministic_decorator("2025-01-15")
        async def my_func():
            return get_snapshot()

        await my_func()
        assert get_snapshot() is None

    def test_decorator_invalid_date_raises(self):
        with pytest.raises(ValueError):

            @deterministic_decorator("not-a-date")
            async def my_func():
                pass
