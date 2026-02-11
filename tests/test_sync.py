from __future__ import annotations

import asyncio
from unittest import mock

import pytest

from agrobr.sync import _get_or_create_event_loop, _SyncModule, run_sync, sync_wrapper


class TestRunSync:
    def test_executes_coroutine(self):
        async def coro():
            return 42

        result = run_sync(coro())
        assert result == 42

    def test_returns_value(self):
        async def coro():
            return {"key": "value"}

        result = run_sync(coro())
        assert result == {"key": "value"}

    def test_propagates_exception(self):
        async def failing():
            raise ValueError("async error")

        with pytest.raises(ValueError, match="async error"):
            run_sync(failing())

    def test_propagates_typed_exception(self):
        async def failing():
            raise RuntimeError("typed")

        with pytest.raises(RuntimeError, match="typed"):
            run_sync(failing())


class TestSyncWrapper:
    def test_wraps_async_func(self):
        async def async_add(a, b):
            return a + b

        sync_add = sync_wrapper(async_add)
        result = sync_add(3, 4)
        assert result == 7

    def test_preserves_name(self):
        async def my_func():
            pass

        wrapped = sync_wrapper(my_func)
        assert wrapped.__name__ == "my_func"

    def test_preserves_doc_with_sync_prefix(self):
        async def documented():
            """Original doc."""
            pass

        wrapped = sync_wrapper(documented)
        assert wrapped.__doc__ is not None
        assert "SYNC" in wrapped.__doc__
        assert "Original doc" in wrapped.__doc__

    def test_no_doc_no_error(self):
        async def no_doc():
            pass

        no_doc.__doc__ = None
        wrapped = sync_wrapper(no_doc)
        assert wrapped.__doc__ is None

    def test_passes_args_and_kwargs(self):
        async def func(a, b, key=None):
            return (a, b, key)

        sync_func = sync_wrapper(func)
        result = sync_func(1, 2, key="val")
        assert result == (1, 2, "val")

    def test_exception_propagation(self):
        async def failing():
            raise TypeError("wrong type")

        sync_fail = sync_wrapper(failing)
        with pytest.raises(TypeError, match="wrong type"):
            sync_fail()


class TestSyncModule:
    def test_wraps_coroutine_functions(self):
        mock_module = mock.MagicMock()

        async def async_method():
            return "async_result"

        mock_module.fetch = async_method
        sync_mod = _SyncModule(mock_module)

        result = sync_mod.fetch()
        assert result == "async_result"

    def test_passes_non_coroutine_through(self):
        mock_module = mock.MagicMock()
        mock_module.CONSTANT = 42

        sync_mod = _SyncModule(mock_module)
        assert sync_mod.CONSTANT == 42

    def test_sync_function_passthrough(self):
        mock_module = mock.MagicMock()

        def regular_func():
            return "sync"

        mock_module.regular = regular_func
        sync_mod = _SyncModule(mock_module)

        result = sync_mod.regular()
        assert result == "sync"

    def test_async_exception_propagation(self):
        mock_module = mock.MagicMock()

        async def failing():
            raise ConnectionError("network down")

        mock_module.failing = failing
        sync_mod = _SyncModule(mock_module)

        with pytest.raises(ConnectionError, match="network down"):
            sync_mod.failing()


class TestModuleLazyLoading:
    def test_valid_module_loads(self):
        import agrobr.sync as sync_module

        with mock.patch("importlib.import_module") as mock_import:
            mock_async_mod = mock.MagicMock()
            mock_import.return_value = mock_async_mod

            sync_module._modules["cepea"] = None

            result = sync_module.__getattr__("cepea")

            mock_import.assert_called_with("agrobr.cepea")
            assert result is not None

            sync_module._modules["cepea"] = None

    def test_invalid_module_raises(self):
        import agrobr.sync as sync_module

        with pytest.raises(AttributeError, match="no attribute"):
            sync_module.__getattr__("nonexistent_module")

    def test_cached_module_not_reimported(self):
        import agrobr.sync as sync_module

        sentinel = mock.MagicMock()
        original = sync_module._modules.get("cepea")
        sync_module._modules["cepea"] = sentinel

        try:
            with mock.patch("importlib.import_module") as mock_import:
                result = sync_module.__getattr__("cepea")

                mock_import.assert_not_called()
                assert result is sentinel
        finally:
            sync_module._modules["cepea"] = original


class TestGetOrCreateEventLoop:
    def test_creates_loop_when_none_exists(self):
        loop = _get_or_create_event_loop()
        assert loop is not None

    def test_running_loop_without_nest_asyncio_raises(self):
        async def inner():
            with (
                mock.patch.dict("sys.modules", {"nest_asyncio": None}),
                mock.patch(
                    "agrobr.sync.asyncio.get_running_loop",
                    return_value=asyncio.get_event_loop(),
                ),
            ):
                pass

        pass
