"""Tests for stability module."""

from __future__ import annotations

import warnings

from agrobr.stability import (
    APIInfo,
    APIStatus,
    deprecated,
    experimental,
    get_api_info,
    get_api_registry,
    internal,
    list_deprecated_apis,
    list_experimental_apis,
    list_stable_apis,
    stable,
)


class TestAPIStatus:
    def test_status_values(self):
        assert APIStatus.STABLE == "stable"
        assert APIStatus.EXPERIMENTAL == "experimental"
        assert APIStatus.DEPRECATED == "deprecated"
        assert APIStatus.INTERNAL == "internal"


class TestStableDecorator:
    def test_stable_decorator(self):
        @stable(since="1.0.0")
        def my_func():
            return "result"

        result = my_func()
        assert result == "result"

        info = get_api_info(my_func)
        assert info is not None
        assert info.status == APIStatus.STABLE
        assert info.since == "1.0.0"

    def test_stable_with_notes(self):
        @stable(since="1.0.0", notes="Important function")
        def important_func():
            pass

        info = get_api_info(important_func)
        assert info.notes == "Important function"

    def test_stable_in_registry(self):
        @stable(since="2.0.0")
        def registered_func():
            pass

        apis = list_stable_apis()
        assert any("registered_func" in name for name in apis)


class TestExperimentalDecorator:
    def test_experimental_warns(self):
        @experimental(since="0.5.0")
        def beta_func():
            return "beta"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = beta_func()

            assert result == "beta"
            assert len(w) == 1
            assert "experimental" in str(w[0].message).lower()

    def test_experimental_info(self):
        @experimental(since="0.5.0", notes="May change")
        def unstable_func():
            pass

        info = get_api_info(unstable_func)
        assert info.status == APIStatus.EXPERIMENTAL
        assert info.notes == "May change"

    def test_experimental_in_registry(self):
        @experimental(since="0.5.0")
        def exp_func():
            pass

        apis = list_experimental_apis()
        assert any("exp_func" in name for name in apis)


class TestDeprecatedDecorator:
    def test_deprecated_warns(self):
        @deprecated(since="1.0.0")
        def old_func():
            return "old"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = old_func()

            assert result == "old"
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)

    def test_deprecated_with_replacement(self):
        @deprecated(since="1.0.0", replacement="new_func")
        def legacy_func():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            legacy_func()

            assert "new_func" in str(w[0].message)

    def test_deprecated_with_removal(self):
        @deprecated(since="1.0.0", removed_in="2.0.0")
        def dying_func():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            dying_func()

            assert "2.0.0" in str(w[0].message)

    def test_deprecated_in_registry(self):
        @deprecated(since="1.0.0")
        def dep_func():
            pass

        apis = list_deprecated_apis()
        assert any("dep_func" in name for name in apis)


class TestInternalDecorator:
    def test_internal_no_warn(self):
        @internal
        def private_func():
            return "private"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = private_func()

            assert result == "private"
            assert len(w) == 0

    def test_internal_info(self):
        @internal
        def hidden_func():
            pass

        info = get_api_info(hidden_func)
        assert info.status == APIStatus.INTERNAL


class TestAPIRegistry:
    def test_get_api_registry(self):
        @stable(since="1.0.0")
        def func_a():
            pass

        @experimental(since="0.5.0")
        def func_b():
            pass

        registry = get_api_registry()
        assert isinstance(registry, dict)
        assert all(isinstance(v, APIInfo) for v in registry.values())
