from __future__ import annotations

import warnings

import pytest

from agrobr.utils.warnings import warn_once, warn_once_reset


@pytest.fixture(autouse=True)
def _clean_state():
    warn_once_reset()
    yield
    warn_once_reset()


def test_warn_once_reset_all():
    warn_once("k1", "msg1")
    warn_once("k2", "msg2")

    warn_once_reset()

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_once("k1", "msg1")
        warn_once("k2", "msg2")
    assert [str(aviso.message) for aviso in caught] == ["msg1", "msg2"]


def _caller_helper() -> list[warnings.WarningMessage]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warn_once("stack_test", "stacklevel check")
    return caught


def test_warn_once_stacklevel():
    caught = _caller_helper()
    assert len(caught) == 1
    assert caught[0].filename == __file__
