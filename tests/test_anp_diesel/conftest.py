from __future__ import annotations

from datetime import date, datetime

import pytest

from agrobr.utils import time as time_utils


@pytest.fixture(autouse=True)
def reference_year(monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow", lambda: datetime(2026, 9, 8))
    monkeypatch.setattr(time_utils, "hoje", lambda: date(2026, 9, 8))
