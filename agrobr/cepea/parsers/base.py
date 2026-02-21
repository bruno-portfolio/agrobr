from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agrobr import models


class BaseParser(ABC):
    version: int
    source: str
    valid_from: date
    valid_until: date | None = None
    expected_fingerprint: dict[str, str] | None = None

    @abstractmethod
    def can_parse(self, html: str) -> tuple[bool, float]:
        pass

    @abstractmethod
    def parse(self, html: str, produto: str) -> list[models.Indicador]:
        pass

    @abstractmethod
    def extract_fingerprint(self, html: str) -> dict[str, str]:
        pass
