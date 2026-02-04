"""Interface base para parsers CEPEA."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agrobr import models


class BaseParser(ABC):
    """Interface base para todos os parsers."""

    version: int
    source: str
    valid_from: date
    valid_until: date | None = None
    expected_fingerprint: dict | None = None

    @abstractmethod
    def can_parse(self, html: str) -> tuple[bool, float]:
        """Verifica se este parser consegue processar o HTML. Retorna (pode_parsear, confianca)."""
        pass

    @abstractmethod
    def parse(self, html: str, produto: str) -> list["models.Indicador"]:
        """Parseia HTML e retorna lista de indicadores."""
        pass

    @abstractmethod
    def extract_fingerprint(self, html: str) -> dict:
        """Extrai assinatura estrutural do HTML."""
        pass
