"""Exceções tipadas do agrobr."""

from __future__ import annotations

from typing import Any


class AgrobrError(Exception):
    """Base para todas as exceções do agrobr."""

    pass


class SourceUnavailableError(AgrobrError):
    """Fonte de dados não disponível após todas as tentativas."""

    def __init__(
        self,
        source: str,
        url: str | None = None,
        last_error: str | None = None,
        errors: list[tuple[str, str, str]] | None = None,
    ) -> None:
        self.source = source
        self.url = url or ""
        self.last_error = last_error or ""
        self.errors = errors or []
        if errors:
            super().__init__(f"All sources failed for {source}: {errors}")
        else:
            super().__init__(f"{source} unavailable: {last_error}")


class NetworkError(AgrobrError):
    """Erro de rede (timeout, HTTP error, DNS)."""

    def __init__(self, source: str, url: str, reason: str) -> None:
        self.source = source
        self.url = url
        self.reason = reason
        super().__init__(f"Network error ({source}): {reason}")


class ContractViolationError(AgrobrError):
    """DataFrame não atende o contrato do dataset (colunas, tipos, ranges)."""

    def __init__(
        self,
        dataset: str,
        violation: str,
        expected: Any = None,
        got: Any = None,
    ) -> None:
        self.dataset = dataset
        self.violation = violation
        self.expected = expected
        self.got = got
        msg = f"Contract violation in {dataset}: {violation}"
        if expected is not None:
            msg += f" (expected={expected}, got={got})"
        super().__init__(msg)


class ParseError(AgrobrError):
    """Falha ao parsear dados da fonte."""

    def __init__(
        self,
        source: str,
        parser_version: int,
        reason: str,
        html_snippet: str = "",
    ) -> None:
        self.source = source
        self.parser_version = parser_version
        self.reason = reason
        self.html_snippet = html_snippet[:500]
        super().__init__(f"Parse failed ({source} v{parser_version}): {reason}")


class ValidationError(AgrobrError):
    """Dados não passaram validação Pydantic ou estatística."""

    def __init__(
        self,
        source: str,
        field: str,
        value: Any,
        reason: str,
    ) -> None:
        self.source = source
        self.field = field
        self.value = value
        self.reason = reason
        super().__init__(f"Validation failed: {field}={value} - {reason}")


class CacheError(AgrobrError):
    """Erro de operação de cache."""

    pass


class FingerprintMismatchError(AgrobrError):
    """Estrutura da página mudou significativamente."""

    def __init__(self, source: str, similarity: float, threshold: float) -> None:
        self.source = source
        self.similarity = similarity
        self.threshold = threshold
        super().__init__(
            f"Layout change detected in {source}: "
            f"similarity {similarity:.2%} < threshold {threshold:.2%}"
        )


class StaleDataWarning(UserWarning):
    """Dados do cache estão expirados mas foram retornados."""

    pass


class PartialDataWarning(UserWarning):
    """Dados retornados estão incompletos."""

    pass


class LayoutChangeWarning(UserWarning):
    """Possível mudança de layout detectada (baixa confiança)."""

    pass


class AnomalyDetectedWarning(UserWarning):
    """Anomalia estatística detectada nos dados."""

    pass


class ParserFallbackWarning(UserWarning):
    """Parser principal falhou, usando fallback."""

    pass
