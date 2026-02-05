"""Testes para exceções do agrobr."""

from agrobr.exceptions import (
    AgrobrError,
    ContractViolationError,
    NetworkError,
    ParseError,
    SourceUnavailableError,
)


class TestAgrobrError:
    def test_base_exception(self):
        err = AgrobrError("test error")
        assert str(err) == "test error"
        assert isinstance(err, Exception)


class TestSourceUnavailableError:
    def test_with_url_and_last_error(self):
        err = SourceUnavailableError(
            source="cepea",
            url="https://example.com",
            last_error="timeout",
        )
        assert err.source == "cepea"
        assert err.url == "https://example.com"
        assert err.last_error == "timeout"
        assert "cepea" in str(err)
        assert "timeout" in str(err)

    def test_with_errors_list(self):
        errors = [
            ("cepea", "network", "timeout"),
            ("cache", "parse", "empty data"),
        ]
        err = SourceUnavailableError(
            source="preco_diario/soja",
            errors=errors,
        )
        assert err.errors == errors
        assert "All sources failed" in str(err)


class TestNetworkError:
    def test_creation(self):
        err = NetworkError(
            source="cepea",
            url="https://example.com",
            reason="Connection timeout",
        )
        assert err.source == "cepea"
        assert err.url == "https://example.com"
        assert err.reason == "Connection timeout"
        assert "Network error" in str(err)
        assert "cepea" in str(err)


class TestContractViolationError:
    def test_basic(self):
        err = ContractViolationError(
            dataset="preco_diario",
            violation="missing column 'valor'",
        )
        assert err.dataset == "preco_diario"
        assert err.violation == "missing column 'valor'"
        assert "preco_diario" in str(err)

    def test_with_expected_got(self):
        err = ContractViolationError(
            dataset="preco_diario",
            violation="wrong type",
            expected="float64",
            got="object",
        )
        assert err.expected == "float64"
        assert err.got == "object"
        assert "expected=float64" in str(err)
        assert "got=object" in str(err)


class TestParseError:
    def test_creation(self):
        err = ParseError(
            source="cepea",
            parser_version=1,
            reason="Table not found",
        )
        assert err.source == "cepea"
        assert err.parser_version == 1
        assert err.reason == "Table not found"
        assert "cepea" in str(err)
        assert "v1" in str(err)
