"""Testes para exceções do agrobr."""

import agrobr
from agrobr.exceptions import (
    AgrobrError,
    CacheMigrationError,
    ContractViolationError,
    InvalidParameterError,
    NetworkError,
    ParseError,
    ResourceLimitError,
    SourceFallbackWarning,
    SourceUnavailableError,
    UnknownNameError,
)


class TestInvalidParameterError:
    def test_inherits_agrobr_error_and_value_error(self):
        err = InvalidParameterError("parâmetro inválido")

        assert isinstance(err, AgrobrError)
        assert isinstance(err, ValueError)

    def test_is_exported_by_package(self):
        assert agrobr.InvalidParameterError is InvalidParameterError
        assert "InvalidParameterError" in agrobr.__all__


class TestSourceFallbackWarning:
    def test_inherits_user_warning(self):
        assert issubclass(SourceFallbackWarning, UserWarning)


class TestSourceUnavailableError:
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


def test_resource_limit_error_guarda_campos_e_mensagem():
    erro = ResourceLimitError("acervo", "download acima de 100 MB", url="https://x")
    assert tuple(getattr(erro, campo, None) for campo in ("source", "reason", "url")) == (
        "acervo",
        "download acima de 100 MB",
        "https://x",
    )
    assert str(erro) == "acervo: limite local excedido: download acima de 100 MB"


def test_contract_violation_sem_expected_nao_cita_expected():
    assert str(ContractViolationError("d", "v")) == "Contract violation in d: v"


def test_cache_migration_error_guarda_versao_e_motivo():
    erro = CacheMigrationError(9, "arquivo divergente")
    assert (getattr(erro, "version", None), getattr(erro, "reason", None)) == (
        9,
        "arquivo divergente",
    )
    assert str(erro).startswith("Falha na migração 9 do cache: arquivo divergente.")


def test_unknown_name_error_e_parametro_invalido_e_key_error_sem_aspas():
    erro = UnknownNameError("Dataset 'xx' não encontrado. Disponíveis: a, b")
    assert isinstance(erro, InvalidParameterError)
    assert isinstance(erro, KeyError)
    assert str(erro) == "Dataset 'xx' não encontrado. Disponíveis: a, b"


def test_parse_error_guarda_errors_e_os_cita_na_mensagem():
    errors = [("cepea", "ParseError", "tabela ausente"), ("noticias", "ParseError", "layout")]
    erro = ParseError("preco_diario", 1, "todas as fontes falharam por layout", errors=errors)
    assert erro.errors == errors
    assert erro.attempted_sources == ["preco_diario"]
    assert str(erro) == (
        f"Parse failed (preco_diario v1): todas as fontes falharam por layout (errors={errors})"
    )


def test_parse_error_sem_errors_mantem_mensagem():
    erro = ParseError("cepea", 2, "sem tabela", attempted_sources=["cepea", "cache"])
    assert (erro.errors, erro.attempted_sources) == ([], ["cepea", "cache"])
    assert str(erro) == "Parse failed (cepea v2): sem tabela"
