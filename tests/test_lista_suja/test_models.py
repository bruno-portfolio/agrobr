from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pydantic
import pytest

from agrobr.contracts import lista_suja as lista_suja_contract
from agrobr.lista_suja import models
from agrobr.normalize import regions
from tests import helpers


@pytest.fixture
def record_data():
    return {
        "empregador": "Empregador de exemplo",
        "cpf_cnpj": "001.234.567-89",
        "estabelecimento": None,
        "uf": None,
        "cnae": "0111-3/01",
        "data_inclusao": datetime(2024, 10, 7),
        "trabalhadores_resgatados": None,
        "ano_acao_fiscal": None,
        "id_registro": "001",
        "data_decisao": None,
        "data_atualizacao": None,
        "data_inclusao_texto": "07/10/2024",
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("id_registro", 1),
        ("id_registro", ""),
        ("id_registro", "1A"),
        ("uf", "XX"),
        ("uf", "mt"),
        ("ano_acao_fiscal", 0),
        ("ano_acao_fiscal", 10000),
        ("ano_acao_fiscal", "2024"),
        ("trabalhadores_resgatados", -1),
        ("trabalhadores_resgatados", True),
        ("trabalhadores_resgatados", 1.5),
        ("trabalhadores_resgatados", 2**63),
        ("cpf_cnpj", 123),
        ("empregador", ""),
        ("extra_field", "unexpected"),
    ],
)
def test_employer_record_rejects_invalid_domain_or_coercion(field, value, record_data):
    with pytest.raises(pydantic.ValidationError):
        models.EmployerRecord(**{**record_data, field: value})


@pytest.fixture
def resource_data():
    return {
        "content": b"publication",
        "requested_url": "https://www.gov.br/source.csv",
        "url": "https://www.gov.br/source.csv",
        "fetched_at": datetime(2026, 9, 6, tzinfo=UTC),
        "sha256": "a" * 64,
        "size_bytes": 11,
    }


def test_resource_normalizes_acquisition_timezone_without_exposing_content(resource_data):
    resource = models.HTTPResource(
        **{
            **resource_data,
            "fetched_at": datetime(2026, 9, 6, 9, tzinfo=timezone(timedelta(hours=-3))),
        }
    )
    assert resource.fetched_at == datetime(2026, 9, 6, 12, tzinfo=UTC)
    assert "publication" not in repr(resource)
    assert "content" not in resource.model_dump(mode="json", exclude={"content"})


@pytest.mark.parametrize(
    "field,value",
    [
        ("fetched_at", datetime(2026, 9, 6)),
        ("sha256", "invalid"),
        ("size_bytes", -1),
    ],
)
def test_resource_rejects_invalid_provenance(field, value, resource_data):
    with pytest.raises(pydantic.ValidationError):
        models.HTTPResource(**{**resource_data, field: value})


def test_contrato_publica_restricoes_e_relata_coluna_ausente_sem_quebrar():
    contrato = lista_suja_contract.LISTA_SUJA_EMPREGADORES_V2
    restricoes = contrato.to_dict()["constraints"]
    assert restricoes["uf_allowed"] == sorted(regions.UFS_VALIDAS)
    assert {chave: restricoes[chave] for chave in restricoes if chave != "uf_allowed"} == {
        "no_duplicates": True,
        "trabalhadores_resgatados_min": 0,
        "ano_acao_fiscal_min": 1,
        "ano_acao_fiscal_max": 9999,
        "id_registro_pattern": "^[0-9]+$",
        "primary_key_scope": "one_resource_content_hash",
        "integer_dtype": "Int64",
        "date_dtype": "datetime64[ns]",
        "date_semantics": "civil_date_without_timezone",
        "blank_strings": "null_for_nullable_fields",
        "compound_inclusion_scalar": "null",
    }
    with helpers.sem_excecao():
        resultado = contrato.validate(contrato.empty_frame().drop(columns="cnae"))
    assert resultado == (False, ["Missing required columns: {'cnae'}"])
