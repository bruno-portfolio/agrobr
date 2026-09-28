from __future__ import annotations

import csv
import hashlib
import io
import re
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pandas as pd
import pytest

from agrobr import contracts, datasets
from agrobr.datasets.deterministic import deterministic
from agrobr.exceptions import InvalidParameterError
from agrobr.lista_suja import api
from tests.helpers import levanta_exatamente

NAME = "empregadores_lista_suja"
CONTRACT = "lista_suja_empregadores"
TEXT_FIELDS = {
    "ID": "id_registro",
    "UF": "uf",
    "Empregador": "empregador",
    "CNPJ/CPF": "cpf_cnpj",
    "Estabelecimento": "estabelecimento",
    "CNAE": "cnae",
}
NUMBER_FIELDS = {
    "Ano da ação fiscal": "ano_acao_fiscal",
    "Trabalhadores envolvidos": "trabalhadores_resgatados",
}


async def test_official_csv_txt_replay_compares_every_published_cell(
    publication_files, lista_suja_replay_http
):
    requests = lista_suja_replay_http()
    frame, meta = await datasets.empregadores_lista_suja(return_meta=True)
    raw = list(
        csv.DictReader(io.StringIO(publication_files["csv"].decode("cp1252")), delimiter=";")
    )
    assert len(raw) == len(frame) == 579 and len(frame.columns) == 12
    for published, actual in zip(raw, frame.to_dict("records"), strict=True):
        for external, column in TEXT_FIELDS.items():
            value = published[external].strip()
            if column in {"empregador", "cpf_cnpj", "estabelecimento"}:
                value = " ".join(value.split())
            assert pd.isna(actual[column]) if not value else actual[column] == value, (
                published["ID"],
                column,
            )
        for external, column in NUMBER_FIELDS.items():
            value = published[external].strip()
            assert pd.isna(actual[column]) if not value else actual[column] == int(value)
        decision = published["Decisão administrativa de procedência"].strip()
        assert (
            pd.isna(actual["data_decisao"])
            if not decision
            else actual["data_decisao"] == pd.Timestamp(datetime.strptime(decision, "%d/%m/%Y"))
        )
        inclusion = published["Inclusão no Cadastro de Empregadores"]
        assert actual["data_inclusao_texto"] == inclusion
        dates = re.findall(r"\d{2}/\d{2}/\d{4}", inclusion)
        assert (
            pd.isna(actual["data_inclusao"])
            if len(dates) != 1
            else actual["data_inclusao"] == pd.Timestamp(datetime.strptime(dates[0], "%d/%m/%Y"))
        )
        assert actual["data_atualizacao"] == pd.Timestamp("2026-09-04")
    assert frame["id_registro"].is_unique and frame["cpf_cnpj"].nunique() == 567
    assert frame["cpf_cnpj"].str.startswith("0").any()
    assert frame["trabalhadores_resgatados"].sum() == 4706
    assert frame.loc[frame["uf"].isna(), "id_registro"].tolist() == ["13", "140", "394"]
    assert frame.loc[frame["trabalhadores_resgatados"].isna(), "id_registro"].tolist() == [
        "140",
        "394",
    ]
    assert frame["data_inclusao"].isna().sum() == 10
    assert contracts.get_contract(CONTRACT).validate(frame) == (True, [])
    assert meta.dataset == NAME and meta.source == f"datasets.{NAME}/lista_suja_csv"
    assert meta.selected_source == "lista_suja_csv" and meta.attempted_sources == ["lista_suja_csv"]
    assert meta.schema_version == meta.contract_version == "2.0" and meta.parser_version == 4
    assert meta.snapshot is None and not meta.from_cache
    assert meta.raw_content_hash == hashlib.sha256(publication_files["csv"]).hexdigest()
    assert meta.raw_content_size == len(publication_files["csv"])
    assert meta.cache_key is None and meta.cache_expires_at is None
    assert meta.fetch_duration_ms >= 0 and meta.parse_duration_ms >= 0
    assert meta.fetched_at.utcoffset() == UTC.utcoffset(
        None
    ) and meta.fetch_timestamp.utcoffset() == timedelta(0)
    assert meta.columns == frame.columns.tolist() and meta.records_count == 579
    details = meta.source_details
    assert details["source_rows"] == details["output_rows"] == 579
    assert details["null_counts_scope"] == "complete_source_before_filters"
    assert details["resource"]["sha256"] == meta.raw_content_hash
    assert details["companion"]["sha256"] == hashlib.sha256(publication_files["txt"]).hexdigest()
    assert details["discovery"]["sha256"] == hashlib.sha256(publication_files["html"]).hexdigest()
    assert pd.Timestamp(details["publication"]["periodic_update"]) == pd.Timestamp("2026-04-06")
    assert pd.Timestamp(details["publication"]["registry_updated_at"]) == pd.Timestamp("2026-09-04")
    assert details["fallback"] is None and meta.validation_warnings == details["warnings"]
    assert len(requests) == 3 and not any(request.url.path.endswith(".pdf") for request in requests)


async def test_deterministic_is_rejected_before_source_warning_or_http(
    monkeypatch, lista_suja_replay_http
):
    requests = lista_suja_replay_http()
    warning = Mock(side_effect=AssertionError("source must not be called"))
    monkeypatch.setattr(api, "warn_once", warning)
    async with deterministic("2026-09-04"):
        with levanta_exatamente(InvalidParameterError):
            await datasets.empregadores_lista_suja()
    assert not requests and not warning.called


@pytest.mark.parametrize("produto", ["soja", None, 0])
async def test_internal_registry_product_only_accepts_empty_sentinel(
    produto, lista_suja_replay_http
):
    requests = lista_suja_replay_http()
    with pytest.raises(InvalidParameterError):
        await datasets.get_dataset(NAME).fetch(produto)
    assert not requests


@pytest.mark.parametrize(
    "arguments",
    [
        {"uf": "XX"},
        {"uf": 1},
        {"id_registro": 1},
        {"id_registro": True},
        {"id_registro": ""},
        {"id_registro": " "},
        {"formato": "xlsx"},
        {"formato": None},
        {"as_polars": 1},
        {"as_polars": None},
        {"as_polars": "false"},
        {"return_meta": 1},
        {"return_meta": None},
        {"return_meta": "false"},
    ],
)
async def test_invalid_flags_and_filters_fail_before_warning_or_http(
    arguments, monkeypatch, lista_suja_replay_http
):
    requests = lista_suja_replay_http()
    warning = Mock(side_effect=AssertionError("warning must follow parameter validation"))
    monkeypatch.setattr(api, "warn_once", warning)
    with levanta_exatamente(InvalidParameterError):
        await datasets.empregadores_lista_suja(**arguments)
    assert not requests and not warning.called
