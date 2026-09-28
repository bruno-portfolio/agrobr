from __future__ import annotations

import hashlib
from datetime import UTC
from unittest.mock import patch

import pandas as pd
import pytest

from agrobr import contracts, lista_suja
from agrobr.exceptions import InvalidParameterError
from agrobr.lista_suja import _pdf, api


async def test_replay_csv_complete_pipeline_and_metadata(replay_http, publication_files):
    requests = replay_http()
    frame, meta = await lista_suja.empregadores(return_meta=True)
    assert len(frame) == 579
    assert len(frame.columns) == 12
    assert meta.source == "lista_suja"
    assert meta.source_method == "httpx+csv"
    assert meta.parser_version == 4
    assert meta.schema_version == meta.contract_version == "2.0"
    assert meta.selected_source == "lista_suja_csv"
    assert meta.attempted_sources == ["lista_suja_csv"]
    assert meta.raw_content_hash == hashlib.sha256(publication_files["csv"]).hexdigest()
    assert meta.raw_content_size == len(publication_files["csv"])
    assert meta.fetched_at.utcoffset() == UTC.utcoffset(None)
    assert meta.records_count == len(frame)
    assert meta.columns == frame.columns.tolist()
    assert meta.dataset == ""
    details = meta.source_details
    assert details["collection"] == "cadastro_de_empregadores"
    assert details["resource"]["sha256"] == meta.raw_content_hash
    assert details["companion"]["sha256"] == hashlib.sha256(publication_files["txt"]).hexdigest()
    assert details["discovery"]["sha256"] == hashlib.sha256(publication_files["html"]).hexdigest()
    assert all("content" not in details[name] for name in ("resource", "companion", "discovery"))
    assert details["source_rows"] == details["output_rows"] == 579
    assert details["null_counts_scope"] == "complete_source_before_filters"
    assert meta.validation_warnings == details["warnings"]
    assert pd.Timestamp(details["publication"]["periodic_update"]) == pd.Timestamp("2026-04-06")
    assert pd.Timestamp(details["publication"]["registry_updated_at"]) == pd.Timestamp("2026-09-04")
    assert not any(request.url.path.endswith(".pdf") for request in requests)
    contracts.validate_dataset(frame, "lista_suja_empregadores")


@pytest.mark.parametrize(
    "arguments,expected_ids",
    [
        ({"id_registro": "1"}, ["1"]),
        ({"id_registro": "01"}, []),
        ({"id_registro": " 1 "}, []),
        ({"id_registro": "999999"}, []),
        ({"uf": " mt ", "id_registro": "2"}, ["2"]),
        ({"uf": "SP", "id_registro": "2"}, []),
        ({"id_registro": "13"}, ["13"]),
    ],
)
async def test_replay_exact_identity_and_uf_filters(arguments, expected_ids, replay_http):
    requests = replay_http()
    frame, meta = await lista_suja.empregadores(**arguments, return_meta=True)
    assert frame["id_registro"].tolist() == expected_ids
    assert meta.records_count == len(expected_ids)
    assert meta.source_details["source_rows"] == 579
    assert meta.source_details["output_rows"] == len(expected_ids)
    assert meta.source_details["query"]["id_registro"] == arguments["id_registro"]
    if "uf" in arguments:
        assert meta.source_details["query"]["uf"] == arguments["uf"].strip().upper()
    assert not any(request.url.path.endswith(".pdf") for request in requests)
    assert str(frame["ano_acao_fiscal"].dtype) == "Int64"
    assert str(frame["data_decisao"].dtype) == "datetime64[ns]"
    contracts.validate_dataset(frame, "lista_suja_empregadores")


@pytest.mark.parametrize(
    "arguments",
    [
        {"uf": "INVALID"},
        {"uf": 1},
        {"uf": ""},
        {"id_registro": 1},
        {"id_registro": True},
        {"id_registro": ""},
        {"id_registro": " "},
        {"formato": "xlsx"},
        {"formato": None},
        {"formato": []},
        {"cpf_cnpj": "001.234.567-89"},
        {"id_regitro": "1"},
    ],
)
async def test_invalid_filters_fail_before_any_http(arguments, replay_http):
    requests = replay_http()
    with (
        patch.object(api, "warn_once") as warning,
        pytest.raises(InvalidParameterError),
    ):
        await lista_suja.empregadores(**arguments)
    assert requests == []
    warning.assert_not_called()


async def test_replay_unavailable_txt_does_not_invent_update_from_fetch_date(replay_http):
    requests = replay_http({"txt": (404, b"unavailable", "text/plain")})
    frame, meta = await lista_suja.empregadores(return_meta=True)
    assert len(frame) == 579
    assert frame["data_atualizacao"].isna().all()
    assert str(frame["data_atualizacao"].dtype) == "datetime64[ns]"
    assert (
        meta.source_details["warnings"]
        == meta.validation_warnings
        == [
            "TXT companheiro indisponível (SourceUnavailableError); contexto de edição não comprovado pelo CSV.",
            "TXT companheiro indisponível: edição e data de atualização não comprovadas.",
            "10 registros com inclusão composta: escalar nulo e texto preservado.",
        ]
    )
    assert meta.source_details["companion"] is None
    assert meta.selected_source == "lista_suja_csv"
    assert not any(request.url.path.endswith(".pdf") for request in requests)


@pytest.mark.slow
@pytest.mark.parametrize("formato", ["auto", "pdf"])
async def test_replay_pdf_route_preserves_publication_and_selected_resource(
    formato, replay_http, publication_files
):
    pytest.importorskip("pdfplumber")
    requests = replay_http({"csv": (404, b"unavailable", "text/plain")})
    frame, meta = await lista_suja.empregadores(formato=formato, id_registro="1", return_meta=True)
    assert frame["id_registro"].tolist() == ["1"]
    assert frame["data_atualizacao"].tolist() == [pd.Timestamp("2026-09-04")]
    assert meta.raw_content_hash == hashlib.sha256(publication_files["pdf"]).hexdigest()
    assert meta.source_method == "httpx+pdf"
    assert meta.selected_source == "lista_suja_pdf"
    assert meta.attempted_sources == (
        ["lista_suja_csv", "lista_suja_pdf"] if formato == "auto" else ["lista_suja_pdf"]
    )
    assert meta.source_details["companion"] is None
    if formato == "auto":
        assert meta.source_details["fallback"]["status_code"] == 404
    else:
        assert meta.source_details["fallback"] is None
        assert not any(request.url.path.endswith(".csv") for request in requests)


@pytest.mark.parametrize("return_meta", [False, True])
@pytest.mark.parametrize("id_registro", ["41", "140", "999999"])
async def test_replay_polars_preserves_nulls_compound_dates_and_empty_schema(
    return_meta, id_registro, replay_http
):
    pl = pytest.importorskip("polars")
    replay_http()
    pandas_frame = await lista_suja.empregadores(id_registro=id_registro)
    result = await lista_suja.empregadores(
        id_registro=id_registro, as_polars=True, return_meta=return_meta
    )
    frame = result[0] if return_meta else result
    assert isinstance(frame, pl.DataFrame)
    assert frame.columns == pandas_frame.columns.tolist()
    assert frame.height == len(pandas_frame)
    assert frame["id_registro"].to_list() == pandas_frame["id_registro"].tolist()
    assert frame["trabalhadores_resgatados"].dtype == pl.Int64
    assert frame["data_atualizacao"].dtype == pl.Datetime("ns")
    assert frame["data_inclusao"].null_count() == int(pandas_frame["data_inclusao"].isna().sum())
    assert frame["data_inclusao_texto"].to_list() == pandas_frame["data_inclusao_texto"].tolist()
    if return_meta:
        meta = result[1]
        assert meta.columns == frame.columns
        assert meta.records_count == frame.height
        assert meta.schema_version == "2.0"


async def test_csv_pipeline_does_not_use_pdf_dependency(replay_http):
    replay_http()
    with patch.object(
        _pdf, "pdfplumber_module", side_effect=AssertionError("PDF dependency accessed")
    ):
        frame = await lista_suja.empregadores(formato="csv")
    assert len(frame) == 579


async def test_pdf_optional_error_is_only_needed_after_csv_transport_failure(replay_http):
    replay_http({"csv": (404, b"unavailable", "text/plain")})
    with (
        patch.object(_pdf, "pdfplumber_module", side_effect=ImportError("Install agrobr[pdf]")),
        pytest.raises(ImportError, match="agrobr"),
    ):
        await lista_suja.empregadores()


async def test_existing_public_data_warning_is_preserved(replay_http):
    replay_http()
    with pytest.warns(UserWarning, match="CPF/CNPJ"):
        await lista_suja.empregadores(id_registro="1")
