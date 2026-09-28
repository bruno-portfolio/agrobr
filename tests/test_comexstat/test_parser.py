from __future__ import annotations

import csv
import io
from dataclasses import replace
from decimal import Decimal, localcontext

import pandas as pd
import pytest

from agrobr.comexstat import parser, query
from agrobr.exceptions import ParseError, ResourceLimitError


@pytest.fixture
def selected_query():
    return query.build_query(fluxo="exportacao", produto="soja", ano=2026)


@pytest.fixture
def raw_rows():
    return [
        [
            "2026",
            "01",
            "12019000",
            "10",
            "000",
            "ND",
            "00",
            "0000000",
            "9007199254740993",
            "10",
            "9007199254740993",
        ],
        ["2026", "01", "12019000", "11", "105", "ND", "01", "0817800", "2", "20", "0.1"],
    ]


@pytest.fixture
def encode():
    def build(rows, *, imports=False, header=None, encoding="utf-8"):
        columns = [
            "CO_ANO",
            "CO_MES",
            "CO_NCM",
            "CO_UNID",
            "CO_PAIS",
            "SG_UF_NCM",
            "CO_VIA",
            "CO_URF",
            "QT_ESTAT",
            "KG_LIQUIDO",
            "VL_FOB",
        ]
        if imports:
            columns += ["VL_FRETE", "VL_SEGURO"]
        output = io.StringIO(newline="")
        writer = csv.writer(output, delimiter=";", lineterminator="\n")
        writer.writerow(header if header is not None else columns)
        writer.writerows(rows)
        return io.BytesIO(output.getvalue().encode(encoding))

    return build


def test_detail_preserves_occurrences_codes_and_large_integers(selected_query, raw_rows, encode):
    rows = [raw_rows[1], raw_rows[0], raw_rows[0]]
    parsed = parser.parse_resource(encode(rows), replace(selected_query, agregacao="detalhado"))
    assert parsed.frame.shape == (3, 11)
    assert parsed.frame["cod_pais"].tolist() == ["105", "000", "000"]
    assert parsed.frame["qtd_estatistica"].tolist() == [2, 9007199254740993, 9007199254740993]
    assert str(parsed.frame["qtd_estatistica"].dtype) == "Int64"
    assert parsed.frame["ncm"].dtype.storage == "python"
    assert parsed.details["source_rows"] == parsed.details["validated_rows"] == 3
    assert parsed.details["selected_rows"] == 3
    assert parsed.details["eof_reached"] is True


def test_monthly_exact_sum_before_binary64_and_no_quantity_aggregation(
    selected_query, raw_rows, encode
):
    with localcontext() as context:
        context.prec = 2
        parsed = parser.parse_resource(encode(raw_rows), selected_query)
    assert list(parsed.frame) == [
        "ano",
        "mes",
        "ncm",
        "uf",
        "kg_liquido",
        "valor_fob_usd",
        "volume_ton",
    ]
    assert parsed.frame.iloc[0]["kg_liquido"] == 30
    assert parsed.frame.iloc[0]["valor_fob_usd"].hex() == float(Decimal("9007199254740993.1")).hex()
    assert parsed.frame.iloc[0]["volume_ton"] == 0.03
    assert (
        parsed.details["statistics"]["selected"]["valor_fob_usd"]["exact_sum"]
        == "9007199254740993.1"
    )


def test_import_freight_and_insurance_preserved_and_summed(selected_query, raw_rows, encode):
    raw_rows[0] += ["0.1", "0.2"]
    raw_rows[1] += ["0.2", "0.3"]
    parsed = parser.parse_resource(
        encode(raw_rows, imports=True), replace(selected_query, fluxo="importacao")
    )
    assert parsed.frame.shape == (1, 9)
    assert parsed.frame["valor_frete_usd"].iloc[0] == 0.3
    assert parsed.frame["valor_seguro_usd"].iloc[0] == 0.5
    detail = parser.parse_resource(
        encode(raw_rows, imports=True),
        replace(selected_query, fluxo="importacao", agregacao="detalhado"),
    )
    assert detail.frame.shape == (2, 13)


@pytest.mark.parametrize("agregacao,columns", [("mensal", 7), ("detalhado", 11)])
def test_empty_selection_is_fully_validated_and_typed(
    selected_query, raw_rows, encode, agregacao, columns
):
    parsed = parser.parse_resource(
        encode(raw_rows), replace(selected_query, agregacao=agregacao, uf="AC")
    )
    assert parsed.frame.shape == (0, columns)
    assert str(parsed.frame["kg_liquido"].dtype) == "Int64"
    assert str(parsed.frame["valor_fob_usd"].dtype) == "float64"
    assert parsed.frame["ncm"].dtype.storage == "python"
    assert parsed.details["validated_rows"] == 2


def test_year_mismatch_outside_filter_fails(selected_query, raw_rows, encode):
    raw_rows[1][0] = "2025"
    raw_rows[1][2] = "01012100"
    with pytest.raises(ParseError, match="ano publicado"):
        parser.parse_resource(encode(raw_rows), selected_query)


def test_nullable_measures_propagate_monthly(selected_query, raw_rows, encode):
    raw_rows[1][-2:] = ["", ""]
    parsed = parser.parse_resource(encode(raw_rows), selected_query)
    assert pd.isna(parsed.frame["kg_liquido"].iloc[0])
    assert pd.isna(parsed.frame["valor_fob_usd"].iloc[0])
    assert pd.isna(parsed.frame["volume_ton"].iloc[0])
    assert parsed.details["statistics"]["source"]["kg_liquido"]["nulls"] == 1


def test_signed_zero_detail_and_monthly(selected_query, raw_rows, encode):
    for row in raw_rows:
        row[-1] = "-0.00"
    for aggregation in ("mensal", "detalhado"):
        frame = parser.parse_resource(
            encode(raw_rows), replace(selected_query, agregacao=aggregation)
        ).frame
        assert all(value.hex() == "-0x0.0p+0" for value in frame["valor_fob_usd"])


def test_int64_month_sum_overflow_fails(selected_query, raw_rows, encode):
    raw_rows[0][-2] = "9223372036854775807"
    with pytest.raises(ParseError, match="soma excede"):
        parser.parse_resource(encode(raw_rows), selected_query)


def test_money_month_sum_overflow_fails(selected_query, raw_rows, encode):
    for row in raw_rows:
        row[-1] = "1e308"
    with pytest.raises(ParseError, match="float64"):
        parser.parse_resource(encode(raw_rows), selected_query)


def test_limit_counts_selected_occurrences_before_grouping(selected_query, raw_rows, encode):
    with pytest.raises(ResourceLimitError, match="max_linhas"):
        parser.parse_resource(encode(raw_rows), replace(selected_query, max_linhas=1))
    result = parser.parse_resource(encode(raw_rows), replace(selected_query, max_linhas=None))
    assert len(result.frame) == 1


def test_memory_limit_does_not_increase(selected_query, raw_rows, encode):
    with pytest.raises(ResourceLimitError, match="max_memoria_bytes"):
        parser.parse_resource(encode(raw_rows), replace(selected_query, max_memoria_bytes=1))


def test_bad_row_width_and_unterminated_quote_fail(selected_query, raw_rows, encode):
    with pytest.raises(ParseError, match="largura"):
        parser.parse_resource(encode([raw_rows[0][:-1]]), selected_query)
    with pytest.raises(ParseError, match="CSV inválido"):
        parser.parse_resource(io.BytesIO(encode([]).getvalue() + b'"unterminated'), selected_query)
