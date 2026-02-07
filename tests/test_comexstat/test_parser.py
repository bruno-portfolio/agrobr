"""Testes para o parser ComexStat."""

import pytest

from agrobr.comexstat.parser import (
    PARSER_VERSION,
    _detect_separator,
    agregar_mensal,
    parse_exportacao,
)
from agrobr.exceptions import ParseError


def _sample_csv(sep=";", ncm="12019000", rows=3):
    """Gera CSV de exportação de exemplo."""
    header = sep.join([
        "CO_ANO", "CO_MES", "CO_NCM", "CO_UNID", "CO_PAIS",
        "SG_UF_NCM", "CO_VIA", "CO_URF", "QT_ESTAT", "KG_LIQUIDO", "VL_FOB",
    ])

    lines = [header]
    for i in range(rows):
        line = sep.join([
            "2024", str((i % 12) + 1), ncm, "10", "160",
            "MT", "4", "817800", "1000", str(5000000 * (i + 1)), str(2000000 * (i + 1)),
        ])
        lines.append(line)

    return "\n".join(lines)


class TestDetectSeparator:
    def test_semicolon(self):
        csv = "CO_ANO;CO_MES;CO_NCM\n2024;1;12019000"
        assert _detect_separator(csv) == ";"

    def test_comma(self):
        csv = "CO_ANO,CO_MES,CO_NCM\n2024,1,12019000"
        assert _detect_separator(csv) == ","


class TestParseExportacao:
    def test_parse_basic(self):
        csv = _sample_csv()
        df = parse_exportacao(csv)

        assert len(df) == 3
        assert "ano" in df.columns
        assert "mes" in df.columns
        assert "ncm" in df.columns
        assert "uf" in df.columns
        assert "kg_liquido" in df.columns
        assert "valor_fob_usd" in df.columns

    def test_parse_renames_columns(self):
        csv = _sample_csv()
        df = parse_exportacao(csv)

        assert "CO_ANO" not in df.columns
        assert "ano" in df.columns

    def test_filter_by_ncm(self):
        lines = _sample_csv(ncm="12019000", rows=2)
        # Add a non-soja row
        lines += "\n" + ";".join([
            "2024", "1", "10059010", "10", "160",
            "MT", "4", "817800", "500", "3000000", "1000000",
        ])

        df = parse_exportacao(lines, ncm="12019000")
        assert len(df) == 2
        assert all(df["ncm"] == "12019000")

    def test_filter_by_uf(self):
        lines = _sample_csv(rows=2)
        # Add a PR row
        lines += "\n" + ";".join([
            "2024", "1", "12019000", "10", "160",
            "PR", "4", "817800", "500", "3000000", "1000000",
        ])

        df = parse_exportacao(lines, uf="PR")
        assert len(df) == 1
        assert df.iloc[0]["uf"] == "PR"

    def test_ncm_zero_padded(self):
        csv = "CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n"
        csv += "2024;1;9011110;10;160;MG;4;817800;100;5000;2000"

        df = parse_exportacao(csv)
        assert df.iloc[0]["ncm"] == "09011110"

    def test_parse_empty_raises(self):
        with pytest.raises(ParseError):
            parse_exportacao("")

    def test_parse_comma_separated(self):
        csv = _sample_csv(sep=",")
        df = parse_exportacao(csv)

        assert len(df) == 3

    def test_numeric_conversion(self):
        csv = _sample_csv(rows=1)
        df = parse_exportacao(csv)

        assert df.iloc[0]["kg_liquido"] == pytest.approx(5000000)
        assert df.iloc[0]["valor_fob_usd"] == pytest.approx(2000000)

    def test_sorted(self):
        csv = "CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n"
        csv += "2024;3;12019000;10;160;MT;4;817800;100;5000;2000\n"
        csv += "2024;1;12019000;10;160;MT;4;817800;100;5000;2000\n"
        csv += "2024;2;12019000;10;160;MT;4;817800;100;5000;2000"

        df = parse_exportacao(csv)
        assert df.iloc[0]["mes"] == 1
        assert df.iloc[2]["mes"] == 3


class TestAgregarMensal:
    def test_basic_aggregation(self):
        csv = "CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n"
        csv += "2024;1;12019000;10;160;MT;4;817800;100;5000000;2000000\n"
        csv += "2024;1;12019000;10;276;MT;4;817800;200;3000000;1500000"

        df = parse_exportacao(csv)
        df_mensal = agregar_mensal(df)

        assert len(df_mensal) == 1
        assert df_mensal.iloc[0]["kg_liquido"] == pytest.approx(8000000)
        assert df_mensal.iloc[0]["valor_fob_usd"] == pytest.approx(3500000)
        assert "volume_ton" in df_mensal.columns
        assert df_mensal.iloc[0]["volume_ton"] == pytest.approx(8000.0)

    def test_empty_df(self):
        import pandas as pd
        df = pd.DataFrame()
        result = agregar_mensal(df)
        assert result.empty


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1
