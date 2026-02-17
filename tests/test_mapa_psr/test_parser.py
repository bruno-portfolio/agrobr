"""Testes para agrobr.alt.mapa_psr.parser."""

from __future__ import annotations

import pytest

from agrobr.alt.mapa_psr.parser import (
    PARSER_VERSION,
    _detect_encoding,
    _detect_separator,
    _parse_numeric,
    parse_apolices,
    parse_sinistros,
)
from agrobr.exceptions import ParseError


def _make_csv(
    rows: list[dict[str, str]] | None = None,
    sep: str = ";",
    encoding: str = "utf-8",
    include_pii: bool = False,
    include_geo: bool = False,
) -> bytes:
    """Gera CSV sintetico de apolices PSR."""
    if rows is None:
        rows = [
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP001",
                "SG_UF_PROPRIEDADE": "MT",
                "NM_MUNICIPIO_PROPRIEDADE": "SORRISO",
                "CD_GEOCMU": "5107925",
                "NM_CULTURA_GLOBAL": "SOJA",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "500.5",
                "VL_PREMIO_LIQUIDO": "15000.00",
                "VL_SUBVENCAO_FEDERAL": "6000.00",
                "VL_LIMITE_GARANTIA": "250000.00",
                "VALOR_INDENIZACAO": "120000.00",
                "EVENTO_PREPONDERANTE": "SECA",
                "NR_PRODUTIVIDADE_ESTIMADA": "60.0",
                "NR_PRODUTIVIDADE_SEGURADA": "48.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "7.5",
                "NM_RAZAO_SOCIAL": "Seguradora ABC",
            },
            {
                "ANO_APOLICE": "2023",
                "NR_APOLICE": "AP002",
                "SG_UF_PROPRIEDADE": "PR",
                "NM_MUNICIPIO_PROPRIEDADE": "LONDRINA",
                "CD_GEOCMU": "4113700",
                "NM_CULTURA_GLOBAL": "MILHO",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "200.0",
                "VL_PREMIO_LIQUIDO": "8000.00",
                "VL_SUBVENCAO_FEDERAL": "3200.00",
                "VL_LIMITE_GARANTIA": "100000.00",
                "VALOR_INDENIZACAO": "0",
                "EVENTO_PREPONDERANTE": "",
                "NR_PRODUTIVIDADE_ESTIMADA": "120.0",
                "NR_PRODUTIVIDADE_SEGURADA": "96.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "6.0",
                "NM_RAZAO_SOCIAL": "Seguradora XYZ",
            },
            {
                "ANO_APOLICE": "2022",
                "NR_APOLICE": "AP003",
                "SG_UF_PROPRIEDADE": "GO",
                "NM_MUNICIPIO_PROPRIEDADE": "RIO VERDE",
                "CD_GEOCMU": "5218805",
                "NM_CULTURA_GLOBAL": "SOJA",
                "NM_CLASSIF_PRODUTO": "AGRICOLA",
                "NR_AREA_TOTAL": "1000.0",
                "VL_PREMIO_LIQUIDO": "30000.00",
                "VL_SUBVENCAO_FEDERAL": "12000.00",
                "VL_LIMITE_GARANTIA": "500000.00",
                "VALOR_INDENIZACAO": "350000.00",
                "EVENTO_PREPONDERANTE": "GEADA",
                "NR_PRODUTIVIDADE_ESTIMADA": "55.0",
                "NR_PRODUTIVIDADE_SEGURADA": "44.0",
                "NivelDeCobertura": "80",
                "PE_TAXA": "8.0",
                "NM_RAZAO_SOCIAL": "Seguradora ABC",
            },
        ]

    headers = list(rows[0].keys())
    if include_pii:
        headers.extend(["NM_SEGURADO", "NR_DOCUMENTO_SEGURADO"])
    if include_geo:
        headers.extend(["LATITUDE", "LONGITUDE", "NR_GRAU_LAT"])

    lines = [sep.join(headers)]
    for row in rows:
        vals = [row.get(h, "") for h in list(rows[0].keys())]
        if include_pii:
            vals.extend(["Joao Silva", "12345678900"])
        if include_geo:
            vals.extend(["-12.5", "-55.7", "12"])
        lines.append(sep.join(vals))

    return "\n".join(lines).encode(encoding)


class TestDetectEncoding:
    def test_utf8(self):
        content = b"ANO;UF\n2023;MT\n"
        assert _detect_encoding(content) == "utf-8"

    def test_iso_8859_1(self):
        content = "ANO;MUNICÍPIO\n2023;LONDRINA\n".encode("iso-8859-1")
        enc = _detect_encoding(content)
        assert enc in ("utf-8", "iso-8859-1")

    def test_utf8_bom(self):
        content = b"\xef\xbb\xbfANO;UF\n2023;MT\n"
        enc = _detect_encoding(content)
        assert enc in ("utf-8", "utf-8-sig")


class TestDetectSeparator:
    def test_semicolon(self):
        assert _detect_separator("A;B;C\n1;2;3") == ";"

    def test_comma(self):
        assert _detect_separator("A,B,C\n1,2,3") == ","

    def test_mais_semicolons(self):
        assert _detect_separator("A;B,C;D;E\n") == ";"


class TestParseNumeric:
    def test_inteiro(self):
        assert _parse_numeric("1234") == 1234.0

    def test_decimal_ponto(self):
        assert _parse_numeric("1234.56") == 1234.56

    def test_decimal_virgula(self):
        assert _parse_numeric("1234,56") == 1234.56

    def test_milhar_virgula_decimal(self):
        assert _parse_numeric("1.234,56") == 1234.56

    def test_vazio(self):
        assert _parse_numeric("") is None

    def test_traco(self):
        assert _parse_numeric("-") is None

    def test_none(self):
        assert _parse_numeric(None) is None

    def test_float_passthrough(self):
        assert _parse_numeric(42.5) == 42.5

    def test_invalido(self):
        assert _parse_numeric("abc") is None


class TestParseApolices:
    def test_basico(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        assert len(df) == 3
        assert "ano_apolice" in df.columns
        assert "uf" in df.columns
        assert "cultura" in df.columns

    def test_separador_virgula(self):
        csv_bytes = _make_csv(sep=",")
        df = parse_apolices(csv_bytes)
        assert len(df) == 3

    def test_encoding_iso(self):
        csv_bytes = _make_csv(encoding="iso-8859-1")
        df = parse_apolices(csv_bytes)
        assert len(df) == 3

    def test_pii_removido(self):
        csv_bytes = _make_csv(include_pii=True)
        df = parse_apolices(csv_bytes)
        assert "NM_SEGURADO" not in df.columns
        assert "NR_DOCUMENTO_SEGURADO" not in df.columns
        assert "nm_segurado" not in df.columns

    def test_geo_removido(self):
        csv_bytes = _make_csv(include_geo=True)
        df = parse_apolices(csv_bytes)
        assert "LATITUDE" not in df.columns
        assert "LONGITUDE" not in df.columns

    def test_tipos_corretos(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        assert df["ano_apolice"].dtype in ("int64", "int32")
        assert df["valor_premio"].dtype == "float64"
        assert df["valor_indenizacao"].dtype == "float64"

    def test_uf_uppercase(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        assert all(v == v.upper() for v in df["uf"].dropna())

    def test_cultura_uppercase(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        assert all(v == v.upper() for v in df["cultura"].dropna())

    def test_sorted_por_ano(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        anos = df["ano_apolice"].tolist()
        assert anos == sorted(anos)

    def test_colunas_output_corretas(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        from agrobr.alt.mapa_psr.models import COLUNAS_APOLICES

        for col in df.columns:
            assert col in COLUNAS_APOLICES

    def test_filtro_uf(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes, uf="MT")
        assert len(df) == 1
        assert df.iloc[0]["uf"] == "MT"

    def test_filtro_cultura(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes, cultura="SOJA")
        assert len(df) == 2
        assert all(df["cultura"] == "SOJA")

    def test_filtro_ano(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes, ano=2022)
        assert len(df) == 1
        assert df.iloc[0]["ano_apolice"] == 2022

    def test_filtro_municipio(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes, municipio="SORRISO")
        assert len(df) == 1

    def test_csv_vazio_raise(self):
        csv_bytes = b""
        with pytest.raises(ParseError):
            parse_apolices(csv_bytes)

    def test_colunas_faltantes_raise(self):
        csv_bytes = b"COLUNA_X;COLUNA_Y\nabc;def\n"
        with pytest.raises(ParseError):
            parse_apolices(csv_bytes)

    def test_valores_monetarios_float64(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        for col in ("valor_premio", "valor_subvencao", "valor_limite_garantia"):
            if col in df.columns:
                assert df[col].dtype == "float64"

    def test_area_float(self):
        csv_bytes = _make_csv()
        df = parse_apolices(csv_bytes)
        assert df["area_total"].dtype == "float64"


class TestParseSinistros:
    def test_basico(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes)
        assert len(df) == 2
        assert all(df["valor_indenizacao"] > 0)

    def test_filtra_indenizacao_zero(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes)
        assert not any(df["valor_indenizacao"] == 0)

    def test_filtra_evento_vazio(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes)
        assert all(df["evento"].str.strip() != "")

    def test_evento_normalizado_lowercase(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes)
        assert all(v == v.lower() for v in df["evento"])

    def test_filtro_evento(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes, evento="seca")
        assert len(df) == 1
        assert "seca" in df.iloc[0]["evento"]

    def test_filtro_cultura(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes, cultura="SOJA")
        assert len(df) == 2

    def test_filtro_uf(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes, uf="MT")
        assert len(df) == 1

    def test_colunas_sinistros(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes)
        from agrobr.alt.mapa_psr.models import COLUNAS_SINISTROS

        for col in df.columns:
            assert col in COLUNAS_SINISTROS

    def test_sem_taxa_em_sinistros(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes)
        assert "taxa" not in df.columns

    def test_sorted_por_ano(self):
        csv_bytes = _make_csv()
        df = parse_sinistros(csv_bytes)
        anos = df["ano_apolice"].tolist()
        assert anos == sorted(anos)


class TestParserVersion:
    def test_parser_version_definido(self):
        assert PARSER_VERSION >= 1
