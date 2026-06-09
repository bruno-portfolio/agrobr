"""Testes para agrobr.alt.sicar.parser."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from agrobr.alt.sicar.models import COLUNAS_IMOVEIS, COLUNAS_IMOVEIS_GEO, MAX_FEATURES_GEO
from agrobr.alt.sicar.parser import PARSER_VERSION, agregar_resumo, parse_imoveis_csv
from agrobr.exceptions import ParseError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "sicar"

SAMPLE_CSV = (
    b"FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,"
    b"area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
    b"sicar.1,DF-001,AT,2014-06-15T10:30:00Z,2023-01-10T14:20:00Z,"
    b"120.5,,DF,BRASILIA,5300108,5.0,IRU\n"
    b"sicar.2,DF-002,PE,2018-03-22T08:15:00Z,,"
    b"45.2,Inscricao,DF,BRASILIA,5300108,1.8,IRU\n"
    b"sicar.3,DF-003,SU,2016-08-10T12:00:00Z,2021-12-20T11:30:00Z,"
    b"80.3,,DF,BRASILIA,5300108,3.2,AST\n"
)


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1


class TestParseImoveisCsv:
    def test_basic_parse(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert len(df) == 3
        assert list(df.columns) == COLUNAS_IMOVEIS

    def test_empty_pages(self):
        df = parse_imoveis_csv([])
        assert len(df) == 0
        assert list(df.columns) == COLUNAS_IMOVEIS

    def test_column_rename(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert "status" in df.columns
        assert "status_imovel" not in df.columns
        assert "area_ha" in df.columns
        assert "area" not in df.columns
        assert "modulos_fiscais" in df.columns
        assert "m_fiscal" not in df.columns
        assert "tipo" in df.columns
        assert "tipo_imovel" not in df.columns
        assert "data_criacao" in df.columns
        assert "dat_criacao" not in df.columns

    def test_fid_not_in_output(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert "FID" not in df.columns

    def test_area_is_float(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert df["area_ha"].dtype == "float64"
        assert df["area_ha"].iloc[0] == pytest.approx(120.5)

    def test_cod_municipio_ibge_is_int(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert df["cod_municipio_ibge"].dtype == "Int64"
        assert df["cod_municipio_ibge"].iloc[0] == 5300108

    def test_modulos_fiscais_is_float(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert df["modulos_fiscais"].dtype == "float64"
        assert df["modulos_fiscais"].iloc[0] == pytest.approx(5.0)

    def test_data_criacao_is_datetime(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert pd.api.types.is_datetime64_any_dtype(df["data_criacao"])

    def test_data_atualizacao_nullable(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert pd.api.types.is_datetime64_any_dtype(df["data_atualizacao"])
        # Second row has no data_atualizacao
        assert pd.isna(df["data_atualizacao"].iloc[1])

    def test_status_uppercase(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert all(s == s.upper() for s in df["status"])

    def test_uf_uppercase(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert all(u == u.upper() for u in df["uf"])

    def test_tipo_uppercase(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert all(t == t.upper() for t in df["tipo"] if t)

    def test_condicao_preserved(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        # Row 2 has "Inscricao"
        assert df["condicao"].iloc[1] == "Inscricao"

    def test_multi_page_concat(self):
        page1 = (
            b"FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,"
            b"area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
            b"s.1,DF-001,AT,2014-01-01T00:00:00Z,,100.0,,DF,BRASILIA,5300108,4.0,IRU\n"
        )
        page2 = (
            b"FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,"
            b"area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
            b"s.2,DF-002,PE,2015-01-01T00:00:00Z,,50.0,,DF,BRASILIA,5300108,2.0,IRU\n"
        )
        df = parse_imoveis_csv([page1, page2])
        assert len(df) == 2

    def test_empty_csv_pages(self):
        empty_csv = b"FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
        df = parse_imoveis_csv([empty_csv])
        assert len(df) == 0

    def test_missing_required_columns_raises(self):
        bad_csv = b"cod_imovel,other_col\nFOO,BAR\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias"):
            parse_imoveis_csv([bad_csv])

    def test_invalid_csv_raises(self):
        # Binary data that can't produce required columns
        bad_csv = b"colA,colB\nfoo,bar\n"
        with pytest.raises(ParseError, match="Colunas obrigatorias"):
            parse_imoveis_csv([bad_csv])

    def test_latin1_encoding_fallback(self):
        latin1_csv = (
            "FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,"
            "area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
            "s.1,DF-001,AT,2014-01-01T00:00:00Z,,100.0,,DF,BRAS\xcdLIA,5300108,4.0,IRU\n"
        ).encode("latin-1")
        df = parse_imoveis_csv([latin1_csv])
        assert len(df) == 1

    def test_index_reset(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert list(df.index) == [0, 1, 2]

    def test_cod_imovel_is_string(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        assert pd.api.types.is_string_dtype(df["cod_imovel"])


class TestParseGoldenData:
    def _load_golden(self, name: str) -> bytes:
        return (GOLDEN_DIR / name / "response.csv").read_bytes()

    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    def test_golden_df_sample(self):
        data = self._load_golden("imoveis_df_sample")
        df = parse_imoveis_csv([data])
        assert len(df) == 10
        assert set(df["status"].unique()) == {"AT", "PE"}
        assert set(df["tipo"].unique()) == {"IRU"}
        assert (df["uf"] == "DF").all()
        assert (df["area_ha"] > 0).all()

    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_mt_municipio" / "response.csv").exists(),
        reason="No golden data",
    )
    def test_golden_mt_municipio(self):
        data = self._load_golden("imoveis_mt_municipio")
        df = parse_imoveis_csv([data])
        assert len(df) == 10
        assert (df["municipio"] == "Sorriso").all()
        assert (df["uf"] == "MT").all()


class TestAgregarResumo:
    def test_basic_aggregation(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        resumo = agregar_resumo(df)
        assert len(resumo) == 1
        assert resumo["total"].iloc[0] == 3
        assert resumo["ativos"].iloc[0] == 1
        assert resumo["pendentes"].iloc[0] == 1
        assert resumo["suspensos"].iloc[0] == 1
        assert resumo["cancelados"].iloc[0] == 0

    def test_area_stats(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        resumo = agregar_resumo(df)
        assert resumo["area_total_ha"].iloc[0] == pytest.approx(120.5 + 45.2 + 80.3)
        assert resumo["area_media_ha"].iloc[0] == pytest.approx((120.5 + 45.2 + 80.3) / 3)

    def test_modulos_fiscais_medio(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        resumo = agregar_resumo(df)
        expected = (5.0 + 1.8 + 3.2) / 3
        assert resumo["modulos_fiscais_medio"].iloc[0] == pytest.approx(expected)

    def test_por_tipo(self):
        df = parse_imoveis_csv([SAMPLE_CSV])
        resumo = agregar_resumo(df)
        assert resumo["por_tipo_IRU"].iloc[0] == 2
        assert resumo["por_tipo_AST"].iloc[0] == 1
        assert resumo["por_tipo_PCT"].iloc[0] == 0

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=COLUNAS_IMOVEIS)
        resumo = agregar_resumo(df)
        assert len(resumo) == 1
        assert resumo["total"].iloc[0] == 0
        assert resumo["ativos"].iloc[0] == 0
        assert resumo["area_total_ha"].iloc[0] == 0.0

    def test_all_same_status(self):
        csv = (
            b"FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,"
            b"area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
            b"s.1,DF-001,AT,2014-01-01T00:00:00Z,,100.0,,DF,BRASILIA,5300108,4.0,IRU\n"
            b"s.2,DF-002,AT,2015-01-01T00:00:00Z,,200.0,,DF,BRASILIA,5300108,8.0,IRU\n"
        )
        df = parse_imoveis_csv([csv])
        resumo = agregar_resumo(df)
        assert resumo["ativos"].iloc[0] == 2
        assert resumo["pendentes"].iloc[0] == 0


GOLDEN_GEO_DIR = GOLDEN_DIR / "imoveis_geo_sample"


class TestParseImoveisGeojson:
    gpd = pytest.importorskip("geopandas")

    @pytest.fixture()
    def golden_geojson(self) -> list[bytes]:
        return [(GOLDEN_GEO_DIR / "response.geojson").read_bytes()]

    @pytest.fixture()
    def parse(self):
        from agrobr.alt.sicar.parser import parse_imoveis_geojson

        return parse_imoveis_geojson

    def test_valid_geojson(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert len(gdf) == 10
        assert "cod_imovel" in gdf.columns
        assert "status" in gdf.columns
        assert "geometry" in gdf.columns

    def test_output_columns_match_schema(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert list(gdf.columns) == COLUNAS_IMOVEIS_GEO

    def test_geometry_column_exists(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert gdf.geometry.name == "geometry"

    def test_geometry_is_valid(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert gdf.geometry.is_valid.all()

    def test_crs_is_4326(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert gdf.crs.to_epsg() == 4326

    def test_area_non_negative(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert (gdf["area_ha"] >= 0).all()

    def test_data_types(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert gdf["area_ha"].dtype == "float64"
        assert gdf["cod_municipio_ibge"].dtype == "Int64"

    def test_status_uppercase(self, parse, golden_geojson):
        gdf = parse(golden_geojson)
        assert all(s == s.upper() for s in gdf["status"])

    def test_empty_features_returns_empty_gdf(self, parse):
        empty = b'{"type":"FeatureCollection","features":[]}'
        gdf = parse([empty])
        assert len(gdf) == 0
        assert list(gdf.columns) == COLUNAS_IMOVEIS_GEO

    def test_invalid_json_raises(self, parse):
        with pytest.raises(ParseError, match="GeoJSON"):
            parse([b"not json at all {{{"])

    def test_geopandas_not_installed(self):
        from unittest.mock import patch

        from agrobr.alt.sicar import parser as _parser

        with (
            patch.object(_parser, "check_geopandas", side_effect=ImportError("no geopandas")),
            pytest.raises(ImportError, match="geopandas"),
        ):
            _parser.parse_imoveis_geojson([b'{"type":"FeatureCollection","features":[]}'])

    def test_truncation_warning(self, parse, golden_geojson):
        import json
        from unittest.mock import patch

        geojson = json.loads(golden_geojson[0])
        while len(geojson["features"]) < MAX_FEATURES_GEO:
            geojson["features"].extend(geojson["features"])
        geojson["features"] = geojson["features"][:MAX_FEATURES_GEO]
        data = json.dumps(geojson).encode()

        with patch("agrobr.utils.geo.logger") as mock_logger:
            parse([data])
            mock_logger.warning.assert_called_once()
            assert mock_logger.warning.call_args[0][0] == "sicar_geo_truncated"


class TestParseImoveisGeojsonPagination:
    gpd = pytest.importorskip("geopandas")

    @pytest.fixture()
    def split_pages(self) -> list[bytes]:
        import json

        geojson = json.loads((GOLDEN_GEO_DIR / "response.geojson").read_bytes())
        features = geojson["features"]
        assert len(features) == 10

        def page(feats: list[dict[str, object]]) -> bytes:
            return json.dumps({"type": "FeatureCollection", "features": feats}).encode()

        return [page(features[:6]), page(features[6:])]

    @pytest.fixture()
    def parse(self):
        from agrobr.alt.sicar.parser import parse_imoveis_geojson

        return parse_imoveis_geojson

    def test_empty_pages_list_returns_empty_gdf(self, parse):
        gdf = parse([])
        assert len(gdf) == 0
        assert list(gdf.columns) == COLUNAS_IMOVEIS_GEO
        assert gdf.geometry.name == "geometry"

    def test_concat_multi_page_record_count(self, parse, split_pages):
        gdf = parse(split_pages)
        assert len(gdf) == 10

    def test_concat_multi_page_preserves_crs(self, parse, split_pages):
        gdf = parse(split_pages)
        assert gdf.crs is not None
        assert gdf.crs.to_epsg() == 4326

    def test_concat_multi_page_preserves_columns(self, parse, split_pages):
        gdf = parse(split_pages)
        assert list(gdf.columns) == COLUNAS_IMOVEIS_GEO

    def test_concat_skips_empty_pages(self, parse):
        empty = b'{"type":"FeatureCollection","features":[]}'
        full = (GOLDEN_GEO_DIR / "response.geojson").read_bytes()
        gdf = parse([empty, full, empty])
        assert len(gdf) == 10

    def test_all_pages_empty_returns_empty_gdf(self, parse):
        empty = b'{"type":"FeatureCollection","features":[]}'
        gdf = parse([empty, empty])
        assert len(gdf) == 0
        assert list(gdf.columns) == COLUNAS_IMOVEIS_GEO

    def test_multi_page_no_truncation_warning_per_page(self, parse, split_pages):
        from unittest.mock import patch

        with patch("agrobr.utils.geo.logger") as mock_logger:
            parse(split_pages, max_features=1)
            mock_logger.warning.assert_not_called()
