from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import numpy as np
import pytest

from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.utils.geo import (
    fetch_arcgis_count,
    fetch_arcgis_layer,
    fetch_wfs,
    parse_wfs_hits,
    validate_bbox,
)
from tests import helpers


class TestParseWfsHits:
    def test_quoted(self):
        content = b'<wfs:FeatureCollection numberMatched="42" numberReturned="0"/>'
        assert parse_wfs_hits(content, source="test") == 42

    def test_unquoted(self):
        content = b"<wfs:FeatureCollection numberMatched=100 numberReturned=0/>"
        assert parse_wfs_hits(content, source="test") == 100

    def test_missing_raises(self):
        content = b"<wfs:FeatureCollection/>"
        with pytest.raises(ParseError, match="numberMatched"):
            parse_wfs_hits(content, source="test")


class TestBuildWfsUrl:
    def test_v2_uses_type_names_and_count(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url("http://base", "ns", "layer", "2.0.0", ["col1"], max_features=100)
        assert "typeNames=ns:layer" in url
        assert "count=100" in url
        assert "typeName=" not in url.split("typeNames")[0]
        assert "maxFeatures=" not in url

    def test_v1_uses_type_name_and_max_features(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url("http://base", "ns", "layer", "1.0.0", ["col1"], max_features=100)
        assert "typeName=ns:layer" in url
        assert "maxFeatures=100" in url
        assert "typeNames=" not in url
        assert "count=" not in url

    def test_v1_1_uses_type_name(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url("http://base", "ns", "layer", "1.1.0", ["col1"], max_features=50)
        assert "typeName=ns:layer" in url
        assert "maxFeatures=50" in url

    def test_output_format_csv(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url("http://base", "ns", "layer", "2.0.0", ["c1"], max_features=10)
        assert "outputFormat=csv" in url

    def test_output_format_json_quoted(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url(
            "http://base",
            "ns",
            "layer",
            "2.0.0",
            ["c1"],
            max_features=10,
            output_format="application/json",
        )
        assert "outputFormat=application" in url

    def test_bbox(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url(
            "http://base",
            "ns",
            "layer",
            "2.0.0",
            ["c1"],
            max_features=10,
            bbox=(-60.0, -15.0, -50.0, -10.0),
        )
        assert "BBOX=-60.0,-15.0,-50.0,-10.0,EPSG:4674" in url

    def test_bbox_none(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url("http://base", "ns", "layer", "2.0.0", ["c1"], max_features=10)
        assert "BBOX" not in url

    def test_cql_filter(self):
        from urllib.parse import unquote

        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url(
            "http://base",
            "ns",
            "layer",
            "2.0.0",
            ["c1"],
            max_features=10,
            cql_filter="uf='MT'",
        )
        assert "CQL_FILTER=" in url
        assert "uf='MT'" in unquote(url)

    def test_start_index(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url(
            "http://base",
            "ns",
            "layer",
            "2.0.0",
            ["c1"],
            max_features=10,
            start_index=5000,
        )
        assert "startIndex=5000" in url

    def test_start_index_none_not_in_url(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url("http://base", "ns", "layer", "2.0.0", ["c1"], max_features=10)
        assert "startIndex" not in url

    def test_result_type(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url(
            "http://base",
            "ns",
            "layer",
            "2.0.0",
            ["c1"],
            max_features=10,
            result_type="hits",
        )
        assert "resultType=hits" in url

    def test_property_names_joined(self):
        from agrobr.utils.geo import build_wfs_url

        url = build_wfs_url(
            "http://base",
            "ns",
            "layer",
            "2.0.0",
            ["col1", "col2", "col3"],
            max_features=10,
        )
        assert "propertyName=col1,col2,col3" in url


class TestBuildArcgisQueryUrl:
    def test_basic_url(self):
        from agrobr.utils.geo import build_arcgis_query_url

        url = build_arcgis_query_url("http://server/FeatureServer/0")
        assert "http://server/FeatureServer/0/query?" in url
        assert "where=1%3D1" in url or "where=1=1" in url
        assert "outSR=4326" in url

    def test_bbox(self):
        from agrobr.utils.geo import build_arcgis_query_url

        url = build_arcgis_query_url(
            "http://server/0",
            bbox=(-60.0, -15.0, -50.0, -10.0),
        )
        assert "geometry=-60.0" in url
        assert "geometryType=esriGeometryEnvelope" in url
        assert "spatialRel=esriSpatialRelIntersects" in url

    def test_count_only(self):
        from agrobr.utils.geo import build_arcgis_query_url

        url = build_arcgis_query_url("http://server/0", return_count_only=True, f="json")
        assert "returnCountOnly=true" in url
        assert "f=json" in url

    def test_pagination(self):
        from agrobr.utils.geo import build_arcgis_query_url

        url = build_arcgis_query_url(
            "http://server/0",
            result_record_count=1000,
            result_offset=5000,
        )
        assert "resultRecordCount=1000" in url
        assert "resultOffset=5000" in url

    def test_no_bbox_no_geometry_param(self):
        from agrobr.utils.geo import build_arcgis_query_url

        url = build_arcgis_query_url("http://server/0")
        assert "geometry=" not in url
        assert "geometryType" not in url

    def test_custom_where(self):
        from agrobr.utils.geo import build_arcgis_query_url

        url = build_arcgis_query_url("http://server/0", where="UF='MT'")
        assert "UF" in url


class TestFetchWfsHtmlGuard:
    def _mock_resp(self, status: int, body: bytes):
        import httpx

        req = httpx.Request("GET", "http://example.com/wfs")
        return httpx.Response(status, content=body, request=req)

    @pytest.mark.asyncio
    async def test_html_response_raises_source_unavailable(self):
        import httpx

        from agrobr.exceptions import SourceUnavailableError
        from agrobr.utils.geo import fetch_wfs

        resp = self._mock_resp(200, b"<!DOCTYPE html><html><body>maintenance</body></html>")

        with patch.object(httpx.AsyncClient, "get", return_value=resp):
            client = httpx.AsyncClient(timeout=httpx.Timeout(10))
            with pytest.raises(SourceUnavailableError, match="HTML"):
                await fetch_wfs(
                    "http://example.com/wfs?service=WFS",
                    source="test",
                    timeout=httpx.Timeout(10),
                    client=client,
                )

    @pytest.mark.asyncio
    async def test_valid_wfs_response_passes(self):
        import httpx

        from agrobr.utils.geo import fetch_wfs

        wfs_body = b'<?xml version="1.0"?>' + b"x" * 100
        resp = self._mock_resp(200, wfs_body)

        with patch.object(httpx.AsyncClient, "get", return_value=resp):
            client = httpx.AsyncClient(timeout=httpx.Timeout(10))
            content = await fetch_wfs(
                "http://example.com/wfs?service=WFS",
                source="test",
                timeout=httpx.Timeout(10),
                client=client,
            )
            assert content == wfs_body

    @pytest.mark.asyncio
    async def test_service_exception_raises_with_message(self):
        import httpx

        from agrobr.exceptions import SourceUnavailableError
        from agrobr.utils.geo import fetch_wfs

        body = (
            b'<?xml version="1.0" ?>\n<ServiceExceptionReport>\n'
            b"   <ServiceException>\n      bbox and cql_filter both specified"
            b" but are mutually exclusive\n   </ServiceException>\n"
            b"</ServiceExceptionReport>"
        )
        resp = self._mock_resp(200, body)

        with patch.object(httpx.AsyncClient, "get", return_value=resp):
            client = httpx.AsyncClient(timeout=httpx.Timeout(10))
            with pytest.raises(SourceUnavailableError, match="bbox and cql_filter"):
                await fetch_wfs(
                    "http://example.com/wfs?service=WFS",
                    source="test",
                    timeout=httpx.Timeout(10),
                    client=client,
                )

    @pytest.mark.asyncio
    async def test_ows_exception_raises_with_message(self):
        import httpx

        from agrobr.exceptions import SourceUnavailableError
        from agrobr.utils.geo import fetch_wfs

        body = (
            b'<?xml version="1.0"?><ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">'
            b"<ows:Exception><ows:ExceptionText>Layer not found</ows:ExceptionText>"
            b"</ows:Exception></ows:ExceptionReport>"
        )
        resp = self._mock_resp(200, body)

        with patch.object(httpx.AsyncClient, "get", return_value=resp):
            client = httpx.AsyncClient(timeout=httpx.Timeout(10))
            with pytest.raises(SourceUnavailableError, match="Layer not found"):
                await fetch_wfs(
                    "http://example.com/wfs?service=WFS",
                    source="test",
                    timeout=httpx.Timeout(10),
                    client=client,
                )

    @pytest.mark.asyncio
    async def test_arcgis_error_json_raises_with_message(self):
        body = b'{"error":{"code":500,"message":"MapServer not started","details":[]}}'
        resp = self._mock_resp(200, body)

        with patch.object(httpx.AsyncClient, "get", return_value=resp):
            client = httpx.AsyncClient(timeout=httpx.Timeout(10))
            with pytest.raises(SourceUnavailableError, match="not started"):
                await fetch_wfs(
                    "http://example.com/FeatureServer/0/query",
                    source="test",
                    timeout=httpx.Timeout(10),
                    client=client,
                )


class TestFetchArcgisCount:
    @pytest.mark.asyncio
    async def test_count_normal(self):
        request = httpx.Request("GET", "http://example.com/FeatureServer/0/query")
        response = httpx.Response(200, json={"count": 42}, request=request)

        with patch.object(
            httpx.AsyncClient,
            "get",
            new_callable=AsyncMock,
            return_value=response,
        ):
            count = await fetch_arcgis_count(
                "http://example.com/FeatureServer/0",
                source="test",
                timeout=httpx.Timeout(10),
            )

        assert count == 42

    @pytest.mark.asyncio
    async def test_error_payload_raises_source_unavailable(self):
        request = httpx.Request("GET", "http://example.com/FeatureServer/0/query")
        response = httpx.Response(
            200,
            json={
                "error": {
                    "code": 500,
                    "message": "Service MapServer not started",
                    "details": [],
                }
            },
            request=request,
        )

        with (
            patch.object(
                httpx.AsyncClient,
                "get",
                new_callable=AsyncMock,
                return_value=response,
            ),
            pytest.raises(SourceUnavailableError, match="not started"),
        ):
            await fetch_arcgis_count(
                "http://example.com/FeatureServer/0",
                source="test",
                timeout=httpx.Timeout(10),
            )

    @pytest.mark.asyncio
    async def test_non_json_payload_raises_source_unavailable(self):
        request = httpx.Request("GET", "http://example.com/FeatureServer/0/query")
        response = httpx.Response(200, text="not json", request=request)

        with (
            patch.object(
                httpx.AsyncClient,
                "get",
                new_callable=AsyncMock,
                return_value=response,
            ),
            pytest.raises(SourceUnavailableError, match="Resposta não é JSON"),
        ):
            await fetch_arcgis_count(
                "http://example.com/FeatureServer/0",
                source="test",
                timeout=httpx.Timeout(10),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("payload", [{}, {"count": "3"}, {"count": None}, {"count": -1}])
    async def test_resposta_sem_count_inteiro_nao_vira_zero(self, payload):
        request = httpx.Request("GET", "http://example.com/FeatureServer/0/query")
        response = httpx.Response(200, json=payload, request=request)

        with (
            patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=response),
            pytest.raises(ParseError, match="sem 'count' inteiro"),
        ):
            await fetch_arcgis_count(
                "http://example.com/FeatureServer/0",
                source="test",
                timeout=httpx.Timeout(10),
            )

    @pytest.mark.asyncio
    async def test_count_zero_explicito_e_vazio_valido(self):
        request = httpx.Request("GET", "http://example.com/FeatureServer/0/query")
        response = httpx.Response(200, json={"count": 0}, request=request)

        with patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock, return_value=response):
            count = await fetch_arcgis_count(
                "http://example.com/FeatureServer/0",
                source="test",
                timeout=httpx.Timeout(10),
            )

        assert count == 0


class TestFetchArcgisLayerMaxFeatures:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("max_registros", [0, -1, True, 1.5])
    async def test_max_registros_invalido_recusado_antes_da_rede(self, max_registros):
        layer = {
            "service_path": "x/FeatureServer/0",
            "max_record_count": 1000,
            "fields": "*",
            "rename_map": {},
            "colunas_saida": [],
            "required_cols": set(),
        }
        with (
            patch.object(httpx.AsyncClient, "get", new_callable=AsyncMock) as get,
            pytest.raises(InvalidParameterError, match="max_registros deve ser inteiro positivo"),
        ):
            await fetch_arcgis_layer(
                "http://example.com",
                layer,
                source="test",
                timeout=httpx.Timeout(10),
                max_registros=max_registros,
            )
        get.assert_not_awaited()


class TestValidateBbox:
    @pytest.mark.parametrize(
        ("bbox", "motivo"),
        [
            (("a", 2, 3, 4), "4 números finitos"),
            ((float("nan"), -15.0, -50.0, -10.0), "4 números finitos"),
            ((True, -15.0, -50.0, -10.0), "4 números finitos"),
            (5, "deve ter 4 valores"),
            ("abcd", "deve ter 4 valores"),
            ((-200.0, -15.0, -190.0, -10.0), "fora dos limites geográficos"),
            ((-60.0, -95.0, -50.0, -10.0), "fora dos limites geográficos"),
        ],
    )
    def test_bbox_invalido(self, bbox, motivo):
        with pytest.raises(InvalidParameterError, match=motivo):
            validate_bbox(bbox)

    @pytest.mark.parametrize(
        "bbox",
        [
            (-60, -15, -50, -10),
            [-60.0, -15.0, -50.0, -10.0],
            np.array([-60.0, -15.0, -50.0, -10.0]),
        ],
    )
    def test_bbox_valido_volta_como_veio(self, bbox):
        assert validate_bbox(bbox) is bbox


class TestCheckGeopandas:
    def test_returns_module(self):
        gpd = pytest.importorskip("geopandas")
        from agrobr.utils.geo import check_geopandas

        result = check_geopandas()
        assert result is gpd

    def test_raises_import_error(self):
        from agrobr.utils import geo

        with (
            patch.object(geo, "check_geopandas", side_effect=ImportError("agrobr[geo]")),
            pytest.raises(ImportError, match="agrobr\\[geo\\]"),
        ):
            geo.check_geopandas()


gpd = pytest.importorskip("geopandas")


class TestParseGeojsonBase:
    def _make_geojson(self, features: list[dict]) -> bytes:
        import json

        return json.dumps({"type": "FeatureCollection", "features": features}).encode()

    def _feature(self, **props: object) -> dict:
        return {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": props,
        }

    @pytest.mark.parametrize(
        "crs,aceito",
        [
            (None, True),
            ({"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}}, True),
            ({"type": "name", "properties": {"name": "EPSG:4326"}}, True),
            ({"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4674"}}, False),
            ({"type": "link", "properties": {"href": "crs.wkt"}}, False),
            ("EPSG:4326", False),
        ],
        ids=["ausente", "urn_4326", "nome_4326", "urn_4674", "sem_nome", "texto"],
    )
    def test_crs_declarado_confere_com_o_pedido(self, crs, aceito):
        import json

        from agrobr.utils.geo import parse_geojson_base

        corpo: dict = {"type": "FeatureCollection", "features": [self._feature(col1="a")]}
        if crs is not None:
            corpo["crs"] = crs
        dados = json.dumps(corpo).encode()
        argumentos: dict = {
            "source": "test",
            "parser_version": 1,
            "required_cols": {"col1"},
            "max_features": 100,
            "output_cols_empty": ["col1", "geometry"],
            "truncation_event": "test_truncated",
        }
        if aceito:
            with helpers.sem_excecao():
                gdf = parse_geojson_base(dados, gpd, **argumentos)
            assert gdf.crs.to_epsg() == 4326
        else:
            with pytest.raises((ParseError, KeyError, TypeError)) as caught:
                parse_geojson_base(dados, gpd, **argumentos)
            assert caught.type is ParseError
            assert "diverge do EPSG:4326 solicitado" in str(caught.value)

    def test_normal(self):
        from agrobr.utils.geo import parse_geojson_base

        data = self._make_geojson([self._feature(col1="a", col2="b")])
        gdf = parse_geojson_base(
            data,
            gpd,
            source="test",
            parser_version=1,
            required_cols={"col1"},
            max_features=100,
            output_cols_empty=["col1", "geometry"],
            truncation_event="test_truncated",
        )
        assert len(gdf) == 1
        assert "col1" in gdf.columns

    def test_empty_returns_gdf(self):
        from agrobr.utils.geo import parse_geojson_base

        data = self._make_geojson([])
        gdf = parse_geojson_base(
            data,
            gpd,
            source="test",
            parser_version=1,
            required_cols=set(),
            max_features=100,
            output_cols_empty=["col1", "geometry"],
            truncation_event="test_truncated",
        )
        assert len(gdf) == 0
        assert isinstance(gdf, gpd.GeoDataFrame)

    def test_empty_raises(self):
        from agrobr.utils.geo import parse_geojson_base

        data = self._make_geojson([])
        with pytest.raises(ParseError, match="sem features"):
            parse_geojson_base(
                data,
                gpd,
                source="test",
                parser_version=1,
                required_cols=set(),
                max_features=100,
                output_cols_empty=["col1", "geometry"],
                truncation_event="test_truncated",
                on_empty="raise",
            )

    def test_missing_required_cols(self):
        from agrobr.utils.geo import parse_geojson_base

        data = self._make_geojson([self._feature(col1="a")])
        with pytest.raises(ParseError, match="Colunas obrigatorias"):
            parse_geojson_base(
                data,
                gpd,
                source="test",
                parser_version=1,
                required_cols={"missing_col"},
                max_features=100,
                output_cols_empty=["col1", "geometry"],
                truncation_event="test_truncated",
            )

    def test_invalid_json(self):
        from agrobr.utils.geo import parse_geojson_base

        with pytest.raises(ParseError, match="GeoJSON"):
            parse_geojson_base(
                b"not json",
                gpd,
                source="test",
                parser_version=1,
                required_cols=set(),
                max_features=100,
                output_cols_empty=["col1", "geometry"],
                truncation_event="test_truncated",
            )
