from unittest.mock import AsyncMock, patch

import pytest

from agrobr.bcb import api
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError


def _mock_sicor_data(
    cd_programa="0050",
    cd_fonte_recurso="0303",
    cd_tipo_seguro="9",
    cd_modalidade="01",
    atividade="1",
):
    return [
        {
            "Safra": "2023/2024",
            "AnoEmissao": 2023,
            "MesEmissao": 9,
            "cdUF": "51",
            "UF": "MT",
            "cdMunicipio": "5107248",
            "Municipio": "SORRISO",
            "Produto": "SOJA",
            "Valor": 285431200.0,
            "AreaFinanciada": 98500.0,
            "QtdContratos": 1240,
            "cdPrograma": cd_programa,
            "cdSubPrograma": "0000",
            "cdFonteRecurso": cd_fonte_recurso,
            "cdTipoSeguro": cd_tipo_seguro,
            "cdModalidade": cd_modalidade,
            "Atividade": atividade,
        },
        {
            "Safra": "2023/2024",
            "AnoEmissao": 2023,
            "MesEmissao": 10,
            "cdUF": "51",
            "UF": "MT",
            "cdMunicipio": "5106752",
            "Municipio": "SINOP",
            "Produto": "SOJA",
            "Valor": 142715600.0,
            "AreaFinanciada": 49250.0,
            "QtdContratos": 620,
            "cdPrograma": cd_programa,
            "cdSubPrograma": "0000",
            "cdFonteRecurso": cd_fonte_recurso,
            "cdTipoSeguro": cd_tipo_seguro,
            "cdModalidade": cd_modalidade,
            "Atividade": atividade,
        },
    ]


def _mock_sicor_data_multi_programa():
    base = _mock_sicor_data(cd_programa="0050")
    extra = _mock_sicor_data(cd_programa="0001")
    extra[0]["Municipio"] = "CUIABA"
    extra[0]["cdMunicipio"] = "5103403"
    extra[1]["Municipio"] = "RONDONOPOLIS"
    extra[1]["cdMunicipio"] = "5107602"
    return base + extra


class TestCreditoRural:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24")

        assert len(df) == 1
        assert "valor" in df.columns
        assert "area_financiada" in df.columns
        assert all(df["produto"] == "soja")

    @pytest.mark.asyncio
    async def test_return_meta_odata(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df, meta = await api.credito_rural("soja", safra="2023/24", return_meta=True)

        assert meta.source == "bcb_credito"
        assert meta.attempted_sources == ["bcb_odata"]
        assert meta.selected_source == "bcb_odata"
        assert meta.source_method == "httpx"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_return_meta_bigquery_fallback(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "bigquery"),
        ):
            df, meta = await api.credito_rural("soja", safra="2023/24", return_meta=True)

        assert meta.source == "bcb_credito"
        assert meta.attempted_sources == ["bcb_odata", "bcb_bigquery"]
        assert meta.selected_source == "bcb_bigquery"
        assert meta.source_method == "bigquery"
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_agregacao_uf(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", agregacao="uf")

        assert len(df) == 1
        assert df.iloc[0]["valor"] == pytest.approx(285431200.0 + 142715600.0)

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ) as mock_fetch:
            df = await api.credito_rural("soja", safra="2023/24", uf=" mt ")

        assert len(df) == 1
        assert all(df["uf"] == "MT")
        assert mock_fetch.call_args.kwargs["cd_uf"] == "51"

    @pytest.mark.asyncio
    async def test_invalid_uf_raises_before_request(self):
        with (
            patch.object(
                api.client,
                "fetch_credito_rural_with_fallback",
                new_callable=AsyncMock,
            ) as mock_fetch,
            pytest.raises(ValueError, match="UF invalida"),
        ):
            await api.credito_rural("soja", safra="2023/24", uf="XX")

        mock_fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_schema_version_2_0(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            _, meta = await api.credito_rural("soja", safra="2023/24", return_meta=True)

        assert meta.schema_version == "2.0"

    @pytest.mark.asyncio
    async def test_municipio_aggregation_rejected_before_request(self):
        with (
            patch.object(
                api.client,
                "fetch_credito_rural_with_fallback",
                new_callable=AsyncMock,
            ) as mock_fetch,
            pytest.raises(InvalidParameterError, match=r"agrobr\[bigquery\]"),
        ):
            await api.credito_rural("soja", agregacao="municipio")

        mock_fetch.assert_not_awaited()


class TestCreditoRuralContractColumns:
    @pytest.mark.asyncio
    async def test_new_columns_present(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24")

        assert df.columns.tolist() == [
            "safra",
            "produto",
            "uf",
            "finalidade",
            "agregacao",
            "programa",
            "cd_programa",
            "qtd_contratos",
            "valor",
            "area_financiada",
            "fonte",
        ]

    @pytest.mark.asyncio
    async def test_enriched_values(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", agregacao="programa")

        row = df.iloc[0]
        assert row["programa"] == "Pronamp"
        assert row["cd_programa"] == "0050"
        assert row["fonte"] == "bcb_odata"


class TestCreditoRuralFilterPrograma:
    @pytest.mark.asyncio
    async def test_filter_programa(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data_multi_programa(), "odata"),
        ):
            df = await api.credito_rural(
                "soja", safra="2023/24", programa="Pronamp", agregacao="programa"
            )

        assert len(df) == 1
        assert all(df["programa"].str.lower() == "pronamp")

    @pytest.mark.asyncio
    async def test_filter_programa_case_insensitive(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data_multi_programa(), "odata"),
        ):
            df = await api.credito_rural(
                "soja", safra="2023/24", programa="pronamp", agregacao="programa"
            )

        assert len(df) == 1

    @pytest.mark.asyncio
    async def test_filter_programa_no_match(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            df = await api.credito_rural(
                "soja", safra="2023/24", programa="Funcafe", agregacao="programa"
            )

        assert len(df) == 0


class TestCreditoRuralFilterTipoSeguro:
    @pytest.mark.asyncio
    async def test_filter_tipo_seguro(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(cd_tipo_seguro="9"), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", tipo_seguro="Nao se aplica")

        assert len(df) == 1

    @pytest.mark.asyncio
    async def test_filter_tipo_seguro_no_match(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(cd_tipo_seguro="9"), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", tipo_seguro="Proagro")

        assert len(df) == 0


class TestCreditoRuralAgregacaoPrograma:
    @pytest.mark.asyncio
    async def test_agregacao_programa(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data_multi_programa(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", agregacao="programa")

        assert "programa" in df.columns
        assert len(df) == 2

    @pytest.mark.asyncio
    async def test_agregacao_programa_sums_values(self):
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data_multi_programa(), "odata"),
        ):
            df = await api.credito_rural("soja", safra="2023/24", agregacao="programa")

        pronamp = df[df["programa"] == "Pronamp"]
        assert len(pronamp) == 1
        assert pronamp.iloc[0]["valor"] == pytest.approx(285431200.0 + 142715600.0)


class TestCreditoRuralFallback:
    @pytest.mark.asyncio
    async def test_odata_success_no_fallback(self):
        mock_odata = AsyncMock(return_value=_mock_sicor_data())
        with patch.object(api.client, "fetch_credito_rural", mock_odata):
            records, source = await api.client.fetch_credito_rural_with_fallback(
                finalidade="custeio",
                produto_sicor="SOJA",
                safra_sicor="2023/2024",
            )

        assert source == "odata"
        assert len(records) == 2
        mock_odata.assert_called_once()

    @pytest.mark.asyncio
    async def test_odata_fails_bigquery_succeeds(self):
        mock_odata = AsyncMock(
            side_effect=SourceUnavailableError(source="bcb", last_error="HTTP 500")
        )
        mock_bq = AsyncMock(return_value=_mock_sicor_data())

        with (
            patch.object(api.client, "fetch_credito_rural", mock_odata),
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                mock_bq,
            ),
        ):
            records, source = await api.client.fetch_credito_rural_with_fallback(
                finalidade="custeio",
                produto_sicor="SOJA",
            )

        assert source == "bigquery"
        assert len(records) == 2

    @pytest.mark.asyncio
    async def test_both_fail_raises(self):
        mock_odata = AsyncMock(
            side_effect=SourceUnavailableError(source="bcb", last_error="HTTP 500")
        )
        mock_bq = AsyncMock(
            side_effect=SourceUnavailableError(source="bcb_bigquery", last_error="Auth failed")
        )

        with (
            patch.object(api.client, "fetch_credito_rural", mock_odata),
            patch(
                "agrobr.bcb.bigquery_client.fetch_credito_rural_bigquery",
                mock_bq,
            ),
            pytest.raises(SourceUnavailableError, match="Ambas as fontes"),
        ):
            await api.client.fetch_credito_rural_with_fallback(
                finalidade="custeio",
            )


class TestCreditoRuralAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        with patch.object(
            api.client,
            "fetch_credito_rural_with_fallback",
            new_callable=AsyncMock,
            return_value=(_mock_sicor_data(), "odata"),
        ):
            result = await api.credito_rural("soja", safra="2023/24", as_polars=True)
        assert isinstance(result, pl.DataFrame)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_credito_rural_live_filtra_uf_e_safra():
    df = await api.credito_rural("soja", safra="2024/25", uf="MT")

    assert not df.empty
    assert set(df["uf"]) == {"MT"}
    assert set(df["safra"]) == {"2024/2025"}
