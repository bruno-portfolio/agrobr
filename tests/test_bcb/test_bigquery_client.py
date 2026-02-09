"""Testes para o fallback BigQuery do BCB/SICOR."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from agrobr.bcb.bigquery_client import (
    BQ_COLUMNS_MAP,
    BQ_DATASET,
    BQ_TABLE,
    _build_query,
    _check_basedosdados,
    fetch_credito_rural_bigquery,
    is_bigquery_available,
)
from agrobr.exceptions import SourceUnavailableError


class TestCheckBasedosdados:
    def test_raises_when_not_installed(self):
        with (
            patch.dict("sys.modules", {"basedosdados": None}),
            pytest.raises(SourceUnavailableError, match="basedosdados"),
        ):
            _check_basedosdados()

    def test_ok_when_installed(self):
        mock_bd = MagicMock()
        with patch.dict("sys.modules", {"basedosdados": mock_bd}):
            _check_basedosdados()


class TestBuildQuery:
    def test_custeio_default(self):
        query = _build_query(finalidade="custeio")
        assert "nome_finalidade = 'CUSTEIO'" in query
        assert "basedosdados.br_bcb_sicor.microdados_operacao" in query
        assert "GROUP BY" in query

    def test_with_produto(self):
        query = _build_query(finalidade="custeio", produto="SOJA")
        assert "SOJA" in query
        assert "LIKE" in query

    def test_with_safra_ano(self):
        query = _build_query(finalidade="custeio", safra_ano=2023)
        assert "ano = 2023" in query

    def test_with_uf(self):
        query = _build_query(finalidade="custeio", uf="MT")
        assert "sigla_uf = 'MT'" in query

    def test_investimento(self):
        query = _build_query(finalidade="investimento")
        assert "INVESTIMENTO" in query

    def test_all_filters(self):
        query = _build_query(
            finalidade="custeio",
            produto="MILHO",
            safra_ano=2024,
            uf="PR",
        )
        assert "CUSTEIO" in query
        assert "MILHO" in query
        assert "ano = 2024" in query
        assert "sigla_uf = 'PR'" in query


class TestFetchCreditoRuralBigquery:
    @pytest.mark.asyncio
    async def test_returns_records(self):
        mock_df = pd.DataFrame(
            [
                {
                    "ano": 2023,
                    "mes": 9,
                    "sigla_uf": "MT",
                    "id_municipio": "5107248",
                    "nome_produto": "SOJA",
                    "nome_finalidade": "CUSTEIO",
                    "valor_parcela": 285431200.0,
                    "area_financiada": 98500.0,
                    "qtd_contratos": 1240,
                },
            ]
        )

        mock_bd = MagicMock()
        mock_bd.read_sql.return_value = mock_df
        mock_bd.config.billing_project_id = "test-project"

        with (
            patch.dict("sys.modules", {"basedosdados": mock_bd}),
            patch(
                "agrobr.bcb.bigquery_client._query_bigquery_sync",
                return_value=mock_df.rename(
                    columns={k: v for k, v in BQ_COLUMNS_MAP.items() if k in mock_df.columns}
                ).to_dict("records"),
            ),
        ):
            records = await fetch_credito_rural_bigquery(
                finalidade="custeio",
                produto_sicor="SOJA",
                safra_sicor="2023/2024",
            )

        assert len(records) == 1
        assert records[0]["uf"] == "MT"
        assert records[0]["valor"] == 285431200.0

    @pytest.mark.asyncio
    async def test_safra_year_extraction(self):
        with patch(
            "agrobr.bcb.bigquery_client._query_bigquery_sync",
            return_value=[],
        ) as mock_query:
            result = await fetch_credito_rural_bigquery(
                finalidade="custeio",
                safra_sicor="2023/2024",
            )

        assert result == []
        call_query = mock_query.call_args[0][0]
        assert "ano = 2023" in call_query

    @pytest.mark.asyncio
    async def test_cd_uf_to_sigla_conversion(self):
        with patch(
            "agrobr.bcb.bigquery_client._query_bigquery_sync",
            return_value=[],
        ) as mock_query:
            await fetch_credito_rural_bigquery(
                finalidade="custeio",
                cd_uf="51",
            )

        call_query = mock_query.call_args[0][0]
        assert "sigla_uf = 'MT'" in call_query

    @pytest.mark.asyncio
    async def test_sigla_uf_passthrough(self):
        with patch(
            "agrobr.bcb.bigquery_client._query_bigquery_sync",
            return_value=[],
        ) as mock_query:
            await fetch_credito_rural_bigquery(
                finalidade="custeio",
                cd_uf="MT",
            )

        call_query = mock_query.call_args[0][0]
        assert "sigla_uf = 'MT'" in call_query

    @pytest.mark.asyncio
    async def test_raises_when_bigquery_fails(self):
        with (
            patch(
                "agrobr.bcb.bigquery_client._query_bigquery_sync",
                side_effect=SourceUnavailableError(source="bcb_bigquery", last_error="Auth failed"),
            ),
            pytest.raises(SourceUnavailableError, match="bcb_bigquery"),
        ):
            await fetch_credito_rural_bigquery(finalidade="custeio")


class TestIsBigqueryAvailable:
    def test_available(self):
        mock_bd = MagicMock()
        with patch.dict("sys.modules", {"basedosdados": mock_bd}):
            assert is_bigquery_available() is True

    def test_not_available(self):
        with patch.dict("sys.modules", {"basedosdados": None}):
            assert is_bigquery_available() is False


class TestConstants:
    def test_dataset_name(self):
        assert BQ_DATASET == "br_bcb_sicor"

    def test_table_name(self):
        assert BQ_TABLE == "microdados_operacao"

    def test_columns_map_has_essential_keys(self):
        assert "sigla_uf" in BQ_COLUMNS_MAP
        assert "nome_produto" in BQ_COLUMNS_MAP
        assert "valor_parcela" in BQ_COLUMNS_MAP
        assert BQ_COLUMNS_MAP["sigla_uf"] == "uf"
        assert BQ_COLUMNS_MAP["nome_produto"] == "produto"
        assert BQ_COLUMNS_MAP["valor_parcela"] == "valor"
