"""Testes para a API pública ComexStat."""

from unittest.mock import AsyncMock, patch

import pytest

from agrobr.comexstat import api


def _mock_csv():
    """CSV de exportação/importação de exemplo (mesmo layout)."""
    return (
        "CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n"
        "2024;1;12019000;10;160;MT;4;817800;1000;50000000;20000000\n"
        "2024;2;12019000;10;160;MT;4;817800;800;40000000;16000000\n"
        "2024;3;12019000;10;276;MT;4;817800;500;30000000;12000000\n"
    )


class TestExportacao:
    @pytest.mark.asyncio
    async def test_default_ano_is_previous_year(self):
        from agrobr.utils.time import utcnow

        with patch.object(
            api.client, "fetch_exportacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ) as mock_fetch:
            await api.exportacao("soja")

        expected_ano = utcnow().year - 1
        assert mock_fetch.call_args[0][0] == expected_ano

    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        with patch.object(
            api.client, "fetch_exportacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df = await api.exportacao("soja", ano=2024)

        assert len(df) > 0
        assert "kg_liquido" in df.columns
        assert "valor_fob_usd" in df.columns
        assert "volume_ton" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch.object(
            api.client, "fetch_exportacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df, meta = await api.exportacao("soja", ano=2024, return_meta=True)

        assert meta.source == "comexstat"
        assert meta.attempted_sources == ["comexstat"]
        assert meta.selected_source == "comexstat"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_mensal_aggregation(self):
        with patch.object(
            api.client, "fetch_exportacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df = await api.exportacao("soja", ano=2024, agregacao="mensal")

        # 3 meses distintos
        assert len(df) == 3

    @pytest.mark.asyncio
    async def test_detalhado(self):
        with patch.object(
            api.client, "fetch_exportacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df = await api.exportacao("soja", ano=2024, agregacao="detalhado")

        assert len(df) == 3
        assert "volume_ton" not in df.columns

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        csv = (
            "CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n"
            "2024;1;12019000;10;160;MT;4;817800;1000;50000000;20000000\n"
            "2024;1;12019000;10;160;PR;4;817800;800;40000000;16000000\n"
        )
        with patch.object(
            api.client, "fetch_exportacao_csv", new_callable=AsyncMock, return_value=csv
        ):
            df = await api.exportacao("soja", ano=2024, uf="PR")

        assert len(df) == 1
        assert df.iloc[0]["uf"] == "PR"

    @pytest.mark.asyncio
    async def test_soybean_oil_generic_and_crude_scopes(self):
        csv = (
            "CO_ANO;CO_MES;CO_NCM;SG_UF_NCM;KG_LIQUIDO;VL_FOB\n"
            "2024;1;15071000;MT;1000;500\n"
            "2024;1;15079011;MT;2000;1000\n"
            "2024;1;23040010;MT;3000;1500\n"
        )
        with patch.object(
            api.client,
            "fetch_exportacao_csv",
            new_callable=AsyncMock,
            return_value=csv,
        ):
            generic = await api.exportacao("oleo_soja", ano=2024)
            crude = await api.exportacao("oleo_soja_bruto", ano=2024)

        assert generic["ncm"].tolist() == ["15071000", "15079011"]
        assert crude["ncm"].tolist() == ["15071000"]


class TestImportacao:
    @pytest.mark.asyncio
    async def test_default_ano_is_previous_year(self):
        from agrobr.utils.time import utcnow

        with patch.object(
            api.client, "fetch_importacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ) as mock_fetch:
            await api.importacao("soja")

        expected_ano = utcnow().year - 1
        assert mock_fetch.call_args[0][0] == expected_ano

    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        with patch.object(
            api.client, "fetch_importacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df = await api.importacao("soja", ano=2024)

        assert len(df) > 0
        assert "kg_liquido" in df.columns
        assert "valor_fob_usd" in df.columns
        assert "volume_ton" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch.object(
            api.client, "fetch_importacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df, meta = await api.importacao("soja", ano=2024, return_meta=True)

        assert meta.source == "comexstat"
        assert "IMP_2024" in meta.source_url
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_mensal_aggregation(self):
        with patch.object(
            api.client, "fetch_importacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df = await api.importacao("soja", ano=2024, agregacao="mensal")

        assert len(df) == 3

    @pytest.mark.asyncio
    async def test_detalhado(self):
        with patch.object(
            api.client, "fetch_importacao_csv", new_callable=AsyncMock, return_value=_mock_csv()
        ):
            df = await api.importacao("soja", ano=2024, agregacao="detalhado")

        assert len(df) == 3
        assert "volume_ton" not in df.columns

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        csv = (
            "CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n"
            "2024;1;12019000;10;160;SP;4;817800;1000;50000000;20000000\n"
            "2024;1;12019000;10;160;RJ;4;817800;800;40000000;16000000\n"
        )
        with patch.object(
            api.client, "fetch_importacao_csv", new_callable=AsyncMock, return_value=csv
        ):
            df = await api.importacao("soja", ano=2024, uf="SP")

        assert len(df) == 1
        assert df.iloc[0]["uf"] == "SP"
