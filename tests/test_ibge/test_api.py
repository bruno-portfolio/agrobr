"""Testes da API IBGE."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import ibge
from agrobr.ibge import client


class TestProdutosLists:
    """Testes das funcoes de listagem."""

    @pytest.mark.asyncio
    async def test_produtos_pam(self):
        """Testa listagem de produtos PAM."""
        produtos = await ibge.produtos_pam()

        assert isinstance(produtos, list)
        assert len(produtos) > 0
        assert "soja" in produtos
        assert "milho" in produtos

    @pytest.mark.asyncio
    async def test_produtos_lspa(self):
        """Testa listagem de produtos LSPA."""
        produtos = await ibge.produtos_lspa()

        assert isinstance(produtos, list)
        assert len(produtos) > 0
        assert "soja" in produtos
        assert "milho_1" in produtos
        assert "milho_2" in produtos

    @pytest.mark.asyncio
    async def test_ufs(self):
        """Testa listagem de UFs."""
        ufs = await ibge.ufs()

        assert isinstance(ufs, list)
        assert len(ufs) == 27
        assert "MT" in ufs
        assert "SP" in ufs
        assert "PR" in ufs


class TestPamValidation:
    """Testes de validacao da funcao PAM."""

    @pytest.mark.asyncio
    async def test_pam_produto_invalido(self):
        """Testa que produto invalido levanta erro."""
        with pytest.raises(ValueError) as exc:
            await ibge.pam("produto_inexistente")

        assert "Produto não suportado" in str(exc.value)

    @pytest.mark.asyncio
    async def test_pam_lista_produtos_no_erro(self):
        """Testa que erro lista produtos disponiveis."""
        with pytest.raises(ValueError) as exc:
            await ibge.pam("xyz")

        assert "Disponíveis:" in str(exc.value)
        assert "soja" in str(exc.value)


class TestLspaValidation:
    """Testes de validacao da funcao LSPA."""

    @pytest.mark.asyncio
    async def test_lspa_produto_invalido(self):
        """Testa que produto invalido levanta erro."""
        with pytest.raises(ValueError) as exc:
            await ibge.lspa("produto_inexistente")

        assert "Produto não suportado" in str(exc.value)


class TestPamMocked:
    """Testes da funcao PAM com mock."""

    @pytest.fixture
    def mock_sidra_response(self):
        """Resposta mockada do SIDRA."""
        return pd.DataFrame(
            {
                "NC": ["3", "3"],
                "NN": ["Unidade da Federação", "Unidade da Federação"],
                "MC": ["51", "41"],
                "MN": ["Mato Grosso", "Paraná"],
                "V": ["15000000", "12000000"],
                "D1C": ["2023", "2023"],
                "D1N": ["2023", "2023"],
                "D2C": ["214", "214"],
                "D2N": ["Área plantada", "Área plantada"],
                "D3C": ["40124", "40124"],
                "D3N": ["Soja (em grão)", "Soja (em grão)"],
            }
        )

    @pytest.mark.asyncio
    async def test_pam_returns_dataframe(self, mock_sidra_response):
        """Testa que PAM retorna DataFrame."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_sidra_response

            df = await ibge.pam("soja", ano=2023)

            assert isinstance(df, pd.DataFrame)
            mock_fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_pam_adds_produto_column(self, mock_sidra_response):
        """Testa que adiciona coluna produto."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_sidra_response

            df = await ibge.pam("soja", ano=2023)

            assert "produto" in df.columns
            assert df["produto"].iloc[0] == "soja"

    @pytest.mark.asyncio
    async def test_pam_adds_fonte_column(self, mock_sidra_response):
        """Testa que adiciona coluna fonte."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_sidra_response

            df = await ibge.pam("soja", ano=2023)

            assert "fonte" in df.columns
            assert df["fonte"].iloc[0] == "ibge_pam"

    @pytest.mark.asyncio
    async def test_pam_calls_sidra_with_correct_params(self, mock_sidra_response):
        """Testa parametros passados ao SIDRA."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_sidra_response

            await ibge.pam("soja", ano=2023, uf="MT", nivel="uf")

            call_args = mock_fetch.call_args
            assert call_args.kwargs["table_code"] == client.TABELAS["pam_nova"]
            assert call_args.kwargs["territorial_level"] == "3"  # UF
            assert call_args.kwargs["period"] == "2023"

    @pytest.mark.asyncio
    async def test_pam_list_of_years(self, mock_sidra_response):
        """Testa lista de anos."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_sidra_response

            await ibge.pam("soja", ano=[2021, 2022, 2023])

            call_args = mock_fetch.call_args
            assert call_args.kwargs["period"] == "2021,2022,2023"


class TestLspaMocked:
    """Testes da funcao LSPA com mock."""

    @pytest.fixture
    def mock_lspa_response(self):
        """Resposta mockada do LSPA."""
        return pd.DataFrame(
            {
                "NC": ["1", "1"],
                "NN": ["Brasil", "Brasil"],
                "MC": ["1", "1"],
                "MN": ["Brasil", "Brasil"],
                "V": ["150000", "45000"],
                "D1C": ["202406", "202406"],
                "D1N": ["junho 2024", "junho 2024"],
                "D2C": ["109", "216"],
                "D2N": ["Área", "Produção"],
                "D3C": ["39443", "39443"],
                "D3N": ["Soja", "Soja"],
            }
        )

    @pytest.mark.asyncio
    async def test_lspa_returns_dataframe(self, mock_lspa_response):
        """Testa que LSPA retorna DataFrame."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_lspa_response

            df = await ibge.lspa("soja", ano=2024, mes=6)

            assert isinstance(df, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_lspa_adds_metadata(self, mock_lspa_response):
        """Testa que adiciona metadata."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_lspa_response

            df = await ibge.lspa("soja", ano=2024, mes=6)

            assert "produto" in df.columns
            assert "fonte" in df.columns
            assert df["fonte"].iloc[0] == "ibge_lspa"

    @pytest.mark.asyncio
    async def test_lspa_period_format(self, mock_lspa_response):
        """Testa formato do periodo."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_lspa_response

            await ibge.lspa("soja", ano=2024, mes=6)

            call_args = mock_fetch.call_args
            assert call_args.kwargs["period"] == "202406"

    @pytest.mark.asyncio
    async def test_lspa_all_months(self, mock_lspa_response):
        """Testa busca de todos os meses."""
        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_lspa_response

            await ibge.lspa("soja", ano=2024, mes=None)

            call_args = mock_fetch.call_args
            period = call_args.kwargs["period"]

            # Deve ter 12 meses
            assert "202401" in period
            assert "202412" in period


class TestPolarsSupport:
    """Testes do suporte a Polars."""

    @pytest.fixture
    def mock_response(self):
        """Resposta mockada."""
        return pd.DataFrame(
            {
                "NC": ["3"],
                "NN": ["UF"],
                "MC": ["51"],
                "MN": ["Mato Grosso"],
                "V": ["15000"],
                "D1N": ["2023"],
                "D2N": ["Área plantada"],
            }
        )

    @pytest.mark.asyncio
    async def test_pam_polars_conversion(self, mock_response):
        """Testa conversao para Polars na PAM."""
        pytest.importorskip("polars")

        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_response

            df = await ibge.pam("soja", ano=2023, as_polars=True)

            import polars as pl

            assert isinstance(df, pl.DataFrame)

    @pytest.mark.asyncio
    async def test_lspa_polars_conversion(self, mock_response):
        """Testa conversao para Polars no LSPA."""
        pytest.importorskip("polars")

        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_response

            df = await ibge.lspa("soja", ano=2024, mes=6, as_polars=True)

            import polars as pl

            assert isinstance(df, pl.DataFrame)

    @pytest.mark.asyncio
    async def test_pam_polars_fallback_pandas(self, mock_response, monkeypatch):
        """Testa fallback para pandas quando polars nao instalado."""
        # Simula polars nao instalado
        import builtins

        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "polars":
                raise ImportError("No module named 'polars'")
            return real_import(name, *args, **kwargs)

        with patch.object(client, "fetch_sidra", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = mock_response

            monkeypatch.setattr(builtins, "__import__", mock_import)

            df = await ibge.pam("soja", ano=2023, as_polars=True)

            # Deve retornar pandas
            assert isinstance(df, pd.DataFrame)


@pytest.mark.integration
class TestPamIntegration:
    """Testes de integracao PAM (requer internet)."""

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_pam_soja_real(self):
        """Teste real da PAM de soja."""
        df = await ibge.pam("soja", ano=2022, nivel="brasil")

        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "produto" in df.columns

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_pam_milho_real(self):
        """Teste real da PAM de milho."""
        df = await ibge.pam("milho", ano=2022, nivel="brasil")

        assert isinstance(df, pd.DataFrame)
        assert not df.empty


@pytest.mark.integration
class TestLspaIntegration:
    """Testes de integracao LSPA (requer internet)."""

    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_lspa_soja_real(self):
        """Teste real do LSPA de soja."""
        df = await ibge.lspa("soja", ano=2024, mes=6)

        assert isinstance(df, pd.DataFrame)
