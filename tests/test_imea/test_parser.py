"""Testes para o parser IMEA."""

import pandas as pd

from agrobr.imea.parser import (
    PARSER_VERSION,
    filter_by_safra,
    filter_by_unidade,
    parse_cotacoes,
)


def _mock_api_records():
    """Simula resposta JSON da API IMEA /cotacoes."""
    return [
        {
            "Localidade": "Médio-Norte",
            "Valor": 125.50,
            "Variacao": -1.2,
            "Safra": "24/25",
            "CadeiaId": 4,
            "IndicadorFinalId": 10,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-15",
            "UnidadeSigla": "R$/sc",
            "UnidadeDescricao": "Reais por saca de 60 kg",
        },
        {
            "Localidade": "Norte",
            "Valor": 120.00,
            "Variacao": 0.5,
            "Safra": "24/25",
            "CadeiaId": 4,
            "IndicadorFinalId": 10,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-15",
            "UnidadeSigla": "R$/sc",
            "UnidadeDescricao": "Reais por saca de 60 kg",
        },
        {
            "Localidade": "Médio-Norte",
            "Valor": 85.5,
            "Variacao": 2.1,
            "Safra": "23/24",
            "CadeiaId": 4,
            "IndicadorFinalId": 20,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-15",
            "UnidadeSigla": "%",
            "UnidadeDescricao": "Percentual",
        },
        {
            "Localidade": "Sudeste",
            "Valor": 130.00,
            "Variacao": -0.8,
            "Safra": "24/25",
            "CadeiaId": 3,
            "IndicadorFinalId": 10,
            "TipoLocalidadeId": 1,
            "DataPublicacao": "2024-06-14",
            "UnidadeSigla": "R$/sc",
            "UnidadeDescricao": "Reais por saca de 60 kg",
        },
    ]


class TestParseCotacoes:
    def test_basic_parse(self):
        df = parse_cotacoes(_mock_api_records())
        assert len(df) == 4
        assert "cadeia" in df.columns
        assert "localidade" in df.columns
        assert "valor" in df.columns
        assert "variacao" in df.columns
        assert "safra" in df.columns
        assert "unidade" in df.columns

    def test_columns_renamed(self):
        df = parse_cotacoes(_mock_api_records())
        # Colunas originais nao devem existir
        assert "Localidade" not in df.columns
        assert "Valor" not in df.columns
        assert "CadeiaId" not in df.columns

    def test_cadeia_resolved(self):
        df = parse_cotacoes(_mock_api_records())
        cadeias = df["cadeia"].unique().tolist()
        assert "soja" in cadeias
        assert "milho" in cadeias

    def test_valor_numeric(self):
        df = parse_cotacoes(_mock_api_records())
        assert df["valor"].dtype in ("float64", "Float64")

    def test_variacao_numeric(self):
        df = parse_cotacoes(_mock_api_records())
        assert df["variacao"].dtype in ("float64", "Float64")

    def test_auxiliary_columns_dropped(self):
        df = parse_cotacoes(_mock_api_records())
        assert "cadeia_id" not in df.columns
        assert "indicador_id" not in df.columns
        assert "tipo_localidade_id" not in df.columns

    def test_sorted(self):
        df = parse_cotacoes(_mock_api_records())
        # Deve estar ordenado por cadeia, localidade, unidade
        first_cadeia = df["cadeia"].iloc[0]
        last_cadeia = df["cadeia"].iloc[-1]
        assert first_cadeia <= last_cadeia

    def test_empty_records(self):
        df = parse_cotacoes([])
        assert df.empty
        assert "cadeia" in df.columns
        assert "localidade" in df.columns
        assert "valor" in df.columns

    def test_missing_optional_fields(self):
        records = [
            {
                "Localidade": "Norte",
                "CadeiaId": 4,
            }
        ]
        df = parse_cotacoes(records)
        assert len(df) == 1
        assert df["localidade"].iloc[0] == "Norte"
        assert df["cadeia"].iloc[0] == "soja"

    def test_unidade_descricao_preserved(self):
        df = parse_cotacoes(_mock_api_records())
        assert "unidade_descricao" in df.columns
        descs = df["unidade_descricao"].unique().tolist()
        assert "Reais por saca de 60 kg" in descs

    def test_data_publicacao_preserved(self):
        df = parse_cotacoes(_mock_api_records())
        assert "data_publicacao" in df.columns
        assert "2024-06-15" in df["data_publicacao"].values


class TestFilterByUnidade:
    def test_filter_reais(self):
        df = parse_cotacoes(_mock_api_records())
        filtered = filter_by_unidade(df, "R$/sc")
        assert len(filtered) == 3
        assert all(filtered["unidade"] == "R$/sc")

    def test_filter_percent(self):
        df = parse_cotacoes(_mock_api_records())
        filtered = filter_by_unidade(df, "%")
        assert len(filtered) == 1
        assert filtered["unidade"].iloc[0] == "%"

    def test_filter_empty_df(self):
        df = pd.DataFrame(columns=["unidade"])
        result = filter_by_unidade(df, "R$/sc")
        assert result.empty

    def test_filter_none_unidade(self):
        df = parse_cotacoes(_mock_api_records())
        result = filter_by_unidade(df, "")
        assert len(result) == len(df)

    def test_filter_nonexistent(self):
        df = parse_cotacoes(_mock_api_records())
        result = filter_by_unidade(df, "USD/t")
        assert result.empty


class TestFilterBySafra:
    def test_filter_safra(self):
        df = parse_cotacoes(_mock_api_records())
        filtered = filter_by_safra(df, "24/25")
        assert len(filtered) == 3
        assert all(filtered["safra"] == "24/25")

    def test_filter_other_safra(self):
        df = parse_cotacoes(_mock_api_records())
        filtered = filter_by_safra(df, "23/24")
        assert len(filtered) == 1
        assert filtered["safra"].iloc[0] == "23/24"

    def test_filter_empty_df(self):
        df = pd.DataFrame(columns=["safra"])
        result = filter_by_safra(df, "24/25")
        assert result.empty

    def test_filter_none_safra(self):
        df = parse_cotacoes(_mock_api_records())
        result = filter_by_safra(df, "")
        assert len(result) == len(df)

    def test_filter_nonexistent(self):
        df = parse_cotacoes(_mock_api_records())
        result = filter_by_safra(df, "99/00")
        assert result.empty


class TestParserVersion:
    def test_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1
