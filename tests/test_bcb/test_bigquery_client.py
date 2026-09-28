"""Testes para o fallback BigQuery do BCB/SICOR."""

import re
import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from agrobr.bcb import bigquery_client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import collect_failures, levanta_exatamente, sem_excecao


def test_basedosdados_ausente_ou_sem_billing_e_recusado(monkeypatch: pytest.MonkeyPatch):
    with (
        patch.dict("sys.modules", {"basedosdados": None}),
        levanta_exatamente(
            SourceUnavailableError,
            match=re.escape(
                "basedosdados não instalado. Instale com: pip install agrobr[bigquery]"
            ),
        ),
    ):
        bigquery_client._check_basedosdados()
    with patch.dict("sys.modules", {"basedosdados": MagicMock()}):
        bigquery_client._check_basedosdados()
    monkeypatch.delenv("AGROBR_BQ_BILLING_PROJECT", raising=False)
    sem_billing = MagicMock()
    sem_billing.config.billing_project_id = None
    with (
        patch.dict("sys.modules", {"basedosdados": sem_billing}),
        levanta_exatamente(SourceUnavailableError, match="AGROBR_BQ_BILLING_PROJECT"),
    ):
        bigquery_client._query_bigquery_sync("SELECT 1")
    sem_billing.read_sql.assert_not_called()


def test_billing_do_ambiente_ou_da_configuracao_e_colunas_renomeadas(
    monkeypatch: pytest.MonkeyPatch,
):
    linha = {
        "ano": 2023,
        "mes": 9,
        "sigla_uf": "MT",
        "id_municipio": "5107248",
        "nome_produto": "SOJA",
        "nome_finalidade": "CUSTEIO",
        "valor_parcela": 285431200.0,
        "area_financiada": 98500.0,
        "qtd_contratos": 1240.0,
    }
    bd = MagicMock()
    bd.config.billing_project_id = "proj-config"
    bd.read_sql.side_effect = [pd.DataFrame(), None, pd.DataFrame([linha])]
    monkeypatch.delenv("AGROBR_BQ_BILLING_PROJECT", raising=False)
    with patch.dict("sys.modules", {"basedosdados": bd}), sem_excecao():
        assert bigquery_client._query_bigquery_sync("SELECT 1") == []
        monkeypatch.setenv("AGROBR_BQ_BILLING_PROJECT", "proj-env")
        assert bigquery_client._query_bigquery_sync("SELECT 2") == []
        registros = bigquery_client._query_bigquery_sync("SELECT 3")
    assert [chamada.kwargs["billing_project_id"] for chamada in bd.read_sql.call_args_list] == [
        "proj-config",
        "proj-env",
        "proj-env",
    ]
    assert registros == [
        {
            "ano_emissao": 2023,
            "mes_emissao": 9,
            "uf": "MT",
            "cd_municipio": "5107248",
            "produto": "SOJA",
            "finalidade": "CUSTEIO",
            "valor": 285431200.0,
            "area_financiada": 98500.0,
            "qtd_contratos": 1240,
        }
    ]
    assert type(registros[0]["qtd_contratos"]) is int
    bd.read_sql.side_effect = RuntimeError("quota")
    with (
        patch.dict("sys.modules", {"basedosdados": bd}),
        levanta_exatamente(SourceUnavailableError, match="BigQuery error: quota"),
    ):
        bigquery_client._query_bigquery_sync("SELECT 4")


def test_consulta_sql_filtra_e_sanitiza():
    with sem_excecao():
        padrao = bigquery_client._build_query(finalidade="custeio")
        completa = bigquery_client._build_query(
            finalidade="custeio", produto='"MILHO"', safra_ano=2024, uf="pr"
        )
        investimento = bigquery_client._build_query("investimento")
        comercializacao = bigquery_client._build_query("comercializacao")
    assert "nome_finalidade = 'CUSTEIO'" in padrao
    assert "basedosdados.br_bcb_sicor.microdados_operacao" in padrao
    assert padrao.endswith(
        "GROUP BY ano, mes, sigla_uf, id_municipio, nome_produto, nome_finalidade\nORDER BY ano, sigla_uf, nome_produto"
    )
    assert (
        "WHERE nome_finalidade = 'CUSTEIO' AND UPPER(nome_produto) = 'MILHO' AND "
        "((ano = 2024 AND mes >= 7) OR (ano = 2025 AND mes < 7)) AND sigla_uf = 'PR'"
    ) in completa
    assert "nome_finalidade = 'INVESTIMENTO'" in investimento
    assert "nome_finalidade = 'COMERCIALIZAÇÃO'" in comercializacao
    with collect_failures() as check:
        for argumentos, motivo in [
            ({"produto": "SOJA'; DROP"}, "Caractere invalido em produto"),
            ({"uf": "M;T"}, "Caractere invalido em uf"),
            ({"uf": "MTT"}, "UF deve ter 2 caracteres: 'MTT'"),
        ]:
            with check(argumentos), levanta_exatamente(ValueError, match=re.escape(motivo)):
                bigquery_client._build_query(**argumentos)


async def test_busca_converte_safra_e_uf_e_propaga_falhas(monkeypatch: pytest.MonkeyPatch):
    consultas: list[str] = []

    def consultar(query: str) -> list[dict]:
        consultas.append(query)
        return []

    monkeypatch.setattr(bigquery_client, "_query_bigquery_sync", consultar)
    with sem_excecao():
        resultados = [
            await bigquery_client.fetch_credito_rural_bigquery(**argumentos)
            for argumentos in [
                {"safra_sicor": "2023/2024", "cd_uf": "51"},
                {"cd_uf": "999"},
            ]
        ]
    assert resultados == [[], []]
    assert "(ano = 2023 AND mes >= 7)" in consultas[0] and "sigla_uf = 'MT'" in consultas[0]
    assert "sigla_uf =" not in consultas[1]
    with levanta_exatamente(ValueError, "abc"):
        await bigquery_client.fetch_credito_rural_bigquery(safra_sicor="abc", cd_uf="MT")
    assert len(consultas) == 2
    falha = SourceUnavailableError(source="bcb_bigquery", last_error="Auth failed")

    def falhar(_query: str) -> list[dict]:
        raise falha

    monkeypatch.setattr(bigquery_client, "_query_bigquery_sync", falhar)
    with levanta_exatamente(SourceUnavailableError) as capturada:
        await bigquery_client.fetch_credito_rural_bigquery(finalidade="custeio")
    assert capturada.value is falha

    def lenta(_query: str) -> list[dict]:
        time.sleep(1)
        return []

    monkeypatch.setattr(bigquery_client, "BQ_TIMEOUT", 0.01)
    monkeypatch.setattr(bigquery_client, "_query_bigquery_sync", lenta)
    with levanta_exatamente(
        SourceUnavailableError, match=re.escape("BigQuery timeout after 0.01s")
    ):
        await bigquery_client.fetch_credito_rural_bigquery(finalidade="custeio")
