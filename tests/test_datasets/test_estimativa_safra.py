from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.contracts import estimativa_safra as safra_contract
from agrobr.datasets.estimativa_safra import (
    EstimativaSafraDataset,
)
from agrobr.exceptions import ContractViolationError, ParseError, SourceUnavailableError
from tests.helpers import collect_failures, isolated_dataset_case, levanta_exatamente

from .conftest import make_source, mock_source_meta


def _sidra_lspa_df(ano: int = 2022, uf: str | None = None) -> pd.DataFrame:
    unidade = {
        "Área plantada": "Hectares",
        "Área colhida": "Hectares",
        "Produção": "Toneladas",
        "Rendimento médio": "Quilogramas por Hectare",
    }
    dados = {
        f"novembro {ano}": {
            "Área plantada": 39_000_000,
            "Área colhida": 38_000_000,
            "Produção": 110_000_000,
            "Rendimento médio": 2894,
        },
        f"dezembro {ano}": {
            "Área plantada": 41_000_000,
            "Área colhida": 40_000_000,
            "Produção": 120_000_000,
            "Rendimento médio": 3000,
        },
    }
    rows = [
        {
            "nivel_territorial": "UF" if uf else "Brasil",
            "localidade": {"MT": "Mato Grosso", "PR": "Paraná"}.get(uf, "Brasil"),
            "localidade_cod": {"MT": 51, "PR": 41}.get(uf, 1),
            "unidade": unidade[var],
            "mes": 11 if mes.startswith("novembro") else 12,
            "variavel": var,
            "ano": ano,
            "valor": valor,
            "produto": "soja",
            "fonte": "ibge_lspa",
        }
        for mes, variaveis in dados.items()
        for var, valor in variaveis.items()
    ]
    return pd.DataFrame(rows)


def _sidra_lspa_all_nan(ano: int = 2022) -> pd.DataFrame:
    df = _sidra_lspa_df(ano)
    df["produto"] = "trigo"
    df["valor"] = float("nan")
    return df


def _mock_df():
    return pd.DataFrame(
        [
            {
                "fonte": "conab",
                "produto": "soja",
                "safra": "2024/25",
                "uf": "MT",
                "area_plantada": 12500.0,
                "area_colhida": float("nan"),
                "produtividade": 3400.0,
                "producao": 42500.0,
                "levantamento": 3,
                "data_publicacao": pd.Timestamp("2025-01-15"),
            },
        ]
    )


class TestEstimativaSafraNormalize:
    async def test_estimativa_safra_normalize_casos_1(self):
        with collect_failures() as check:
            case = "test_normalize_adds_produto_fonte"
            with check(case), isolated_dataset_case(case):
                df = _mock_df().drop(columns=["produto", "fonte"])
                dataset = EstimativaSafraDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                result = await dataset.fetch("soja")

                assert result["produto"].iloc[0] == "soja"
                assert result["fonte"].iloc[0] == "conab"
            case = "test_normalize_keeps_existing_produto_fonte"
            with check(case), isolated_dataset_case(case):
                dataset = EstimativaSafraDataset()
                dataset.info.sources[0].fetch_fn = make_source(_mock_df())

                result = await dataset.fetch("soja")

                assert result["produto"].iloc[0] == "soja"
                assert result["fonte"].iloc[0] == "conab"


class TestEstimativaSafraFallback:
    async def test_estimativa_safra_fallback_casos_1(self):
        with collect_failures() as check:
            case = "test_all_sources_fail"
            with check(case), isolated_dataset_case(case):
                dataset = EstimativaSafraDataset()
                dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))
                dataset.info.sources[1].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("test"))

                with pytest.raises(SourceUnavailableError):
                    await dataset.fetch("soja")
            case = "test_no_source_has_data_raises_not_contract_violation"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.estimativa_safra import _fetch_conab, _fetch_ibge_lspa

                dataset = EstimativaSafraDataset()
                dataset.info.sources[0].fetch_fn = _fetch_conab
                dataset.info.sources[1].fetch_fn = _fetch_ibge_lspa

                with (
                    patch(
                        "agrobr.conab.safras",
                        new_callable=AsyncMock,
                        side_effect=SourceUnavailableError(source="conab"),
                    ),
                    patch(
                        "agrobr.ibge.lspa",
                        new_callable=AsyncMock,
                        return_value=(_sidra_lspa_all_nan(2023), mock_source_meta()),
                    ),
                    pytest.raises(SourceUnavailableError),
                ):
                    await dataset.fetch("trigo", safra="2022/23")


async def test_safra_corrente_sem_conab_nao_pede_ao_lspa_o_ano_futuro():
    from agrobr.datasets.estimativa_safra import _fetch_conab, _fetch_ibge_lspa
    from agrobr.ibge import client as ibge_client
    from agrobr.utils import time as time_utils

    with isolated_dataset_case("safra_corrente") as monkeypatch:
        monkeypatch.setattr(
            time_utils, "utcnow_aware", lambda: datetime(2026, 9, 27, 15, 0, tzinfo=UTC)
        )
        sidra = AsyncMock(return_value=_sidra_lspa_df(2027))
        monkeypatch.setattr(ibge_client, "fetch_sidra", sidra)
        dataset = EstimativaSafraDataset()
        dataset.info.sources[0].fetch_fn = _fetch_conab
        dataset.info.sources[1].fetch_fn = _fetch_ibge_lspa

        with (
            patch(
                "agrobr.conab.safras",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="conab", last_error="CONAB fora do ar"),
            ),
            levanta_exatamente(SourceUnavailableError) as erro,
        ):
            await dataset.fetch("soja", safra="2026/27")

    assert "CONAB fora do ar" in str(erro.value)
    assert "o LSPA ainda não publica 2027" in str(erro.value)
    assert sidra.await_count == 0


class TestNormalizeLspa:
    def test_non_sidra_input_raises_contract_violation(self):
        from agrobr.datasets.estimativa_safra import _normalize_lspa

        with levanta_exatamente(ContractViolationError):
            _normalize_lspa(_mock_df(), "soja", "2022/23", None)

    def test_normalize_lspa_casos_1(self):
        with collect_failures() as check:
            case = "test_reduces_to_latest_month_and_converts_units"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.estimativa_safra import _normalize_lspa

                df = _normalize_lspa(_sidra_lspa_df(2023), "soja", "2022/23", None)

                assert len(df) == 1
                row = df.iloc[0]
                assert row["fonte"] == "ibge_lspa"
                assert row["produto"] == "soja"
                assert row["safra"] == "2022/23"
                assert row["uf"] is None
                assert row["area_plantada"] == pytest.approx(41000.0)
                assert row["area_colhida"] == pytest.approx(40000.0)
                assert row["producao"] == pytest.approx(120000.0)
                assert row["produtividade"] == pytest.approx(3000.0)
                assert pd.isna(row["levantamento"])
                assert pd.isna(row["data_publicacao"])
                assert row["ano_lspa"] == 2023
                assert row["mes_lspa"] == 12
            case = "test_aggregates_subsafras_and_recomputes_yield"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.estimativa_safra import _normalize_lspa

                df_multi = pd.concat(
                    [
                        _sidra_lspa_df(2024).assign(produto="milho_1"),
                        _sidra_lspa_df(2024).assign(produto="milho_2"),
                    ],
                    ignore_index=True,
                )

                df = _normalize_lspa(df_multi, "milho", "2023/24", None)

                row = df.iloc[0]
                assert row["area_plantada"] == pytest.approx(82000.0)
                assert row["area_colhida"] == pytest.approx(80000.0)
                assert row["producao"] == pytest.approx(240000.0)
                assert row["produtividade"] == pytest.approx(3000.0)
            case = "test_uf_is_uppercased"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.estimativa_safra import _normalize_lspa

                df = _normalize_lspa(_sidra_lspa_df(2023, "MT"), "soja", "2022/23", "mt")

                assert df.iloc[0]["uf"] == "MT"
            case = "test_empty_input_returns_contract_columns"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.estimativa_safra import _SAFRA_OUTPUT_COLS, _normalize_lspa

                try:
                    df = _normalize_lspa(pd.DataFrame(), "soja", "2022/23", None)
                except Exception as erro:
                    raise AssertionError(f"LSPA vazio quebrou: {erro!r}") from erro

                assert len(df) == 0
                assert list(df.columns) == _SAFRA_OUTPUT_COLS
            case = "test_all_nan_returns_empty"
            with check(case), isolated_dataset_case(case):
                from agrobr.datasets.estimativa_safra import _SAFRA_OUTPUT_COLS, _normalize_lspa

                try:
                    df = _normalize_lspa(_sidra_lspa_all_nan(2023), "trigo", "2022/23", None)
                except Exception as erro:
                    raise AssertionError(f"LSPA sem valor publicado quebrou: {erro!r}") from erro

                assert len(df) == 0
                assert list(df.columns) == _SAFRA_OUTPUT_COLS


def _fontes_reais() -> EstimativaSafraDataset:
    from agrobr.datasets.estimativa_safra import _fetch_conab, _fetch_ibge_lspa

    dataset = EstimativaSafraDataset()
    dataset.info.sources[0].fetch_fn = _fetch_conab
    dataset.info.sources[1].fetch_fn = _fetch_ibge_lspa
    return dataset


def _conab(**kwargs):
    return patch("agrobr.conab.safras", new_callable=AsyncMock, **kwargs)


def _lspa(**kwargs):
    return patch("agrobr.ibge.lspa", new_callable=AsyncMock, **kwargs)


CONAB_VAZIA = {"return_value": (pd.DataFrame(), mock_source_meta())}
LSPA_VAZIO = {"return_value": (_sidra_lspa_all_nan(2023), mock_source_meta())}


class TestTodasAsFontesVazias:
    async def test_as_duas_vazias_devolvem_o_vazio_do_contrato_com_aviso(self):
        with isolated_dataset_case("vazias"):
            dataset = _fontes_reais()
            with _conab(**CONAB_VAZIA), _lspa(**LSPA_VAZIO), pytest.warns(UserWarning) as avisos:
                df, meta = await dataset.fetch("trigo", safra="2022/23", return_meta=True)
        esperado = safra_contract.ESTIMATIVA_SAFRA_V3_1.empty_frame()
        assert df.empty
        assert df.columns.tolist() == esperado.columns.tolist()
        assert df.dtypes.equals(esperado.dtypes)
        assert meta.attempted_sources == ["conab", "ibge_lspa"]
        assert [str(aviso.message) for aviso in avisos] == meta.validation_warnings
        assert "conab, ibge_lspa" in meta.validation_warnings[0]

    async def test_levantamento_so_consulta_a_conab_e_o_vazio_dela_basta(self):
        with isolated_dataset_case("levantamento"):
            dataset = _fontes_reais()
            with _conab(**CONAB_VAZIA), _lspa(**LSPA_VAZIO) as lspa, pytest.warns(UserWarning):
                df = await dataset.fetch("soja", safra="2022/23", levantamento=3)
        assert df.empty
        lspa.assert_not_awaited()

    async def test_vazio_mais_falha_de_rede_mantem_source_unavailable(self):
        with isolated_dataset_case("vazio_rede"):
            dataset = _fontes_reais()
            with (
                _conab(side_effect=httpx.ConnectError("fora")),
                _lspa(**LSPA_VAZIO),
                levanta_exatamente(SourceUnavailableError),
            ):
                await dataset.fetch("trigo", safra="2022/23")

    async def test_vazio_mais_falha_de_layout_mantem_parse_error(self):
        with isolated_dataset_case("vazio_layout"):
            dataset = _fontes_reais()
            with (
                _conab(side_effect=ParseError(source="conab", parser_version=7, reason="layout")),
                _lspa(**LSPA_VAZIO),
                levanta_exatamente(ParseError) as erro,
            ):
                await dataset.fetch("trigo", safra="2022/23")
        assert erro.value.parser_version == 7
        assert [nome for nome, _, _ in erro.value.errors] == ["conab", "ibge_lspa"]
