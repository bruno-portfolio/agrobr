import copy
import inspect
import sys
from datetime import date
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, patch

import openpyxl
import pandas as pd
import pytest

from agrobr import constants
from agrobr.conab.parsers.v1 import ConabParserV1
from agrobr.contracts import validate_dataset
from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.producao_anual import (
    PRODUCAO_ANUAL_INFO,
    ProducaoAnualDataset,
    _fetch_conab,
    producao_anual,
)
from agrobr.exceptions import SourceUnavailableError
from agrobr.ibge import pam_parser
from agrobr.utils import time as time_utils
from tests.helpers import collect_failures, fixture_instance, isolated_dataset_case

from .conftest import make_source, mock_source_meta


@pytest.fixture(autouse=True)
def _isolate_producao_sources(monkeypatch):
    monkeypatch.setattr(ProducaoAnualDataset, "info", copy.deepcopy(PRODUCAO_ANUAL_INFO))


_case_fixture__isolate_producao_sources = inspect.unwrap(_isolate_producao_sources)


def _mock_df():
    return pd.DataFrame(
        [
            {
                "ano": 2023,
                "localidade": "Mato Grosso",
                "produto": "soja",
                "area_plantada": 12000000.0,
                "area_colhida": 11900000.0,
                "producao": 43000000.0,
                "rendimento": 3583.0,
                "valor_producao": 200000000000.0,
                "fonte": "ibge_pam",
            },
        ]
    )


def _mock_conab_df():
    return pd.DataFrame(
        [
            {
                "fonte": "conab",
                "produto": "soja",
                "safra": "2022/23",
                "uf": "MT",
                "area_plantada": 12000.0,
                "area_colhida": 11981.0,
                "produtividade": 3583.0,
                "producao": 43000.0,
                "levantamento": 12,
                "data_publicacao": pd.Timestamp("2023-09-01"),
            }
        ]
    )


class TestProducaoAnualFetch:
    async def test_producao_anual_fetch_casos_1(self):
        with collect_failures() as check:
            case = "test_snapshot_sets_ano_minus_1"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(
                    _case_fixture__isolate_producao_sources, monkeypatch=monkeypatch
                ) as _isolate_producao_sources,
            ):
                dataset = ProducaoAnualDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-06-15"):
                    await dataset.fetch("soja")

                _, kwargs = mock_fn.call_args
                assert kwargs["ano"] == 2024
            case = "test_snapshot_does_not_override_explicit_ano"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(
                    _case_fixture__isolate_producao_sources, monkeypatch=monkeypatch
                ) as _isolate_producao_sources,
            ):
                dataset = ProducaoAnualDataset()
                mock_fn = make_source(_mock_df())
                dataset.info.sources[0].fetch_fn = mock_fn

                async with deterministic("2025-06-15"):
                    await dataset.fetch("soja", ano=2022)

                _, kwargs = mock_fn.call_args
                assert kwargs["ano"] == 2022


class TestProducaoAnualNormalize:
    async def test_producao_anual_normalize_casos_1(self):
        with collect_failures() as check:
            case = "test_normalize_adds_produto_fonte"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(
                    _case_fixture__isolate_producao_sources, monkeypatch=monkeypatch
                ) as _isolate_producao_sources,
            ):
                df = _mock_df().drop(columns=["produto", "fonte"])
                dataset = ProducaoAnualDataset()
                dataset.info.sources[0].fetch_fn = make_source(df)

                result = await dataset.fetch("soja")

                assert result["produto"].iloc[0] == "soja"
                assert result["fonte"].iloc[0] == "ibge_pam"
            case = "test_normalize_keeps_existing_produto_fonte"
            with (
                check(case),
                isolated_dataset_case(case) as monkeypatch,
                fixture_instance(
                    _case_fixture__isolate_producao_sources, monkeypatch=monkeypatch
                ) as _isolate_producao_sources,
            ):
                dataset = ProducaoAnualDataset()
                dataset.info.sources[0].fetch_fn = make_source(_mock_df())

                result = await dataset.fetch("soja")

                assert result["produto"].iloc[0] == "soja"
                assert result["fonte"].iloc[0] == "ibge_pam"


class TestProducaoAnualFallback:
    @pytest.mark.asyncio
    async def test_conab_aggregates_brasil(self):
        conab_df = pd.concat(
            [
                _mock_conab_df(),
                _mock_conab_df().assign(
                    uf="PR",
                    area_plantada=6000.0,
                    area_colhida=5900.0,
                    producao=22000.0,
                    produtividade=3729.0,
                ),
            ],
            ignore_index=True,
        )

        with patch(
            "agrobr.conab.safras",
            new_callable=AsyncMock,
            return_value=(conab_df, mock_source_meta()),
        ) as mock_safras:
            df, _ = await _fetch_conab("soja", ano=2023, nivel="brasil", uf="MT")

        assert len(df) == 1
        assert df.iloc[0]["localidade"] == "Brasil"
        assert df.iloc[0]["area_plantada"] == pytest.approx(18_000_000.0)
        assert pd.isna(df.iloc[0]["area_colhida"])
        assert df.iloc[0]["producao"] == pytest.approx(65_000_000.0)
        assert df.iloc[0]["rendimento"] == pytest.approx(65_000_000 * 1000 / 18_000_000)
        mock_safras.assert_awaited_once_with(
            "soja",
            safra="2022/23",
            uf=None,
            return_meta=True,
        )

    @pytest.mark.asyncio
    async def test_conab_brasil_uf_sem_a_cultura_entra_como_zero_e_parte_ausente_anula(self):
        boletim = (
            Path(__file__).parents[1] / "golden_data/reconciliacao_r3_20260918/c3c31afbe3e52d92.xls"
        )
        linhas = ConabParserV1().parse_safra_produto(BytesIO(boletim.read_bytes()), "milho_3")
        publicado = pd.DataFrame([s.model_dump() for s in linhas if s.safra == "2018/19" and s.uf])
        sem_cultura = publicado["area_plantada"].isna() & publicado["producao"].eq(0)
        assert sem_cultura.sum() == 22

        meta = mock_source_meta()
        with patch("agrobr.conab.safras", new_callable=AsyncMock, return_value=(publicado, meta)):
            df, meta_saida = await _fetch_conab("milho_3", ano=2019, nivel="brasil")
        assert df.iloc[0]["area_plantada"] == pytest.approx(511_000.0)
        assert df.iloc[0]["producao"] == pytest.approx(1_218_700.0)
        assert df.iloc[0]["rendimento"] == pytest.approx(1_218_700 * 1000 / 511_000)
        assert not [a for a in meta_saida.validation_warnings if a.startswith("producao_anual:")]

        misto = publicado[~sem_cultura].copy()
        misto.loc[misto["uf"] == "BA", "area_plantada"] = None
        with patch(
            "agrobr.conab.safras", new_callable=AsyncMock, return_value=(misto, mock_source_meta())
        ):
            df, meta_saida = await _fetch_conab("milho_3", ano=2019, nivel="brasil")
        assert pd.isna(df.iloc[0]["area_plantada"]) and pd.isna(df.iloc[0]["rendimento"])
        assert df.iloc[0]["producao"] == pytest.approx(1_218_700.0)
        assert [a for a in meta_saida.validation_warnings if a.startswith("producao_anual:")] == [
            "producao_anual: area_plantada do Brasil (CONAB) sai nula: sem o valor em Bahia; "
            "a soma das UFs conhecidas não é o total"
        ]


class TestProducaoAnualFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_conab_normalizes_real_golden(self):
        path = Path(__file__).parents[1] / "golden_data" / "reconciliacao_r3_20260918"
        metadata = {
            "url": "golden://reconciliacao_r3_20260918/7cd4df7946e5c57f.xlsx",
            "safra": "2025/26",
            "levantamento": 12,
        }

        with (
            patch(
                "agrobr.conab.client.fetch_safra_xlsx",
                new_callable=AsyncMock,
                return_value=(BytesIO((path / "7cd4df7946e5c57f.xlsx").read_bytes()), metadata),
            ) as mock_fetch,
            patch("agrobr.conab.api._hoje", return_value=date(2026, 9, 18)),
        ):
            result_df, _ = await _fetch_conab("soja", ano=2026, nivel="uf", uf="MT")

        assert result_df.columns.tolist() == [
            "ano",
            "localidade",
            "produto",
            "area_plantada",
            "area_colhida",
            "producao",
            "rendimento",
            "valor_producao",
            "fonte",
            "unidade_producao",
            "unidade_rendimento",
            "unidade_valor_producao",
            "condicao_produto",
        ]
        assert len(result_df) == 1
        row = result_df.iloc[0]
        assert row["ano"] == 2026
        assert row["localidade"] == "Mato Grosso"
        assert row["area_plantada"] == pytest.approx(13_006_200.0)
        assert pd.isna(row["area_colhida"])
        assert row["producao"] == pytest.approx(51_621_600.0)
        assert row["rendimento"] == pytest.approx(3969.0)
        assert pd.isna(row["valor_producao"])
        assert row["fonte"] == "conab"
        validate_dataset(result_df, "producao_anual")
        mock_fetch.assert_awaited_once_with(safra="2025/26", levantamento=None)


class TestProducaoAnualSpecific:
    @pytest.mark.asyncio
    async def test_nivel_invalido_raises(self):
        with pytest.raises(ValueError, match="nível inválido"):
            await producao_anual("soja", nivel=51)
        with pytest.raises(ValueError, match="nível inválido"):
            await producao_anual("soja", nivel="estado")


@pytest.mark.asyncio
async def test_fallback_conab_sem_ano_entrega_a_ultima_safra_fechada(monkeypatch):
    golden = (
        Path(__file__).parents[1] / "golden_data/reconciliacao_r3_20260918/7cd4df7946e5c57f.xlsx"
    )
    livro = openpyxl.load_workbook(golden, read_only=True, data_only=True)
    aba = livro["Trigo"]
    assert (aba["H6"].value, aba["I6"].value, aba["A42"].value, aba["A44"].value) == (
        "Safra 2025",
        "Safra 2026",
        "BRASIL",
        "Nota: Estimativa em setembro/2026.",
    )
    ufs = [
        linha
        for linha in aba.iter_rows(min_row=8, max_row=41)
        if linha[0].value in constants.CONAB_UFS
    ]
    fechada = sum(linha[7].value or 0 for linha in ufs)
    corrente = sum(linha[8].value or 0 for linha in ufs)
    assert (fechada, corrente) == (pytest.approx(aba["H42"].value), pytest.approx(aba["I42"].value))
    edicao = {
        "url": "golden://reconciliacao_r3_20260918/7cd4df7946e5c57f.xlsx",
        "levantamento": 12,
        "safra": "2025/26",
        "ano_inicio": 2025,
        "ano_fim": 26,
        "data_publicacao": date(2026, 9, 15),
    }
    fetch = AsyncMock(return_value=(BytesIO(golden.read_bytes()), dict(edicao)))
    monkeypatch.setattr("agrobr.conab.client.list_levantamentos", AsyncMock(return_value=[edicao]))
    monkeypatch.setattr("agrobr.conab.client.fetch_safra_xlsx", fetch)
    monkeypatch.setattr("agrobr.conab.api._hoje", lambda: date(2026, 9, 25))
    monkeypatch.setattr(
        sys.modules[ProducaoAnualDataset.__module__], "_hoje", lambda: date(2026, 9, 25)
    )
    ibge_fora = AsyncMock(
        side_effect=SourceUnavailableError(source="ibge_pam", last_error="fora do ar")
    )
    fonte_ibge = ProducaoAnualDataset.info.sources[0]
    monkeypatch.setattr(fonte_ibge, "fetch_fn", ibge_fora)

    frame = await producao_anual("trigo", nivel="brasil")

    ibge_fora.assert_awaited_once()
    assert frame[["ano", "fonte"]].to_dict("records") == [{"ano": 2025, "fonte": "conab"}]
    assert frame["producao"].iloc[0] == pytest.approx(fechada * 1000)
    assert frame["producao"].iloc[0] != pytest.approx(corrente * 1000)
    fetch.assert_awaited_once_with(safra="2024/25", levantamento=None)


@pytest.mark.asyncio
async def test_fallback_conab_sem_ano_usa_o_ano_civil_anterior_de_hoje(monkeypatch):
    fetch = AsyncMock(side_effect=SourceUnavailableError(source="conab", last_error="sem rede"))
    monkeypatch.setattr("agrobr.conab.client.fetch_safra_xlsx", fetch)
    monkeypatch.setattr("agrobr.conab.client.list_levantamentos", AsyncMock(return_value=[]))
    with pytest.raises(SourceUnavailableError):
        await _fetch_conab("trigo", nivel="brasil")
    ano = time_utils.hoje().year - 1
    fetch.assert_awaited_once_with(safra=f"{ano - 1}/{ano % 100:02d}", levantamento=None)


async def test_as_polars_empilha_produtos_com_e_sem_condicao_do_produto(monkeypatch):
    pl = pytest.importorskip("polars")
    fonte_ibge = ProducaoAnualDataset.info.sources[0]
    frames = []
    for produto in ("soja", "cafe"):
        fonte = pam_parser.add_unit_columns(_mock_df().assign(produto=produto), produto)
        monkeypatch.setattr(fonte_ibge, "fetch_fn", make_source(fonte))
        frames.append(await producao_anual(produto, ano=2023, as_polars=True))
    soja, cafe = frames
    assert soja["condicao_produto"].to_list() == [None]
    assert cafe["condicao_produto"].to_list() == ["beneficiado"]
    assert soja.schema == cafe.schema
    assert pl.concat(frames)["condicao_produto"].to_list() == [None, "beneficiado"]
