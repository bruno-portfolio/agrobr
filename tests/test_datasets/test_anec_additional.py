from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd

from agrobr import anec, contracts, datasets
from agrobr.models import MetaInfo
from tests.helpers import collect_failures, isolated_dataset_case

_DATASETS = [
    ("embarques_mensais_anec", "embarques_mensais"),
    ("comparacao_anual_anec", "comparacao_anual"),
    ("destinos_anec", "destinos"),
]


def _source_frame(source: str) -> pd.DataFrame:
    if source == "embarques_mensais":
        frame = pd.DataFrame(
            {
                "ano": pd.Series([2025, 2026], dtype="Int64"),
                "mes": pd.Series([12, 3], dtype="Int64"),
                "produto": ["soybean", "soybean"],
                "valor_ton": pd.Series([100.0, pd.NA], dtype="Float64"),
                "valor_min_ton": pd.Series([pd.NA, 200.0], dtype="Float64"),
                "valor_max_ton": pd.Series([pd.NA, 300.0], dtype="Float64"),
                "eh_estimativa": [False, True],
            }
        )
    elif source == "comparacao_anual":
        frame = pd.DataFrame(
            {
                "mes": pd.Series([3, 3], dtype="Int64"),
                "produto": ["soybean", "total_products"],
                "ano_base": pd.Series([2025, 2025], dtype="Int64"),
                "ano_comparacao": pd.Series([2026, 2026], dtype="Int64"),
                "valor_2025": pd.Series([100.0, 100.0], dtype="Float64"),
                "valor_2026": pd.Series([pd.NA, pd.NA], dtype="Float64"),
                "valor_base_ton": pd.Series([100.0, 100.0], dtype="Float64"),
                "valor_comparacao_ton": pd.Series([pd.NA, pd.NA], dtype="Float64"),
                "eh_estimativa": [True, True],
            }
        )
    else:
        frame = pd.DataFrame(
            {
                "produto": ["soybean", "soybean"],
                "destino": ["CHINA", "OTHERS"],
                "share_pct": pd.Series([75.0, 25.0], dtype="Float64"),
                "ano": pd.Series([2025, 2025], dtype="Int64"),
                "mes_inicio": pd.Series([1, 1], dtype="Int64"),
                "mes_fim": pd.Series([12, 12], dtype="Int64"),
            }
        )
    frame["ano_relatorio"] = pd.Series([2026] * len(frame), dtype="Int64")
    frame["semana_relatorio"] = pd.Series([12] * len(frame), dtype="Int64")
    frame["edicao_id"] = "edition-week-12"
    frame["publicado_em"] = pd.Timestamp("2026-03-24T12:00:00Z").as_unit("ns")
    frame["revisado_em"] = pd.Timestamp("2026-03-25T13:30:00Z").as_unit("ns")
    return frame


def _source_meta(frame: pd.DataFrame) -> MetaInfo:
    return MetaInfo(
        source="anec",
        source_url="https://www.anec.com.br/uploads/edition-week-12.pdf",
        source_method="httpx+pdfplumber",
        fetched_at=datetime(2026, 3, 25, 14, tzinfo=UTC),
        records_count=len(frame),
        columns=frame.columns.tolist(),
        from_cache=True,
        parser_version=2,
        schema_version="1.2" if "ano_base" in frame else "1.1",
        selected_source="anec",
        attempted_sources=["anec"],
        validation_warnings=["Recorte de teste com valores ausentes publicados"],
    )


async def test_anec_additional_casos_1():
    with collect_failures() as check:
        for name, source in _DATASETS:
            case = f"test_additional_dataset_preserves_source_provenance[{(name, source)!r}]"
            with check(case), isolated_dataset_case(case):
                raw = _source_frame(source)
                source_meta = _source_meta(raw)
                with patch.object(
                    anec, source, new_callable=AsyncMock, return_value=(raw, source_meta)
                ) as fetch:
                    frame, meta = await getattr(datasets, name)(
                        ano=2026, semana=12, produto="soja", use_cache=False, return_meta=True
                    )
                assert fetch.await_count == 1
                assert fetch.call_args.kwargs["ano"] == 2026
                assert fetch.call_args.kwargs["semana"] == 12
                assert fetch.call_args.kwargs["produto"] in ("soja", "soybean")
                assert fetch.call_args.kwargs["use_cache"] is False
                assert fetch.call_args.kwargs["return_meta"] is True
                pd.testing.assert_frame_equal(
                    frame[
                        [
                            "ano_relatorio",
                            "semana_relatorio",
                            "edicao_id",
                            "publicado_em",
                            "revisado_em",
                        ]
                    ],
                    raw[
                        [
                            "ano_relatorio",
                            "semana_relatorio",
                            "edicao_id",
                            "publicado_em",
                            "revisado_em",
                        ]
                    ],
                )
                assert meta.dataset == name
                assert meta.contract_version == "1.0"
                assert meta.schema_version == "1.0"
                assert meta.source_url == source_meta.source_url
                assert meta.fetched_at == source_meta.fetched_at
                assert meta.parser_version == source_meta.parser_version
                assert meta.validation_warnings == source_meta.validation_warnings
                assert meta.from_cache
                assert meta.selected_source == "anec"
                assert meta.attempted_sources == ["anec"]
                assert meta.records_count == len(frame)
                contracts.validate_dataset(frame, name)
        for name, source in _DATASETS:
            case = f"test_additional_dataset_empty_retains_contract_and_warning[{(name, source)!r}]"
            with check(case), isolated_dataset_case(case):
                raw = _source_frame(source).iloc[:0].copy()
                source_meta = _source_meta(raw)
                with patch.object(
                    anec, source, new_callable=AsyncMock, return_value=(raw, source_meta)
                ):
                    frame, meta = await getattr(datasets, name)(ano=2026, return_meta=True)
                assert frame.empty
                assert meta.records_count == 0
                assert meta.validation_warnings == source_meta.validation_warnings
                assert str(frame["ano_relatorio"].dtype) == "Int64"
                assert str(frame["publicado_em"].dt.tz) == "UTC"
                value_column = {
                    "embarques_mensais": "valor_ton",
                    "comparacao_anual": "valor_base_ton",
                }.get(source, "share_pct")
                assert str(frame[value_column].dtype) == "Float64"
                contracts.validate_dataset(frame, name)
        for name, source in _DATASETS:
            case = f"test_additional_contract_accepts_separate_revisions[{(name, source)!r}]"
            with check(case), isolated_dataset_case(case):
                raw = _source_frame(source)
                with patch.object(anec, source, new_callable=AsyncMock, return_value=raw):
                    original = await getattr(datasets, name)(ano=2026)
                revised = original.copy()
                revised["revisado_em"] = pd.Timestamp("2026-03-26T13:30:00Z")
                history = pd.concat([original, revised], ignore_index=True)
                contracts.validate_dataset(history, name)
                assert len(history) == 2 * len(original)


async def test_anec_additional_casos_2():
    with collect_failures() as check:
        case = "test_comparison_dataset_normalizes_year_columns_without_filling_missing"
        with check(case), isolated_dataset_case(case):
            raw = _source_frame("comparacao_anual")
            with patch.object(anec, "comparacao_anual", new_callable=AsyncMock, return_value=raw):
                frame = await datasets.comparacao_anual_anec(ano=2026)
            assert frame["valor_base_ton"].tolist() == [100.0, 100.0]
            assert frame["valor_comparacao_ton"].isna().all()
            assert frame["ano_base"].tolist() == [2025, 2025]
            assert frame["ano_comparacao"].tolist() == [2026, 2026]
            assert frame["eh_estimativa"].all()
            assert "valor_2025" not in frame
            assert "valor_2026" not in frame
        case = "test_comparison_dataset_accepts_total_products"
        with check(case), isolated_dataset_case(case):
            raw = _source_frame("comparacao_anual").iloc[[1]].reset_index(drop=True)
            with patch.object(
                anec, "comparacao_anual", new_callable=AsyncMock, return_value=raw
            ) as fetch:
                frame = await datasets.comparacao_anual_anec(ano=2026, produto="total_products")
            assert fetch.call_args.kwargs["produto"] == "total_products"
            assert frame["produto"].tolist() == ["total_products"]
