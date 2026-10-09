from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import pytest

from agrobr import anec, datasets, exceptions
from agrobr.anec import parser
from tests.helpers import collect_failures

GOLDEN = Path(__file__).parents[1] / "golden_data/reconciliacao_boletins_anec_anda_deral_20260918"
MANIFEST = json.loads((GOLDEN / "anec_manifest.json").read_text(encoding="utf-8"))
SEMANAS = json.loads((GOLDEN.parent / "anec/semanas_20260925/manifest.json").read_bytes())
SEMANA_30_DE_2026 = pd.Timestamp(SEMANAS["calendario"]["inicio_da_semana_30_de_2026"])
CASES = MANIFEST["cases"]
TABLES = {
    "weekly_shipments": ("embarques", "embarques_anec"),
    "monthly_shipments": ("embarques_mensais", "embarques_mensais_anec"),
    "yoy_comparison": ("comparacao_anual", "comparacao_anual_anec"),
    "destinations": ("destinos", "destinos_anec"),
}
KEYS = {
    "weekly_shipments": ["porto", "produto", "periodo"],
    "monthly_shipments": ["ano", "mes", "produto"],
    "yoy_comparison": ["mes", "produto"],
    "destinations": ["produto", "destino", "ano", "mes_inicio", "mes_fim"],
}
PRODUCT_SELECTORS = [
    ("monthly_shipments", "milho", "maize"),
    ("yoy_comparison", "total_products", "total_products"),
    ("destinations", "farelo de soja", "soybean_meal"),
]
WEEKLY_SELECTORS = [
    ("paranagua", "PARANAGUÁ", "soja", "soybean", "efetivado", "last_week"),
    (
        "BARRA DOS COQUEIROS",
        "BARRA DOS COQUEIROS",
        "sorghum",
        "sorghum",
        "programado",
        "current_week",
    ),
    ("porto inexistente", None, "soybean", "soybean", "efetivado", "last_week"),
]
REAL_CLIENT = httpx.AsyncClient


def _case(week: int) -> dict[str, Any]:
    return next(case for case in CASES if case["article"]["week"] == week)


def _expected(case: dict[str, Any], table: str, target: str) -> pd.DataFrame:
    data = case["tables"][table]
    rows = []
    if table == "weekly_shipments":
        article = case["article"]
        for row in data["rows"]:
            for index, value in enumerate(row["values"]):
                inicio = SEMANA_30_DE_2026 + pd.Timedelta(weeks=article["week"] - 30 + index // 6)
                rows.append(
                    {
                        "porto": row["porto"],
                        "produto": data["products"][index % 6],
                        "periodo": data["periods"][index // 6],
                        "valor_ton": value,
                        "ano": article["year"],
                        "semana": article["week"],
                        "data_inicio": inicio,
                        "data_fim": inicio + pd.Timedelta(days=6),
                    }
                )
    elif table == "monthly_shipments":
        for row in data["rows"]:
            for product, value in zip(data["products"][:6], row["values"][:6], strict=True):
                interval = isinstance(value, list)
                rows.append(
                    {
                        "ano": data["ano"],
                        "mes": row["mes"],
                        "produto": product,
                        "valor_ton": None if interval else value,
                        "eh_estimativa": row["eh_estimativa"],
                        "valor_min_ton": value[0] if interval else None,
                        "valor_max_ton": value[1] if interval else None,
                    }
                )
    elif table == "yoy_comparison":
        for section in data:
            for row in section["rows"]:
                rows.append(
                    {
                        "mes": row["mes"],
                        "produto": section["produto"],
                        "valor_2025": row["values"][0],
                        "valor_2026": row["values"][1],
                        "valor_base_ton": row["values"][0],
                        "valor_comparacao_ton": row["values"][1],
                        "ano_base": section["years"][0],
                        "ano_comparacao": section["years"][1],
                        "eh_estimativa": row["eh_estimativa"],
                    }
                )
    else:
        for section in data:
            for row in section["rows"]:
                rows.append(
                    {
                        "produto": section["produto"],
                        "destino": row["destino"],
                        "share_pct": row["value"],
                        "ano": section["ano"],
                        "mes_inicio": section["mes_inicio"],
                        "mes_fim": section["mes_fim"],
                    }
                )
    frame = pd.DataFrame(rows)
    if table == "destinations" and not rows:
        frame = pd.DataFrame(
            columns=["produto", "destino", "share_pct", "ano", "mes_inicio", "mes_fim"]
        )
    if target == "dataset" and table == "yoy_comparison":
        frame = frame.drop(columns=["valor_2025", "valor_2026"])
    if target != "parser" and table != "weekly_shipments":
        article = case["article"]
        frame["ano_relatorio"] = article["year"]
        frame["semana_relatorio"] = article["week"]
        frame["edicao_id"] = article["cuid"]
        frame["publicado_em"] = pd.Timestamp(article["created_at"])
        frame["revisado_em"] = pd.Timestamp(article["media_updated_at"])
    return frame


def _assert_frame(frame: pd.DataFrame, expected: pd.DataFrame, table: str) -> None:
    keys = KEYS[table]
    assert not frame.duplicated(keys).any()
    pd.testing.assert_frame_equal(
        frame.sort_values(keys).reset_index(drop=True),
        expected.sort_values(keys).reset_index(drop=True),
        check_dtype=False,
        check_exact=True,
        check_like=True,
    )


@pytest.fixture
def transport(monkeypatch: pytest.MonkeyPatch) -> set[str]:
    monkeypatch.setenv("AGROBR_ANEC_LIST_TTL", "0")
    responses = {
        receipt["url"]: ((GOLDEN / receipt["file"]).read_bytes(), "text/html; charset=utf-8")
        for receipt in MANIFEST["catalog"]
    }
    for case in CASES:
        responses[case["url"]] = (GOLDEN / case["file"]).read_bytes(), "application/pdf"
    seen: set[str] = set()

    def handle(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        assert url in responses, url
        seen.add(url)
        content, content_type = responses[url]
        return httpx.Response(200, content=content, headers={"content-type": content_type})

    class Client(REAL_CLIENT):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            kwargs["transport"] = httpx.MockTransport(handle)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Client)
    return seen


async def _fetch(case: dict[str, Any], table: str, target: str, **kwargs: Any) -> pd.DataFrame:
    options = {"ano": case["article"]["year"], "semana": case["article"]["week"], **kwargs}
    namespace, method = (
        (anec, TABLES[table][0]) if target == "source" else (datasets, TABLES[table][1])
    )
    frame, meta = await getattr(namespace, method)(return_meta=True, **options)
    assert meta.records_count == len(frame)
    assert meta.selected_source == "anec"
    assert meta.attempted_sources == ["anec"]
    if table == "destinations" and frame.empty:
        assert any("não confirma ausência" in warning for warning in meta.validation_warnings)
    return frame


def test_pdf_original_todas_linhas_celulas_periodos_e_edicao():
    fingerprints = {}
    with collect_failures() as check:
        for case in CASES:
            with check(case["id"]):
                report = parser.parse_anec_pdf((GOLDEN / case["file"]).read_bytes())
                fingerprints[case["id"]] = report.fingerprint
                for table in TABLES:
                    with check(f"{case['id']}-{table}"):
                        _assert_frame(
                            getattr(report, table), _expected(case, table, "parser"), table
                        )
        with check("fingerprint"):
            assert len(set(fingerprints.values())) == len(CASES)


@pytest.mark.parametrize("target", ["source", "dataset"])
async def test_fonte_e_dataset_preservam_celulas_edicao_e_seletores(target, transport):
    edition, without_destinations, latest = _case(34), _case(8), CASES[-1]
    with collect_failures() as check:
        for table in TABLES:
            with check(table):
                frame = await _fetch(edition, table, target)
                _assert_frame(frame, _expected(edition, table, target), table)
        for table, selector, product in PRODUCT_SELECTORS:
            with check(f"{table}-{selector}"):
                frame = await _fetch(edition, table, target, produto=selector)
                expected = _expected(edition, table, target)
                _assert_frame(frame, expected[expected["produto"] == product], table)
        for port, canonical, selector, product, kind, period in WEEKLY_SELECTORS:
            with check(f"{port}-{selector}-{kind}"):
                frame = await _fetch(
                    edition, "weekly_shipments", target, porto=port, produto=selector, tipo=kind
                )
                expected = _expected(edition, "weekly_shipments", target)
                expected = expected[
                    (expected["porto"] == canonical)
                    & (expected["produto"] == product)
                    & (expected["periodo"] == period)
                ]
                _assert_frame(frame, expected, "weekly_shipments")
        with check("destinos em imagem"):
            frame = await _fetch(without_destinations, "destinations", target)
            _assert_frame(
                frame, _expected(without_destinations, "destinations", target), "destinations"
            )
        with check("edição mais recente"):
            frame = await _fetch(latest, "monthly_shipments", target, semana=None)
            _assert_frame(
                frame, _expected(latest, "monthly_shipments", target), "monthly_shipments"
            )
        with check("downloads"):
            catalog = {receipt["url"] for receipt in MANIFEST["catalog"]}
            assert transport == catalog | {
                edition["url"],
                without_destinations["url"],
                latest["url"],
            }


def test_cabecalho_incompleto_ou_ano_incoerente_recusa():
    with collect_failures() as check:
        for mutation, message in [
            ("weekly_missing_sorghum", "Cabeçalhos semanais incompatíveis"),
            ("monthly_missing_ddgs", "Cabeçalhos mensais incompatíveis"),
            ("yoy_wrong_year", "pares de anos consecutivos"),
        ]:
            with check(mutation), pytest.raises(exceptions.ParseError, match=message):
                parser.parse_anec_pdf((GOLDEN / f"anec/mutations/{mutation}.pdf").read_bytes())
