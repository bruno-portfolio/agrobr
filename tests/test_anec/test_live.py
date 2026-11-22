"""Live tests contra ANEC — rodam com `pytest -m integration`.

Detectam quebras upstream:
- Sitemap da biblioteca ou página do boletim semanal mudou
- Layout do PDF mudou (fingerprint diverge do baseline conhecido)
- Site fora do ar

Não rodam em CI default (deselected via `addopts = "-m 'not benchmark and not slow'"`).
Rode manualmente: `pytest tests/test_anec/test_live.py -m integration -v`.

Os 5 testes que dependem do PDF compartilham um unico fetch+parse via
`_get_latest_report()` (cache module-level), evitando download repetido contra
o servidor ANEC zona_cinza.
"""

from __future__ import annotations

from typing import Any

import pytest

from agrobr.anec import client, parser

_latest_cache: tuple[bytes, Any, parser.ParsedReport] | None = None


async def _get_latest_report() -> tuple[bytes, Any, parser.ParsedReport]:
    global _latest_cache
    if _latest_cache is None:
        pdf_bytes, _, article = await client.fetch_latest_pdf(year=2026, use_cache=False)
        report = parser.parse_anec_pdf(pdf_bytes)
        _latest_cache = (pdf_bytes, article, report)
    return _latest_cache


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_list_2026_articles_minimum():
    articles = await client.list_articles(2026)
    assert articles, (
        "Nenhum boletim semanal de 2026 no sitemap da biblioteca ANEC. "
        "Possível: slug do boletim mudou, site reorganizado, ou site fora."
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_fetch_latest_pdf_size():
    pdf_bytes, article, _ = await _get_latest_report()
    assert len(pdf_bytes) > 100_000, (
        f"PDF de {article.title_en!r} muito pequeno ({len(pdf_bytes)} bytes). "
        "Possível: ANEC publicou versão preview ou layout reduzido."
    )
    assert pdf_bytes.startswith(b"%PDF"), "Magic bytes %PDF ausentes — não é PDF"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_parse_latest_full():
    _, _, report = await _get_latest_report()

    assert len(report.weekly_shipments) > 0, "weekly_shipments vazio — parser falhou"
    assert report.weekly_shipments["porto"].nunique() >= 15, (
        "menos de 15 portos detectados — possível mudança de layout"
    )

    expected_produtos = set(parser.PRODUCT_HEADER_ORDER)
    actual_produtos = set(report.weekly_shipments["produto"].unique())
    assert actual_produtos == expected_produtos, (
        f"set de produtos drift: faltam {expected_produtos - actual_produtos}, "
        f"extras {actual_produtos - expected_produtos}"
    )

    assert len(report.monthly_shipments) > 0
    assert len(report.fingerprint) == 16


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_destinations_not_empty():
    _, _, report = await _get_latest_report()

    assert len(report.destinations) > 0, (
        "destinations vazio — possível: header 'Brazilian ... Importers' sumiu/renomeado, "
        "ou layout das paginas de destinos mudou"
    )
    expected_cols = {"produto", "destino", "share_pct"}
    assert expected_cols.issubset(set(report.destinations.columns)), (
        f"schema destinations drift: faltam {expected_cols - set(report.destinations.columns)}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_yoy_comparison_not_empty():
    _, _, report = await _get_latest_report()

    assert len(report.yoy_comparison) > 0, (
        "yoy_comparison vazio — possível: header 'YoY Comparison' sumiu/renomeado, "
        "ou layout das paginas YoY mudou"
    )
    expected_cols = {"mes", "produto", "valor_2025", "valor_2026"}
    assert expected_cols.issubset(set(report.yoy_comparison.columns)), (
        f"schema yoy drift: faltam {expected_cols - set(report.yoy_comparison.columns)}"
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_live_fingerprint_format_stable():
    _, _, report = await _get_latest_report()

    assert isinstance(report.fingerprint, str)
    assert len(report.fingerprint) == 16
    assert all(c in "0123456789abcdef" for c in report.fingerprint), (
        f"Fingerprint não-hex: {report.fingerprint!r}"
    )
