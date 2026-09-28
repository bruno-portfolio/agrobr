from __future__ import annotations

import copy
import warnings
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.anec import api, parser
from agrobr.anec.models import ANECArticle
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.utils import result
from agrobr.utils.warnings import warn_once_reset
from tests.helpers import collect_failures, make_mock_async_client, make_mock_response

ARTICLE = ANECArticle(
    id=999,
    cuid="cuid-w04",
    title_en="ANEC - 04.2026 Accumulated Exports",
    slug_en="anec-042026-accumulated-exports",
    created_at=datetime(2026, 2, 5, tzinfo=UTC),
    pdf_url="https://www.anec.com.br/uploads/test-w04.pdf",
    media_updated_at=datetime(2026, 2, 5, tzinfo=UTC),
)
FUNCTIONS = ["embarques", "embarques_mensais", "comparacao_anual", "destinos"]


def _report() -> parser.ParsedReport:
    return parser.ParsedReport(
        pd.DataFrame(columns=["porto", "produto", "periodo", "valor_ton"]),
        pd.DataFrame(
            columns=[
                "ano",
                "mes",
                "produto",
                "valor_ton",
                "eh_estimativa",
                "valor_min_ton",
                "valor_max_ton",
            ]
        ),
        parser._parse_yoy_comparison([], []),
        parser._parse_destinations([], []),
        "abc123",
    )


def _source() -> AsyncMock:
    return AsyncMock(return_value=(_report(), _aquisicao(ARTICLE.pdf_url), ARTICLE))


def _aquisicao(url: str) -> api.client.Aquisicao:
    return api.client.Aquisicao(b"%PDF-1.7", url, False, datetime(2026, 3, 25, tzinfo=UTC), {})


async def test_parametro_desconhecido_recusado_antes_da_rede(monkeypatch):
    with collect_failures() as check:
        for name in FUNCTIONS:
            with check(name):
                source = _source()
                monkeypatch.setattr(api, "_fetch_and_parse", source)
                with pytest.raises(TypeError, match="ignorado"):
                    await getattr(api, name)(ano=2026, ignorado="soja")
                source.assert_not_awaited()


async def test_filtros_do_semanal_recusados_antes_da_rede(monkeypatch):
    with collect_failures() as check:
        for alvo, kwargs, mensagem in [
            (api.embarques, {"tipo": "invalido"}, "tipo inválido"),
            (api.embarques, {"produto": "cafe"}, "produto desconhecido"),
            (api.embarques, {"semana": 0}, "semana"),
            (api.embarques, {"semana": 54}, "semana"),
            (datasets.embarques_anec, {"produto": "cafe"}, "produto desconhecido"),
            (datasets.embarques_anec, {"tipo": "invalido"}, "tipo inválido"),
        ]:
            with check(f"{alvo.__name__}{kwargs}"):
                source = _source()
                monkeypatch.setattr(api, "_fetch_and_parse", source)
                with pytest.raises(InvalidParameterError, match=mensagem):
                    await alvo(ano=2026, **kwargs)
                source.assert_not_awaited()


async def test_artigo_fora_do_padrao_nao_derruba_a_listagem(
    monkeypatch, category_2026_p1_payload, html_factory
):
    monkeypatch.setenv("AGROBR_ANEC_LIST_TTL", "0")
    payload = copy.deepcopy(category_2026_p1_payload)
    artigos = payload["props"]["pageProps"]["paginatedArticles"]["articles"]
    esperados = [artigo["titleEN"] for artigo in artigos]
    del esperados[1]
    artigos[1]["titleEN"] = "ANEC - Annual Report 2026"
    payload["props"]["pageProps"]["paginatedArticles"]["total"] = len(artigos)
    mock_client = make_mock_async_client()
    mock_client.get = AsyncMock(return_value=make_mock_response(200, text=html_factory(payload)))
    with (
        patch("agrobr.anec.client.httpx.AsyncClient", return_value=mock_client),
        warnings.catch_warnings(record=True) as captured,
    ):
        warnings.simplefilter("always")
        listados = await api.client.list_articles(2026)
        assert [artigo.title_en for artigo in listados] == esperados
        disponiveis = await api.articles_disponiveis(2026)
        with (
            patch.object(
                api.client,
                "_acquire_pdf",
                new_callable=AsyncMock,
                return_value=_aquisicao("https://www.anec.com.br/uploads/w14.pdf"),
            ) as download,
            patch.object(api.parser, "parse_anec_pdf", return_value=_report()),
        ):
            await api.embarques(ano=2026, semana=14)
    assert [item["week"] for item in disponiveis] == [16, 14, 13, 12, 11, 10, 9, 8, 7]
    assert download.await_args.args[0].week_year == (14, 2026)
    assert any("ANEC - Annual Report 2026" in str(aviso.message) for aviso in captured)


async def test_semana_indisponivel():
    with (
        patch.object(api.client, "list_articles", new_callable=AsyncMock, return_value=[ARTICLE]),
        patch.object(
            api.client,
            "_acquire_pdf",
            new_callable=AsyncMock,
            return_value=_aquisicao(ARTICLE.pdf_url),
        ) as download,
        patch.object(api.parser, "parse_anec_pdf", return_value=_report()),
        pytest.raises(SourceUnavailableError, match="Semana 5/2026"),
    ):
        await api.embarques(ano=2026, semana=5)
    download.assert_not_awaited()


async def test_articles_disponiveis():
    with patch.object(api.client, "list_articles", new_callable=AsyncMock, return_value=[ARTICLE]):
        artigos = await api.articles_disponiveis(2026)
    assert artigos == [
        {
            "id": 999,
            "title": "ANEC - 04.2026 Accumulated Exports",
            "slug": "anec-042026-accumulated-exports",
            "pdf_url": "https://www.anec.com.br/uploads/test-w04.pdf",
            "created_at": "2026-02-05T00:00:00+00:00",
            "media_updated_at": "2026-02-05T00:00:00+00:00",
            "week": 4,
            "year": 2026,
        }
    ]


async def test_aviso_de_licenca_zona_cinza(monkeypatch):
    monkeypatch.setattr(api, "_fetch_and_parse", _source())
    with collect_failures() as check:
        for name in FUNCTIONS:
            with check(name):
                warn_once_reset("anec_license")
                with warnings.catch_warnings(record=True) as captured:
                    warnings.simplefilter("always")
                    await getattr(api, name)(ano=2026)
                assert any("zona_cinza" in str(w.message) for w in captured)


async def test_as_polars_encaminhado(monkeypatch):
    chamadas: list[tuple[str, bool]] = []

    def spy(origem: str):
        def finalize(frame, meta, *, as_polars, return_meta):
            chamadas.append((origem, as_polars))
            return (frame, meta) if return_meta else frame

        return finalize

    monkeypatch.setattr(api, "_fetch_and_parse", _source())
    monkeypatch.setattr(api, "finalize_result", spy("fonte"))
    monkeypatch.setattr(result, "finalize_result", spy("dataset"))
    await api.embarques(ano=2026, as_polars=True)
    await api.embarques_mensais(ano=2026, as_polars=True)
    await datasets.embarques_mensais_anec(ano=2026, as_polars=True)
    assert chamadas == [
        ("fonte", True),
        ("fonte", True),
        ("fonte", False),
        ("dataset", True),
    ]
