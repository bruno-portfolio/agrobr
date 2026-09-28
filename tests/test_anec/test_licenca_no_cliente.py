from __future__ import annotations

import contextlib
import warnings
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from agrobr.anec import client
from agrobr.anec.models import ANECArticle
from agrobr.utils.warnings import warn_once_reset

ARTIGO = ANECArticle(
    id=1,
    cuid="c1",
    title_en="Weekly shipments",
    slug_en="weekly-shipments",
    created_at=datetime(2026, 1, 5, tzinfo=UTC),
    pdf_url="https://anec.com.br/uploads/relatorio.pdf",
    media_updated_at=datetime(2026, 1, 5, tzinfo=UTC),
)


class _SemRede(Exception):
    pass


def _sem_rede(*_args: object, **_kwargs: object) -> None:
    raise _SemRede


@pytest.mark.parametrize(
    "chamar",
    [
        lambda: client.list_articles(2026),
        lambda: client.fetch_latest_pdf(use_cache=False),
        lambda: client.fetch_pdf_bytes(ARTIGO, use_cache=False),
    ],
    ids=["list_articles", "fetch_latest_pdf", "fetch_pdf_bytes"],
)
async def test_funcao_publica_do_cliente_avisa_a_licenca_zona_cinza(chamar):
    warn_once_reset("anec_license")
    client._LIST_CACHE.clear()
    with (
        warnings.catch_warnings(record=True) as avisos,
        patch.object(client.httpx, "AsyncClient", side_effect=_sem_rede),
    ):
        warnings.simplefilter("always")
        with contextlib.suppress(Exception):
            await chamar()

    assert [str(aviso.message) for aviso in avisos if "ANEC" in str(aviso.message)] == [
        "ANEC publica os dados sem termos de uso explícitos (zona_cinza). "
        "Uso comercial pode requerer autorização da associação."
    ]
