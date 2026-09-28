from __future__ import annotations

import warnings
from types import SimpleNamespace

import httpx
import pytest

from agrobr import comexstat
from agrobr.comexstat import client as comexstat_client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import comexstat_csv, levanta_exatamente, sem_excecao

CSV = comexstat_csv()
AVISO = "comexstat: tamanho do arquivo não conferido (sem Content-Length no GET nem no HEAD)"


def _servir(
    monkeypatch,
    *,
    corpo: bytes,
    get: dict[str, str],
    head: dict[str, str],
    head_status: int = 200,
    head_sem_rede: bool = False,
) -> list[str]:
    """O GET entrega ``corpo`` com os cabeçalhos ``get``; o HEAD do mesmo arquivo, os ``head``."""
    metodos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        metodos.append(request.method)
        if request.method == "HEAD" and head_sem_rede:
            raise httpx.ConnectError("sem rede", request=request)
        cabecalhos = head if request.method == "HEAD" else get
        return httpx.Response(
            head_status if request.method == "HEAD" else 200,
            headers={"Content-Type": "text/csv", "ETag": "synthetic", **cabecalhos},
            stream=httpx.ByteStream(b"" if request.method == "HEAD" else corpo),
            request=request,
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(comexstat_client, "httpx", namespace)
    return metodos


def _conferencia(meta) -> str | None:
    return meta.source_details["acquisition"].get("size_check")


async def test_get_com_content_length_confere_sem_head(monkeypatch):
    metodos = _servir(monkeypatch, corpo=CSV, get={"Content-Length": str(len(CSV))}, head={})

    with sem_excecao():
        _, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)

    assert metodos == ["GET"]
    assert _conferencia(meta) == "content_length"
    assert meta.validation_warnings == []


async def test_get_sem_content_length_confere_pelo_head(monkeypatch):
    metodos = _servir(monkeypatch, corpo=CSV, get={}, head={"Content-Length": str(len(CSV))})

    with sem_excecao():
        _, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)

    assert metodos == ["GET", "HEAD"]
    assert _conferencia(meta) == "head"
    assert meta.validation_warnings == []


async def test_corte_na_quebra_de_linha_sem_content_length_e_recusado_pelo_head(monkeypatch):
    corte = CSV.rindex(b"\n", 0, len(CSV) - 1) + 1
    _servir(monkeypatch, corpo=CSV[:corte], get={}, head={"Content-Length": str(len(CSV))})

    with levanta_exatamente(SourceUnavailableError, "Content-Length diverge"):
        await comexstat.exportacao("soja", ano=2024)


@pytest.mark.parametrize(
    ("head_status", "head_sem_rede"),
    [(200, False), (405, False), (200, True)],
    ids=["head_sem_content_length", "head_405", "head_sem_rede"],
)
async def test_sem_tamanho_no_get_nem_no_head_avisa(monkeypatch, head_status, head_sem_rede):
    _servir(
        monkeypatch,
        corpo=CSV,
        get={},
        head={"Content-Length": str(len(CSV))} if head_status != 200 else {},
        head_status=head_status,
        head_sem_rede=head_sem_rede,
    )

    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)

    assert len(frame) == 3
    assert _conferencia(meta) is None
    assert meta.validation_warnings == [AVISO]
    assert [str(aviso.message) for aviso in emitidos if "não conferido" in str(aviso.message)] == [
        AVISO
    ]
