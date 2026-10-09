from __future__ import annotations

import csv
import hashlib
import json
import warnings
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from agrobr import datasets, queimadas
from agrobr.queimadas import api, client
from tests.helpers import conferir_corpo, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "queimadas"
SETEMBRO = GOLDEN / "mes_corrente_202609"
AGOSTO = GOLDEN / "mensal_202408"
CSV_SETEMBRO = f"{client.BASE_URL}/mensal/Brasil/focos_mensal_br_202609.csv"
CSV_AGOSTO = f"{client.BASE_URL}/mensal/Brasil/focos_mensal_br_202408.csv"
ASYNC_CLIENT_REAL = httpx.AsyncClient


def _golden(pasta: Path) -> tuple[bytes, dict]:
    manifesto = json.loads((pasta / "manifest.json").read_bytes())
    corpo = (pasta / manifesto["recorte"]["arquivo"]).read_bytes()
    assert (hashlib.sha256(corpo).hexdigest(), len(corpo)) == (
        manifesto["recorte"]["sha256"],
        manifesto["recorte"]["bytes"],
    )
    return corpo, manifesto


def _servir(
    monkeypatch: pytest.MonkeyPatch, url: str, corpo: bytes, last_modified: str | None
) -> None:
    cabecalhos = {"Last-Modified": last_modified} if last_modified else {}

    def responder(request: httpx.Request) -> httpx.Response:
        if str(request.url) != url:
            return httpx.Response(404)
        return httpx.Response(200, content=corpo, headers=cabecalhos)

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: ASYNC_CLIENT_REAL(transport=httpx.MockTransport(responder), **kwargs),
    )


def _aviso(last_modified: str | None, ultimo: str) -> str:
    return (
        "queimadas: o arquivo mensal de 2026-09 ainda está sendo atualizado pelo INPE "
        f"(Last-Modified {last_modified or 'ausente'}); o resultado vai até o foco de {ultimo} GMT e muda "
        "durante o mês."
    )


def _ultimo_foco(corpo: bytes) -> str:
    return max(
        linha["data_hora_gmt"] for linha in csv.DictReader(corpo.decode("utf-8").splitlines())
    )[:16]


async def test_mes_corrente_sai_com_aviso_de_parcial(monkeypatch):
    corpo, manifesto = _golden(SETEMBRO)
    inteiro = manifesto["arquivo_inteiro"]
    last_modified = inteiro["cabecalhos_http"]["last-modified"]
    assert last_modified == "Sat, 26 Sep 2026 21:56:25 GMT"
    ultimo = _ultimo_foco(corpo)
    assert ultimo == inteiro["ultimo_foco_gmt"][:16] == "2026-09-25 23:50"
    _servir(monkeypatch, CSV_SETEMBRO, corpo, last_modified)
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 9, 26, 23, 13, 38))
    with sem_excecao(), warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        df, meta = await queimadas.focos(ano=2026, mes=9, return_meta=True)
    assert len(df) == len(corpo.splitlines()) - 1
    assert meta.source_details == {
        "mes_parcial": True,
        "ultimo_foco": ultimo,
        "last_modified": last_modified,
    }
    assert meta.validation_warnings == [_aviso(last_modified, ultimo)]
    assert [str(aviso.message) for aviso in emitidos if aviso.category is UserWarning] == [
        _aviso(last_modified, ultimo)
    ]
    conferir_corpo(meta, corpo)


async def test_dataset_repassa_o_aviso_de_mes_parcial(monkeypatch):
    corpo, manifesto = _golden(SETEMBRO)
    last_modified = manifesto["arquivo_inteiro"]["cabecalhos_http"]["last-modified"]
    _servir(monkeypatch, CSV_SETEMBRO, corpo, last_modified)
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 9, 26, 23, 13, 38))
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await datasets.queimadas(ano=2026, mes=9, uf="MT", return_meta=True)
    assert meta.validation_warnings == [_aviso(last_modified, _ultimo_foco(corpo))]
    parcial = {
        chave: meta.source_details.get(chave)
        for chave in ("mes_parcial", "ultimo_foco", "last_modified")
    }
    assert parcial == {
        "mes_parcial": True,
        "ultimo_foco": _ultimo_foco(corpo),
        "last_modified": last_modified,
    }


@pytest.mark.parametrize(
    ("last_modified", "agora", "parcial"),
    [
        ("Sat, 26 Sep 2026 21:56:25 GMT", datetime(2026, 10, 2), True),
        ("Thu, 01 Oct 2026 00:00:00 GMT", datetime(2026, 10, 1, 1), True),
        ("Thu, 01 Oct 2026 23:49:59 GMT", datetime(2026, 10, 3), True),
        ("Thu, 01 Oct 2026 23:50:00 GMT", datetime(2026, 10, 1, 23, 55), False),
        ("Thu, 01 Oct 2026 23:56:24 GMT", datetime(2026, 10, 2), False),
        (None, datetime(2026, 9, 26), True),
        (None, datetime(2026, 10, 2, 0, 55), True),
        (None, datetime(2026, 10, 2, 0, 56), False),
        ("not a date", datetime(2026, 9, 26), True),
    ],
)
async def test_regra_do_mes_parcial(monkeypatch, last_modified, agora, parcial):
    corpo, _ = _golden(SETEMBRO)
    _servir(monkeypatch, CSV_SETEMBRO, corpo, last_modified)
    monkeypatch.setattr(api, "utcnow", lambda: agora)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await queimadas.focos(ano=2026, mes=9, return_meta=True)
    esperado = (
        {"mes_parcial": True, "ultimo_foco": _ultimo_foco(corpo), "last_modified": last_modified}
        if parcial
        else {}
    )
    assert meta.source_details == esperado
    assert len(meta.validation_warnings) == int(parcial)


async def test_mes_fechado_sai_sem_aviso(monkeypatch):
    corpo, manifesto = _golden(AGOSTO)
    last_modified = manifesto["arquivo_inteiro"]["cabecalhos_http"]["last-modified"]
    assert last_modified == "Wed, 03 Jun 2026 18:07:50 GMT"
    _servir(monkeypatch, CSV_AGOSTO, corpo, last_modified)
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 9, 26, 23, 13, 38))
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await queimadas.focos(ano=2024, mes=8, return_meta=True)
    assert "mes_parcial" not in meta.source_details
    assert not [
        aviso for aviso in meta.validation_warnings if "ainda está sendo atualizado" in aviso
    ]


DIARIO = GOLDEN / "dia_corrente_20260926"
CSV_DIARIO = f"{client.BASE_URL}/diario/Brasil/focos_diario_br_20260926.csv"


def _aviso_diario(last_modified: str | None, ultimo: str) -> str:
    return (
        "queimadas: o arquivo diário de 2026-09-26 ainda está sendo atualizado pelo INPE "
        f"(Last-Modified {last_modified or 'ausente'}); o resultado vai até o foco de {ultimo} GMT e muda "
        "durante o dia."
    )


async def test_dia_corrente_sai_com_aviso_de_parcial(monkeypatch):
    corpo, manifesto = _golden(DIARIO)
    inteiro = manifesto["arquivo_inteiro"]
    last_modified = inteiro["cabecalhos_http"]["last-modified"]
    assert last_modified == "Sat, 26 Sep 2026 23:06:03 GMT"
    ultimo = _ultimo_foco(corpo)
    assert ultimo == inteiro["ultimo_foco_gmt"][:16] == "2026-09-26 22:40"
    _servir(monkeypatch, CSV_DIARIO, corpo, last_modified)
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 9, 26, 23, 33, 44))
    with sem_excecao(), warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        df, meta = await queimadas.focos(ano=2026, mes=9, dia=26, return_meta=True)
    assert len(df) == len(corpo.splitlines()) - 1
    assert meta.source_details == {
        "dia_parcial": True,
        "ultimo_foco": ultimo,
        "last_modified": last_modified,
    }
    assert meta.validation_warnings == [_aviso_diario(last_modified, ultimo)]
    assert [str(aviso.message) for aviso in emitidos if aviso.category is UserWarning] == [
        _aviso_diario(last_modified, ultimo)
    ]
    conferir_corpo(meta, corpo)


@pytest.mark.parametrize(
    ("last_modified", "agora", "parcial"),
    [
        ("Sat, 26 Sep 2026 23:06:03 GMT", datetime(2026, 9, 28), True),
        ("Sun, 27 Sep 2026 11:59:59 GMT", datetime(2026, 9, 30), True),
        ("Sun, 27 Sep 2026 12:00:00 GMT", datetime(2026, 9, 27, 11), False),
        ("Sun, 27 Sep 2026 12:05:03 GMT", datetime(2026, 9, 27, 12, 30), False),
        (None, datetime(2026, 9, 26, 12), True),
        (None, datetime(2026, 9, 27, 13, 4), True),
        (None, datetime(2026, 9, 27, 13, 5), False),
    ],
)
async def test_regra_do_dia_parcial(monkeypatch, last_modified, agora, parcial):
    corpo, _ = _golden(DIARIO)
    _servir(monkeypatch, CSV_DIARIO, corpo, last_modified)
    monkeypatch.setattr(api, "utcnow", lambda: agora)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await queimadas.focos(ano=2026, mes=9, dia=26, return_meta=True)
    esperado = (
        {"dia_parcial": True, "ultimo_foco": _ultimo_foco(corpo), "last_modified": last_modified}
        if parcial
        else {}
    )
    assert meta.source_details == esperado
    assert len(meta.validation_warnings) == int(parcial)
