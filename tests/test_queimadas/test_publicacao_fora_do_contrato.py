from __future__ import annotations

import csv
import hashlib
import json
import warnings
from collections import Counter
from pathlib import Path

import httpx
import pandas as pd
import pytest

from agrobr import datasets, queimadas
from agrobr.queimadas import client
from agrobr.queimadas.models import CHAVE
from tests.helpers import conferir_corpo, sem_excecao

R48 = Path(__file__).parents[1] / "golden_data" / "queimadas" / "r48_202408"
RECORTE = "focos_mensal_br_202408_recorte.csv"
CSV_AGOSTO = f"{client.BASE_URL}/mensal/Brasil/focos_mensal_br_202408.csv"
ASYNC_CLIENT_REAL = httpx.AsyncClient
CHAVE_REPETIDA = ("2024-08-29", "17:07", -6.85496, -51.81877, "NOAA-20")
AVISO_CHAVE = (
    "queimadas: 2 foco(s) repetem a chave (data, hora_gmt, lat, lon, satelite) com valores diferentes "
    "e saíram do resultado (1 chave(s) em source_details['chaves_repetidas'])."
)
AVISO_FRP_DIVERGENTE = (
    "queimadas: 1 foco(s) publicados mais de uma vez com FRP diferente saíram uma vez só, com frp nulo "
    "(2 linha(s))."
)
AVISO_IGUAIS = "queimadas: 1 foco(s) publicados mais de uma vez, iguais em todas as colunas, saíram uma vez só."


def _aviso_frp(anulados: int) -> str:
    return (
        f"queimadas: {anulados} foco(s) com FRP negativo publicado pela fonte (fisicamente impossível) "
        "saíram com frp nulo."
    )


def _recorte() -> tuple[bytes, list[dict[str, str]]]:
    manifesto = json.loads((R48 / "manifest.json").read_bytes())
    corpo = (R48 / RECORTE).read_bytes()
    assert (hashlib.sha256(corpo).hexdigest(), len(corpo)) == (
        manifesto["recorte"]["sha256"],
        manifesto["recorte"]["bytes"],
    )
    linhas = corpo.decode("utf-8").splitlines()
    assert [hashlib.sha256(linha.encode()).hexdigest() for linha in linhas[1:]] == [
        item["sha256"] for item in manifesto["recorte"]["linhas"]
    ]
    return corpo, list(csv.DictReader(linhas))


def _chave(registro: dict[str, str]) -> tuple[str, str, float, float, str]:
    momento = registro["data_hora_gmt"]
    return (
        momento[:10],
        momento[11:16],
        float(registro["lat"]),
        float(registro["lon"]),
        registro["satelite"],
    )


def _negativo(registro: dict[str, str]) -> bool:
    return float(registro["frp"]) < 0


def _servir(monkeypatch: pytest.MonkeyPatch, corpo: bytes) -> list[str]:
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        if str(request.url) != CSV_AGOSTO:
            return httpx.Response(404)
        return httpx.Response(200, content=corpo)

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: ASYNC_CLIENT_REAL(transport=httpx.MockTransport(responder), **kwargs),
    )
    return pedidos


def _chaves(df: pd.DataFrame) -> list[tuple[str, str, float, float, str]]:
    return [(f"{data:%Y-%m-%d}", *resto) for data, *resto in df[CHAVE].itertuples(index=False)]


def _saida(df: pd.DataFrame) -> dict[tuple[str, str, float, float, str], float | None]:
    frp = [None if pd.isna(valor) else float(valor) for valor in df["frp"]]
    return dict(zip(_chaves(df), frp, strict=True))


async def test_agosto_2024_sai_com_frp_nulo_no_negativo_e_na_chave_repetida(monkeypatch):
    corpo, registros = _recorte()
    pedidos = _servir(monkeypatch, corpo)
    with sem_excecao(), warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        df, meta = await queimadas.focos(ano=2024, mes=8, return_meta=True)
    contagem = Counter(_chave(registro) for registro in registros)
    assert [chave for chave, vezes in contagem.items() if vezes > 1] == [CHAVE_REPETIDA]
    assert [registro["frp"] for registro in registros if _chave(registro) == CHAVE_REPETIDA] == [
        "5.5",
        "8.5",
    ]
    esperado = {
        _chave(registro): None
        if _negativo(registro) or _chave(registro) == CHAVE_REPETIDA
        else float(registro["frp"])
        for registro in registros
    }
    assert _saida(df) == esperado
    assert _chaves(df).count(CHAVE_REPETIDA) == 1
    assert len(df) == len(registros) - 1
    assert sum(_negativo(registro) for registro in registros) == 28
    assert meta.source_details == {
        "frp_divergente": {"focos": 1, "linhas": 2},
        "frp_negativo_anulado": 28,
    }
    assert meta.validation_warnings == [AVISO_FRP_DIVERGENTE, _aviso_frp(28)]
    assert [str(aviso.message) for aviso in emitidos if aviso.category is UserWarning] == [
        AVISO_FRP_DIVERGENTE,
        _aviso_frp(28),
    ]
    assert meta.parser_version == 2
    assert pedidos == [CSV_AGOSTO]
    conferir_corpo(meta, corpo)


async def test_dataset_de_agosto_2024_cumpre_o_contrato(monkeypatch):
    corpo, registros = _recorte()
    _servir(monkeypatch, corpo)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df, meta = await datasets.queimadas(ano=2024, mes=8, return_meta=True)
    assert len(df) == len(registros) - 1
    assert int(df["frp"].isna().sum()) == 29
    assert bool((df["frp"].dropna() >= 0).all())
    assert not df.duplicated(CHAVE).any()
    assert meta.validation_warnings == [AVISO_FRP_DIVERGENTE, _aviso_frp(28)]


async def test_aviso_conta_so_os_focos_do_filtro(monkeypatch):
    corpo, registros = _recorte()
    _servir(monkeypatch, corpo)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df, meta = await queimadas.focos(ano=2024, mes=8, uf="MT", return_meta=True)
    mato_grosso = [registro for registro in registros if registro["estado"] == "MATO GROSSO"]
    anulados = sum(_negativo(registro) for registro in mato_grosso)
    assert anulados == 16
    assert len(df) == len(mato_grosso)
    assert int(df["frp"].isna().sum()) == anulados
    assert bool((df["frp"].dropna() >= 0).all())
    assert meta.source_details == {"frp_negativo_anulado": anulados}
    assert meta.validation_warnings == [_aviso_frp(anulados)]


async def test_foco_publicado_2_vezes_igual_sai_uma_vez(monkeypatch):
    corpo, registros = _recorte()
    linhas = corpo.split(b"\n")
    repetida = linhas[1]
    _servir(monkeypatch, b"\n".join([*linhas[:-1], repetida, b""]))
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df, meta = await queimadas.focos(ano=2024, mes=8, return_meta=True)
    alvo = _chave(next(csv.DictReader([linhas[0].decode(), repetida.decode()])))
    assert _chaves(df).count(alvo) == 1
    assert len(df) == len(registros) - 1
    assert meta.source_details == {
        "duplicatas_colapsadas": 1,
        "frp_divergente": {"focos": 1, "linhas": 2},
        "frp_negativo_anulado": 28,
    }
    assert meta.validation_warnings == [AVISO_IGUAIS, AVISO_FRP_DIVERGENTE, _aviso_frp(28)]


async def test_chave_repetida_com_outra_coluna_diferente_sai_do_resultado(monkeypatch):
    corpo, registros = _recorte()
    linhas = corpo.split(b"\n")
    [indice] = [i for i, linha in enumerate(linhas) if linha.startswith(b"8a3e42ea-")]
    assert linhas[indice].count(b",1.0,Amaz") == 1
    linhas[indice] = linhas[indice].replace(b",1.0,Amaz", b",0.9,Amaz")
    _servir(monkeypatch, b"\n".join(linhas))
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df, meta = await queimadas.focos(ano=2024, mes=8, return_meta=True)
    assert CHAVE_REPETIDA not in _chaves(df)
    assert len(df) == len(registros) - 2
    chave = {
        "data": "2024-08-29",
        "hora_gmt": "17:07",
        "lat": "-6.85496",
        "lon": "-51.81877",
        "satelite": "NOAA-20",
    }
    assert meta.source_details == {
        "chaves_repetidas": {"linhas": 2, "chaves": [chave]},
        "frp_negativo_anulado": 28,
    }
    assert meta.validation_warnings == [AVISO_CHAVE, _aviso_frp(28)]


async def test_csv_sem_frp_colapsa_a_copia_sem_ler_o_frp(monkeypatch):
    linha = b"a,-10.1,-50.2,2024-08-01 12:00:00,AQUA_M-T\n"
    _servir(
        monkeypatch,
        b"id,lat,lon,data_hora_gmt,satelite\n"
        + linha * 2
        + b"b,-10.2,-50.3,2024-08-01 12:00:00,AQUA_M-T\n",
    )
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df, meta = await queimadas.focos(ano=2024, mes=8, return_meta=True)
    assert "frp" not in df.columns
    assert df[["lat", "lon"]].to_numpy().tolist() == [[-10.1, -50.2], [-10.2, -50.3]]
    assert meta.source_details == {"duplicatas_colapsadas": 1}
    assert meta.validation_warnings == [AVISO_IGUAIS]
