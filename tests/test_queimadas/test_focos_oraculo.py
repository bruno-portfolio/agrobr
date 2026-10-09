from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd
import pytest

from agrobr import contracts, queimadas
from agrobr.exceptions import InvalidParameterError
from agrobr.queimadas import api, client
from agrobr.utils import time as time_utils
from tests.helpers import (
    assert_replay_samples,
    conferir_corpo,
    levanta_exatamente,
    sem_excecao,
)

R12 = Path(__file__).parents[1] / "golden_data/reconciliacao_uso_solo_ambiente_20260918"
CASO = next(
    case
    for case in json.loads((R12 / "manifest.json").read_text(encoding="utf-8"))["cases"]
    if case["id"] == "queimadas_mensal_202504"
)
RECORTE_ABRIL_2025 = (R12 / "queimadas/focos_mensal_br_202504_recorte.csv").read_bytes()
RECORTE_SETEMBRO_2026 = (R12 / "queimadas/focos_diario_br_20260910_recorte.csv").read_bytes()
MEMBRO_UNICO_DO_ZIP_MENSAL = "focos_mensal_br_202504.csv"
MEMBRO_UNICO_DO_ZIP_ANUAL_2025 = "tmp/focos_br_todos-sats_2025.csv"
CSV_MENSAL = f"{client.BASE_URL}/mensal/Brasil/focos_mensal_br_202504.csv"
CSV_DIARIO = f"{client.BASE_URL}/diario/Brasil/focos_diario_br_20260910.csv"
ZIP_MENSAL = f"{client.BASE_URL}/mensal/Brasil/focos_mensal_br_202504.zip"
ZIP_ANUAL = f"{client.ANUAL_URL}/focos_br_todos-sats_2025.zip"
ASYNC_CLIENT_REAL = httpx.AsyncClient


def _transporte(membro: str, conteudo: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(membro, conteudo)
    return buffer.getvalue()


def _servir(monkeypatch: pytest.MonkeyPatch, corpos: dict[str, bytes]) -> list[str]:
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        corpo = corpos.get(str(request.url))
        return httpx.Response(404) if corpo is None else httpx.Response(200, content=corpo)

    monkeypatch.setattr(
        client.httpx,
        "AsyncClient",
        lambda **kwargs: ASYNC_CLIENT_REAL(transport=httpx.MockTransport(responder), **kwargs),
    )
    return pedidos


@pytest.mark.parametrize(
    ("rota", "corpos", "pedidos_esperados"),
    [
        (
            ZIP_MENSAL,
            {ZIP_MENSAL: _transporte(MEMBRO_UNICO_DO_ZIP_MENSAL, RECORTE_ABRIL_2025)},
            [CSV_MENSAL, ZIP_MENSAL],
        ),
        (
            ZIP_ANUAL,
            {
                ZIP_ANUAL: _transporte(
                    MEMBRO_UNICO_DO_ZIP_ANUAL_2025,
                    RECORTE_ABRIL_2025
                    + RECORTE_SETEMBRO_2026[RECORTE_SETEMBRO_2026.index(b"\n") + 1 :],
                )
            },
            [CSV_MENSAL, ZIP_MENSAL, ZIP_ANUAL],
        ),
    ],
    ids=["zip_mensal", "zip_anual"],
)
async def test_rota_zip_entrega_o_mes_publicado_no_csv(
    rota, corpos, pedidos_esperados, monkeypatch: pytest.MonkeyPatch
):
    _servir(monkeypatch, {CSV_MENSAL: RECORTE_ABRIL_2025})
    esperado, meta_csv = await queimadas.focos(ano=2025, mes=4, return_meta=True)
    pedidos = _servir(monkeypatch, corpos)
    with sem_excecao():
        frame, meta = await queimadas.focos(ano=2025, mes=4, return_meta=True)
    assert pedidos == pedidos_esperados
    assert meta.source_url == rota
    conferir_corpo(meta_csv, RECORTE_ABRIL_2025)
    conferir_corpo(meta, corpos[rota])
    assert meta.schema_version == contracts.get_contract("queimadas").version == "1.1"
    assert len(frame) == CASO["period"]["rows"]
    assert str(frame["data"].dtype) == "datetime64[ns]"
    pd.testing.assert_frame_equal(frame, esperado)
    assert_replay_samples(frame, CASO)


@pytest.mark.parametrize(
    ("recorte", "consulta", "rota"),
    [
        (RECORTE_ABRIL_2025, {"ano": 2025, "mes": 4, "satelite": "NOAA-21"}, CSV_MENSAL),
        (
            RECORTE_SETEMBRO_2026,
            {"ano": 2026, "mes": 9, "dia": 10, "satelite": "METOP-B"},
            CSV_DIARIO,
        ),
    ],
    ids=["mensal_noaa21", "diario_metopb"],
)
async def test_focos_geo_por_satelite_confere_o_csv_publicado(
    recorte, consulta, rota, monkeypatch: pytest.MonkeyPatch
):
    pytest.importorskip("geopandas")
    publicados = [
        linha
        for linha in csv.DictReader(io.StringIO(recorte.decode("utf-8")))
        if linha["satelite"] == consulta["satelite"]
    ]
    _servir(monkeypatch, {rota: recorte})
    with sem_excecao():
        frame, meta = await queimadas.focos_geo(**consulta, return_meta=True)
    assert meta.source_url == rota
    conferir_corpo(meta, recorte)
    assert frame.crs.to_epsg() == 4326
    assert len(frame) == len(publicados)
    for posicao, (linha, publicado) in enumerate(
        zip(frame.itertuples(index=False), publicados, strict=True)
    ):
        lat, lon = float(publicado["lat"]), float(publicado["lon"])
        assert (linha.geometry.x, linha.geometry.y, linha.lat, linha.lon) == (lon, lat, lat, lon)
        assert f"{linha.data:%Y-%m-%d} {linha.hora_gmt}" == publicado["data_hora_gmt"][:16], posicao
        assert (linha.satelite, linha.municipio, linha.estado, linha.bioma) == (
            publicado["satelite"],
            publicado["municipio"],
            publicado["estado"],
            publicado["bioma"],
        ), posicao
        assert linha.municipio_id == int(publicado["municipio_id"]), posicao
        for coluna in ("numero_dias_sem_chuva", "precipitacao", "risco_fogo", "frp"):
            bruto = publicado[coluna]
            if bruto == "" or float(bruto) == -999:
                assert pd.isna(getattr(linha, coluna)), (posicao, coluna)
            else:
                assert getattr(linha, coluna) == float(bruto), (posicao, coluna)


NUMERICAS = ("numero_dias_sem_chuva", "precipitacao", "risco_fogo", "frp")


async def test_satelite_casa_sem_caixa_com_o_nome_publicado(monkeypatch: pytest.MonkeyPatch):
    publicados = sum(
        linha["satelite"] == "NOAA-21"
        for linha in csv.DictReader(io.StringIO(RECORTE_ABRIL_2025.decode("utf-8")))
    )
    _servir(monkeypatch, {CSV_MENSAL: RECORTE_ABRIL_2025})
    with sem_excecao():
        frame = await queimadas.focos(ano=2025, mes=4, satelite=" noaa-21 ")
    assert len(frame) == publicados == 111
    assert set(frame["satelite"]) == {"NOAA-21"}


async def test_satelite_fora_do_arquivo_levanta_com_os_publicados(monkeypatch: pytest.MonkeyPatch):
    pedidos = _servir(monkeypatch, {CSV_MENSAL: RECORTE_ABRIL_2025})
    with levanta_exatamente(InvalidParameterError, "não aparece no arquivo de 2025-04") as erro:
        await queimadas.focos(ano=2025, mes=4, satelite="XX")
    assert "AQUA_M-T, GOES-16, GOES-19, NOAA-20, NOAA-21, NPP-375D, TERRA_M-M, TERRA_M-T" in str(
        erro.value
    )
    assert pedidos == [CSV_MENSAL]


@pytest.mark.parametrize("satelite", [21, "", "  ", ["NOAA-21"]])
async def test_satelite_que_nao_e_texto_recusa_antes_da_rede(
    satelite, monkeypatch: pytest.MonkeyPatch
):
    pedidos = _servir(monkeypatch, {CSV_MENSAL: RECORTE_ABRIL_2025})
    with levanta_exatamente(InvalidParameterError, "satelite deve ser texto não vazio"):
        await queimadas.focos(ano=2025, mes=4, satelite=satelite)
    assert pedidos == []


async def test_numericas_saem_float64_no_cheio_e_no_recorte_vazio(monkeypatch: pytest.MonkeyPatch):
    _servir(monkeypatch, {CSV_MENSAL: RECORTE_ABRIL_2025})
    with sem_excecao():
        cheio = await queimadas.focos(ano=2025, mes=4)
        vazio = await queimadas.focos(ano=2025, mes=4, uf="RS", satelite="TERRA_M-M")
    assert len(cheio) == 902 and vazio.empty
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert {str(cheio[coluna].dtype) for coluna in NUMERICAS} == {"float64"}


@pytest.mark.parametrize(
    ("agora_utc", "ano", "aceito"),
    [
        (datetime(2027, 1, 1, 2, 59, tzinfo=UTC), 2027, False),
        (datetime(2027, 1, 1, 3, 0, tzinfo=UTC), 2027, True),
    ],
    ids=["ainda_2026_em_brasilia", "ja_2027_em_brasilia"],
)
def test_ano_corrente_e_o_de_brasilia(agora_utc, ano, aceito, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: agora_utc)
    if aceito:
        assert api._validate_period(ano, 1, None) == (ano, 1, None)
    else:
        with levanta_exatamente(InvalidParameterError, "ano não pode ser posterior a 2026"):
            api._validate_period(ano, 1, None)
