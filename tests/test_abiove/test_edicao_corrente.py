from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import openpyxl
import pytest

from agrobr import abiove, datasets
from agrobr.abiove import client
from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.utils import time as time_utils
from tests import helpers

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/abiove"
BASE = "https://abiove.org.br/abiove_content/Abiove"
URL_202608 = f"{BASE}/exp_202608.xlsx"
URL_202609 = f"{BASE}/exp_202609.xlsx"
URL_202512 = f"{BASE}/exp_202512.xlsx"
SECOES = {
    "grao": (9, "grão"),
    "farelo": (28, "farelo"),
    "oleo": (47, "óleo"),
    "milho": (66, "milho"),
}
MESES = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]


def _celulas(conteudo: bytes, valor: str, peso: str, ano: int) -> dict[tuple[str, int], tuple]:
    sheet = openpyxl.load_workbook(io.BytesIO(conteudo), data_only=True).worksheets[0]
    celulas = {}
    for produto, (titulo, rotulo) in SECOES.items():
        assert rotulo in sheet[f"B{titulo}"].value
        assert sheet[f"C{titulo + 1}"].value == "Valor FOB (US$ 1.000)"
        assert sheet[f"F{titulo + 1}"].value == "Peso Líquido (mil t)"
        assert sheet[f"{valor}{titulo + 2}"].value.year == ano
        assert sheet[f"{peso}{titulo + 2}"].value.year == ano
        for mes in range(1, 13):
            linha = titulo + 2 + mes
            assert sheet[f"B{linha}"].value == MESES[mes - 1]
            if sheet[f"{peso}{linha}"].value is not None:
                celulas[(produto, mes)] = (
                    sheet[f"{peso}{linha}"].value * 1000,
                    sheet[f"{valor}{linha}"].value,
                )
    return celulas


@pytest.fixture(scope="module")
def edicoes():
    manifest = json.loads((GOLDEN / "edicao_202608/manifest.json").read_text(encoding="utf-8"))
    anterior = json.loads((GOLDEN / "exportacao_sample/metadata.json").read_text(encoding="utf-8"))
    corrente = (GOLDEN / "edicao_202608/exp_202608.xlsx").read_bytes()
    original = (GOLDEN / "exportacao_sample/response.xlsx").read_bytes()
    return {
        "corpos": {URL_202608: corrente, URL_202512: original},
        "sha256": {URL_202608: manifest["arquivos"][0]["sha256"], URL_202512: anterior["sha256"]},
        "2025": _celulas(corrente, "C", "F", 2025),
        "2025_original": _celulas(original, "D", "G", 2025),
        "2026": _celulas(corrente, "D", "G", 2026),
    }


@pytest.fixture
def servidor(monkeypatch, edicoes):
    pedidos: list[str] = []
    corpos = dict(edicoes["corpos"])

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        corpo = corpos.get(str(request.url))
        if isinstance(corpo, int):
            return httpx.Response(corpo, content=b"<html>erro</html>")
        if corpo is None:
            return httpx.Response(404, content=b"<html>Not Found</html>")
        return httpx.Response(200, content=corpo)

    real = httpx.AsyncClient

    class Cliente(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(responder)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(client.httpx, "AsyncClient", Cliente)
    monkeypatch.setattr(
        time_utils, "utcnow_aware", lambda: datetime(2026, 9, 25, 12, 0, tzinfo=UTC)
    )
    return pedidos, corpos


def _obtido(frame) -> dict[tuple[str, int], tuple]:
    return {(r.produto, r.mes): (r.volume_ton, r.receita_usd_mil) for r in frame.itertuples()}


@pytest.mark.asyncio
async def test_ano_anterior_sai_da_edicao_corrente(servidor, edicoes):
    pedidos, _ = servidor
    corrente, original = edicoes["2025"], edicoes["2025_original"]
    assert len(corrente) == 48
    revistas = sum(
        abs(a - b) > 1e-6
        for chave in corrente
        for a, b in zip(corrente[chave], original[chave], strict=True)
    )
    assert revistas == 19
    assert corrente[("farelo", 12)] == pytest.approx((1_990_304.323, 697_848.225))
    assert original[("farelo", 12)] == pytest.approx((2_020_365.023, 708_233.257))

    frame, meta = await abiove.exportacao(2025, return_meta=True)

    obtido = _obtido(frame)
    assert set(obtido) == set(corrente)
    for chave, esperado in corrente.items():
        assert obtido[chave] == pytest.approx(esperado, rel=1e-12), chave
    assert set(frame["ano"]) == {2025}
    assert meta.source_url == URL_202608
    assert meta.source_details.get("edicao") == {"arquivo": "exp_202608.xlsx", "mes": "2026-08"}
    assert meta.raw_content_hash == edicoes["sha256"][URL_202608]
    helpers.conferir_corpo(meta, edicoes["corpos"][URL_202608])
    assert pedidos == [URL_202609, URL_202608]


@pytest.mark.asyncio
async def test_edicao_explicita_traz_o_numero_original(servidor, edicoes):
    pedidos, _ = servidor

    frame, meta = await abiove.exportacao(
        2025, mes=12, produto="farelo", edicao="2025-12", return_meta=True
    )

    assert _obtido(frame) == {("farelo", 12): pytest.approx((2_020_365.023, 708_233.257))}
    assert meta.source_url == URL_202512
    assert meta.source_details.get("edicao") == {"arquivo": "exp_202512.xlsx", "mes": "2025-12"}
    assert meta.raw_content_hash == edicoes["sha256"][URL_202512]
    assert pedidos == [URL_202512]


@pytest.mark.asyncio
async def test_falha_na_edicao_corrente_nao_cai_para_edicao_antiga(servidor):
    pedidos, corpos = servidor
    corpos[URL_202608] = 503

    with pytest.raises(SourceUnavailableError, match="503"):
        await abiove.exportacao(2025)

    assert URL_202512 not in pedidos


@pytest.mark.asyncio
async def test_sem_edicao_publicada_levanta(servidor):
    pedidos, corpos = servidor
    corpos.clear()

    with pytest.raises(
        SourceUnavailableError, match="HTTP 404: nenhuma edição publicada com dados de 2025"
    ):
        await abiove.exportacao(2025)

    assert pedidos == [f"{BASE}/exp_2026{mes:02d}.xlsx" for mes in range(9, 0, -1)] + [
        f"{BASE}/exp_2025{mes:02d}.xlsx" for mes in range(12, 0, -1)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "argumentos",
    [
        {"mes": 0},
        {"mes": 13},
        {"mes": "6"},
        {"edicao": "2024-12"},
        {"edicao": "2027-01"},
        {"edicao": "202608"},
        {"edicao": "2026-13"},
        {"produto": "cafe"},
        {"agregacao": "anual"},
    ],
)
async def test_selecao_invalida_recusada_antes_da_rede(servidor, argumentos):
    pedidos, _ = servidor

    with pytest.raises(InvalidParameterError):
        await abiove.exportacao(2025, **argumentos)

    assert pedidos == []


@pytest.mark.asyncio
@pytest.mark.parametrize("ano", ["2025", 2025.0, True, 2009, 9999])
async def test_ano_fora_do_dominio_recusado_antes_da_rede(servidor, ano):
    pedidos, _ = servidor

    with pytest.raises(InvalidParameterError, match="ano deve ser um inteiro de 2010 a"):
        await abiove.exportacao(ano)

    assert pedidos == []


def test_edicao_candidata_segue_o_mes_de_brasilia(monkeypatch):
    monkeypatch.setattr(time_utils, "utcnow_aware", lambda: datetime(2026, 10, 1, 2, 0, tzinfo=UTC))

    assert client.edicoes_candidatas(2026)[0] == "2026-09"


@pytest.mark.asyncio
@pytest.mark.usefixtures("servidor")
async def test_ano_corrente_so_traz_os_meses_publicados(edicoes):
    publicados = {
        chave: valores
        for chave, valores in edicoes["2026"].items()
        if not any(isinstance(valor, str) for valor in valores)
    }
    assert sorted({mes for _, mes in publicados}) == list(range(1, 9))

    frame = await abiove.exportacao(2026)

    obtido = _obtido(frame)
    assert set(obtido) == set(publicados)
    for chave, esperado in publicados.items():
        assert obtido[chave] == pytest.approx(esperado, rel=1e-12), chave


@pytest.mark.asyncio
async def test_dataset_exportacao_pede_o_ano_informado(servidor, monkeypatch):
    pedidos, _ = servidor
    monkeypatch.setattr(
        "agrobr.comexstat.exportacao", AsyncMock(side_effect=httpx.ConnectError("offline"))
    )
    original = (GOLDEN / "exportacao_sample/response.xlsx").read_bytes()
    celulas = _celulas(original, "C", "F", 2024)

    frame, meta = await datasets.exportacao("soja", ano=2024, return_meta=True)

    assert sorted(frame["mes"]) == list(range(1, 13))
    assert set(frame["ano"]) == {2024}
    for linha in frame.itertuples():
        peso, valor = celulas[("grao", linha.mes)]
        assert linha.kg_liquido == pytest.approx(peso * 1000, rel=1e-12)
        assert linha.valor_fob_usd == pytest.approx(valor * 1000, rel=1e-12)
    assert meta.source_details.get("edicao") == {"arquivo": "exp_202512.xlsx", "mes": "2025-12"}
    assert pedidos == [URL_202512]
