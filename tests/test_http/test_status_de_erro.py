from __future__ import annotations

import ast
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from agrobr import (
    abiove,
    alt,
    anda,
    anec,
    b3,
    bcb,
    cftc,
    comexstat,
    conab,
    datasets,
    defensivos,
    deral,
    desmatamento,
    embrapa_solos,
    funai,
    ibama,
    ibge,
    icmbio,
    imea,
    incra,
    inmet,
    lista_suja,
    mapbiomas,
    nasa_power,
    queimadas,
    rnc,
    sfb,
    unica,
    usda,
    zarc,
)
from agrobr.exceptions import SourceUnavailableError
from agrobr.http import responses
from tests.helpers import levanta_exatamente

PACOTE = Path(__file__).resolve().parents[2] / "agrobr"
FORA_DO_HELPER = {
    ("http/responses.py", "raise_for_status"): "o próprio helper",
    ("alerts/notifier.py", "_send_slack"): "alerta, não é API de dado",
    ("alerts/notifier.py", "_send_discord"): "alerta, não é API de dado",
    ("alerts/notifier.py", "_send_email"): "alerta, não é API de dado",
    ("antaq/client.py", "_get_sync"): "requests, convertido no _download_zip",
    ("comexstat/client.py", "_attempt"): "o try grava o recibo e o _open_resource converte",
    ("conab/custo_producao/client.py", "fetch_custos_page"): "o try local converte",
    ("conab/custo_producao/client.py", "download_xlsx"): "o try local converte",
    ("conab/custo_producao/client.py", "_crawl_folder"): "o try local converte",
    ("conab/serie_historica/client.py", "download_xls"): "o try local converte",
    ("http/wfs_transport.py", "fetch"): "o try local lê o status do HTTPStatusError",
    (
        "ibge/client.py",
        "_do_fetch",
    ): "o retry trata SourceUnavailableError como retentável, e o try "
    "do chamador lê o 403 do Cloudflare pelo HTTPStatusError",
    ("ibge/ftp_client.py", "download_legacy_zip"): "o try local converte",
}
PEDIDOS: dict[str, Callable[[], Any]] = {
    "abiove": lambda: abiove.exportacao(ano=2024),
    "anda": lambda: anda.entregas(ano=2024),
    "anec": lambda: anec.embarques(ano=2026),
    "anp_diesel": lambda: alt.anp_diesel.precos_diesel(uf="DF"),
    "antt_pedagio": lambda: alt.antt_pedagio.pracas_pedagio(),
    "b3": lambda: b3.ajustes(data="2026-09-25"),
    "bcb_sgs": lambda: bcb.sgs("selic", ultimos=5),
    "bcb_ptax": lambda: bcb.ptax(data="25/09/2026"),
    "bcb_focus": lambda: bcb.focus(),
    "cftc": lambda: cftc.cot(),
    "comexstat": lambda: comexstat.exportacao("soja", ano=2024),
    "conab_custo": lambda: conab.custo_producao("soja", uf="MT"),
    "defensivos": lambda: defensivos.tecnicos(),
    "deral": lambda: deral.condicao_lavouras(),
    "desmatamento": lambda: desmatamento.prodes(bioma="Cerrado", ano=2022, uf="DF"),
    "embrapa_solos": lambda: embrapa_solos.perfis(uf="DF"),
    "funai": lambda: funai.terras_indigenas(uf="DF"),
    "ibama": lambda: ibama.embargos(uf="DF"),
    "ibge_censo_legado": lambda: ibge.censo_agro_legado("tecnologia", uf="DF"),
    "ibge_sidra": lambda: ibge.pam("soja", ano=2023, nivel="uf"),
    "producao_anual": lambda: datasets.producao_anual("soja", ano=2023),
    "icmbio": lambda: icmbio.ucs(uf="DF"),
    "imea": lambda: imea.cotacoes(),
    "incra": lambda: incra.quilombolas(uf="DF"),
    "inmet": lambda: inmet.historico("A001", 2000),
    "inmet_api": lambda: inmet.estacoes(),
    "lista_suja": lambda: lista_suja.empregadores(),
    "mapa_psr": lambda: alt.mapa_psr.sinistros(ano=2023),
    "mapbiomas": lambda: mapbiomas.cobertura(estado="DF", ano=2022),
    "nasa_power": lambda: nasa_power.clima_ponto(-12.6, -56.1, "2024-01-01", "2024-01-05"),
    "queimadas": lambda: queimadas.focos(ano=2026, mes=9, dia=20, uf="MT"),
    "rnc": lambda: rnc.protegidas(),
    "sfb": lambda: sfb.concessoes(),
    "sicar": lambda: alt.sicar.imoveis("DF"),
    "unica": lambda: unica.safra_resumo(),
    "usda": lambda: usda.psd("soja", country="BR", market_year=2024),
    "zarc": lambda: zarc.zoneamento(cultura="soja", uf="MT"),
}
SEM_ERRO = {("usda", 404): "o PSD responde 404 para ano sem dado (guia §75)"}
NA_MENSAGEM = {"ibge_sidra": "SIDRA: HTTP {status}", "producao_anual": "SIDRA: HTTP {status}"}


class _Chamadas(ast.NodeVisitor):
    """Cada ``.raise_for_status()`` direto, pela função mais interna que o contém."""

    def __init__(self, arquivo: str) -> None:
        self.arquivo = arquivo
        self.funcoes: list[str] = []
        self.achadas: set[tuple[str, str]] = set()

    def _funcao(self, no: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self.funcoes.append(no.name)
        self.generic_visit(no)
        self.funcoes.pop()

    visit_FunctionDef = _funcao
    visit_AsyncFunctionDef = _funcao

    def visit_Call(self, no: ast.Call) -> None:
        if (
            isinstance(no.func, ast.Attribute)
            and no.func.attr == "raise_for_status"
            and not (isinstance(no.func.value, ast.Name) and no.func.value.id == "responses")
        ):
            self.achadas.add((self.arquivo, self.funcoes[-1] if self.funcoes else "<modulo>"))
        self.generic_visit(no)


def _chamadas_diretas() -> set[tuple[str, str]]:
    achadas: set[tuple[str, str]] = set()
    for arquivo in PACOTE.rglob("*.py"):
        visitor = _Chamadas(arquivo.relative_to(PACOTE).as_posix())
        visitor.visit(ast.parse(arquivo.read_text(encoding="utf-8")))
        achadas |= visitor.achadas
    return achadas


def test_status_de_erro_passa_pelo_helper():
    diretas = _chamadas_diretas()

    assert sorted(set(diretas) - set(FORA_DO_HELPER)) == []
    assert sorted(set(FORA_DO_HELPER) - set(diretas)) == []


@pytest.fixture
def servidor(monkeypatch: pytest.MonkeyPatch) -> Callable[[int], None]:
    """Todo pedido HTTP do processo recebe o status dado, com uma página HTML de erro.

    O 403 vem como o desafio do Cloudflare (``cf-mitigated``), que a SIDRA trata à parte.
    """
    monkeypatch.setenv("AGROBR_USDA_API_KEY", "chave-de-teste")

    def instalar(status: int) -> None:
        def responder(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status,
                headers={"content-type": "text/html"}
                | ({"cf-mitigated": "challenge"} if status == 403 else {}),
                stream=httpx.ByteStream(b"<html>erro</html>"),
                request=request,
            )

        async def assincrono(_transporte: Any, request: httpx.Request) -> httpx.Response:
            return responder(request)

        monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", assincrono)
        monkeypatch.setattr(
            httpx.HTTPTransport, "handle_request", lambda _t, request: responder(request)
        )

    zarc.cache.clear()
    return instalar


@pytest.mark.parametrize(
    ("fonte", "status"),
    [
        (fonte, status)
        for fonte in PEDIDOS
        for status in (403, 404)
        if (fonte, status) not in SEM_ERRO
    ],
)
async def test_status_de_erro_sai_como_fonte_indisponivel_com_o_status(servidor, fonte, status):
    servidor(status)

    trecho = NA_MENSAGEM.get(fonte, "HTTP {status}").format(status=status)

    with levanta_exatamente(SourceUnavailableError, trecho):
        await PEDIDOS[fonte]()


@pytest.mark.parametrize(
    ("status", "motivo"),
    [(403, "a fonte recusou o pedido"), (404, "o recurso não existe na URL"), (502, "Bad Gateway")],
)
def test_helper_diz_a_fonte_a_url_e_o_status(status, motivo):
    pedido = httpx.Request("GET", "https://exemplo.gov.br/arquivo.csv")

    with levanta_exatamente(SourceUnavailableError, f"HTTP {status}: {motivo}") as capturada:
        responses.raise_for_status(httpx.Response(status, request=pedido), source="exemplo")

    assert (capturada.value.source, capturada.value.url) == ("exemplo", str(pedido.url))
    assert isinstance(capturada.value.__cause__, httpx.HTTPStatusError)
