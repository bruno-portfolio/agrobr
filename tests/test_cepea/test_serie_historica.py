from __future__ import annotations

import asyncio
import hashlib
import io
import json
import warnings
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from unittest.mock import AsyncMock

import duckdb
import httpx
import pandas as pd
import pytest

from agrobr import constants, datasets
from agrobr.cache import duckdb_store
from agrobr.cepea import api, client, serie
from agrobr.cepea.parsers.detector import get_parser_with_fallback
from agrobr.exceptions import ParseError, SourceUnavailableError
from agrobr.models import Indicador
from tests.helpers import levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data" / "cepea" / "serie_historica_20260926"
MANIFESTO = json.loads((GOLDEN / "manifest.json").read_bytes())
HOJE = date(2026, 9, 26)
SERIES = {
    ("soja", "92"): "soja_92.xls",
    ("suino", "129"): "suino_129.xls",
    ("leite", "leitep"): "leite_leitep.xls",
    ("bezerro", "8"): "bezerro_8.xls",
    ("bezerro", "174"): "bezerro_174.xls",
}
AGOSTO = {"inicio": "2026-08-01", "fim": "2026-08-20"}
BAIXADA = datetime(2026, 9, 27, 2, 30)

pytestmark = pytest.mark.usefixtures("serie_historica")


def corpo(nome: str) -> bytes:
    recurso = next(item for item in MANIFESTO["resources"] if item["file"] == nome)
    conteudo = (GOLDEN / nome).read_bytes()
    assert hashlib.sha256(conteudo).hexdigest() == recurso["sha256"]
    return conteudo


def planilha(nome: str) -> pd.DataFrame:
    tabela = pd.read_excel(io.BytesIO(corpo(nome)), engine="calamine", header=None)
    return tabela.iloc[4:].dropna(how="all")


def diario(nome: str, inicio: str, fim: str) -> pd.DataFrame:
    tabela = planilha(nome)
    dias = pd.to_datetime(tabela[0], format="%d/%m/%Y")
    return tabela[(dias >= inicio) & (dias <= fim)]


def avisos_de(emitidos: list[warnings.WarningMessage], prefixo: str) -> list[str]:
    return [str(aviso.message) for aviso in emitidos if str(aviso.message).startswith(prefixo)]


@pytest.fixture
def cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    store = duckdb_store.DuckDBStore(constants.CacheSettings(cache_dir=tmp_path / "cache"))
    monkeypatch.setattr(api, "get_store", lambda: store)
    monkeypatch.setattr(duckdb_store, "get_store", lambda: store)
    monkeypatch.setattr(api, "_today", lambda: HOJE)
    yield store
    store.close()


@pytest.fixture
def baixar(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    async def servir(pagina: str, identificador: str) -> client.SerieBaixada:
        url = f"https://www.cepea.org.br/br/indicador/series/{pagina}.aspx?id={identificador}"
        return client.SerieBaixada(corpo(SERIES[(pagina, identificador)]), url, BAIXADA)

    fetch = AsyncMock(side_effect=servir)
    monkeypatch.setattr(api.client, "fetch_serie", fetch)
    return fetch


@pytest.fixture
def sem_pagina(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    fetch = AsyncMock(side_effect=AssertionError("período fechado não volta à página"))
    monkeypatch.setattr(api, "_fetch_and_parse", fetch)
    return fetch


@pytest.mark.parametrize(
    ("produto", "nome", "pracas"),
    [
        ("soja", "soja_92.xls", {"Paranaguá/PR"}),
        ("suino", "suino_129.xls", set(constants.CEPEA_PRACAS_REGIONAIS["suino"])),
        ("bezerro", "bezerro_8.xls", {"Mato Grosso do Sul"}),
        ("leite", "leite_leitep.xls", None),
    ],
)
def test_serie_publica_cada_valor_positivo_da_planilha(produto, nome, pracas):
    tabela = planilha(nome)
    valores = (
        tabela.iloc[:, 3:4]
        if produto == "leite"
        else tabela.iloc[:, 1 : 6 if produto == "suino" else 2]
    )
    positivos = int((valores.apply(pd.to_numeric, errors="coerce") > 0).sum().sum())

    with sem_excecao():
        indicadores = serie.parse_serie(corpo(nome), produto)

    assert len(indicadores) == positivos
    esperadas = pracas or {str(estado).strip() for estado in tabela[2]}
    assert {ind.praca for ind in indicadores} == esperadas
    assert {ind.parser_version for ind in indicadores} == {constants.CEPEA_SERIE_PARSER_VERSION}


def test_serie_de_outro_indicador_e_recusada():
    with levanta_exatamente(ParseError, "título da série"):
        serie.parse_serie(corpo("soja_92.xls"), "suino")


def test_cabecalho_fora_do_padrao_e_recusado(monkeypatch: pytest.MonkeyPatch):
    tabela = serie.ler_planilha(corpo("soja_92.xls"))
    tabela.iat[3, 1] = "A prazo R$"
    monkeypatch.setattr(serie, "ler_planilha", lambda _conteudo: tabela)
    with levanta_exatamente(ParseError, "cabeçalho da série"):
        serie.parse_serie(b"", "soja")


def test_recuo_para_o_xlrd_le_o_mesmo_e_recusa_arquivo_truncado(monkeypatch: pytest.MonkeyPatch):
    conteudo = corpo("soja_92.xls")
    pelo_calamine = serie.ler_planilha(conteudo)
    ler_excel, abrir_excel = pd.read_excel, pd.ExcelFile

    def sem_calamine(*args, **kwargs):
        if kwargs.get("engine") == "calamine":
            raise ValueError("calamine indisponível")
        return ler_excel(*args, **kwargs)

    monkeypatch.setattr(serie.pd, "read_excel", sem_calamine)
    pelo_xlrd = serie.ler_planilha(conteudo)
    assert pelo_xlrd.fillna("").astype(str).values.tolist() == (
        pelo_calamine.fillna("").astype(str).values.tolist()
    )

    def truncado(*args, **kwargs):
        livro = abrir_excel(*args, **kwargs)
        livro.book.sheet_by_index(0)._dimnrows += 10
        return livro

    monkeypatch.setattr(serie.pd, "ExcelFile", truncado)
    with levanta_exatamente(ParseError, "série truncada"):
        serie.ler_planilha(conteudo)


@pytest.mark.parametrize(
    ("produto", "pagina", "nome"),
    [
        ("soja", "soja_pagina.html", "soja_92.xls"),
        ("suino", "suino_pagina.html", "suino_129.xls"),
        ("bezerro", "bezerro_pagina.html", "bezerro_8.xls"),
        ("leite", "leite_pagina.html", "leite_leitep.xls"),
    ],
)
async def test_linhas_recentes_da_serie_batem_com_a_pagina(produto, pagina, nome):
    _, da_pagina = await get_parser_with_fallback(corpo(pagina).decode("utf-8"), produto)
    da_serie = {
        (ind.praca, ind.data): float(ind.valor) for ind in serie.parse_serie(corpo(nome), produto)
    }

    pares = [(float(ind.valor), da_serie.get((ind.praca, ind.data))) for ind in da_pagina]

    assert len(pares) >= 15
    if produto == "leite":
        centavos = [
            float(Decimal(str(publicado)).quantize(Decimal("0.01"), ROUND_HALF_UP))
            for publicado, _ in pares
        ]
        assert centavos == [gravado for _, gravado in pares]
        assert any(
            centavo != publicado for centavo, (publicado, _) in zip(centavos, pares, strict=True)
        )
    else:
        assert [publicado for publicado, _ in pares] == [gravado for _, gravado in pares]


@pytest.mark.usefixtures("sem_pagina")
async def test_periodo_fechado_vem_da_serie_e_depois_do_cache(cache, baixar):
    esperado = diario("soja_92.xls", **AGOSTO)

    with sem_excecao():
        frame, meta = await api.indicador("soja", **AGOSTO, return_meta=True)
        de_novo, meta_cache = await api.indicador("soja", **AGOSTO, return_meta=True)

    assert frame["valor"].tolist() == esperado[1].astype(float).tolist()
    assert frame["valor_usd"].tolist() == esperado[2].astype(float).tolist()
    assert (meta.selected_source, meta.from_cache, meta.source_method) == (
        "cepea",
        False,
        "httpx+xls",
    )
    sha_da_serie = hashlib.sha256(corpo("soja_92.xls")).hexdigest()
    assert meta.raw_content_hash == sha_da_serie
    assert [recurso["sha256"] for recurso in meta.source_details.get("resources", [])] == [
        meta.raw_content_hash
    ]
    assert (meta.cache_expires_at, meta_cache.cache_expires_at) == (None, None)
    assert (meta_cache.from_cache, meta_cache.selected_source) == (True, "cache")
    assert de_novo["valor"].tolist() == frame["valor"].tolist()
    assert baixar.await_count == 1
    assert cache.serie_cobertura("soja") == (date(2026, 9, 25), BAIXADA)
    assert meta.fetched_at == meta.fetch_timestamp == BAIXADA.replace(tzinfo=UTC)
    assert meta.source_details["resources"][0]["fetched_at"] == "2026-09-27T02:30:00+00:00"


@pytest.mark.usefixtures("cache", "sem_pagina")
async def test_periodo_depois_do_que_a_serie_cobre_baixa_de_novo(baixar, monkeypatch):
    with sem_excecao():
        await api.indicador("soja", **AGOSTO)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 11, 30))
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 11, 30, 15))
    with sem_excecao():
        await api.indicador("soja", **AGOSTO)
    assert baixar.await_count == 1
    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame = await api.indicador("soja", inicio="2026-10-01", fim="2026-10-31")
    assert baixar.await_count == 2
    assert frame.empty
    assert avisos_de(emitidos, "cepea: sem dado de 'soja' entre 2026-10-01 e 2026-10-31")


@pytest.mark.usefixtures("cache", "baixar", "sem_pagina")
async def test_preco_diario_do_periodo_fechado_vem_da_serie():
    esperado = diario("soja_92.xls", **AGOSTO)

    with sem_excecao():
        frame, meta = await datasets.preco_diario("soja", **AGOSTO, return_meta=True)

    obtidos = dict(zip(frame["data"].dt.strftime("%d/%m/%Y"), frame["valor"], strict=True))
    assert obtidos == dict(zip(esperado[0], esperado[1].astype(float), strict=True))
    assert meta.from_cache is False
    assert [recurso["papel"] for recurso in meta.source_details.get("resources", [])] == ["serie"]


@pytest.mark.usefixtures("cache", "sem_pagina")
async def test_periodo_fechado_sem_serie_e_sem_cache_levanta(monkeypatch: pytest.MonkeyPatch):
    falha = SourceUnavailableError("cepea", last_error="série histórica indisponível: HTTP 403")
    monkeypatch.setattr(api.client, "fetch_serie", AsyncMock(side_effect=falha))

    with levanta_exatamente(SourceUnavailableError, "HTTP 403") as erro:
        await api.indicador("soja", **AGOSTO)

    assert erro.value.attempted_sources == ["cepea", "cache"]


@pytest.mark.usefixtures("cache")
async def test_serie_fora_com_a_pagina_no_ar_avisa(monkeypatch: pytest.MonkeyPatch):
    falha = SourceUnavailableError("cepea", last_error="série histórica indisponível: HTTP 403")
    monkeypatch.setattr(api.client, "fetch_serie", AsyncMock(side_effect=falha))
    pagina = client.FetchResult(corpo("soja_pagina.html").decode("utf-8"), "cepea")
    monkeypatch.setattr(api.client, "fetch_indicador_page", AsyncMock(return_value=pagina))

    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await api.indicador(
            "soja", inicio="2026-08-01", fim="2026-09-25", return_meta=True
        )

    prefixo = "cepea: série histórica de 'soja' indisponível"
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith(prefixo)] == avisos_de(
        emitidos, prefixo
    )
    assert len(avisos_de(emitidos, prefixo)) == 1
    assert frame["data"].min() == pd.Timestamp("2026-09-04")


@pytest.mark.usefixtures("baixar", "sem_pagina")
async def test_leite_vale_a_pagina_e_a_serie_completa_o_historico(cache):
    _, da_pagina = await get_parser_with_fallback(
        corpo("leite_pagina.html").decode("utf-8"), "leite"
    )
    cache.indicadores_upsert(api._indicadores_to_dicts(da_pagina))
    publicados = {ind.data: float(ind.valor) for ind in da_pagina if ind.praca == "RS"}
    tabela = planilha("leite_leitep.xls")
    meses = {
        "JAN": 1,
        "FEV": 2,
        "MAR": 3,
        "ABR": 4,
        "MAI": 5,
        "JUN": 6,
        "JUL": 7,
        "AGO": 8,
        "SET": 9,
        "OUT": 10,
        "NOV": 11,
        "DEZ": 12,
    }
    gravados = {
        date(int(ano), meses[str(mes)], 1): float(preco)
        for ano, mes, estado, preco in tabela.itertuples(index=False)
        if str(estado).strip() == "RS" and int(ano) >= 2025
    }

    with sem_excecao():
        frame, meta = await api.indicador(
            "leite", inicio="2025-01-01", fim="2026-07-31", praca="RS", return_meta=True
        )

    obtidos = dict(zip(frame["data"].dt.date, frame["valor"], strict=True))
    assert obtidos == {**gravados, **publicados}
    assert any(publicados[dia] != gravados[dia] for dia in publicados)
    assert meta.source_details.get("pagina_desde") == min(publicados).isoformat()


@pytest.mark.usefixtures("cache", "baixar", "sem_pagina")
async def test_bezerro_do_historico_traz_o_peso_medio():
    pesos = diario("bezerro_174.xls", "2025-01-02", "2025-01-31")

    with sem_excecao():
        frame, meta = await api.indicador(
            "bezerro", inicio="2025-01-02", fim="2025-01-31", return_meta=True
        )

    assert frame["peso_medio_kg"].tolist() == pesos[1].astype(float).tolist()
    assert [recurso.get("papel") for recurso in meta.source_details.get("resources", [])] == [
        "serie",
        "serie_peso",
    ]
    assert meta.fetched_at == meta.fetch_timestamp == BAIXADA.replace(tzinfo=UTC)


@pytest.mark.usefixtures("cache", "sem_pagina")
async def test_laranja_sem_serie_avisa_e_nao_baixa(baixar):
    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await api.indicador(
            "laranja_industria", inicio="2025-01-01", fim="2025-01-31", return_meta=True
        )

    prefixo = "cepea: o CEPEA não publica série histórica de 'laranja_industria'"
    assert frame.empty
    assert avisos_de(emitidos, prefixo)
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith(prefixo)]
    baixar.assert_not_awaited()


@pytest.fixture
def circuito(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(client, "_circuit_state", {})


def _resposta(url: str, status: int, conteudo: bytes) -> httpx.Response:
    return httpx.Response(status, content=conteudo, request=httpx.Request("GET", url))


@pytest.mark.usefixtures("circuito")
async def test_fetch_serie_devolve_o_xls_e_pula_o_endereco_bloqueado(
    monkeypatch: pytest.MonkeyPatch,
):
    conteudo = corpo("soja_92.xls")
    pedidos: list[str] = []

    async def get(url: str, _headers: dict[str, str]) -> httpx.Response:
        pedidos.append(url)
        if len(pedidos) == 1:
            bloqueio = _resposta(url, 403, b"Forbidden")
            raise httpx.HTTPStatusError(
                "403 Forbidden", request=bloqueio.request, response=bloqueio
            )
        return _resposta(url, 200, conteudo)

    monkeypatch.setattr(client, "_get", get)
    baixada = await client.fetch_serie("soja", "92")
    de_novo = await client.fetch_serie("soja", "92")

    assert (baixada.conteudo, de_novo.conteudo) == (conteudo, conteudo)
    assert baixada.url == pedidos[1] == pedidos[2]
    assert all(url.endswith("/br/indicador/series/soja.aspx?id=92") for url in pedidos)
    assert len(pedidos) == 3


@pytest.mark.usefixtures("circuito")
async def test_fetch_serie_recusa_html_e_arquivo_acima_do_teto(monkeypatch: pytest.MonkeyPatch):
    respostas = iter([b"<!DOCTYPE html>" + b" " * 600, corpo("soja_92.xls")])

    async def get(url: str, _headers: dict[str, str]) -> httpx.Response:
        return _resposta(url, 200, next(respostas))

    monkeypatch.setattr(client, "_get", get)
    monkeypatch.setattr(constants, "CEPEA_SERIE_MAX_BYTES", 1000)
    with levanta_exatamente(SourceUnavailableError, "acima do teto") as erro:
        await client.fetch_serie("soja", "92")
    assert erro.value.attempted_sources == ["cepea"]


@pytest.mark.usefixtures("cache")
async def test_force_refresh_baixa_serie_e_pagina_e_lista_os_2_corpos(baixar, monkeypatch):
    pagina = corpo("soja_pagina.html")
    fetch = AsyncMock(return_value=client.FetchResult(pagina.decode("utf-8"), "cepea"))
    monkeypatch.setattr(api.client, "fetch_indicador_page", fetch)
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 9, 27, 3))

    with sem_excecao():
        await api.indicador("soja", **AGOSTO)
        _, meta = await api.indicador("soja", **AGOSTO, force_refresh=True, return_meta=True)

    recursos = meta.source_details.get("resources", [])
    assert baixar.await_count == 2
    assert fetch.await_count == 1
    assert [recurso["papel"] for recurso in recursos] == ["serie", "pagina"]
    sha_da_pagina = hashlib.sha256(pagina).hexdigest()
    assert recursos[1]["sha256"] == sha_da_pagina
    assert (meta.raw_content_hash, meta.raw_content_size) == (None, 0)
    horas = [datetime.fromisoformat(recurso["fetched_at"]) for recurso in recursos]
    assert horas == [BAIXADA.replace(tzinfo=UTC), datetime(2026, 9, 27, 3, tzinfo=UTC)]
    assert meta.fetched_at == meta.fetch_timestamp == max(horas)


@pytest.mark.usefixtures("sem_pagina", "baixar")
async def test_cache_com_erro_ainda_devolve_a_serie(cache, monkeypatch):
    def quebrado(*_args, **_kwargs):
        raise duckdb.Error("cache corrompido")

    monkeypatch.setattr(cache, "indicadores_query", quebrado)
    esperado = diario("soja_92.xls", **AGOSTO)

    with sem_excecao():
        frame = await api.indicador("soja", **AGOSTO)

    assert frame["valor"].tolist() == esperado[1].astype(float).tolist()


def _erro_de_abertura(caminho: Path) -> duckdb.Error | None:
    """O erro da abertura que o cache faz em toda operação (conexão e esquema, como no
    `_get_conn`); consulta com filtro pode não chegar ao bloco que falta."""
    try:
        with duckdb.connect(str(caminho)) as conexao:
            duckdb_store.DuckDBStore._init_schema(conexao)
    except duckdb.Error as erro:
        return erro
    return None


def _danificar(caminho: Path) -> None:
    """Trunca o banco pela metade; se o DuckDB instalado ainda o abre (o 2.0 abre), zera o bloco
    de cabeçalho com a assinatura do arquivo, que nenhuma versão abre."""
    with caminho.open("r+b") as arquivo:
        arquivo.truncate(caminho.stat().st_size // 2)
    if _erro_de_abertura(caminho) is None:
        with caminho.open("r+b") as arquivo:
            arquivo.write(bytes(4096))
    erro = _erro_de_abertura(caminho)
    assert erro is not None and duckdb_store._arquivo_danificado(erro), erro


@pytest.mark.usefixtures("sem_pagina")
async def test_cache_danificado_vai_para_o_lado_e_a_consulta_segue(cache, baixar):
    await api.indicador("soja", **AGOSTO)
    _danificar(cache.db_path)
    esperado = diario("soja_92.xls", **AGOSTO)

    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame, meta = await api.indicador("soja", **AGOSTO, return_meta=True)
        de_novo, meta_cache = await api.indicador("soja", **AGOSTO, return_meta=True)

    movidos = list(cache.db_path.parent.glob("agrobr.duckdb.corrompido-*"))
    assert len(movidos) == 1
    assert frame["valor"].tolist() == esperado[1].astype(float).tolist()
    assert de_novo["valor"].tolist() == frame["valor"].tolist()
    assert (meta.from_cache, meta_cache.from_cache) == (False, True)
    assert baixar.await_count == 2
    [aviso] = avisos_de(emitidos, "agrobr: o cache em")
    assert f"o cache em {cache.db_path} está danificado" in aviso
    assert f"movido para {movidos[0]}" in aviso


@pytest.mark.usefixtures("cache", "sem_pagina")
async def test_serie_de_layout_novo_e_sem_cache_levanta_parse_error(monkeypatch):
    url = "https://www.cepea.org.br/br/indicador/series/soja.aspx?id=92"
    trocada = client.SerieBaixada(corpo("suino_129.xls"), url, datetime(2026, 9, 27, 2, 30))
    monkeypatch.setattr(api.client, "fetch_serie", AsyncMock(return_value=trocada))

    with levanta_exatamente(ParseError, "título da série"):
        await api.indicador("soja", **AGOSTO)


def test_cobertura_da_serie_sem_conexao_e_nula(cache, monkeypatch):
    monkeypatch.setattr(cache, "_get_conn", lambda: None)
    cache.serie_registrar("soja", date(2026, 9, 25), BAIXADA)
    assert cache.serie_cobertura("soja") is None


@pytest.mark.usefixtures("sem_pagina")
async def test_leite_com_a_pagina_no_cache_baixa_a_serie_uma_vez(cache, baixar, monkeypatch):
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 9, 27, 3))
    _, da_pagina = await get_parser_with_fallback(
        corpo("leite_pagina.html").decode("utf-8"), "leite"
    )
    cache.indicadores_upsert(api._indicadores_to_dicts(da_pagina))

    for _ in range(3):
        with sem_excecao():
            await api.indicador("leite", inicio="2025-01-01", fim="2026-08-31")

    assert baixar.await_count == 1
    assert cache.serie_cobertura("leite") == (date(2026, 7, 1), BAIXADA)


@pytest.mark.usefixtures("cache", "sem_pagina")
async def test_dentro_da_validade_a_serie_nao_se_baixa_de_novo(baixar, monkeypatch):
    monkeypatch.setattr(api, "utcnow", lambda: datetime(2026, 9, 27, 3))
    with sem_excecao():
        await api.indicador("soja", **AGOSTO)
    monkeypatch.setattr(api, "_today", lambda: date(2026, 11, 30))
    with sem_excecao():
        await api.indicador("soja", inicio="2026-10-01", fim="2026-10-31")
    assert baixar.await_count == 1


@pytest.fixture
def transporte(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    monkeypatch.setattr(client, "_circuit_state", {})
    recursos = {recurso["url"]: recurso for recurso in MANIFESTO["resources"]}
    original = httpx.AsyncClient
    servidos: list[str] = []

    async def responder(request: httpx.Request) -> httpx.Response:
        recurso = recursos[str(request.url)]
        servidos.append(recurso["file"])
        await asyncio.sleep(0)
        return httpx.Response(
            200,
            content=corpo(recurso["file"]),
            headers={"Content-Type": recurso["content_type"]},
            request=request,
        )

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kwargs: original(transport=httpx.MockTransport(responder), **kwargs),
    )
    return servidos


def _por_dia(frame: pd.DataFrame) -> dict[str, float]:
    return dict(zip(frame["data"].dt.strftime("%d/%m/%Y"), frame["valor"], strict=True))


@pytest.mark.parametrize(
    "periodos",
    [
        pytest.param([("2020-01-01", "2020-12-31")] * 2, id="mesmo_pedido"),
        pytest.param(
            [("2020-01-01", "2020-12-31"), ("2021-01-01", "2021-12-31")], id="anos_diferentes"
        ),
    ],
)
@pytest.mark.usefixtures("cache")
async def test_consultas_simultaneas_trazem_a_serie_inteira(transporte, periodos):
    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        resultados = await asyncio.gather(
            *(
                api.indicador("soja", inicio=inicio, fim=fim, return_meta=True)
                for inicio, fim in periodos
            )
        )

    assert transporte == ["soja_92.xls", "soja_92.xls"]
    for (inicio, fim), (frame, meta) in zip(periodos, resultados, strict=True):
        esperado = diario("soja_92.xls", inicio, fim)
        assert len(frame) == len(esperado) > 200
        assert _por_dia(frame) == dict(zip(esperado[0], esperado[1].astype(float), strict=True))
        assert (meta.selected_source, meta.from_cache, meta.validation_warnings) == (
            "cepea",
            False,
            [],
        )
    assert not avisos_de(emitidos, "cepea: sem dado")


async def test_force_refresh_com_o_cache_cheio_traz_a_serie_inteira(cache, transporte):
    ano = {"inicio": "2020-01-01", "fim": "2020-12-31"}
    recente = {"inicio": "2026-01-01", "fim": "2026-09-26"}
    ano_no_cache = {"inicio": datetime(2020, 1, 1), "fim": datetime(2020, 12, 31, 23, 59)}

    with sem_excecao():
        await api.indicador("soja", **ano)
        no_cache = cache.indicadores_query("soja", **ano_no_cache)
        fechado = await api.indicador("soja", **ano, force_refresh=True)
        aberto, meta = await api.indicador("soja", **recente, force_refresh=True, return_meta=True)

    assert transporte == [
        "soja_92.xls",
        "soja_92.xls",
        "soja_pagina.html",
        "soja_92.xls",
        "soja_pagina.html",
    ]
    for frame, periodo in ((fechado, ano), (aberto, recente)):
        esperado = diario("soja_92.xls", **periodo)
        assert len(frame) == len(esperado) > 150
        assert _por_dia(frame) == dict(zip(esperado[0], esperado[1].astype(float), strict=True))
    assert [recurso["papel"] for recurso in meta.source_details["resources"]] == [
        "serie",
        "pagina",
    ]
    assert cache.indicadores_query("soja", **ano_no_cache) == no_cache


@pytest.mark.usefixtures("cache", "baixar", "sem_pagina")
async def test_periodo_que_a_serie_nao_cobre_avisa_ja_no_primeiro_download(monkeypatch):
    monkeypatch.setattr(api, "_today", lambda: date(2026, 11, 30))

    with warnings.catch_warnings(record=True) as emitidos, sem_excecao():
        warnings.simplefilter("always")
        frame = await api.indicador("soja", inicio="2026-10-01", fim="2026-10-31")

    assert frame.empty
    assert avisos_de(emitidos, "cepea: sem dado de 'soja' entre 2026-10-01 e 2026-10-31")


MANTIDO = '["valor_mantido"]'


def mantidos_na_planilha() -> set[str]:
    tabela = planilha("soja_92.xls")
    ordem = tabela.assign(dia=pd.to_datetime(tabela[0], format="%d/%m/%Y")).sort_values("dia")
    ordem = ordem[pd.to_numeric(ordem[1], errors="coerce") > 0]
    valores = pd.to_numeric(ordem[1])
    repete = valores.eq(valores.shift()) & (ordem["dia"] < "2015-05-04")
    return set(ordem.loc[repete, "dia"].dt.strftime("%Y-%m-%d"))


def marcadas(frame: pd.DataFrame) -> set[str]:
    return set(frame.loc[frame["anomalies"] == MANTIDO, "data"].dt.strftime("%Y-%m-%d"))


@pytest.mark.usefixtures("cache", "baixar", "sem_pagina")
async def test_valor_mantido_do_segundo_pregao_do_trecho_em_diante():
    periodo = {"inicio": "2014-09-24", "fim": "2014-10-03"}

    with sem_excecao():
        da_serie = await api.indicador("soja", **periodo)
        do_cache = await api.indicador("soja", **periodo)
        offline = await api.indicador("soja", **periodo, offline=True)
        meio_do_trecho = await api.indicador("soja", inicio="2014-10-01", fim="2014-10-03")

    esperado = {
        "2014-09-24",
        "2014-09-25",
        "2014-09-26",
        "2014-09-30",
        "2014-10-01",
        "2014-10-02",
        "2014-10-03",
    }
    assert esperado == mantidos_na_planilha() & set(da_serie["data"].dt.strftime("%Y-%m-%d"))
    for frame in (da_serie, do_cache, offline):
        assert marcadas(frame) == esperado
        assert frame.loc[frame["data"] == "2014-09-29", "anomalies"].isna().all()
        assert frame["valor"].tolist() == diario("soja_92.xls", **periodo)[1].astype(float).tolist()
    assert marcadas(meio_do_trecho) == {"2014-10-01", "2014-10-02", "2014-10-03"}


@pytest.mark.usefixtures("cache", "baixar", "sem_pagina")
async def test_valor_mantido_na_serie_inteira_e_nada_desde_04_05_2015():
    with sem_excecao():
        ate_o_corte = await api.indicador("soja", inicio="2006-01-01", fim="2015-05-03")
        depois = await api.indicador("soja", inicio="2015-05-04", fim="2015-08-31")

    assert marcadas(ate_o_corte) == mantidos_na_planilha()
    assert len(marcadas(ate_o_corte)) == 551
    repetido = depois.set_index("data")["valor"]
    assert repetido["2015-05-12"] == repetido["2015-05-11"]
    assert depois["anomalies"].isna().all()


async def test_valor_mantido_so_na_soja_paranagua(cache):
    repetidos = [
        Indicador(
            fonte=constants.Fonte.CEPEA,
            produto="soja_parana",
            praca="Paraná",
            data=date(2014, 10, dia),
            valor=Decimal("55.00"),
            unidade="BRL/sc60kg",
        )
        for dia in (1, 2, 3)
    ]
    cache.indicadores_upsert(api._indicadores_to_dicts(repetidos))

    with sem_excecao():
        frame = await api.indicador(
            "soja_parana", inicio="2014-10-01", fim="2014-10-03", offline=True
        )

    assert len(frame) == 3
    assert frame["anomalies"].isna().all()


@pytest.mark.usefixtures("cache", "baixar", "sem_pagina")
async def test_valor_mantido_chega_ao_preco_diario_e_ao_deterministico():
    periodo = {"inicio": "2014-09-29", "fim": "2014-10-03"}

    with sem_excecao():
        frame = await datasets.preco_diario("soja", **periodo)
        async with datasets.deterministic("2014-12-31"):
            congelado = await datasets.preco_diario("soja", **periodo)

    esperado = {"2014-09-30", "2014-10-01", "2014-10-02", "2014-10-03"}
    assert marcadas(frame) == marcadas(congelado) == esperado


@pytest.mark.usefixtures("baixar", "sem_pagina")
async def test_valor_mantido_sem_cache_usa_a_serie_baixada(cache, monkeypatch):
    monkeypatch.setattr(cache, "_get_conn", lambda: None)
    periodo = {"inicio": "2014-10-01", "fim": "2014-10-03"}

    with sem_excecao():
        da_fonte = await api.indicador("soja", **periodo)
        do_dataset = await datasets.preco_diario("soja", **periodo)

    for frame in (da_fonte, do_dataset):
        assert marcadas(frame) == {"2014-10-01", "2014-10-02", "2014-10-03"}
