from __future__ import annotations

import warnings
from types import SimpleNamespace

import httpx
import pytest

from agrobr import rio_verde
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.rio_verde import client, parser
from tests import helpers
from tests.test_rio_verde import oficial

SAFRAS = ("2023/2024", "2024/2025", "2025/2026")
LINHAS = [
    {
        "empresa": "Agroeste",
        "cultivar": "AS 3800 i2x",
        "gm": "8.0",
        "ciclo": "113",
        "e1": "96,7",
        "e2": "82,9",
        "e3": "81,5",
        "e4": "77,5",
        "media": "84,7",
    },
    {
        "empresa": "HO Sementes",
        "cultivar": "HO Mogi i2x",
        "gm": "6.9",
        "ciclo": "103",
        "e1": "-",
        "e2": "91",
        "e3": "84,9",
        "e4": "75,2",
        "media": "83,7",
    },
    {
        "empresa": "Brasmax",
        "cultivar": "BMX Guepardo IPRO",
        "gm": "6.7",
        "ciclo": "105",
        "e1": "-",
        "e2": "-",
        "e3": "95,9",
        "e4": "87,7",
        "media": "91,8",
    },
    {
        "empresa": "Sementes (Brasil)",
        "cultivar": "SB 71 (i2x)",
        "gm": "7.1",
        "ciclo": "108",
        "e1": "-",
        "e2": "88,0",
        "e3": "80,2",
        "e4": "79,1",
        "media": "82,4",
    },
]
RESUMO = "Empresa Cultivar G.M. Ciclo Produtividade média (sc/ha)"
RESULTADOS = f"Resultados por época de semeadura. {RESUMO}"
GRAFICO: list[list[str | None]] = [
    ["", "Produtividade (sc/ha)", ""],
    *([f"{n}ª época", "90,0", ""] for n in range(1, 5)),
]


def instalar(monkeypatch: pytest.MonkeyPatch, trocar: dict[str, bytes] | None = None) -> list[str]:
    servidos = {**oficial.corpos(), **(trocar or {})}
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        corpo = servidos.get(str(request.url))
        if corpo is None:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200, content=corpo, headers={"content-type": "application/pdf"}, request=request
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(client, "httpx", namespace)
    return pedidos


def tabela(linhas: list[dict[str, str]], coluna_nova: bool = False) -> list[list[str | None]]:
    altura = ["85"] if coluna_nova else []
    cabecalho: list[str | None] = [
        "Empresa",
        "Cultivar",
        "G.M.",
        "Ciclo",
        *(["Altura (cm)"] if coluna_nova else []),
        "Produtividade (sc/ha)",
        None,
        None,
        None,
        None,
    ]
    corpo: list[list[str | None]] = [
        [
            linha["empresa"],
            linha["cultivar"],
            linha["gm"],
            linha["ciclo"],
            *altura,
            *(linha[epoca] for epoca in ("e1", "e2", "e3", "e4", "media")),
        ]
        for linha in linhas
    ]
    vazia: list[str | None] = [None] * len(cabecalho)
    return [cabecalho, vazia, vazia, *corpo]


def pdf_falso(
    monkeypatch: pytest.MonkeyPatch, *paginas: tuple[str, list[list[list[str | None]]]]
) -> None:
    documento = SimpleNamespace(
        pages=[
            SimpleNamespace(
                extract_text=lambda texto=texto: texto,
                extract_tables=lambda tabelas=tabelas: tabelas,
            )
            for texto, tabelas in paginas
        ],
        close=lambda: None,
    )
    monkeypatch.setattr(
        parser, "_check_pdfplumber", lambda: SimpleNamespace(open=lambda _: documento)
    )


async def test_cada_safra_publicada_confere_com_o_pdf(monkeypatch):
    pytest.importorskip("pdfplumber")
    pedidos = instalar(monkeypatch)
    dados = oficial.oraculo()

    with helpers.collect_failures() as check:
        for safra in SAFRAS:
            with check(safra):
                pedidos.clear()
                with helpers.sem_excecao():
                    df, meta = await rio_verde.ensaio_soja(safra, return_meta=True)
                url = dados["safras"][safra]["url"]
                assert pedidos == [url]
                assert list(df.columns) == oficial.COLUNAS
                assert oficial.publicado(df) == oficial.esperado(safra)
                assert meta.source_url == url
                helpers.conferir_corpo(meta, oficial.corpos()[url])
                assert meta.records_count == len(dados["safras"][safra]["linhas"])
    assert await rio_verde.safras_disponiveis() == list(SAFRAS)


async def test_layout_novo_na_pagina_do_resumo(monkeypatch):
    instalar(monkeypatch)

    with helpers.collect_failures() as check:
        with check("gráfico ao lado da tabela-resumo"):
            pdf_falso(monkeypatch, (RESUMO, [GRAFICO, tabela(LINHAS)]))
            with helpers.sem_excecao():
                df = await rio_verde.ensaio_soja("2024/2025")
            assert oficial.publicado(df) == oficial.esperado("2024/2025", LINHAS)
        with check("linha em branco entre as cultivares"):
            espacada = tabela(LINHAS)
            espacada.insert(5, [None] * len(espacada[0]))
            pdf_falso(monkeypatch, (RESUMO, [espacada]))
            with helpers.sem_excecao():
                df = await rio_verde.ensaio_soja("2024/2025")
            assert oficial.publicado(df) == oficial.esperado("2024/2025", LINHAS)
        with check("nome quebrado em duas linhas dentro da célula"):
            quebrada = tabela(LINHAS)
            quebrada[5][1] = "BMX Guepardo\nIPRO"
            pdf_falso(monkeypatch, (RESUMO, [quebrada]))
            with helpers.sem_excecao():
                df = await rio_verde.ensaio_soja("2024/2025")
            assert oficial.publicado(df) == oficial.esperado("2024/2025", LINHAS)
        with check("coluna nova entre o ciclo e as épocas"):
            pdf_falso(monkeypatch, (RESUMO, [tabela(LINHAS, coluna_nova=True)]))
            with helpers.levanta_exatamente(ParseError, match="esperadas 9 células, recebidas 10"):
                await rio_verde.ensaio_soja("2024/2025")
        with check("mesmo cabeçalho na seção de resultados"):
            pdf_falso(
                monkeypatch,
                (RESUMO, [tabela(LINHAS)]),
                (RESULTADOS, [tabela(LINHAS[:1])]),
            )
            with helpers.sem_excecao():
                df = await rio_verde.ensaio_soja("2024/2025")
            assert oficial.publicado(df) == oficial.esperado("2024/2025", LINHAS)
        with check("tabela-resumo ausente"):
            pdf_falso(monkeypatch, (RESUMO, [GRAFICO]))
            with helpers.levanta_exatamente(ParseError, match="Nenhum registro"):
                await rio_verde.ensaio_soja("2024/2025")


async def test_filtros_de_cultivar_e_empresa(monkeypatch):
    instalar(monkeypatch)
    pdf_falso(monkeypatch, (RESUMO, [tabela(LINHAS)]))

    with helpers.collect_failures() as check:
        for filtro, indice in (
            ({"cultivar": "mogi"}, 1),
            ({"empresa": "AGROESTE"}, 0),
            ({"cultivar": "SB 71 (i2x)"}, 3),
            ({"empresa": "Sementes (Brasil)"}, 3),
        ):
            with check(filtro):
                with helpers.sem_excecao():
                    df = await rio_verde.ensaio_soja("2024/2025", **filtro)
                assert oficial.publicado(df) == oficial.esperado("2024/2025", [LINHAS[indice]])


async def test_safras_fora_do_catalogo_recusadas_antes_da_rede(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for safra, mensagem in (
            ("2022/2023", "layout que o agrobr não lê"),
            ("2019/2020", "não disponível"),
            ("2024/25", "não disponível"),
            ([], "safra deve ser texto, recebido \\[\\]. Opções"),
            (2023, "safra deve ser texto, recebido 2023. Opções"),
        ):
            with (
                check(repr(safra)),
                helpers.levanta_exatamente(InvalidParameterError, match=mensagem),
            ):
                await rio_verde.ensaio_soja(safra)
    assert pedidos == []


async def test_argumento_desconhecido_recusado_antes_da_rede(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.levanta_exatamente(TypeError, match="produtividade"):
        await rio_verde.ensaio_soja("2025/2026", produtividade=80)
    assert pedidos == []


async def test_download_invalido_nao_vira_dado():
    pytest.importorskip("pdfplumber")
    url = oficial.oraculo()["safras"]["2025/2026"]["url"]
    pdf = oficial.corpos()[url]
    with helpers.collect_failures() as check:
        for nome, corpo, erro, mensagem in (
            (
                "html",
                b"<html><body>manutencao</body></html>" * 3000,
                SourceUnavailableError,
                "Assinatura",
            ),
            ("pequeno", pdf[:20_000], SourceUnavailableError, "muito pequeno"),
            ("truncado", pdf[:200_000], ParseError, "PDF|registro"),
        ):
            with check(nome), pytest.MonkeyPatch.context() as mp:
                instalar(mp, {url: corpo})
                with helpers.levanta_exatamente(erro, match=mensagem):
                    await rio_verde.ensaio_soja("2025/2026")


async def test_aviso_de_licenca(monkeypatch):
    instalar(monkeypatch)
    pdf_falso(monkeypatch, (RESUMO, [tabela(LINHAS)]))

    with warnings.catch_warnings(record=True) as avisos, helpers.sem_excecao():
        warnings.simplefilter("always")
        await rio_verde.ensaio_soja("2023/2024")

    assert [str(a.message) for a in avisos if "Rio Verde" in str(a.message)] == [
        (
            "Fundação Rio Verde: fonte privada sem licença de reutilização dos resultados "
            "localizada. Classificação: zona_cinza. Preserve a atribuição; ela não substitui "
            "eventual permissão necessária. Veja https://www.agrobr.dev/docs/licenses/."
        )
    ]


async def test_as_polars_publica_os_mesmos_valores(monkeypatch):
    pl = pytest.importorskip("polars")
    instalar(monkeypatch)
    pdf_falso(monkeypatch, (RESUMO, [tabela(LINHAS)]))

    with helpers.sem_excecao():
        frame = await rio_verde.ensaio_soja("2024/2025", as_polars=True)

    assert isinstance(frame, pl.DataFrame)
    assert oficial.publicado(frame.to_pandas()) == oficial.esperado("2024/2025", LINHAS)


async def test_ciclo_dias_inteiro_no_cheio_e_no_vazio(monkeypatch):
    instalar(monkeypatch)
    pdf_falso(monkeypatch, (RESUMO, [tabela(LINHAS)]))

    with helpers.sem_excecao():
        cheio = await rio_verde.ensaio_soja("2024/2025")
        vazio = await rio_verde.ensaio_soja("2024/2025", cultivar="nenhuma")

    assert vazio.empty
    assert str(cheio["ciclo_dias"].dtype) == "Int64"
    assert cheio.dtypes.to_dict() == vazio.dtypes.to_dict()
    assert cheio["ciclo_dias"].tolist() == [113, 103, 105, 108]
