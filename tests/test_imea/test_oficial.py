from __future__ import annotations

import hashlib
import warnings
from types import SimpleNamespace

import httpx
import pytest

from agrobr import imea
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.imea import client
from tests import helpers
from tests.test_imea import oficial

SOJA = 4


def instalar(monkeypatch: pytest.MonkeyPatch, trocar: dict[str, bytes] | None = None) -> list[str]:
    servidos = {**oficial.corpos(), **(trocar or {})}
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        corpo = servidos.get(str(request.url))
        if corpo is None:
            return httpx.Response(404, request=request)
        return httpx.Response(
            200,
            content=corpo,
            headers={"content-type": "application/json; charset=utf-8"},
            request=request,
        )

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(client, "httpx", namespace)
    return pedidos


async def test_cada_cadeia_publicada_confere_com_a_fonte(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for cadeia, nome in oficial.CADEIAS.items():
            with check(nome):
                pedidos.clear()
                with helpers.sem_excecao():
                    df, meta = await imea.cotacoes(nome, return_meta=True)
                cotacoes = f"{oficial.BASE}/{cadeia}/cotacoes"
                indicadores = f"{oficial.BASE}/{cadeia}/indicadores"
                esperado = [oficial.esperado(cadeia, r) for r in oficial.registros(cadeia)]
                assert list(df.columns) == oficial.COLUNAS
                assert oficial.publicado(df) == oficial.ordenado(esperado)
                assert sorted(pedidos) == sorted([cotacoes, indicadores])
                assert meta.source_url == cotacoes
                helpers.conferir_corpo(meta, oficial.corpos()[cotacoes])
                catalogo = oficial.corpos()[indicadores]
                sha_catalogo = hashlib.sha256(catalogo).hexdigest()
                assert meta.source_details == {
                    "indicadores_url": indicadores,
                    "indicadores_sha256": sha_catalogo,
                    "indicadores_bytes": len(catalogo),
                    "duplicatas_colapsadas": {"linhas": 0, "indicadores": []},
                    "chaves_repetidas": {"linhas": 0, "indicadores": []},
                }
                assert meta.records_count == len(esperado)


async def test_filtros_de_safra_e_unidade(monkeypatch):
    instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for filtro in ({"safra": "24/25"}, {"safra": "26/27"}, {"unidade": "R$/bag"}):
            with check(filtro):
                with helpers.sem_excecao():
                    df = await imea.cotacoes("soja", **filtro)
                campo = "Safra" if "safra" in filtro else "UnidadeSigla"
                alvo = next(iter(filtro.values()))
                esperado = [
                    oficial.esperado(SOJA, r) for r in oficial.registros(SOJA) if r[campo] == alvo
                ]
                assert esperado
                assert oficial.publicado(df) == oficial.ordenado(esperado)


async def test_nomes_e_numeros_de_cadeia(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for pedido, cadeia in (
            ("soybeans", 4),
            ("boi", 2),
            ("5", 5),
            ("conjuntura", 5),
            ("custo_producao", 10),
            ("10", 10),
        ):
            with check(pedido):
                pedidos.clear()
                with helpers.sem_excecao():
                    df = await imea.cotacoes(pedido)
                assert set(df["cadeia"]) == {oficial.CADEIAS[cadeia]}
                assert pedidos[0] == f"{oficial.BASE}/{cadeia}/cotacoes"
        pedidos.clear()
        for inativa in ("6", "9", "11", "madeira", " 4x", [], 4):
            with (
                check(repr(inativa)),
                helpers.levanta_exatamente(
                    InvalidParameterError, match=r"Cadeia desconhecida: .*Opções: \['soja'"
                ),
            ):
                await imea.cotacoes(inativa)
        assert pedidos == []


async def test_safra_fora_do_formato_recusada_antes_da_rede(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.collect_failures() as check:
        for safra in ("2024", "24/26", "safra", ["24/25"]):
            with check(repr(safra)), helpers.levanta_exatamente(InvalidParameterError):
                await imea.cotacoes("soja", safra=safra)
    assert pedidos == []


async def test_safra_em_qualquer_formato_aceito_filtra_a_safra_do_imea(monkeypatch):
    instalar(monkeypatch)

    curta = await imea.cotacoes("soja", safra="24/25")
    for safra in ("2024/25", "2024/2025"):
        assert (await imea.cotacoes("soja", safra=safra)).equals(curta)
    assert set(curta["safra"]) == {"24/25"}


async def test_argumento_desconhecido_recusado_antes_da_rede(monkeypatch):
    pedidos = instalar(monkeypatch)

    with helpers.levanta_exatamente(TypeError, match="municipio"):
        await imea.cotacoes("soja", municipio="Sorriso")
    assert pedidos == []


async def test_layout_sem_chave_publicada_vira_parse_error():
    corpos = oficial.corpos()
    with helpers.collect_failures() as check:
        for alvo, antes, depois, chave in (
            ("cotacoes", b'"Valor":', b'"Preco":', "Valor"),
            ("indicadores", b'"Nome":', b'"Titulo":', "Nome"),
        ):
            url = f"{oficial.BASE}/{SOJA}/{alvo}"
            with check(alvo), pytest.MonkeyPatch.context() as mp:
                instalar(mp, {url: corpos[url].replace(antes, depois)})
                with helpers.levanta_exatamente(ParseError, match=chave):
                    await imea.cotacoes("soja")


async def test_lista_vazia_da_fonte_e_layout_e_nao_tabela_vazia():
    with helpers.collect_failures() as check:
        for alvo, rotulo in (("cotacoes", "Cotações"), ("indicadores", "Catálogo de indicadores")):
            with check(alvo), pytest.MonkeyPatch.context() as mp:
                instalar(mp, {f"{oficial.BASE}/{SOJA}/{alvo}": b"[]"})
                with helpers.levanta_exatamente(ParseError, match=f"{rotulo} do IMEA vazio"):
                    await imea.cotacoes("soja")


async def test_filtro_sem_correspondencia_mantem_os_dtypes_do_cheio(monkeypatch):
    instalar(monkeypatch)

    cheio = await imea.cotacoes("soja")
    vazio = await imea.cotacoes("soja", safra="10/11")

    assert len(cheio) and vazio.empty
    assert list(vazio.columns) == oficial.COLUNAS
    assert vazio.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert cheio["data_publicacao"].dtype == "datetime64[ns]"


async def test_status_de_erro_nao_vira_dado(monkeypatch):
    url = f"{oficial.BASE}/{SOJA}/cotacoes"
    servidos = oficial.corpos()

    def responder(request: httpx.Request) -> httpx.Response:
        if str(request.url) == url:
            return httpx.Response(403, content=b"[]", request=request)
        return httpx.Response(200, content=servidos[str(request.url)], request=request)

    namespace = SimpleNamespace(**vars(httpx))
    namespace.AsyncClient = lambda **kwargs: httpx.AsyncClient(
        transport=httpx.MockTransport(responder), **kwargs
    )
    monkeypatch.setattr(client, "httpx", namespace)

    with helpers.levanta_exatamente((httpx.HTTPStatusError, SourceUnavailableError)):
        await imea.cotacoes("soja")


async def test_resposta_fora_do_formato_nao_vira_dado():
    with helpers.collect_failures() as check:
        for nome, alvo in (("cotacoes", "cotacoes"), ("indicadores", "indicadores")):
            with check(nome), pytest.MonkeyPatch.context() as mp:
                instalar(mp, {f"{oficial.BASE}/{SOJA}/{alvo}": b'{"erro": "manutencao"}'})
                with helpers.levanta_exatamente(SourceUnavailableError, match="JSON inesperado"):
                    await imea.cotacoes("soja")


async def test_indicador_ausente_do_catalogo_fica_sem_nome(monkeypatch):
    catalogo = oficial.corpos()[f"{oficial.BASE}/{SOJA}/indicadores"]
    sem_semente = catalogo.replace(b'"Id":"3",', b'"Id":"3x",')
    instalar(monkeypatch, {f"{oficial.BASE}/{SOJA}/indicadores": sem_semente})

    with helpers.capturar_logs() as logs, helpers.sem_excecao():
        df = await imea.cotacoes("soja")

    linhas = oficial.publicado(df)
    assert [linha["indicador"] for linha in linhas if linha["indicador_id"] == "3"] == [None]
    assert all(linha["indicador"] for linha in linhas if linha["indicador_id"] != "3")
    avisos = [e["registros"] for e in logs if e["event"] == "imea_indicador_sem_nome"]
    assert avisos == [1]

    instalar(monkeypatch)
    with helpers.capturar_logs() as logs, helpers.sem_excecao():
        await imea.cotacoes("soja")
    assert not [e for e in logs if e["event"] == "imea_indicador_sem_nome"]


async def test_aviso_de_licenca_do_recorte_publico(monkeypatch):
    instalar(monkeypatch)

    with warnings.catch_warnings(record=True) as avisos, helpers.sem_excecao():
        warnings.simplefilter("always")
        await imea.cotacoes("leite")

    assert [str(a.message) for a in avisos if "IMEA" in str(a.message)] == [
        (
            "IMEA: classificação zona_cinza para as séries públicas; licença de reutilização "
            "não comprovada. Arquivos não públicos exigem autorização escrita para "
            "compartilhamento. Ref: https://imea.com.br/imea-site/termo-de-uso.html. Veja "
            "https://www.agrobr.dev/docs/licenses/."
        )
    ]


async def test_as_polars_publica_os_mesmos_valores(monkeypatch):
    pl = pytest.importorskip("polars")
    instalar(monkeypatch)

    with helpers.sem_excecao():
        frame = await imea.cotacoes("leite", as_polars=True)

    assert isinstance(frame, pl.DataFrame)
    esperado = [oficial.esperado(8, r) for r in oficial.registros(8)]
    assert oficial.publicado(frame.to_pandas()) == oficial.ordenado(esperado)
