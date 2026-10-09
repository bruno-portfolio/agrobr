from __future__ import annotations

import hashlib
import json
import warnings
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from agrobr import mapbiomas_alerta
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from agrobr.mapbiomas_alerta import api, client, models
from tests.helpers import (
    capturar_logs,
    conferir_corpo,
    levanta_exatamente,
    make_sleep_tracker,
    sem_excecao,
)
from tests.test_mapbiomas_alerta.oficial import FIM, GOLDEN, INICIO, MANIFESTO, ORACULO, servir

wkt = pytest.importorskip("shapely.wkt")

CAIXA = (-55.0, -8.0, -50.0, -3.0)
COLUNAS = [
    "alert_code",
    "area_ha",
    "data_deteccao",
    "data_publicacao",
    "status",
    "fonte",
    "lat",
    "lon",
]
SEMANA = {"token": "t", "inicio": INICIO, "fim": FIM, "max_registros": None}
DA_SEMANA = {"limit", "page", "sortField", "sortDirection", "dateType", "startDate", "endDate"}


def _recibos_da_semana() -> list[dict]:
    return [
        recibo
        for recibo in MANIFESTO["recibos"]
        if recibo["query"] == models.ALERTS_QUERY
        and set(recibo["variables"]) == DA_SEMANA
        and recibo["variables"]["dateType"] == "DetectedAt"
    ]


def _esperado(alertas: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(alertas, columns=[*COLUNAS, "posicao", "geometry_wkt_sha256"])[COLUNAS]
    df["data_deteccao"] = pd.to_datetime(df["data_deteccao"])
    df["data_publicacao"] = pd.to_datetime(df["data_publicacao"])
    return df


def _observado(df: pd.DataFrame) -> pd.DataFrame:
    return df[COLUNAS].astype({"status": object, "fonte": object}).reset_index(drop=True)


def _editar(corpo: bytes, editar) -> bytes:
    dado = json.loads(corpo)
    editar(dado["data"]["alerts"])
    return json.dumps(dado).encode()


def _publicadas() -> dict:
    referencia = json.loads((GOLDEN / "referencia_antes.json").read_bytes())
    return {
        a["alertCode"]: wkt.loads(a["geometryWkt"])
        for a in referencia["data"]["alerts"]["collection"]
    }


async def _com_avisos(**kwargs):
    with warnings.catch_warnings(record=True) as registro:
        warnings.simplefilter("always")
        with sem_excecao():
            df, meta = await mapbiomas_alerta.alertas(return_meta=True, **kwargs)
    return df, meta, [str(aviso.message) for aviso in registro if aviso.category is UserWarning]


async def test_semana_paginada_bate_com_a_referencia_em_pagina_unica(monkeypatch):
    visto = servir(monkeypatch)
    df, meta, avisos = await _com_avisos(**SEMANA)

    assert [v["page"] for v in visto["servidos"]] == [1, 2, 3]
    assert visto["autorizacao"] == ["Bearer t"] * 3
    pd.testing.assert_frame_equal(_observado(df), _esperado(ORACULO["alertas"]), check_dtype=False)
    assert round(df["area_ha"].sum(), 4) == ORACULO["area_ha_total"]
    assert meta.records_count == ORACULO["total_anunciado"] == 83
    assert avisos == meta.validation_warnings == []
    assert meta.raw_content_hash is None and meta.raw_content_size == 0
    assert meta.source_details["tipo_data"] == "deteccao"
    assert meta.source_details["total_anunciado"] == 83
    assert [(c["pagina"], c["sha256"], c["bytes"]) for c in meta.source_details["corpos"]] == [
        (recibo["variables"]["page"], recibo["sha256"], recibo["bytes"])
        for recibo in _recibos_da_semana()
    ]


async def test_geo_traz_a_geometria_publicada(monkeypatch):
    servir(monkeypatch)
    with sem_excecao():
        gdf, meta = await mapbiomas_alerta.alertas_geo(**SEMANA, return_meta=True)

    publicadas = _publicadas()
    assert meta.selected_source == "mapbiomas_alerta_graphql_geo"
    assert meta.records_count == 83 and len(meta.source_details["corpos"]) == 3
    assert gdf.crs == "EPSG:4326"
    pd.testing.assert_frame_equal(_observado(gdf), _esperado(ORACULO["alertas"]), check_dtype=False)
    diferentes = [
        codigo
        for codigo, geometria in zip(gdf["alert_code"], gdf.geometry, strict=True)
        if geometria is None or not geometria.equals(publicadas[codigo])
    ]
    assert diferentes == []


async def test_wkt_invalido_ou_nulo_vira_geometria_nula_sem_perder_o_alerta(monkeypatch):
    def sem_geometria(alerts):
        alerts["collection"][0]["geometryWkt"] = "POLYGON ((0 0"
        alerts["collection"][1]["geometryWkt"] = None

    servir(monkeypatch, lambda v, corpo: _editar(corpo, sem_geometria) if v["page"] == 1 else corpo)
    with sem_excecao():
        gdf = await mapbiomas_alerta.alertas_geo(**SEMANA)

    publicadas = _publicadas()
    assert len(gdf) == 83
    assert list(gdf.geometry.iloc[:2]) == [None, None]
    assert all(
        g.equals(publicadas[c])
        for c, g in zip(gdf["alert_code"][2:], gdf.geometry[2:], strict=True)
    )


async def test_wkt_aninhado_vira_geometria_nula_sem_chamar_o_shapely(monkeypatch):
    aninhado = "GEOMETRYCOLLECTION (" * 40 + "POINT (1 1)" + ")" * 40

    def aninhar(alerts):
        alerts["collection"][0]["geometryWkt"] = aninhado

    servir(monkeypatch, lambda v, corpo: _editar(corpo, aninhar) if v["page"] == 1 else corpo)
    lidos: list[str] = []
    loads = wkt.loads

    def registrar(texto, *args, **kwargs):
        lidos.append(texto)
        return loads(texto, *args, **kwargs)

    monkeypatch.setattr(wkt, "loads", registrar)
    with capturar_logs() as logs, sem_excecao():
        gdf = await mapbiomas_alerta.alertas_geo(**SEMANA)

    assert len(gdf) == 83
    assert gdf.geometry.iloc[0] is None
    assert aninhado not in lidos
    assert [e["event"] for e in logs].count("mapbiomas_alerta_invalid_wkt") == 1


@pytest.mark.parametrize(
    ("filtro", "arquivo", "incluir"),
    [
        (
            {"bbox": CAIXA},
            "caixa_pagina_1.json",
            lambda a: CAIXA[0] <= a["lon"] <= CAIXA[2] and CAIXA[1] <= a["lat"] <= CAIXA[3],
        ),
        (
            {"sources": ["Sad"]},
            "fontes_sad_pagina_1.json",
            lambda a: "SAD" in a["fonte"].split(", "),
        ),
    ],
    ids=["caixa", "fonte"],
)
async def test_filtro_vai_como_a_api_espera(monkeypatch, filtro, arquivo, incluir):
    visto = servir(monkeypatch)
    df, meta, _ = await _com_avisos(token="t", inicio=INICIO, fim=FIM, **filtro)

    assert visto["sem_golden"] == []
    esperado = _esperado([a for a in ORACULO["alertas"] if incluir(a)])
    assert len(esperado) > 0
    pd.testing.assert_frame_equal(_observado(df), esperado, check_dtype=False)
    conferir_corpo(meta, (GOLDEN / arquivo).read_bytes())


async def test_max_registros_corta_pelo_codigo_e_avisa(monkeypatch):
    visto = servir(monkeypatch)
    df, meta, avisos = await _com_avisos(token="t", inicio=INICIO, fim=FIM, max_registros=40)

    esperado = [
        "max_registros=40: 40 de 83 alertas, os de menor código; restrinja o período ou use "
        "max_registros=None"
    ]
    assert [v["page"] for v in visto["servidos"]] == [1, 2]
    assert list(df["alert_code"]) == [a["alert_code"] for a in ORACULO["alertas"][:40]]
    assert avisos == meta.validation_warnings == esperado
    assert meta.source_details["total_anunciado"] == 83


async def test_tipo_data_publicacao_filtra_pela_publicacao(monkeypatch):
    visto = servir(monkeypatch)
    monkeypatch.setattr(api.time_utils, "hoje", lambda: date.fromisoformat(FIM) + timedelta(days=1))
    df, meta, avisos = await _com_avisos(
        token="t", inicio=INICIO, fim=FIM, max_registros=10, tipo_data="publicacao"
    )

    assert visto["sem_golden"] == []
    publicacao = json.loads((GOLDEN / "publicacao_pagina_1.json").read_bytes())["data"]["alerts"]
    assert list(df["alert_code"]) == [a["alertCode"] for a in publicacao["collection"]]
    assert df["data_publicacao"].between(INICIO, f"{FIM} 23:59:59").all()
    assert meta.source_details["tipo_data"] == "publicacao"
    assert avisos == [
        f"max_registros=10: 10 de {publicacao['metadata']['totalCount']} alertas, os de menor "
        "código; restrinja o período ou use max_registros=None"
    ]


@pytest.mark.parametrize(("dias", "avisa"), [(292, True), (293, False)], ids=["dentro", "fora"])
async def test_deteccao_recente_avisa_pela_janela_medida(monkeypatch, dias, avisa):
    servir(monkeypatch)
    monkeypatch.setattr(
        api.time_utils, "hoje", lambda: date.fromisoformat(FIM) + timedelta(days=dias)
    )
    df, meta, avisos = await _com_avisos(**SEMANA)

    esperado = [
        f"tipo_data='deteccao' até {FIM}: 99% dos alertas são publicados em até 293 dias da "
        "detecção, então este período ainda ganha alertas enquanto a publicação chega; use "
        "tipo_data='publicacao' para o que foi publicado no período"
    ]
    assert len(df) == 83
    assert avisos == meta.validation_warnings == (esperado if avisa else [])


async def test_periodo_aberto_por_deteccao_avisa_ate_hoje(monkeypatch):
    buscar = AsyncMock(return_value=(client.Coleta(), models.GRAPHQL_URL))
    monkeypatch.setattr(client, "fetch_alertas", buscar)
    monkeypatch.setattr(api.time_utils, "hoje", lambda: date(2026, 9, 26))
    _, meta, avisos = await _com_avisos(token="t", inicio=INICIO)

    assert avisos == meta.validation_warnings
    assert [aviso.split(":")[0] for aviso in avisos] == ["tipo_data='deteccao' até 2026-09-26"]


async def test_total_que_muda_no_meio_vira_aviso(monkeypatch):
    def trocar(variables, corpo):
        if variables["page"] != 2:
            return corpo
        return _editar(corpo, lambda alerts: alerts["metadata"].update(totalCount=84))

    servir(monkeypatch, trocar)
    df, meta, avisos = await _com_avisos(**SEMANA)

    esperado = [
        "totalCount mudou de 83 para 84 durante a paginação; a plataforma pode ter publicado "
        "alertas durante a consulta",
        "totalCount mudou de 84 para 83 durante a paginação; a plataforma pode ter publicado "
        "alertas durante a consulta",
    ]
    assert len(df) == 83
    assert avisos == meta.validation_warnings == esperado


async def test_colecao_vazia_mantem_colunas_e_tipos_do_cheio(monkeypatch):
    with pytest.MonkeyPatch.context() as golden:
        servir(golden)
        cheio, _, _ = await _com_avisos(**SEMANA)
        with sem_excecao():
            cheio_geo = await mapbiomas_alerta.alertas_geo(**SEMANA)

    def vazia(alerts):
        alerts["collection"] = []
        alerts["metadata"].update(totalCount=0, totalPages=0)

    servir(monkeypatch, lambda _variables, corpo: _editar(corpo, vazia))
    df, meta, avisos = await _com_avisos(**SEMANA)
    with sem_excecao():
        gdf = await mapbiomas_alerta.alertas_geo(**SEMANA)

    assert len(cheio) == len(cheio_geo) == 83
    assert list(df.columns) == models.COLUNAS_SAIDA
    assert df.empty and meta.records_count == 0 and avisos == []
    assert df.dtypes.to_dict() == cheio.dtypes.to_dict()
    assert str(cheio["alert_code"].dtype) == "Int64"
    assert list(gdf.columns) == models.COLUNAS_SAIDA_GEO and gdf.empty
    assert gdf.dtypes.to_dict() == cheio_geo.dtypes.to_dict()
    assert gdf.crs == cheio_geo.crs == "EPSG:4326"


async def test_alerta_sem_fonte_sai_com_texto_vazio(monkeypatch):
    def sem_fonte(alerts):
        alerts["collection"][0]["sources"] = []
        alerts["collection"][1]["sources"] = None

    servir(monkeypatch, lambda v, corpo: _editar(corpo, sem_fonte) if v["page"] == 1 else corpo)
    df, _, _ = await _com_avisos(**SEMANA)

    esperado = [a["fonte"] for a in ORACULO["alertas"]]
    esperado[:2] = ["", ""]
    assert list(df["fonte"]) == esperado


async def test_paginas_depois_da_quinta_esperam(monkeypatch):
    chamadas, dormir = make_sleep_tracker()
    servir(monkeypatch)
    monkeypatch.setattr(client, "_THROTTLE_AFTER_PAGE", 1)
    monkeypatch.setattr(client.asyncio, "sleep", dormir)
    await _com_avisos(**SEMANA)

    assert chamadas == [client._THROTTLE_DELAY] * 2


def _sem_campo(alerts):
    for alerta in alerts["collection"]:
        alerta.pop("areaHa")


@pytest.mark.parametrize(
    ("paginas", "editar", "mensagem"),
    [
        (
            {2},
            lambda alerts: alerts["collection"].insert(0, {"alertCode": 1359780}),
            "Alerta 1359780 repetido na paginação",
        ),
        (
            {3},
            lambda alerts: alerts["collection"].pop(),
            "Coleção incompleta: 82 de 83 alertas anunciados",
        ),
        ({1}, lambda alerts: alerts.pop("metadata"), "sem metadata.totalCount/totalPages"),
        ({1}, lambda alerts: alerts.pop("collection"), "Resposta sem alerts.collection"),
        ({1, 2, 3}, _sem_campo, "Campos obrigatorios ausentes: {'areaHa'}"),
    ],
    ids=[
        "codigo_repetido",
        "abaixo_do_anunciado",
        "sem_metadata",
        "sem_colecao",
        "campo_renomeado",
    ],
)
async def test_resposta_inconsistente_e_recusada(monkeypatch, paginas, editar, mensagem):
    def trocar(variables, corpo):
        return _editar(corpo, editar) if variables["page"] in paginas else corpo

    servir(monkeypatch, trocar)
    with levanta_exatamente(ParseError, match=mensagem):
        await mapbiomas_alerta.alertas(**SEMANA)


async def test_erro_graphql_sai_como_indisponibilidade(monkeypatch):
    erro = b'{"errors": [{"message": "Token de acesso inv\\u00e1lido"}]}'
    servir(monkeypatch, lambda _variables, _corpo: erro)
    with levanta_exatamente(
        SourceUnavailableError, match="GraphQL error: Token de acesso inválido"
    ):
        await mapbiomas_alerta.alertas(**SEMANA)


@pytest.mark.parametrize(
    ("corpo", "mensagem"),
    [
        (b'{"errors": [{"message": "Invalid token: token-do-argumento"}]}', "GraphQL error"),
        (b"gateway rejected credential: token-do-argumento", "não é JSON"),
    ],
    ids=["graphql", "nao_json"],
)
async def test_token_ecoado_na_resposta_sai_mascarado(monkeypatch, corpo, mensagem):
    servir(monkeypatch, lambda _variables, _corpo: corpo)
    with levanta_exatamente(SourceUnavailableError, match=mensagem) as erro:
        await mapbiomas_alerta.alertas(**{**SEMANA, "token": "token-do-argumento"})
    assert "token-do-argumento" not in str(erro.value)
    assert "[REDACTED]" in str(erro.value)


async def test_token_da_variavel_de_ambiente_vai_no_bearer(monkeypatch):
    visto = servir(monkeypatch)
    monkeypatch.setenv("AGROBR_MAPBIOMAS_ALERTA_TOKEN", "do_ambiente")
    await _com_avisos(**{**SEMANA, "token": None})

    assert visto["autorizacao"] == ["Bearer do_ambiente"] * 3


async def test_sem_token_falha_antes_da_rede(monkeypatch):
    visto = servir(monkeypatch)
    monkeypatch.delenv("AGROBR_MAPBIOMAS_ALERTA_TOKEN", raising=False)
    with levanta_exatamente(SourceUnavailableError, match="Token nao encontrado"):
        await mapbiomas_alerta.alertas(**{**SEMANA, "token": None})
    assert visto["autorizacao"] == []


async def test_alerta_info_traz_o_intervalo_e_a_ultima_publicacao(monkeypatch):
    visto = servir(monkeypatch)
    with sem_excecao():
        info = await mapbiomas_alerta.alerta_info()

    intervalo = json.loads((GOLDEN / "info_date_range.json").read_bytes())["data"]
    publicacao = json.loads((GOLDEN / "info_last_publication.json").read_bytes())["data"]
    assert info == {
        "date_range": intervalo["alertDateRange"],
        "last_publication": publicacao["lastAlertPublication"],
    }
    assert visto["autorizacao"] == [None, None]


@pytest.mark.parametrize(
    ("consulta", "corpo"),
    [
        ("alertDateRange", b"{}"),
        ("alertDateRange", b'{"data": {}}'),
        ("alertDateRange", b'{"data": {"alertDateRange": {"minDetectedAt": "2019-01-01"}}}'),
        ("lastAlertPublication", b'{"data": {"lastAlertPublication": null}}'),
        ("lastAlertPublication", b'{"data": {"lastAlertPublication": {"total": 1}}}'),
    ],
    ids=["corpo_vazio", "data_vazio", "intervalo_parcial", "publicacao_nula", "publicacao_parcial"],
)
async def test_alerta_info_sem_os_campos_e_erro_de_layout(monkeypatch, consulta, corpo):
    real = json.loads(
        (GOLDEN / "info_date_range.json").read_bytes()
        if consulta == "lastAlertPublication"
        else (GOLDEN / "info_last_publication.json").read_bytes()
    )

    def handler(request: httpx.Request) -> httpx.Response:
        pedido = json.loads(request.content)["query"]
        conteudo = corpo if consulta in pedido else json.dumps(real).encode()
        return httpx.Response(200, content=conteudo, headers={"content-type": "application/json"})

    original = httpx.AsyncClient

    class Simulado(original):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = httpx.MockTransport(handler)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Simulado)
    with levanta_exatamente(ParseError, consulta):
        await mapbiomas_alerta.alerta_info()


@pytest.mark.parametrize(
    ("inicio", "fim"),
    [
        ("13/01/2025", "19/01/2025"),
        (date(2025, 1, 13), date(2025, 1, 19)),
        (datetime(2025, 1, 13, 8, 30), datetime(2025, 1, 19, 23, 59)),
    ],
    ids=["brasileiro", "date", "datetime"],
)
async def test_data_de_entrada_vai_em_iso(monkeypatch, inicio, fim):
    visto = servir(monkeypatch)
    with sem_excecao():
        df = await mapbiomas_alerta.alertas(token="t", inicio=inicio, fim=fim, max_registros=None)

    assert visto["sem_golden"] == []
    assert isinstance(df, pd.DataFrame) and len(df) == 83


@pytest.mark.parametrize(
    ("kwargs", "mensagem"),
    [
        ({"inicio": "2025-01-31", "fim": "2025-01-01"}, "posterior a fim"),
        ({"inicio": "2025/01/13"}, "inicio deve ser date, datetime ou texto AAAA-MM-DD"),
        ({"fim": 20250119}, "fim deve ser date, datetime ou texto AAAA-MM-DD"),
        ({"max_registros": 0}, "max_registros deve ser inteiro positivo ou None"),
        ({"max_registros": True}, "max_registros deve ser inteiro positivo ou None"),
        ({"tipo_data": "detectado"}, "tipo_data='detectado' fora de"),
    ],
    ids=["datas_invertidas", "formato_da_data", "data_inteira", "zero", "booleano", "tipo_data"],
)
async def test_parametro_impossivel_e_recusado_antes_da_rede(monkeypatch, kwargs, mensagem):
    buscar = AsyncMock()
    monkeypatch.setattr(client, "fetch_alertas", buscar)
    with levanta_exatamente(InvalidParameterError, match=mensagem):
        await mapbiomas_alerta.alertas(token="t", **kwargs)
    buscar.assert_not_awaited()


def test_enum_de_fontes_e_o_da_introspeccao_da_api():
    recibo = next(r for r in MANIFESTO["recibos"] if r["arquivo"] == "introspeccao.json")
    corpo = (GOLDEN / "introspeccao.json").read_bytes()
    assert (len(corpo), hashlib.sha256(corpo).hexdigest()) == (recibo["bytes"], recibo["sha256"])

    publicadas = {valor["name"] for valor in json.loads(corpo)["data"]["src"]["enumValues"]}

    assert publicadas == models.FONTES


@pytest.mark.parametrize(
    "sources",
    [["DETER", "SAD"], ["Sad", "sad"], "Sad"],
    ids=["nomes_da_coluna_fonte", "caixa_errada", "texto_em_vez_de_lista"],
)
async def test_fonte_fora_do_enum_recusa_antes_da_rede(sources, monkeypatch):
    rede = AsyncMock()
    monkeypatch.setattr(client, "fetch_alertas", rede)

    with levanta_exatamente(InvalidParameterError, "SourceTypes"):
        await mapbiomas_alerta.alertas(token="t", inicio=INICIO, fim=FIM, sources=sources)

    assert rede.await_count == 0


async def test_fontes_do_enum_passam_para_a_consulta(monkeypatch):
    rede = AsyncMock(side_effect=RuntimeError("parou depois da validação"))
    monkeypatch.setattr(client, "fetch_alertas", rede)

    with levanta_exatamente(RuntimeError, "parou depois da validação"):
        await mapbiomas_alerta.alertas(
            token="t", inicio=INICIO, fim=FIM, sources=["All", "DeterbAmazonia", "Sad"]
        )

    assert rede.await_args.kwargs["sources"] == ["All", "DeterbAmazonia", "Sad"]
