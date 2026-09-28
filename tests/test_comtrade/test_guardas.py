from __future__ import annotations

import json
import warnings
from datetime import datetime

import httpx
import pandas as pd
import pydantic
import pytest

from agrobr import comtrade, datasets, deterministic
from agrobr.comtrade import acquisition, api, client, parser, query
from agrobr.exceptions import InvalidParameterError, ParseError, SourceUnavailableError
from tests.helpers import levanta_exatamente
from tests.test_comtrade.replay import record


def selection(**changes):
    return query.build_query(
        **{
            "reporter": 76,
            "partner": 0,
            "hs_codes": ["1201"],
            "flow": "X",
            "period": "2023",
            "freq": "A",
            **changes,
        }
    )


@pytest.mark.parametrize(
    "changes,motivo",
    [
        ({"periods": ["2023", "2023"]}, "Períodos duplicados"),
        ({"periods": ["202301"]}, "Período incompatível com frequência"),
    ],
)
def test_consulta_recusa_periodo_duplicado_ou_de_outra_frequencia(changes, motivo):
    with levanta_exatamente(pydantic.ValidationError, match=motivo):
        acquisition.TradeQuery.model_validate({**selection().model_dump(), **changes})


def test_recurso_sem_fuso_e_recusado():
    with levanta_exatamente(pydantic.ValidationError, match="Aquisição sem fuso"):
        acquisition.TradeResource(
            partition_id="p",
            role="data",
            access="guest",
            requested_url="u",
            url="u",
            parameters={},
            fetched_at=datetime(2026, 9, 25, 12, 0),
            sha256="0" * 64,
            size_bytes=0,
            status_code=200,
            requested_limit=500,
            effective_limit=500,
        )


@pytest.mark.parametrize("argumento", ["as_polars", "return_meta"])
def test_opcoes_nao_booleanas_recusadas_antes_da_rede(argumento):
    with levanta_exatamente(InvalidParameterError, match="booleanos"):
        api.prepare_query("1201", periodo=2023, **{argumento: "sim"})


def test_paises_lista_iso_unicos_e_ordenados():
    paises = comtrade.paises()
    assert paises == sorted(set(paises))
    assert {"BRA", "CHN", "USA", "WLD", "EU"} <= set(paises)
    assert len(paises) == 22


def test_envelope_recusa_credencial_escapada_no_json():
    raw = b'{"count": 1, "data": {"nota": "\\u0053EGREDO123"}, "error": ""}'
    assert b"SEGREDO123" not in raw
    with levanta_exatamente(ParseError, match="eco de credencial"):
        client._parse_envelope(acquisition.TradeCountResult, raw, "SEGREDO123")


async def test_redirecionamento_sem_fim_levanta_no_limite():
    pedidos = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(request)
        return httpx.Response(302, headers={"location": f"/volta/{len(pedidos)}"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(responder), max_redirects=3) as http:
        with levanta_exatamente(SourceUnavailableError, match="Limite de redirects"):
            await client._get_same_origin(
                http, "https://comtradeapi.un.org/public/v1/preview/C/A/HS", {}, {}, None
            )
    assert len(pedidos) == 4


@pytest.mark.parametrize(
    "mudanca,motivo",
    [
        ({"cmdCode": "1005", "aggrLevel": 4}, "fora do período ou HS"),
        ({"classificationSearchCode": "H6"}, "Classificação de busca incompatível"),
        ({"partnerCode": 156}, "dimensões incompatíveis"),
    ],
)
async def test_registro_fora_da_consulta_e_recusado(mudanca, motivo, replay_http, captures):
    def override(request, _index):
        if request.url.params.get("countOnly") == "true":
            return httpx.Response(200, json={"count": 1, "data": {}, "error": ""})
        corpo = record(captures, "soy_br_world_2023", **mudanca)
        return httpx.Response(200, json={"count": 1, "data": [corpo], "error": ""})

    replay_http(override)
    with levanta_exatamente(ParseError, match=motivo):
        await client.fetch_trade_acquisition(selection())


async def test_uniao_das_particoes_acima_da_contagem_e_recusada(replay_http, captures):
    soja = record(captures, "soy_br_world_2023")
    milho = record(captures, "soy_br_world_2023", cmdCode="1005", aggrLevel=4)
    outro = record(captures, "soy_br_world_2023", cmdCode="1005", aggrLevel=4, partnerCode=156)

    def override(request, _index):
        codigos = request.url.params["cmdCode"].split(",")
        if request.url.params.get("countOnly") == "true":
            return httpx.Response(200, json={"count": 2, "data": {}, "error": ""})
        linhas = {"1201": [soja], "1005": [milho, outro]}
        dados = [soja] if len(codigos) > 1 else linhas[codigos[0]]
        return httpx.Response(200, json={"count": len(dados), "data": dados, "error": ""})

    replay_http(override)
    with levanta_exatamente(ParseError, match="União de partições excede contagem"):
        await client.fetch_trade_acquisition(selection(partner=None, hs_codes=["1201", "1005"]))


async def test_aquisicao_recusa_objeto_que_nao_e_consulta():
    with levanta_exatamente(InvalidParameterError, match="TradeQuery validada"):
        await client.fetch_trade_acquisition("1201")


async def test_aquisicao_revalida_consulta_alterada_depois_de_criada():
    consulta = selection()
    consulta.periods = ["2023", "2023"]
    with levanta_exatamente(InvalidParameterError, match="Consulta Comtrade inválida"):
        await client.fetch_trade_acquisition(consulta)


@pytest.mark.parametrize(
    "mudanca,campo",
    [
        ({"cmdCode": "12A1"}, "cmdCode"),
        ({"refPeriodId": 20230102}, "refPeriodId"),
        ({"aggrLevel": 6}, "aggrLevel"),
        ({"classificationSearchCode": "H5"}, "classificationSearchCode"),
    ],
)
def test_registro_incoerente_e_recusado_pelo_modelo(mudanca, campo, captures):
    with levanta_exatamente(pydantic.ValidationError, match=campo):
        acquisition.models.TradeRecord.model_validate(record(captures, **mudanca))


def test_resposta_com_frequencias_misturadas_e_recusada(captures):
    anual = record(captures)
    mensal = record(
        captures, period="202303", refYear=2023, refMonth=3, refPeriodId=20230301, freqCode="M"
    )
    with levanta_exatamente(ParseError, match="mistura frequências"):
        parser.parse_trade_data([anual, mensal])


def test_resposta_com_chave_repetida_e_recusada(captures):
    anual = record(captures)
    with levanta_exatamente(ParseError, match="chave bilateral duplicada"):
        parser.parse_trade_data([anual, dict(anual)])


def test_layout_de_registro_em_dicionario_igual_ao_do_modelo(captures):
    bruto = record(captures)
    frame = parser.parse_trade_data([bruto])
    modelo = acquisition.models.TradeRecord.model_validate(bruto)
    de_dict = parser.parse_details([bruto], frame)["layout_fingerprint"]
    do_modelo = parser.parse_details([modelo], frame)["layout_fingerprint"]
    assert de_dict["layouts"] == [sorted(bruto)]
    assert de_dict["sha256"] == do_modelo["sha256"]


def _pernas(captures):
    return (
        parser.parse_trade_data([record(captures)]),
        parser.parse_trade_data([record(captures, "soy_cn_br_2023_mirror")]),
    )


@pytest.mark.parametrize(
    "estrago,motivo",
    [
        (lambda left, right: (left.drop(columns=["peso_bruto_kg"]), right), "colunas bilaterais"),
        (lambda left, right: (left.assign(fluxo_code="M"), right), "fluxo incompatível"),
        (lambda left, right: (left, right.assign(partner_code=pd.NA)), "país ausente"),
        (lambda left, right: (left.assign(reporter_iso="ARG"), right), "entre pernas"),
    ],
)
def test_espelho_recusa_perna_incoerente(estrago, motivo, captures):
    left, right = estrago(*_pernas(captures))
    with levanta_exatamente(ParseError, match=motivo):
        parser.parse_mirror(left, right, "BRA", "CHN")


def test_espelho_recusa_identidade_diferente_da_selecao(captures):
    left, right = _pernas(captures)
    with levanta_exatamente(ParseError, match="com seleção em reporter_iso"):
        parser.parse_mirror(left, right, "ARG", "CHN")


async def test_mes_fora_do_formato_recusado_antes_da_rede():
    with levanta_exatamente(InvalidParameterError, match="YYYYMM"):
        await comtrade.comercio("1201", periodo="20231", freq="M")


@pytest.mark.parametrize("hs_codes", ["1201", [], [1201]])
def test_consulta_exige_lista_de_codigos_textuais(hs_codes):
    with levanta_exatamente(InvalidParameterError, match="lista de códigos textuais"):
        selection(hs_codes=hs_codes)


def test_particao_de_varios_periodos_divide_por_periodo():
    pai = query.make_partition(["2022", "2023"], ["1201", "1005"])
    filhas = query.split_partition(pai)
    assert [f.periods for f in filhas] == [["2022"], ["2023"]]
    assert all(f.hs_codes == ["1201", "1005"] and f.parent_id == pai.partition_id for f in filhas)


def test_proveniencia_do_dataset_sem_meta_da_fonte():
    dataset = datasets.registry.get_dataset("comercio_internacional")
    assert dataset._resolve_provenance("comtrade", None, ["comtrade"]) == (
        "comtrade",
        ["comtrade"],
    )


def test_envelope_recusa_credencial_escapada_dentro_da_lista(captures):
    corpo = record(captures, "soy_br_world_2023", nota="@@")
    envelope = json.dumps({"count": 1, "data": [corpo], "error": ""})
    raw = envelope.replace('"@@"', '"\\u0053EGREDO123"').encode()
    assert b"SEGREDO123" not in raw
    with levanta_exatamente(ParseError, match="eco de credencial"):
        client._parse_envelope(acquisition.TradeEnvelope, raw, "SEGREDO123")


def test_envelope_de_contagem_com_erro_declarado_e_recusado():
    with levanta_exatamente(ParseError, match="error"):
        client._parse_envelope(
            acquisition.TradeCountResult, b'{"count": 1, "data": {}, "error": "limite"}'
        )


@pytest.mark.parametrize(
    "changes,motivo",
    [
        ({"period": "202311-202313", "freq": "M"}, "entre 01 e 12"),
        ({"period": "202302-202301", "freq": "M"}, "invertido"),
        ({"period": "2023-2022"}, "invertido"),
        ({"period": "2021-2022-2023"}, "Intervalo de períodos inválido"),
        ({"period": 2023.0}, "Período deve ser ano"),
        ({"freq": None}, "freq deve ser A ou M"),
        ({"flow": None}, "flow deve ser X ou M"),
    ],
)
def test_consulta_invalida_recusada_com_o_motivo(changes, motivo):
    with levanta_exatamente(InvalidParameterError, match=motivo):
        selection(**changes)


async def test_falha_de_transporte_nao_retriavel_vira_fonte_indisponivel(replay_http):
    def override(_request, _index):
        return httpx.Response(200, headers={"content-encoding": "gzip"}, content=b"nao e gzip")

    replay_http(override)
    with levanta_exatamente(SourceUnavailableError, match="Falha de transporte"):
        await client.fetch_trade_acquisition(selection())


def test_campo_novo_da_api_entra_na_impressao_do_layout(captures):
    bruto = record(captures, campoNovo=1)
    frame = parser.parse_trade_data([bruto])
    modelo = acquisition.models.TradeRecord.model_validate(bruto)
    impressao = parser.parse_details([modelo], frame)["layout_fingerprint"]
    assert impressao["layouts"] == [sorted(bruto)]


async def test_aviso_de_licenca_na_primeira_consulta(replay_http):
    replay_http()
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        await comtrade.comercio("1201", partner="CN", periodo=2023)
    licenca = [str(aviso.message) for aviso in avisos if "zona_cinza" in str(aviso.message)]
    assert len(licenca) == 1
    assert "uncomtrade.org/docs/policy-on-use-and-re-dissemination" in licenca[0]


@pytest.mark.parametrize(
    "partner,omitido,captura",
    [("all", True, "soy_br_omitted_2023"), (None, False, "soy_br_world_2023")],
)
async def test_meta_declara_parceiro_omitido_e_url_dos_dados(
    partner, omitido, captura, replay_http, captures
):
    requests, _ = replay_http()
    frame, meta = await comtrade.comercio("1201", partner=partner, periodo=2023, return_meta=True)
    assert meta.source_details["query"]["partner_parameter_omitted"] is omitido
    dados = [str(r.url) for r in requests if r.url.params.get("countOnly") != "true"]
    assert meta.source_url in dados
    publicados = json.loads(captures["bodies"][captura])["data"]
    esperado = {(r["partnerCode"], r["cmdCode"]): r["netWgt"] for r in publicados}
    obtido = {
        (linha.partner_code, linha.hs_code): None
        if pd.isna(linha.peso_liquido_kg)
        else linha.peso_liquido_kg
        for linha in frame.itertuples()
    }
    assert obtido == esperado


async def test_snapshot_declara_que_so_fixa_o_ano_padrao(replay_http):
    replay_http()
    async with deterministic("2023-12-31"):
        _, meta = await datasets.comercio_internacional("1201", partner="CN", return_meta=True)
    assert meta.snapshot == "2023-12-31"
    assert meta.source_details.get("snapshot_scope") == (
        "default_year_only; source revisions are not frozen"
    )


async def test_cobertura_parcial_sem_metadados_avisa(replay_http, captures):
    def override(request, _index):
        if request.url.params.get("countOnly", "").lower() == "true":
            corpo = json.loads(captures["bodies"]["agro5_2023_count"])
            corpo["count"] = 599
            return httpx.Response(200, json=corpo)
        return None

    replay_http(override)
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame = await comtrade.comercio("1201", partner="CN", periodo=2023)
    assert len(frame) == 1
    cobertura = [str(aviso.message) for aviso in avisos if "Cobertura" in str(aviso.message)]
    assert cobertura == ["Cobertura partial: 1 registros de 599 contados."]
