from __future__ import annotations

import csv
import hashlib
import io
import itertools
import json
import re
import warnings
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree

import pandas as pd
import pytest

from agrobr import b3, datasets
from agrobr.b3 import client, parser
from agrobr.utils import time as agrobr_time
from tests.helpers import (
    assert_replay_served,
    conferir_corpo,
    install_replay_http,
    sem_excecao,
)

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data"
PREGOES = GOLDEN / "b3/pregoes_20260921_20260922"
DIAS = [date(2026, 9, 21), date(2026, 9, 22)]
AJUSTES = {
    date(2026, 9, 17): GOLDEN / "reconciliacao_r9_20260918/b3/b3_ajustes_PR260917_recorte.zip",
    **{dia: PREGOES / f"b3_ajustes_PR{dia:%y%m%d}_recorte.zip" for dia in DIAS},
}
MESES = dict(zip("FGHJKMNQUVXZ", range(1, 13), strict=True))
CONTRATOS = [
    ("boi", "BGI"),
    ("milho", "CCM"),
    ("cafe_arabica", "ICF"),
    ("cafe_conillon", "CNL"),
    ("etanol", "ETH"),
    ("soja_cross", "SJC"),
    ("soja_fob", "SOY"),
]
CONTRATOS_OI = [par for par in CONTRATOS if par[1] in {"BGI", "CCM", "ICF", "ETH", "SJC"}]


def _caso(ajustes=DIAS, posicoes=DIAS) -> dict:
    requisicoes = [
        {
            "match": {
                "path": client.BASE_URL_ZIP,
                "params": {"filelist": f"PR{dia:%y%m%d}.zip"},
                "skip": 0,
            },
            "file": str(AJUSTES[dia]),
            "content_type": "application/zip",
        }
        for dia in ajustes
    ]
    for dia in posicoes:
        token = json.loads((PREGOES / f"b3_oi_token_{dia:%Y%m%d}.json").read_text(encoding="utf-8"))
        requisicoes += [
            {
                "match": {
                    "path": f"{client.BASE_URL_ARQUIVOS}/requestname",
                    "params": {
                        "fileName": "DerivativesOpenPosition",
                        "date": f"{dia:%Y-%m-%d}",
                        "recaptchaToken": "",
                    },
                    "skip": 0,
                },
                "file": str(PREGOES / f"b3_oi_token_{dia:%Y%m%d}.json"),
                "content_type": "application/json",
            },
            {
                "match": {
                    "path": client.BASE_URL_ARQUIVOS,
                    "params": {"token": token["token"]},
                    "skip": 0,
                },
                "file": str(PREGOES / f"b3_oi_{dia:%Y%m%d}_agribusiness.csv"),
                "content_type": "text/csv",
            },
        ]
    return {"requests": requisicoes}


def _congelar_relogio(monkeypatch, agora: datetime) -> None:
    monkeypatch.setattr(agrobr_time, "utcnow", lambda: agora)


def _numero(texto: str | None) -> float | None:
    return None if texto in (None, "") else float(texto)


def _ajustes_oficiais(dia: date, ticker: str) -> list[tuple]:
    with zipfile.ZipFile(AJUSTES[dia]) as externo:
        interno_nome = next(n for n in externo.namelist() if n.endswith(".zip"))
        with zipfile.ZipFile(io.BytesIO(externo.read(interno_nome))) as interno:
            xml = interno.read(sorted(n for n in interno.namelist() if n.endswith(".xml"))[-1])
    linhas = []
    for relatorio in ElementTree.fromstring(xml).iter():
        if not relatorio.tag.endswith("}PricRpt"):
            continue
        simbolo = relatorio.findtext("{*}SctyId/{*}TckrSymb")
        casado = re.fullmatch(rf"{ticker}([FGHJKMNQUVXZ])(\d{{2}})", simbolo or "")
        if not casado or relatorio.findtext("{*}TradDt/{*}Dt") != dia.isoformat():
            continue
        atributos = relatorio.find("{*}FinInstrmAttrbts")
        linhas.append(
            (
                dia.isoformat(),
                ticker,
                simbolo[3:],
                MESES[casado.group(1)],
                2000 + int(casado.group(2)),
                _numero(atributos.findtext("{*}PrvsAdjstdQt")),
                _numero(atributos.findtext("{*}AdjstdQt")),
                _numero(atributos.findtext("{*}VartnPts")),
                _numero(atributos.findtext("{*}AdjstdValCtrct")),
            )
        )
    return linhas


def _posicoes_oficiais(dia: date, ticker: str) -> list[tuple]:
    texto = (PREGOES / f"b3_oi_{dia:%Y%m%d}_agribusiness.csv").read_text(encoding="utf-8")
    return [
        (
            linha["RptDt"],
            linha["Asst"],
            linha["TckrSymb"],
            linha["XprtnCd"],
            "futuro" if linha["TckrSymb"] == linha["Asst"] + linha["XprtnCd"] else "opcao",
            int(linha["OpnIntrst"]),
            int(linha["VartnOpnIntrst"]),
        )
        for linha in csv.DictReader(io.StringIO(texto), delimiter=";")
        if linha["Asst"] == ticker and linha["SgmtNm"] == "AGRIBUSINESS"
    ]


def _publicado_ajustes(frame) -> list[tuple]:
    return [
        (
            linha.data.date().isoformat(),
            linha.ticker,
            linha.vencimento_codigo,
            linha.vencimento_mes,
            linha.vencimento_ano,
            *(
                None if valor != valor else valor
                for valor in (
                    linha.ajuste_anterior,
                    linha.ajuste_atual,
                    linha.variacao,
                    linha.ajuste_por_contrato,
                )
            ),
        )
        for linha in frame.itertuples()
    ]


def _publicado_posicoes(frame) -> list[tuple]:
    return [
        (
            linha.data.date().isoformat(),
            linha.ticker,
            linha.ticker_completo,
            linha.vencimento_codigo,
            linha.tipo,
            int(linha.posicoes_abertas),
            int(linha.variacao_posicoes),
        )
        for linha in frame.itertuples()
    ]


@pytest.mark.parametrize(("contrato", "ticker"), CONTRATOS)
async def test_historico_reune_os_pregoes_publicados(contrato, ticker, monkeypatch):
    visto = install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao(), warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await b3.historico(
            contrato=contrato, inicio=DIAS[0], fim=DIAS[1], return_meta=True
        )
    assert_replay_served(visto)
    esperado = [linha for dia in DIAS for linha in _ajustes_oficiais(dia, ticker)]
    assert esperado
    assert sorted(_publicado_ajustes(frame)) == sorted(esperado)
    assert set(frame["descricao"]) == {contrato}
    assert meta.source_details["coverage"] == {
        "status": "all_requests_succeeded",
        "requested_dates": [dia.isoformat() for dia in DIAS],
        "failed_dates": [],
        "empty_dates": [],
    }
    assert meta.validation_warnings == []
    assert not [aviso for aviso in avisos if "incompleto" in str(aviso.message)]
    vencimento = esperado[0][2]
    with sem_excecao():
        filtrado = await b3.historico(
            contrato=contrato, inicio=DIAS[0], fim=DIAS[1], vencimento=vencimento.lower()
        )
    assert sorted(_publicado_ajustes(filtrado)) == sorted(
        linha for linha in esperado if linha[2] == vencimento
    )


async def test_historico_marca_o_pregao_sem_arquivo(monkeypatch):
    visto = install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao(), warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await b3.historico(
            contrato="boi", inicio=DIAS[0], fim=date(2026, 9, 23), return_meta=True
        )
    assert [url.rsplit("=", 1)[1] for url in visto["unmatched"]] == ["PR260923.zip"]
    assert sorted(_publicado_ajustes(frame)) == sorted(
        linha for dia in DIAS for linha in _ajustes_oficiais(dia, "BGI")
    )
    cobertura = meta.source_details["coverage"]
    assert cobertura["status"] == "partial"
    assert cobertura["requested_dates"] == ["2026-09-21", "2026-09-22", "2026-09-23"]
    assert [(item["data"], item["error_type"]) for item in cobertura["failed_dates"]] == [
        ("2026-09-23", "SourceUnavailableError")
    ]
    assert "404" in cobertura["failed_dates"][0]["error"]
    mensagem = "Histórico B3 incompleto; falha nas datas: 2026-09-23"
    assert meta.validation_warnings == [mensagem]
    assert [str(aviso.message) for aviso in avisos if "incompleto" in str(aviso.message)] == [
        mensagem
    ]


@pytest.mark.parametrize(("contrato", "ticker"), CONTRATOS_OI)
async def test_posicoes_abertas_historico_reune_as_posicoes_publicadas(
    contrato, ticker, monkeypatch
):
    visto = install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao():
        frame, meta = await b3.posicoes_abertas_historico(
            contrato=contrato, inicio=DIAS[0], fim=DIAS[1], return_meta=True
        )
    assert_replay_served(visto)
    esperado = [linha for dia in DIAS for linha in _posicoes_oficiais(dia, ticker)]
    assert esperado
    assert sorted(_publicado_posicoes(frame)) == sorted(esperado)
    assert meta.validation_warnings == []
    for tipo in ("futuro", "opcao"):
        with sem_excecao():
            filtrado = await b3.posicoes_abertas_historico(
                contrato=contrato, inicio=DIAS[0], fim=DIAS[1], tipo=tipo
            )
        assert sorted(filtrado["ticker_completo"]) == sorted(
            linha[2] for linha in esperado if linha[4] == tipo
        )


async def test_posicoes_abertas_historico_avisa_o_pregao_sem_arquivo(monkeypatch):
    visto = install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao():
        frame, meta = await b3.posicoes_abertas_historico(
            contrato="boi", inicio=DIAS[0], fim=date(2026, 9, 23), return_meta=True
        )
    assert len(visto["unmatched"]) == 1 and "date=2026-09-23" in visto["unmatched"][0]
    assert sorted(_publicado_posicoes(frame)) == sorted(
        linha for dia in DIAS for linha in _posicoes_oficiais(dia, "BGI")
    )
    assert meta.validation_warnings == [
        "Sem posições retornadas para o filtro nos dias úteis: 2026-09-23"
    ]


async def test_futuros_sem_data_procura_cinco_pregoes_uteis(monkeypatch):
    _congelar_relogio(monkeypatch, datetime(2026, 9, 23, 18, tzinfo=UTC))
    visto = install_replay_http(
        monkeypatch, _caso(ajustes=[date(2026, 9, 17)], posicoes=[]), GOLDEN
    )
    with sem_excecao():
        frame = await datasets.futuros_agricolas("boi")
    assert [url.rsplit("=", 1)[1] for url in visto["unmatched"]] == [
        "PR260923.zip",
        "PR260922.zip",
        "PR260921.zip",
        "PR260918.zip",
    ]
    assert sorted(_publicado_ajustes(frame)) == sorted(_ajustes_oficiais(date(2026, 9, 17), "BGI"))


async def test_posicoes_sem_data_trazem_o_pregao_mais_recente(monkeypatch):
    _congelar_relogio(monkeypatch, datetime(2026, 9, 22, 18, tzinfo=UTC))
    install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao():
        frame = await datasets.futuros_agricolas("boi", tipo="posicoes")
    assert isinstance(frame, pd.DataFrame)
    assert sorted(_publicado_posicoes(frame)) == sorted(
        _posicoes_oficiais(date(2026, 9, 22), "BGI")
    )


@pytest.mark.parametrize("tipo", ["historico", "oi_historico"])
async def test_dataset_historico_repassa_o_periodo(tipo, monkeypatch):
    install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao():
        frame = await datasets.futuros_agricolas(
            "boi", tipo=tipo, inicio="2026-09-21", fim="2026-09-22"
        )
    assert isinstance(frame, pd.DataFrame)
    if tipo == "historico":
        esperado = [linha for dia in DIAS for linha in _ajustes_oficiais(dia, "BGI")]
        assert sorted(_publicado_ajustes(frame)) == sorted(esperado)
    else:
        esperado = [linha for dia in DIAS for linha in _posicoes_oficiais(dia, "BGI")]
        assert sorted(_publicado_posicoes(frame)) == sorted(esperado)


def _resumo(nome: str, dados: bytes) -> dict:
    return {"nome": nome, "sha256": hashlib.sha256(dados).hexdigest(), "bytes": len(dados)}


def _identidade(corpo: bytes) -> dict:
    with zipfile.ZipFile(io.BytesIO(corpo)) as externo:
        [nome_interno] = [nome for nome in externo.namelist() if nome.endswith(".zip")]
        interno = externo.read(nome_interno)
    with zipfile.ZipFile(io.BytesIO(interno)) as zip_interno:
        nome_xml = max(nome for nome in zip_interno.namelist() if nome.endswith(".xml"))
        xml = zip_interno.read(nome_xml)
    return {"zip_interno": _resumo(nome_interno, interno), "xml": _resumo(nome_xml, xml)}


async def test_ajustes_publicam_o_corpo_recebido_e_o_xml_lido(monkeypatch):
    dia = DIAS[0]
    corpo = AJUSTES[dia].read_bytes()
    recebido = datetime(2026, 9, 21, 21, tzinfo=UTC)
    montado = datetime(2026, 9, 21, 22, tzinfo=UTC)
    _congelar_relogio(monkeypatch, recebido)
    parse = parser.parse_ajustes_zip

    def parse_depois_do_download(zip_bytes):
        _congelar_relogio(monkeypatch, montado)
        return parse(zip_bytes)

    monkeypatch.setattr(parser, "parse_ajustes_zip", parse_depois_do_download)
    visto = install_replay_http(monkeypatch, _caso(ajustes=[dia], posicoes=[]), GOLDEN)
    with sem_excecao():
        _, meta = await b3.ajustes(data=dia, contrato="boi", return_meta=True)
        _, meta_dataset = await datasets.futuros_agricolas(
            "boi", data=dia.isoformat(), return_meta=True
        )
    assert_replay_served(visto)
    conferir_corpo(meta, corpo)
    assert meta.source_url == f"{client.BASE_URL_ZIP}?filelist=PR{dia:%y%m%d}.zip"
    assert meta.source_details == _identidade(corpo)
    assert (meta.fetched_at, meta.fetch_timestamp) == (recebido, recebido)
    herdado = (
        meta_dataset.source_url,
        meta_dataset.raw_content_hash,
        meta_dataset.raw_content_size,
    )
    assert herdado == (meta.source_url, meta.raw_content_hash, meta.raw_content_size)
    assert meta_dataset.source_details == meta.source_details


async def test_ajustes_identidade_estavel_entre_downloads_do_mesmo_pregao(monkeypatch, tmp_path):
    dia = DIAS[0]
    with zipfile.ZipFile(io.BytesIO(AJUSTES[dia].read_bytes())) as externo:
        [membro] = externo.infolist()
        with zipfile.ZipFile(io.BytesIO(externo.read(membro))) as interno:
            [nome_xml] = interno.namelist()
            xml = interno.read(nome_xml)
    interno = io.BytesIO()
    with zipfile.ZipFile(interno, "w", zipfile.ZIP_DEFLATED) as novo:
        novo.writestr("BVBG.086.01_BV000328202609210328000001837000000.xml", b"<anterior/>")
        novo.writestr(nome_xml, xml)
    downloads = []
    for minuto in (19, 20):
        externo = io.BytesIO()
        with zipfile.ZipFile(externo, "w") as novo:
            data_hora = (2026, 9, 26, 16, minuto, 0)
            novo.writestr(zipfile.ZipInfo(membro.filename, data_hora), interno.getvalue())
        caminho = tmp_path / f"download_{minuto}.zip"
        caminho.write_bytes(externo.getvalue())
        downloads.append(caminho)
    [pedido] = _caso(ajustes=[dia], posicoes=[])["requests"]
    caso = {
        "requests": [
            {**pedido, "match": {**pedido["match"], "occurrence": ordem}, "file": str(caminho)}
            for ordem, caminho in enumerate(downloads, start=1)
        ]
    }
    visto = install_replay_http(monkeypatch, caso, GOLDEN)
    with sem_excecao():
        metas = [
            (await b3.ajustes(data=dia, contrato="boi", return_meta=True))[1] for _ in downloads
        ]
    assert_replay_served(visto)
    for meta, caminho in zip(metas, downloads, strict=True):
        conferir_corpo(meta, caminho.read_bytes())
    assert metas[0].raw_content_hash != metas[1].raw_content_hash
    identidade = _identidade(downloads[0].read_bytes())
    assert metas[0].source_details == metas[1].source_details == identidade
    assert identidade["xml"]["nome"] == nome_xml


@pytest.mark.parametrize("fim", [DIAS[0], date(2026, 9, 23)], ids=["um_corpo", "varios_corpos"])
async def test_historico_lista_cada_corpo_recebido(fim, monkeypatch):
    relogio = itertools.count()
    base = datetime(2026, 9, 23, 18, tzinfo=UTC)
    monkeypatch.setattr(agrobr_time, "utcnow", lambda: base + timedelta(seconds=next(relogio)))
    install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _, meta = await b3.historico(contrato="boi", inicio=DIAS[0], fim=fim, return_meta=True)
    esperado = []
    for dia in (dia for dia in DIAS if dia <= fim):
        corpo = AJUSTES[dia].read_bytes()
        url = f"{client.BASE_URL_ZIP}?filelist=PR{dia:%y%m%d}.zip"
        digest = hashlib.sha256(corpo).hexdigest()
        esperado.append(
            {"data": dia.isoformat(), "url": url, "sha256": digest, "bytes": len(corpo)}
            | _identidade(corpo)
        )
    corpos = meta.source_details.get("corpos", [])
    assert [{k: v for k, v in c.items() if k != "fetch_timestamp"} for c in corpos] == esperado
    assert all(c["fetch_timestamp"].endswith("+00:00") for c in corpos)
    topo = (meta.source_url, meta.raw_content_hash, meta.raw_content_size)
    if len(esperado) == 1:
        assert topo == (esperado[0]["url"], esperado[0]["sha256"], esperado[0]["bytes"])
    else:
        assert topo == (client.BASE_URL_ZIP, None, 0)
    aquisicoes = [datetime.fromisoformat(c["fetch_timestamp"]) for c in corpos]
    assert meta.fetched_at == meta.fetch_timestamp == max(aquisicoes)


async def test_historico_sem_dia_util_nao_lista_corpo(monkeypatch):
    visto = install_replay_http(monkeypatch, {"requests": []}, GOLDEN)
    with sem_excecao():
        frame, meta = await b3.historico(
            contrato="boi", inicio=date(2026, 9, 26), fim=date(2026, 9, 27), return_meta=True
        )
    assert visto == {"served": [], "unmatched": []}
    assert frame.empty
    assert meta.source_details["corpos"] == []
    assert (meta.source_url, meta.raw_content_hash, meta.raw_content_size) == (
        client.BASE_URL_ZIP,
        None,
        0,
    )


@pytest.mark.parametrize("fim", [DIAS[0], date(2026, 9, 23)], ids=["um_corpo", "varios_corpos"])
async def test_posicoes_abertas_historico_lista_cada_corpo_recebido(fim, monkeypatch):
    relogio = itertools.count()
    base = datetime(2026, 9, 23, 18, tzinfo=UTC)
    monkeypatch.setattr(agrobr_time, "utcnow", lambda: base + timedelta(seconds=next(relogio)))
    install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao():
        _, meta = await b3.posicoes_abertas_historico(
            contrato="boi", inicio=DIAS[0], fim=fim, return_meta=True
        )
    esperado = []
    for dia in (dia for dia in DIAS if dia <= fim):
        corpo = (PREGOES / f"b3_oi_{dia:%Y%m%d}_agribusiness.csv").read_bytes()
        digest = hashlib.sha256(corpo).hexdigest()
        esperado.append(
            {"data": dia.isoformat(), "sha256": digest, "bytes": len(corpo)}
            | {"ticket_url": client.ticket_url(dia.isoformat())}
        )
    corpos = meta.source_details.get("corpos", [])
    chaves = ("data", "sha256", "bytes", "ticket_url")
    assert [{chave: c[chave] for chave in chaves} for c in corpos] == esperado
    assert all(c["url"] == f"{client.BASE_URL_ARQUIVOS}?token=[REDACTED]" for c in corpos)
    topo = (meta.source_url, meta.raw_content_hash, meta.raw_content_size)
    if len(esperado) == 1:
        assert topo == (corpos[0]["url"], esperado[0]["sha256"], esperado[0]["bytes"])
    else:
        assert topo == (client.BASE_URL_ARQUIVOS, None, 0)
    aquisicoes = [datetime.fromisoformat(c["fetch_timestamp"]) for c in corpos]
    assert meta.fetched_at == meta.fetch_timestamp == max(aquisicoes)
