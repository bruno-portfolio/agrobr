from __future__ import annotations

import csv
import hashlib
import io
import json
import zipfile
from numbers import Real
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests

from agrobr import contracts, datasets
from agrobr.antaq import api, client
from agrobr.exceptions import SourceUnavailableError
from tests.helpers import assert_replay_structure

GOLDEN = (
    Path(__file__).resolve().parents[1]
    / "golden_data/reconciliacao_movimentacao_portuaria_20260918"
)
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
ORACLE = json.loads((GOLDEN / "oracle.json").read_text(encoding="utf-8"))
CASES = {case["id"]: case for case in MANIFEST["cases"]}
FILTROS = [case["id"] for case in MANIFEST["cases"] if case["id"].startswith("filtro_")]
MEMBROS = MANIFEST["transport_container"]["membros"]
ANO_ZIP_URL = "https://estatistica.antaq.gov.br/ea/txt/2024.zip"
MERCADORIA_ZIP_URL = "https://estatistica.antaq.gov.br/ea/txt/Mercadoria.zip"
AVISO_URL = (
    "https://www.gov.br/antaq/pt-br/central-de-conteudos/publicacoes-da-antaq/"
    "publicacoes-off/painel-estatistico-aquaviario-indisponivel"
)


def _corpo(nome: str) -> bytes:
    return (GOLDEN / f"../antaq/movimentacao_sample/{nome}").resolve().read_bytes()


def _zip(membros: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as arquivo:
        for nome, conteudo in membros.items():
            arquivo.writestr(nome, conteudo)
    return buffer.getvalue()


def _resposta(
    url: str, corpo: bytes, content_type: str, final_url: str | None = None, status: int = 200
) -> Any:
    response = requests.Response()
    response.status_code = status
    response._content = corpo
    response.url = final_url or url
    response.headers["Content-Type"] = content_type
    return response


def install_replay_antaq(
    monkeypatch: pytest.MonkeyPatch,
    *,
    atracacao: bytes | None = None,
    carga: bytes | None = None,
    mercadoria: bytes | None = None,
    indisponivel: bool = False,
) -> dict[str, list[str]]:
    """Serve os TXT preservados em containers de transporte, substituindo só ``requests.get``."""
    ano_zip = _zip(
        {
            MEMBROS["atracacao.txt"]: atracacao
            if atracacao is not None
            else _corpo("atracacao.txt"),
            MEMBROS["carga.txt"]: carga if carga is not None else _corpo("carga.txt"),
        }
    )
    mercadoria_zip = _zip(
        {
            MEMBROS["mercadoria.txt"]: mercadoria
            if mercadoria is not None
            else _corpo("mercadoria.txt")
        }
    )
    corpos = {ANO_ZIP_URL: ano_zip, MERCADORIA_ZIP_URL: mercadoria_zip}
    seen: dict[str, list[str]] = {"served": [], "unmatched": []}

    def get(url: str, **_kwargs: Any) -> Any:
        if indisponivel:
            seen["served"].append(url)
            return _resposta(
                url, b"<!DOCTYPE html><html>aviso</html>", "text/html;charset=utf-8", AVISO_URL
            )
        corpo = corpos.get(url)
        if corpo is None:
            seen["unmatched"].append(url)
            return _resposta(url, b"", "text/plain", status=404)
        seen["served"].append(url)
        return _resposta(url, corpo, "application/zip")

    monkeypatch.setattr(client.requests, "get", get)
    return seen


def _cabecalho(nome: str) -> list[str]:
    texto = _corpo(nome).decode("utf-8")
    return next(csv.reader(io.StringIO(texto), delimiter=";"))


def _normalizar(valor: Any) -> Any:
    if pd.isna(valor):
        return None
    if isinstance(valor, pd.Timestamp):
        return valor.strftime("%d/%m/%Y %H:%M:%S")
    if isinstance(valor, Real) and not isinstance(valor, bool):
        return float(valor)
    return str(valor)


def assert_celula_a_celula(frame: pd.DataFrame, esperado: dict[str, Any], case_id: str) -> int:
    colunas = esperado["colunas"]
    linhas = esperado["linhas"]
    assert list(frame.columns) == colunas, f"{case_id}: colunas {list(frame.columns)}"
    assert len(frame) == len(linhas), f"{case_id}: {len(frame)} linhas, esperado {len(linhas)}"
    for posicao, linha in enumerate(linhas):
        for coluna in colunas:
            obtido = _normalizar(frame.iloc[posicao][coluna])
            alvo = _normalizar(linha[coluna])
            assert obtido == alvo, f"{case_id}[{posicao}].{coluna}: {obtido!r} != {alvo!r}"
    return len(linhas) * len(colunas)


def assert_amostras(frame: pd.DataFrame, case: dict[str, Any]) -> None:
    for amostra in case["samples"]:
        chave = amostra["key"]
        if "posicao" in chave:
            linha = frame.iloc[chave["posicao"]]
        else:
            mascara = pd.Series(True, index=frame.index)
            for coluna, valor in chave.items():
                mascara &= frame[coluna].astype(str) == str(valor)
            selecionadas = frame[mascara]
            assert len(selecionadas) == 1, f"{case['id']}: chave {chave} -> {len(selecionadas)}"
            linha = selecionadas.iloc[0]
        obtido = _normalizar(linha[amostra["column"]])
        assert obtido == _normalizar(amostra["value"]), (
            f"{case['id']} {chave} {amostra['column']}: {obtido!r} != {amostra['value']!r}"
        )


async def _fonte(**selecao: Any) -> Any:
    return await api.movimentacao(**selecao, return_meta=True)


def test_manifesto_confere_sha256_e_tamanho():
    for entrada in MANIFEST["files"]:
        dados = (GOLDEN / entrada["file"]).resolve().read_bytes()
        assert hashlib.sha256(dados).hexdigest() == entrada["sha256"], entrada["file"]
        assert len(dados) == entrada["bytes"], entrada["file"]
        assert entrada["captured_at"], entrada["file"]
        assert len(dados) <= 2 * 1024 * 1024, entrada["file"]
    assert (GOLDEN / "manifest.json").stat().st_size <= 2 * 1024 * 1024


def test_estrutura_inventaria_todos_os_campos_publicados():
    case = CASES["fonte_2024_completo"]
    publicados = {
        MEMBROS["atracacao.txt"]: _cabecalho("atracacao.txt"),
        MEMBROS["carga.txt"]: _cabecalho("carga.txt"),
        MEMBROS["mercadoria.txt"]: _cabecalho("mercadoria.txt"),
    }
    inventariados: dict[str, list[str]] = {membro: [] for membro in publicados}
    for item in case["structure"]:
        membro = item["locator"]["archive_member"]
        inventariados[membro].append(item["campo"])
        assert item["estado"] in ("mapeada", "join", "ignorada"), item
        assert item["motivo"], item
        if item["estado"] == "mapeada":
            assert item["destino"], item
        else:
            assert item["destino"] is None, item
    for membro, campos in publicados.items():
        assert inventariados[membro] == campos, membro
    assert sum(len(campos) for campos in publicados.values()) == 62
    destinos = [item["destino"] for item in case["structure"] if item["estado"] == "mapeada"]
    assert sorted(destinos) == sorted(case["columns"])
    assert len(destinos) == len(set(destinos)) == 21


async def test_fonte_completa_bate_com_o_oraculo(monkeypatch: pytest.MonkeyPatch):
    case = CASES["fonte_2024_completo"]
    seen = install_replay_antaq(monkeypatch)
    frame, meta = await _fonte(**case["selection"])
    assert seen["served"] == [ANO_ZIP_URL, MERCADORIA_ZIP_URL]
    assert not seen["unmatched"]
    assert assert_celula_a_celula(frame, ORACLE["fonte"], case["id"]) == 210
    assert sorted(frame.to_dict("records"), key=repr) == sorted(
        pd.DataFrame(ORACLE["fonte"]["linhas"])
        .assign(
            data_atracacao=lambda df: pd.to_datetime(
                df["data_atracacao"], format="%d/%m/%Y %H:%M:%S"
            )
        )
        .astype(frame.dtypes)
        .to_dict("records"),
        key=repr,
    )
    assert_replay_structure(frame, case)
    assert_amostras(frame, case)
    assert str(frame["ano"].dtype) == "Int64"
    assert str(frame["mes"].dtype) == "Int64"
    assert str(frame["peso_bruto_ton"].dtype) == "float64"
    assert meta.source == "antaq"
    assert meta.source_url == ANO_ZIP_URL
    assert meta.source_method == "requests+zip"
    assert meta.attempted_sources == ["antaq_ea"]
    assert meta.selected_source == "antaq_ea"
    assert meta.parser_version == 2
    assert meta.records_count == len(frame)
    assert meta.fetch_timestamp is not None


async def test_dataset_agregado_bate_com_o_oraculo(monkeypatch: pytest.MonkeyPatch):
    case = CASES["dataset_2024_agregado"]
    install_replay_antaq(monkeypatch)
    frame, meta = await datasets.movimentacao_portuaria(**case["selection"], return_meta=True)
    ordem = [coluna.name for coluna in contracts.get_contract("movimentacao_portuaria").columns]
    assert sorted(ordem) == sorted(ORACLE["dataset"]["colunas"])
    assert assert_celula_a_celula(frame, {**ORACLE["dataset"], "colunas": ordem}, case["id"]) == 126
    assert_replay_structure(frame, case)
    assert_amostras(frame, case)
    assert meta.dataset == "movimentacao_portuaria"
    assert meta.contract_version == "2.0"
    assert meta.schema_version == "2.0"
    assert meta.attempted_sources == ["antaq"]
    assert meta.selected_source == "antaq"
    assert meta.parser_version == 2
    assert meta.records_count == len(frame) == case["period"]["rows"]
    assert meta.validation_passed


@pytest.mark.parametrize("case_id", FILTROS)
async def test_filtro_publico(case_id: str, monkeypatch: pytest.MonkeyPatch):
    case = CASES[case_id]
    install_replay_antaq(monkeypatch)
    frame, _ = await _fonte(**case["selection"])
    referencia = "filtro_sentido_desembarque" if case_id == "filtro_sentido_vazio" else case_id
    esperado = ORACLE["filtros"][referencia]
    assert len(frame) == len(esperado["linhas"])
    if esperado["linhas"]:
        assert assert_celula_a_celula(
            frame, {"colunas": ORACLE["fonte"]["colunas"], "linhas": esperado["linhas"]}, case_id
        )
    else:
        assert frame.empty
        assert list(frame.columns) == ORACLE["fonte"]["colunas"]


async def test_cardinalidade_dos_joins(monkeypatch: pytest.MonkeyPatch):
    cardinalidade = ORACLE["cardinalidade"]
    install_replay_antaq(monkeypatch)
    frame, _ = await _fonte(ano=2024)
    assert cardinalidade["cargas"] == cardinalidade["linhas_de_saida"] == len(frame) == 10
    assert cardinalidade["atracacoes"] == 10
    assert cardinalidade["atracacoes_referenciadas"] == 6
    assert cardinalidade["atracacoes_sem_carga"] == 4
    assert cardinalidade["maior_fanout_atracacao"] == 5
    assert cardinalidade["cargas_orfas_sem_atracacao"] == 0
    assert cardinalidade["cargas_orfas_sem_mercadoria"] == 0
    assert cardinalidade["mercadorias"] == cardinalidade["mercadorias_distintas"] == 1403
    fanout = frame[frame["data_atracacao"] == pd.Timestamp("2023-12-22 17:32:00")]
    assert len(fanout) == 5
    assert fanout["porto"].nunique() == 1


async def test_ultima_linha_de_cada_membro(monkeypatch: pytest.MonkeyPatch):
    cardinalidade = ORACLE["cardinalidade"]
    atracacao = list(
        csv.DictReader(io.StringIO(_corpo("atracacao.txt").decode("utf-8")), delimiter=";")
    )
    carga = list(csv.DictReader(io.StringIO(_corpo("carga.txt").decode("utf-8")), delimiter=";"))
    mercadoria = list(
        csv.DictReader(io.StringIO(_corpo("mercadoria.txt").decode("utf-8")), delimiter=";")
    )
    assert atracacao[-1]["IDAtracacao"] == cardinalidade["ultima_linha_atracacao"] == "1406473"
    assert carga[-1]["IDCarga"] == cardinalidade["ultima_linha_carga"] == "35454468"
    assert mercadoria[-1]["CDMercadoria"] == cardinalidade["ultima_linha_mercadoria"] == "20T0"

    install_replay_antaq(monkeypatch)
    frame, _ = await _fonte(ano=2024)
    assert carga[-1]["IDAtracacao"] not in {"1406473"}
    assert (frame["porto"] == "Belém").sum() == 1
    assert frame.loc[2, "peso_bruto_ton"] == 419.915
    assert frame.loc[2, "cd_mercadoria"] == carga[-1]["CDMercadoria"] == "2207"
    assert "Cais Público - Miramar" in frame["terminal"].tolist()
    assert "20T0" not in frame["cd_mercadoria"].tolist()
    assert len(frame) == len(carga)


def _mutar(texto: str, mutacao: str) -> str:
    linhas = texto.split("\r\n")
    if mutacao == "noop":
        return "\r\n".join(linhas)
    if mutacao == "peso":
        return "\r\n".join(linha.replace(";0;0;54354", ";0;0;54355") for linha in linhas)
    if mutacao == "chave_de_join":
        return "\r\n".join(linha.replace("1406197;", "9999999;", 1) for linha in linhas)
    if mutacao == "data":
        return "\r\n".join(
            linha.replace("22/12/2023 17:32:00", "22/12/2024 17:32:00") for linha in linhas
        )
    if mutacao == "ultima_linha_ausente":
        return "\r\n".join(linhas[:-2] + [linhas[-1]])
    if mutacao == "registro_repetido":
        return "\r\n".join(linhas[:-1] + [linhas[-2], linhas[-1]])
    raise AssertionError(mutacao)


@pytest.mark.parametrize(
    "mutacao,membro,motivo",
    [
        ("noop", "carga.txt", ""),
        ("peso", "carga.txt", r"peso\[3\]\.peso_bruto_ton: 54355\.0 != 54354\.0"),
        ("chave_de_join", "atracacao.txt", r"chave_de_join\[5\]\.ano: None != 2024\.0"),
        ("data", "atracacao.txt", r"data\[5\]\.data_atracacao: '22/12/2024 17:32:00'"),
        ("ultima_linha_ausente", "carga.txt", r"ultima_linha_ausente: 9 linhas, esperado 10"),
        ("registro_repetido", "carga.txt", r"registro_repetido: 11 linhas, esperado 10"),
    ],
)
async def test_mutacao_reprova(
    mutacao: str, membro: str, motivo: str, monkeypatch: pytest.MonkeyPatch
):
    original = _corpo(membro).decode("utf-8")
    mutado = _mutar(original, mutacao).encode("utf-8")
    assert (mutado == original.encode("utf-8")) == (mutacao == "noop"), mutacao
    install_replay_antaq(monkeypatch, **{membro.removesuffix(".txt"): mutado})
    frame, _ = await _fonte(ano=2024)
    if mutacao == "noop":
        assert assert_celula_a_celula(frame, ORACLE["fonte"], "noop") == 210
        return
    with pytest.raises(AssertionError, match=motivo):
        assert_celula_a_celula(frame, ORACLE["fonte"], mutacao)


async def test_mercadoria_orfa_mantem_a_carga_sem_rotulo(monkeypatch: pytest.MonkeyPatch):
    carga = _corpo("carga.txt").decode("utf-8").replace(";2606;", ";XX99;", 1).encode("utf-8")
    install_replay_antaq(monkeypatch, carga=carga)
    frame, _ = await _fonte(ano=2024)
    assert len(frame) == 10
    orfa = frame[frame["cd_mercadoria"] == "XX99"]
    assert len(orfa) == 1
    assert orfa["mercadoria"].isna().all()
    assert orfa["grupo_mercadoria"].isna().all()
    assert orfa["peso_bruto_ton"].iloc[0] == 54354.0
    with pytest.raises(
        AssertionError, match=r"mercadoria_orfa\[3\]\.cd_mercadoria: 'XX99' != '2606'"
    ):
        assert_celula_a_celula(frame, ORACLE["fonte"], "mercadoria_orfa")


async def test_carga_orfa_sem_atracacao_sai_do_dataset(monkeypatch: pytest.MonkeyPatch):
    carga = _corpo("carga.txt").decode("utf-8").replace(";1406449;", ";9999999;", 1).encode("utf-8")
    install_replay_antaq(monkeypatch, carga=carga)
    fonte, _ = await _fonte(ano=2024)
    orfa = fonte[fonte["porto"].isna()]
    assert len(fonte) == 10
    assert len(orfa) == 1
    assert orfa["ano"].isna().all()
    assert orfa["mes"].isna().all()
    install_replay_antaq(monkeypatch, carga=carga)
    agregado = await datasets.movimentacao_portuaria(ano=2024)
    assert len(agregado) == 5
    assert "2207" not in agregado["cd_mercadoria"].tolist()


async def test_layout_tipos_e_nulabilidade(monkeypatch: pytest.MonkeyPatch):
    install_replay_antaq(monkeypatch)
    frame, _ = await datasets.movimentacao_portuaria(ano=2024, return_meta=True)
    observados = {
        **MANIFEST["dtypes_observados"]["dataset"],
        "data_atracacao": "datetime64[ns]",
        "qt_carga": "float64",
        "teu": "Int64",
    }
    observados.update(
        {
            coluna.name: str(pd.Series([""]).dtype)
            for coluna in contracts.get_contract("movimentacao_portuaria").columns
            if coluna.type == contracts.ColumnType.STRING
        }
    )
    for coluna, dtype in observados.items():
        assert str(frame[coluna].dtype) == dtype, coluna
    assert frame["ano"].notna().all()
    assert frame["mes"].notna().all()
    assert (frame["peso_bruto_ton"] >= 0).all()
    assert (frame["teu"] >= 0).all()
    assert set(frame["sentido"]) <= {"Embarcados", "Desembarcados"}
    assert frame.loc[frame["porto"] == "Terminal Navecunha", "data_atracacao"].isna().all()
    assert frame["terminal"].notna().all()


async def test_fonte_indisponivel_vira_source_unavailable_error(monkeypatch: pytest.MonkeyPatch):
    install_replay_antaq(monkeypatch, indisponivel=True)
    with pytest.raises(SourceUnavailableError) as erro:
        await api.movimentacao(2024)
    assert isinstance(erro.value, client.OfficialOutageError)
    assert erro.value.notice_url == AVISO_URL

    install_replay_antaq(monkeypatch, indisponivel=True)
    with pytest.raises(SourceUnavailableError):
        await datasets.movimentacao_portuaria(ano=2024)


def test_pendencia_live_registrada():
    pendencias = {entrada["item"]: entrada for entrada in MANIFEST["pendencias"]}
    zip_oficial = pendencias["ZIP anual oficial"]
    assert zip_oficial["estado"] == "indisponivel"
    assert zip_oficial["evidencia"].endswith("live_probe.json")
    assert MANIFEST["n3"]["estado"] == "nao aplicavel"
    assert MANIFEST["transport_container"]["declaracao"].startswith("container de transporte")
