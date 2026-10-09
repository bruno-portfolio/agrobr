from __future__ import annotations

import csv
import io
import warnings
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from agrobr import datasets
from agrobr.alt.mapa_psr import api, client, models, parser
from tests.helpers import binary_stream, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data"
PSR = GOLDEN / "mapa_psr"
RECONCILIACAO = GOLDEN / "reconciliacao_registros_precos_zoneamento_seguro_20260918" / "psr"
VIGENCIA_NAO_PUBLICADA = (
    "PSR: vigência não publicada (início igual ao fim) em {n} registro(s); saem nulas. "
    "Use data_apolice"
)
HEADER = "ANO_APOLICE;SG_UF_PROPRIEDADE;NM_CULTURA_GLOBAL;NR_APOLICE;NM_MUNICIPIO_PROPRIEDADE"


def _linhas(caminho: Path) -> list[dict[str, str]]:
    corpo = caminho.read_bytes()
    try:
        texto = corpo.decode("utf-8")
    except UnicodeDecodeError:
        texto = corpo.decode("windows-1252")
    return list(csv.DictReader(io.StringIO(texto, newline=""), delimiter=";"))


def _oraculo(linhas: list[dict[str, str]]) -> Counter[tuple[Any, ...]]:
    def vigencia(linha: dict[str, str], campo: str) -> str | None:
        nao_publicada = linha["DT_INICIO_VIGENCIA"] == linha["DT_FIM_VIGENCIA"]
        return None if nao_publicada else linha[campo]

    return Counter(
        (
            linha["NR_APOLICE"].strip(),
            int(linha["ANO_APOLICE"]),
            vigencia(linha, "DT_INICIO_VIGENCIA"),
            vigencia(linha, "DT_FIM_VIGENCIA"),
            linha["DT_APOLICE"],
        )
        for linha in linhas
    )


def _publicado(frame: pd.DataFrame) -> Counter[tuple[Any, ...]]:
    def texto(valor: Any) -> str | None:
        return None if pd.isna(valor) else valor.strftime("%d/%m/%Y")

    return Counter(
        (nr, int(ano), texto(inicio), texto(fim), texto(apolice))
        for nr, ano, inicio, fim, apolice in frame[
            ["nr_apolice", "ano_apolice", *models.COLUNAS_DATA]
        ].itertuples(index=False, name=None)
    )


def _servir(monkeypatch: pytest.MonkeyPatch, corpos: dict[str, bytes]) -> None:
    def abrir(periodo: str):
        return binary_stream(corpos[periodo])

    monkeypatch.setattr(client, "open_periodo", abrir)


async def _chamar(chamada: Any) -> tuple[Any, list[str]]:
    with warnings.catch_warnings(record=True) as avisos, sem_excecao():
        warnings.simplefilter("always")
        resultado = await chamada
    return resultado, [str(aviso.message) for aviso in avisos]


@pytest.mark.parametrize(
    "caminho,periodo,inicio,fim",
    [
        (PSR / "caxias_do_sul_2024_20260918/apolices.csv", "2016-2024", 2016, 2024),
        (PSR / "sem_geocodigo_20260918/apolices.csv", "2016-2024", 2016, 2024),
        (RECONCILIACAO / "psr_2016-2024.csv", "2016-2024", 2016, 2024),
        (RECONCILIACAO / "psr_2025.csv", "2025", 2025, 2025),
    ],
    ids=["caxias_2024", "sem_geocodigo", "reconciliacao_2016_2024", "reconciliacao_2025"],
)
async def test_datas_iguais_as_publicadas_desde_2016(monkeypatch, caminho, periodo, inicio, fim):
    linhas = _linhas(caminho)
    esperado = _oraculo(linhas)
    assert all(None not in chave for chave in esperado)
    _servir(monkeypatch, {periodo: caminho.read_bytes()})
    (frame, meta), avisos = await _chamar(
        api.apolices(ano_inicio=inicio, ano_fim=fim, return_meta=True)
    )
    publicado = _publicado(frame)
    assert set(publicado) == set(esperado)
    assert len(frame) + meta.source_details["duplicatas_colapsadas"]["linhas"] == len(linhas)
    assert not [aviso for aviso in avisos if "vigência" in aviso or "NaT" in aviso]
    assert {str(frame[coluna].dtype) for coluna in models.COLUNAS_DATA} == {"datetime64[ns]"}
    sinistros, _ = await _chamar(api.sinistros(ano_inicio=inicio, ano_fim=fim))
    assert set(_publicado(sinistros)) <= set(esperado)


@pytest.mark.parametrize(
    "caminho",
    [PSR / "seguradoras_20260918/apolices.csv", PSR / "apolices_sample/response.csv"],
    ids=["seguradoras", "apolices_sample"],
)
async def test_vigencia_nao_publicada_ate_2015_sai_nula_com_aviso(monkeypatch, caminho):
    linhas = _linhas(caminho)
    assert all(linha["DT_INICIO_VIGENCIA"] == linha["DT_FIM_VIGENCIA"] for linha in linhas)
    aviso = VIGENCIA_NAO_PUBLICADA.format(n=len(linhas))
    _servir(monkeypatch, {"2006-2015": caminho.read_bytes()})
    (frame, meta), avisos = await _chamar(
        api.apolices(ano_inicio=2006, ano_fim=2015, return_meta=True)
    )
    assert frame["inicio_vigencia"].isna().all() and frame["fim_vigencia"].isna().all()
    assert set(_publicado(frame)) == set(_oraculo(linhas))
    assert frame["data_apolice"].dt.year.tolist() == frame["ano_apolice"].tolist()
    assert len(frame) + meta.source_details["duplicatas_colapsadas"]["linhas"] == len(linhas)
    assert avisos.count(aviso) == 1 and meta.validation_warnings.count(aviso) == 1
    (_, meta_dataset), avisos = await _chamar(
        datasets.seguro_rural(ano_inicio=2006, ano_fim=2015, return_meta=True)
    )
    assert avisos.count(aviso) == 1 and aviso in meta_dataset.validation_warnings


def test_parse_direto_avisa_a_vigencia_nao_publicada_uma_vez():
    corpo = (PSR / "sinistros_sample/response.csv").read_bytes()
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame = parser.parse_sinistros(corpo)
    assert len(frame) and frame["data_apolice"].notna().all()
    assert [str(aviso.message) for aviso in avisos if "vigência" in str(aviso.message)] == [
        VIGENCIA_NAO_PUBLICADA.format(n=len(frame))
    ]


def test_arquivo_sem_as_colunas_sai_com_datas_nulas():
    corpo = f"{HEADER}\n2024;MT;SOJA;A1;SORRISO\n".encode()
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame = parser.parse_apolices(corpo)
    assert len(frame) == 1
    for coluna in models.COLUNAS_DATA:
        assert str(frame[coluna].dtype) == "datetime64[ns]" and frame[coluna].isna().all()
    assert not avisos


async def test_data_ilegivel_vira_nat_com_um_aviso_por_coluna_na_consulta(monkeypatch):
    cabecalho = f"{HEADER};DT_INICIO_VIGENCIA;DT_FIM_VIGENCIA;DT_APOLICE\n"
    _servir(
        monkeypatch,
        {
            "2006-2015": (
                f"{cabecalho}2010;MT;SOJA;A1;SORRISO;01/07/2010;30/06/2011;31/02/2010\n"
            ).encode(),
            "2016-2024": (
                f"{cabecalho}2020;MT;SOJA;A2;SORRISO;01/13/2020;30/06/2021;xx\n"
                "2020;MT;SOJA;A3;SORRISO;01/07/2020;30/06/2021;15/06/2020\n"
            ).encode(),
        },
    )
    (frame, meta), avisos = await _chamar(
        api.apolices(ano_inicio=2010, ano_fim=2020, return_meta=True)
    )
    esperados = [
        "mapa_psr: 1 valor(es) de inicio_vigencia viraram NaT (data ilegível ou com ano fora "
        "de 1900–2099).",
        "mapa_psr: 2 valor(es) de data_apolice viraram NaT (data ilegível ou com ano fora "
        "de 1900–2099).",
    ]
    assert [aviso for aviso in avisos if "NaT" in aviso] == esperados
    assert [aviso for aviso in meta.validation_warnings if "NaT" in aviso] == esperados
    assert frame["data_apolice"].isna().tolist() == [True, True, False]
    assert frame["fim_vigencia"].notna().all()


async def test_fim_de_vigencia_com_ano_fora_da_faixa_vira_nat(monkeypatch):
    cabecalho = f"{HEADER};DT_INICIO_VIGENCIA;DT_FIM_VIGENCIA;DT_APOLICE\n"
    _servir(
        monkeypatch,
        {
            "2016-2024": (
                f"{cabecalho}2019;MT;SOJA;5800001707;SORRISO;09/05/2019;09/05/5207;08/05/2019\n"
                "2020;MT;SOJA;02010111324;SORRISO;25/06/2020;25/06/2922;24/06/2020\n"
            ).encode(),
        },
    )
    (frame, meta), avisos = await _chamar(
        api.apolices(ano_inicio=2019, ano_fim=2020, return_meta=True)
    )
    esperado = (
        "mapa_psr: 2 valor(es) de fim_vigencia viraram NaT (data ilegível ou com ano fora "
        "de 1900–2099)."
    )
    assert [aviso for aviso in avisos if "NaT" in aviso] == [esperado]
    assert [aviso for aviso in meta.validation_warnings if "NaT" in aviso] == [esperado]
    assert frame["fim_vigencia"].isna().all()
    assert frame["inicio_vigencia"].dt.strftime("%d/%m/%Y").tolist() == ["09/05/2019", "25/06/2020"]
    assert str(frame["fim_vigencia"].dtype) == "datetime64[ns]"


def test_contagem_soma_os_blocos_do_arquivo():
    corpo = (
        f"{HEADER};DT_APOLICE\n2020;MT;SOJA;A1;SORRISO;xx\n2020;MT;SOJA;A2;SORRISO;yy\n"
    ).encode()
    detalhes: dict[str, Any] = {}
    blocos = list(parser.iter_apolices(io.BytesIO(corpo), chunk_size=1, detalhes=detalhes))
    assert len(blocos) == 2
    assert parser.avisos_de_datas(detalhes["datas"]) == [
        "mapa_psr: 2 valor(es) de data_apolice viraram NaT (data ilegível ou com ano fora "
        "de 1900–2099)."
    ]
