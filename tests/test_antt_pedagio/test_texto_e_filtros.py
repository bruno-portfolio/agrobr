from __future__ import annotations

import csv
import io
import warnings
from decimal import Decimal

import pytest

from agrobr.alt.antt_pedagio import api, parser
from agrobr.exceptions import InvalidParameterError
from tests.helpers import install_anttpedagio_source, levanta_exatamente
from tests.test_antt_pedagio import oficial


async def _fluxo(monkeypatch, pracas=None, **filtros):
    install_anttpedagio_source(
        monkeypatch,
        {"volume-2023.csv": oficial.load("mensal_2023.csv")},
        plazas=oficial.load("pracas.csv") if pracas is None else pracas,
    )
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame, meta = await api.fluxo_pedagio(ano=2023, return_meta=True, **filtros)
    return frame, meta, [str(aviso.message) for aviso in avisos]


async def test_texto_do_fluxo_sem_espaco_externo_e_sentido_em_maiusculas(monkeypatch):
    frame, _, _ = await _fluxo(monkeypatch)
    assert set(frame["sentido"]) == {"CRESCENTE", "DECRESCENTE"}
    for coluna in ("concessionaria", "praca", "sentido", "categoria_eixo", "tipo_cobranca"):
        textos = frame[coluna].dropna()
        assert textos.eq(textos.str.strip()).all(), coluna
    assert int(frame["volume"].sum()) == 1_175_641
    assert frame.groupby("sentido")["volume"].sum().to_dict() == {
        "CRESCENTE": 815_020,
        "DECRESCENTE": 360_621,
    }
    assert int(frame.loc[frame["uf"].notna(), "volume"].sum()) == 1_175_641


@pytest.mark.parametrize(
    ("filtros", "mensagem"),
    [
        ({"inicio": "2024-01-01"}, "inicio=2024-01-01 fora dos anos solicitados"),
        ({"fim": "2023-03-15"}, r"fim=2023-03-15.*primeiro dia do mês \(2023-03-01\)"),
    ],
    ids=["ano", "dia"],
)
async def test_erro_de_data_mensal_diz_o_limite_e_o_valor(filtros, mensagem):
    with levanta_exatamente(InvalidParameterError, mensagem):
        await api.fluxo_pedagio(ano=2023, **filtros)


async def test_filtro_geografico_sem_praca_no_cadastro_avisa(monkeypatch):
    frame, meta, avisos = await _fluxo(monkeypatch, uf="AC")
    assert frame.empty
    aviso = "Nenhuma praça do cadastro ANTT casa uf='AC'"
    assert [texto for texto in meta.validation_warnings if aviso in texto]
    assert [texto for texto in avisos if aviso in texto]


@pytest.mark.parametrize("filtro", ["uf", "rodovia"])
async def test_vinculo_ambiguo_com_praca_compativel_nao_diz_que_falta_praca(monkeypatch, filtro):
    linha = next(
        row for row in oficial.rows(oficial.load("pracas.csv")) if row["uf"] and row["rodovia"]
    )
    municipio = "municipio" if "municipio" in linha else "municipal"
    cadastro = io.StringIO(newline="")
    escritor = csv.DictWriter(
        cadastro, fieldnames=list(linha), delimiter=";", lineterminator="\r\n"
    )
    escritor.writeheader()
    escritor.writerows([linha, linha | {municipio: linha[municipio] + " (segundo cadastro)"}])
    frame, meta, avisos = await _fluxo(
        monkeypatch, cadastro.getvalue().encode("cp1252"), **{filtro: linha[filtro]}
    )
    assert frame.empty
    assert not [texto for texto in meta.validation_warnings + avisos if "Nenhuma praça" in texto]
    assert [texto for texto in meta.validation_warnings if "sem vínculo único" in texto]


async def test_recorte_temporal_vazio_nao_avisa_filtro_geografico(monkeypatch):
    frame, meta, _ = await _fluxo(monkeypatch, uf="RJ", inicio="2023-12-01", fim="2023-12-01")
    assert not [texto for texto in meta.validation_warnings if "Nenhuma praça" in texto]


CABECALHO = "concessionaria;praca_de_pedagio;rodovia;uf;{};latitude;longitude\n"


@pytest.mark.parametrize(
    ("colunas", "avisa"),
    [("municipal", True), ("municipio", False), ("municipio;municipal", True)],
    ids=["so_municipal", "so_municipio", "as_duas"],
)
async def test_coluna_municipal_e_depreciada_com_aviso(monkeypatch, colunas, avisa):
    valores = ";".join("Sinop" for _ in colunas.split(";"))
    corpo = (CABECALHO.format(colunas) + f"CRO;P1;BR-163;MT;{valores};-10;-55\n").encode()
    install_anttpedagio_source(monkeypatch, {}, plazas=corpo)
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame, meta = await api.pracas_pedagio(return_meta=True)
    futuros = [str(aviso.message) for aviso in capturados if aviso.category is FutureWarning]
    assert frame["municipio"].tolist() == ["Sinop"]
    assert ("municipal" in frame) is avisa
    assert bool(futuros) is avisa
    assert [texto for texto in meta.validation_warnings if "'municipal'" in texto] == futuros


def _volume(linhas: list[dict[str, str]]) -> int:
    return sum(
        int(Decimal(linha["volume_total"].replace(",", ".")))
        for linha in linhas
        if oficial.countable(linha["volume_total"])
    )


@pytest.mark.parametrize("filtro", [{"uf": "MT"}, {"rodovia": "BR-163"}])
async def test_nome_anterior_da_concessionaria_casa_o_cadastro(monkeypatch, filtro):
    frame, _, avisos = await _fluxo(monkeypatch, **filtro)
    cro = [
        linha
        for linha in oficial.rows(oficial.load("mensal_2023.csv"))
        if linha["concessionaria"] == "CRO"
    ]
    da_cro = frame[frame["concessionaria"] == "CRO"]
    assert set(frame["concessionaria"]) == {"CRO"}
    assert int(da_cro["volume"].sum()) == _volume(cro) > 0
    assert set(zip(da_cro["praca"], da_cro["rodovia"], da_cro["uf"])) == {("P1", "BR-163", "MT")}
    assert not [aviso for aviso in avisos if "sem vínculo" in aviso]


async def test_caixa_mista_casa_o_cadastro_e_par_sem_cadastro_segue_no_aviso(monkeypatch):
    corpo = oficial.load("abril_2021_caixa.csv")
    install_anttpedagio_source(
        monkeypatch, {"volume-2021.csv": corpo}, plazas=oficial.load("pracas.csv")
    )
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame, meta = await api.fluxo_pedagio(
            ano=2021, inicio="2021-04-01", fim="2021-04-01", uf="RS", return_meta=True
        )
    linhas = oficial.rows(corpo)
    ecosul = [linha for linha in linhas if linha["concessionaria"] == "Ecosul"]
    via_bahia = [linha for linha in linhas if linha["concessionaria"] == "VIA BAHIA"]
    assert set(frame["concessionaria"]) == {"Ecosul"} and set(frame["uf"]) == {"RS"}
    assert int(frame["volume"].sum()) == _volume(ecosul) > 0
    exclusoes = [str(aviso.message) for aviso in capturados if "sem vínculo" in str(aviso.message)]
    assert len(exclusoes) == 1 and exclusoes[0] in meta.validation_warnings
    assert f"{len(via_bahia)} registros (volume={_volume(via_bahia)}) de 1 pares" in exclusoes[0]
    assert "(VIA BAHIA/Praça 1)" in exclusoes[0]


def test_chave_sem_caixa_nem_espaco_nao_funde_concessionarias_nem_tira_acento():
    mapa, diagnostico = parser.build_pracas_enrichment(
        parser.parse_pracas(oficial.load("pracas.csv"))
    )
    assert diagnostico["conflicting_keys"] == 0
    assert mapa[parser.chave_praca("CRO", "P1")][:2] == ("BR-163", "MT")
    assert mapa[parser.chave_praca("CRO", "P2")][:2] == ("BR-364", "MT")
    assert mapa[parser.chave_praca("AUTOPISTA LITORAL SUL", "P1")][:2] == ("BR-376", "PR")
    assert parser.chave_praca("CRO", "P1") != parser.chave_praca("AUTOPISTA LITORAL SUL", "P1")
    assert parser.chave_praca("MSVIA", "P1-Mundo Novo") == parser.chave_praca(
        "PANTANAL", "P1-Mundo Novo"
    )
    assert parser.chave_praca(" Ecosul ", "Praça  Capão Seco") == parser.chave_praca(
        "ECOSUL", "PRAÇA CAPÃO SECO"
    )
    assert parser.chave_praca("ECOSUL", "Praca Capao Seco") not in mapa
