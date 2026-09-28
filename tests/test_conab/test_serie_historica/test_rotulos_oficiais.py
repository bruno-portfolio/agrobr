from __future__ import annotations

import re
from io import BytesIO
from pathlib import Path

import pandas as pd
import pytest
import xlrd

from agrobr import datasets
from agrobr.conab.serie_historica import parser
from agrobr.exceptions import ParseError
from tests import helpers

MANIFEST = helpers.load_serie_historica_manifest()
ZEROS = helpers.SERIE_HISTORICA_GOLDEN.parent / "serie_historica_zeros_20260923"
PLANILHAS = [
    *[
        (case["product"], helpers.SERIE_HISTORICA_GOLDEN / case["file"])
        for case in MANIFEST["cases"]
    ],
    *[(produto, ZEROS / f"{produto}.xls") for produto in ("amendoim_2", "feijao_3", "mamona")],
]
REGIAO_IBGE = {
    **dict.fromkeys(["AC", "AM", "AP", "PA", "RO", "RR", "TO"], "NORTE"),
    **dict.fromkeys(["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"], "NORDESTE"),
    **dict.fromkeys(["DF", "GO", "MS", "MT"], "CENTRO-OESTE"),
    **dict.fromkeys(["ES", "MG", "RJ", "SP"], "SUDESTE"),
    **dict.fromkeys(["PR", "RS", "SC"], "SUL"),
}
AREA_DO_TITULO = {
    "área plantada": "area_plantada_mil_ha",
    "área total": "area_plantada_mil_ha",
    "área colhida": "area_colhida_mil_ha",
    "área em produção": "area_em_producao_mil_ha",
    "área em formação": "area_formacao_mil_ha",
}


def _regiao_na_planilha(caminho: Path) -> dict[str, str | None]:
    regioes: dict[str, str | None] = {}
    for aba in xlrd.open_workbook(str(caminho)).sheets():
        atual = None
        for rotulo in aba.col_values(0):
            texto = str(rotulo).strip().upper()
            if texto in REGIAO_IBGE.values():
                atual = texto
            elif texto == "BRASIL":
                atual = None
            elif texto in REGIAO_IBGE:
                assert regioes.setdefault(texto, atual) == atual, (caminho.name, aba.name, texto)
    return regioes


@pytest.mark.parametrize(("produto", "caminho"), PLANILHAS, ids=[item[0] for item in PLANILHAS])
def test_regiao_de_cada_uf_e_a_da_planilha(produto: str, caminho: Path):
    publicada = _regiao_na_planilha(caminho)
    assert publicada == {uf: REGIAO_IBGE[uf] for uf in publicada}
    frame = parser.records_to_dataframe(
        parser.parse_serie_historica(BytesIO(caminho.read_bytes()), produto)
    )
    assert set(frame["uf"]) <= set(publicada)
    assert frame["regiao"].tolist() == frame["uf"].map(publicada).tolist()


def test_subregiao_com_nome_de_macrorregiao_nao_muda_a_regiao():
    bruto = pd.read_excel(
        helpers.SERIE_HISTORICA_GOLDEN / "cafe.xls", sheet_name="Produção", header=None
    )
    rotulos = bruto[0].astype(str).str.strip().tolist()
    sul, es = rotulos.index("Sul e Centro-Oeste"), rotulos.index("ES")
    frame = bruto.drop(index=range(sul + 1, es)).reset_index(drop=True)
    assert frame[0].tolist()[sul : sul + 2] == ["Sul e Centro-Oeste", "ES"]
    regioes = {
        registro.uf: registro.regiao
        for registro in parser.parse_sheet(frame, "cafe", "producao_mil_ton")
    }
    assert {uf: regioes[uf] for uf in ("MG", "ES", "RJ", "SP", "PR")} == {
        "MG": "SUDESTE",
        "ES": "SUDESTE",
        "RJ": "SUDESTE",
        "SP": "SUDESTE",
        "PR": "SUL",
    }


@pytest.mark.parametrize(("produto", "caminho"), PLANILHAS, ids=[item[0] for item in PLANILHAS])
def test_area_de_cada_aba_conforme_o_titulo(produto: str, caminho: Path):
    livro = xlrd.open_workbook(str(caminho))
    decisoes = parser.resolve_sheets(produto, livro.sheet_names())
    for aba in livro.sheets():
        campo = decisoes[aba.name].campo
        if campo is not None and campo.startswith("area"):
            titulo = aba.cell_value(2, 0).lower()
            assert [AREA_DO_TITULO[nome] for nome in AREA_DO_TITULO if nome in titulo] == [campo], (
                aba.name,
                titulo,
            )


@pytest.mark.asyncio
async def test_cana_publica_a_area_colhida_da_aba_area(monkeypatch: pytest.MonkeyPatch):
    helpers.install_serie_historica_http(
        monkeypatch, next(case for case in MANIFEST["cases"] if case["id"] == "cana")
    )
    aba = xlrd.open_workbook(str(helpers.SERIE_HISTORICA_GOLDEN / "cana.xls")).sheet_by_name("Área")
    assert aba.cell_value(2, 0) == "Série Histórica de Área Colhida"
    assert (aba.cell_value(33, 0), aba.cell_value(5, 16), aba.cell_value(33, 16)) == (
        "SP",
        "2020/21",
        4444.21,
    )
    publicado: dict[tuple[str, str], float] = {}
    for coluna in range(1, aba.ncols):
        safra = str(aba.cell_value(5, coluna)).strip()
        celulas = {
            str(aba.cell_value(linha, 0)).strip(): aba.cell_value(linha, coluna)
            for linha in range(6, aba.nrows)
            if str(aba.cell_value(linha, 0)).strip() in REGIAO_IBGE
            and isinstance(aba.cell_value(linha, coluna), float)
        }
        if re.fullmatch(r"\d{4}/\d{2}", safra) and any(celulas.values()):
            publicado.update({(uf, safra): valor for uf, valor in celulas.items()})
    frame = await datasets.serie_historica_safra("cana")
    sp = frame.loc[(frame["uf"] == "SP") & (frame["safra"] == "2020/21")]
    assert sp["area_colhida_mil_ha"].tolist() == [4444.21]
    assert frame["area_plantada_mil_ha"].isna().all()
    colhida = frame.dropna(subset=["area_colhida_mil_ha"])
    assert (
        dict(zip(zip(colhida["uf"], colhida["safra"]), colhida["area_colhida_mil_ha"])) == publicado
    )


def test_uf_sob_macrorregiao_errada_e_erro_de_layout():
    bruto = pd.read_excel(
        helpers.SERIE_HISTORICA_GOLDEN / "cafe.xls", sheet_name="Produção", header=None
    )
    rotulos = bruto[0].astype(str).str.strip().tolist()
    bruto.iloc[rotulos.index("Norte, Jequitinhonha e Mucuri"), 0] = "Norte"
    with pytest.raises(ParseError, match="UF ES listada sob NORTE"):
        parser.parse_sheet(bruto, "cafe", "producao_mil_ton")
