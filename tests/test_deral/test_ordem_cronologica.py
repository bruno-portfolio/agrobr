from __future__ import annotations

import io
import warnings
from pathlib import Path
from unittest.mock import AsyncMock

import openpyxl
import pandas as pd
import pytest

from agrobr import datasets
from agrobr.deral import api, parser
from agrobr.exceptions import ParseError
from agrobr.utils.result import ATRIBUTO_AVISOS

CAPTURA = Path(__file__).parents[1] / "golden_data" / "deral" / "pc_20260915" / "response.xls"
ABAS_DIVERGENTES = [
    "deral: a aba '19-09-2021' publica a data 20/09/2021 na planilha; "
    "o quadro usa a data da planilha, e não o nome da aba.",
    "deral: a aba '18-12-2017' publica a data 08/01/2018 na planilha; "
    "o quadro usa a data da planilha, e não o nome da aba.",
]


def _avisos_da_fonte(emitidos: list[warnings.WarningMessage]) -> list[str]:
    return [str(aviso.message) for aviso in emitidos if str(aviso.message).startswith("deral:")]


def test_quadro_sai_em_ordem_cronologica_por_produto():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = parser.parse_pc_xls(CAPTURA.read_bytes())

    assert df["data"].dtype == "datetime64[ns]"
    por_produto = df.groupby("produto")["data"]
    assert por_produto.apply(lambda serie: serie.is_monotonic_increasing).all()
    assert por_produto.last().to_dict() == por_produto.max().to_dict()
    assert df.iloc[-1][["produto", "data"]].tolist() == ["trigo", pd.Timestamp("2026-09-14")]


def test_aba_com_nome_diferente_da_data_avisa():
    with warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        df = parser.parse_pc_xls(CAPTURA.read_bytes())

    assert df.attrs.get(ATRIBUTO_AVISOS) == ABAS_DIVERGENTES
    assert _avisos_da_fonte(emitidos) == ABAS_DIVERGENTES
    assert df["data"].isin(pd.to_datetime(["2021-09-20", "2018-01-08"])).any()


@pytest.mark.parametrize(
    ("aba", "data_ref", "avisa"),
    [("6-11-2017", "06/11/2017", False), ("4-11-2015", "04/11/2020", True)],
)
def test_aba_com_dia_de_um_digito(aba, data_ref, avisa):
    assert (parser._aviso_de_aba_divergente(aba, data_ref) is not None) is avisa


def _quadro(aba: str, data_da_celula: str) -> tuple[str, str, list[dict[str, str]]]:
    return aba, data_da_celula, [{"produto": "soja", "data": data_da_celula}]


def test_data_da_celula_copiada_de_outra_aba_perde_para_o_nome_da_aba():
    registros, avisos = parser._datar_quadros(
        [
            _quadro("Atual", "09/11/2026"),
            _quadro("14-11-2022", "16/11/2021"),
            _quadro("16-11-2021", "16/11/2021"),
            _quadro("09-11-2020", "09/11/2020"),
            _quadro("18-12-2017", "08/01/2018"),
            _quadro("9-11-2015", "09/11/2020"),
        ]
    )

    assert [registro["data"] for registro in registros] == [
        "09/11/2026",
        "14/11/2022",
        "16/11/2021",
        "09/11/2020",
        "08/01/2018",
        "09/11/2015",
    ]
    assert avisos == [
        "deral: a aba '14-11-2022' publica na planilha a data 16/11/2021, que é a de outra aba; "
        "o quadro usa a data do nome da aba, 14/11/2022.",
        ABAS_DIVERGENTES[1],
        "deral: a aba '9-11-2015' publica na planilha a data 09/11/2020, que é a de outra aba; "
        "o quadro usa a data do nome da aba, 09/11/2015.",
    ]


@pytest.mark.parametrize(
    "abas",
    [("Atual", "Anterior"), ("31-02-2020", "09-11-2020"), ("9-11-2015", "09-11-15")],
    ids=["sem_data_no_nome", "nome_fora_do_calendario", "nomes_com_a_mesma_data"],
)
def test_abas_que_ficam_com_a_mesma_data_levantam_parse_error(abas):
    with pytest.raises(ParseError, match="publicam a mesma data de referência"):
        parser._datar_quadros([_quadro(aba, "09/11/2020") for aba in abas])


def _planilha(abas: dict[str, tuple[str, list[int] | None]]) -> bytes:
    livro = openpyxl.Workbook()
    livro.remove(livro.active)
    for nome, (data_da_celula, soja) in abas.items():
        aba = livro.create_sheet(title=nome)
        for linha in (
            ["Condições das lavouras", None, None, None, None, None, None],
            [f"Referência: {data_da_celula}", None, None, None, None, None, None],
            ["Cultura", "Plantada", "Colhida", "Ruim", "Média", "Boa", None],
            ["Soja", 100, 0, *soja, None]
            if soja
            else ["Soja", "Sem informação - Feriado", None, None, None, None, None],
            ["Fonte: DERAL", None, None, None, None, None, None],
            [None, None, None, None, None, None, "fim"],
        ):
            aba.append(linha)
    saida = io.BytesIO()
    livro.save(saida)
    return saida.getvalue()


async def test_dataset_cumpre_o_contrato_com_data_de_celula_repetida(monkeypatch):
    """As abas do PC.xls de 09/11/2026 com data repetida: a de 2015 e a do feriado de 2022."""
    corpo = _planilha(
        {
            "14-11-2022": ("16/11/2021", None),
            "16-11-2021": ("16/11/2021", [0, 4, 96]),
            "09-11-2020": ("09/11/2020", [2, 23, 75]),
            "9-11-2015": ("09/11/2020", [0, 2, 98]),
        }
    )
    monkeypatch.setattr(api.client, "fetch_pc_xls", AsyncMock(return_value=corpo))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df, meta = await datasets.condicao_lavouras("soja", return_meta=True)

    boa = df[df["condicao"] == "boa"]
    assert boa["data"].tolist() == [
        pd.Timestamp("2015-11-09"),
        pd.Timestamp("2020-11-09"),
        pd.Timestamp("2021-11-16"),
    ]
    assert boa["pct"].tolist() == [98.0, 75.0, 96.0]
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith("deral:")] == [
        "deral: a aba '9-11-2015' publica na planilha a data 09/11/2020, que é a de outra aba; "
        "o quadro usa a data do nome da aba, 09/11/2015."
    ]


@pytest.mark.parametrize(
    "consulta",
    [
        lambda: api.condicao_lavouras("soja", return_meta=True),
        lambda: datasets.condicao_lavouras("soja", return_meta=True),
    ],
    ids=["fonte", "dataset"],
)
async def test_aviso_de_aba_chega_ao_meta_com_filtro(monkeypatch, consulta):
    monkeypatch.setattr(api.client, "fetch_pc_xls", AsyncMock(return_value=CAPTURA.read_bytes()))

    with warnings.catch_warnings(record=True) as emitidos:
        warnings.simplefilter("always")
        df, meta = await consulta()

    assert set(df["produto"]) == {"soja"}
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith("deral:")] == (
        ABAS_DIVERGENTES
    )
    assert _avisos_da_fonte(emitidos) == ABAS_DIVERGENTES
