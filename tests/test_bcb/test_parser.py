import csv
import io
import json
import math
import warnings
from pathlib import Path

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.bcb import parser
from tests.helpers import sem_excecao

ORACULO = Path(__file__).parents[1] / "golden_data/bcb/oraculo_20260923"
R9_SICOR = (
    Path(__file__).parents[1]
    / "golden_data/reconciliacao_r9_20260918/sicor/sicor_custeio_soja_2024_2025_MT.json"
)
CORPOS = {
    "custeio": (R9_SICOR, "VlCusteio", "QtdCusteio"),
    "investimento": (ORACULO / "sicor_investimento_bovinos_mt_000.json", "VlInvest", "QtdInvest"),
    "comercializacao": (
        ORACULO / "sicor_comercializacao_soja_mt_001.json",
        "VlComerc",
        "QtdComerc",
    ),
}


def tabela(nome: str, delimitador: str) -> dict[str, str]:
    linhas = csv.reader(
        io.StringIO((ORACULO / nome).read_bytes().decode("cp1252")), delimiter=delimitador
    )
    return {
        codigo: descricao.split(" - ", 1)[0].replace('"', "").strip()
        for codigo, descricao, *_resto in list(linhas)[1:]
    }


PROGRAMAS = tabela("dominio_Programa.csv", ";")
SEGUROS = tabela("dominio_TipoGarantiaEmpreendimento.csv", ",")


def safra(registro: dict) -> str:
    ano, mes = int(registro["AnoEmissao"]), int(registro["MesEmissao"])
    inicio = ano if mes >= 7 else ano - 1
    return f"{inicio}/{(inicio + 1) % 100:02d}"


def linhas(frame: pd.DataFrame, colunas: list[str]) -> list[tuple]:
    return sorted(
        tuple(None if pd.isna(valor) else valor for valor in linha)
        for linha in frame[colunas].itertuples(index=False, name=None)
    )


@pytest.mark.parametrize("finalidade", list(CORPOS))
def test_parse_publica_cada_registro_do_corpo_oficial(finalidade: str):
    caminho, campo_valor, campo_qtd = CORPOS[finalidade]
    registros = json.loads(caminho.read_bytes())["value"]

    frame = parser.parse_credito_rural(registros, finalidade=finalidade)

    colunas = [
        "safra",
        "uf",
        "produto",
        "finalidade",
        "ano_emissao",
        "mes_emissao",
        "valor",
        "qtd_contratos",
        "cd_programa",
        "programa",
        "cd_tipo_seguro",
        "tipo_seguro",
    ]
    assert linhas(frame, colunas) == sorted(
        (
            safra(registro),
            registro["nomeUF"],
            registro["nomeProduto"].strip('"').lower(),
            finalidade,
            int(registro["AnoEmissao"]),
            int(registro["MesEmissao"]),
            registro[campo_valor],
            registro[campo_qtd],
            registro["cdPrograma"],
            PROGRAMAS[registro["cdPrograma"]],
            registro["cdTipoSeguro"],
            SEGUROS[registro["cdTipoSeguro"]],
        )
        for registro in registros
    )
    assert str(frame["valor"].dtype) == "float64"
    assert [str(frame[coluna].dtype) for coluna in ["ano_emissao", "mes_emissao"]] == ["Int64"] * 2
    if finalidade == "custeio":
        assert frame["area_financiada"].isna().all()
        assert {registro["AreaCusteio"] for registro in registros} == {None}


def test_agregacoes_somam_o_corpo_oficial_preservando_nulos():
    registros = json.loads(R9_SICOR.read_bytes())["value"]
    frame = parser.parse_credito_rural(registros)

    por_uf = parser.agregar_por_uf(frame)
    por_programa = parser.agregar_por_programa(frame)

    colunas = ["safra", "uf", "produto", "finalidade"]
    medidas = ["valor", "area_financiada", "qtd_contratos"]
    assert por_uf.columns.tolist() == colunas + medidas
    assert por_programa.columns.tolist() == colunas + ["programa", "cd_programa"] + medidas
    assert por_uf[["safra", "uf", "produto", "finalidade"]].values.tolist() == [
        ["2024/25", "MT", "soja", "custeio"]
    ]
    assert por_uf["valor"].tolist() == [pytest.approx(math.fsum(r["VlCusteio"] for r in registros))]
    assert por_uf["qtd_contratos"].tolist() == [sum(r["QtdCusteio"] for r in registros)]
    assert por_uf["area_financiada"].isna().all()
    esperado: dict[tuple[str, str], list[float]] = {}
    for registro in registros:
        chave = (PROGRAMAS[registro["cdPrograma"]], registro["cdPrograma"])
        esperado.setdefault(chave, []).append(registro["VlCusteio"])
    assert por_programa[["programa", "cd_programa"]].apply(tuple, axis=1).tolist() == sorted(
        esperado
    )
    assert por_programa["valor"].tolist() == pytest.approx(
        [math.fsum(esperado[chave]) for chave in sorted(esperado)]
    )
    assert por_programa["area_financiada"].isna().all()
    vazio = pd.DataFrame()
    assert parser.agregar_por_uf(vazio) is vazio
    assert parser.agregar_por_programa(vazio) is vazio


def test_parse_vazio_devolve_frame_do_contrato_com_aviso():
    with warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame = parser.parse_credito_rural([])
    assert [(aviso.category, str(aviso.message)) for aviso in avisos] == [
        (
            UserWarning,
            "Nenhum registro para produto/safra/UF/finalidade; em investimento os produtos "
            "são itens de investimento, ex.: BOVINOS, CAFÉ, CANA-DE-AÇUCAR, BANANA e tratores.",
        )
    ]
    assert frame.empty
    assert contracts.get_contract("credito_rural").validate(frame) == (True, [])


def test_registros_do_bigquery_sem_dimensoes_publicam_sem_programa():
    registros = [
        {
            "ano_emissao": ano,
            "mes_emissao": mes,
            "uf": "mt",
            "cd_municipio": "5107248",
            "produto": '"SOJA"',
            "finalidade": "custeio",
            "valor": valor,
            "area_financiada": 98500.0,
            "qtd_contratos": 1240,
        }
        for ano, mes, valor in [(2023, 9, 285431200.0), (2024, 6, 1.5)]
    ]

    with sem_excecao():
        frame = parser.parse_credito_rural(registros)

    assert linhas(frame, ["safra", "uf", "produto", "valor", "qtd_contratos"]) == [
        ("2023/24", "MT", "soja", 1.5, 1240),
        ("2023/24", "MT", "soja", 285431200.0, 1240),
    ]
    assert not {"programa", "tipo_seguro", "fonte_recurso", "modalidade", "atividade"} & set(
        frame.columns
    )
