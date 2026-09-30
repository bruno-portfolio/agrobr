from __future__ import annotations

import csv
import io
import json
import shutil
import sys
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.icmbio import api
from scripts import reconciliar_icmbio as reconciliacao
from tests.helpers import levanta_exatamente, sem_excecao


@pytest.fixture(autouse=True)
def sem_rede(monkeypatch):
    rede = AsyncMock(side_effect=AssertionError("rede antes da validação"))
    monkeypatch.setattr(api, "_fetch_tabular", rede)
    monkeypatch.setattr(api.client, "fetch_ucs_count", rede)
    monkeypatch.setattr(api.client, "fetch_ucs_geo", rede)


async def test_ucs_as_polars_sem_polars(monkeypatch):
    monkeypatch.setitem(sys.modules, "polars", None)
    with levanta_exatamente(ImportError, r"Instale agrobr\[polars\] para usar as_polars=True"):
        await api.ucs(as_polars=True)


@pytest.mark.parametrize("bbox", [(-60.0, -15.0, -50.0), "abcd"], ids=["tres", "texto"])
async def test_ucs_bbox_sem_quatro_coordenadas(bbox):
    with levanta_exatamente(InvalidParameterError, "BBOX deve conter quatro coordenadas"):
        await api.ucs(bbox=bbox)


async def test_ucs_bbox_fora_dos_limites():
    with levanta_exatamente(
        InvalidParameterError, "BBOX fora dos limites geográficos de longitude/latitude"
    ):
        await api.ucs(bbox=(-200.0, -15.0, -190.0, -10.0))


async def test_ucs_argumento_desconhecido():
    with levanta_exatamente(TypeError, r"Argumentos desconhecidos em icmbio\.ucs: \['estado'\]"):
        await api.ucs(estado="MT")


@pytest.mark.parametrize(
    ("filtro", "mensagem"),
    [
        ({"uf": "XX"}, "UF inválida"),
        ({"grupo": "ZZ"}, "Grupo invalido"),
        ({"bioma": "Marte"}, "Bioma inválido"),
    ],
    ids=["uf", "grupo", "bioma"],
)
async def test_ucs_geo_valida_antes_da_rede(filtro, mensagem):
    with levanta_exatamente(InvalidParameterError, mensagem):
        await api.ucs_geo(**filtro)


@pytest.mark.parametrize(
    "bbox",
    [(float("nan"), -15.0, -50.0, -10.0), (True, -15.0, -50.0, -10.0)],
    ids=["nan", "booleano"],
)
async def test_ucs_bbox_com_coordenada_nao_numerica(bbox):
    with levanta_exatamente(
        InvalidParameterError, "BBOX deve conter coordenadas numéricas finitas"
    ):
        await api.ucs(bbox=bbox)


def rodar_n1(monkeypatch: pytest.MonkeyPatch, *argumentos: str) -> int:
    monkeypatch.setattr(sys, "argv", ["reconciliar_icmbio.py", *argumentos])
    with sem_excecao():
        return reconciliacao.main()


def test_n1_icmbio_main_sobre_corpos_locais(monkeypatch, tmp_path):
    relatorio = tmp_path / "relatorio.json"
    assert rodar_n1(monkeypatch, "--output", str(relatorio)) == 0
    checks = json.loads(relatorio.read_text(encoding="utf-8"))["checks"]
    assert [check["status"] for check in checks] == ["ok"] * 4


def test_n1_icmbio_main_acusa_corpo_divergente(monkeypatch, tmp_path):
    entrada = tmp_path / "corpos"
    shutil.copytree(reconciliacao.GOLDEN, entrada)
    alvo = entrada / "icmbio_national_007.csv"
    linhas = alvo.read_bytes().splitlines(keepends=True)
    alvo.write_bytes(b"".join([*linhas, linhas[-1]]))
    relatorio = tmp_path / "relatorio.json"
    assert rodar_n1(monkeypatch, "--input-dir", str(entrada), "--output", str(relatorio)) == 1
    checks = json.loads(relatorio.read_text(encoding="utf-8"))["checks"]
    assert [(check["file"], check["status"]) for check in checks if check["status"] != "ok"] == [
        ("icmbio_national_007.csv", "mismatch")
    ]


MANIFESTO = json.loads((reconciliacao.GOLDEN / "manifest.json").read_bytes())


def csv_derivado(mutacao: str) -> bytes:
    recurso = MANIFESTO["resources"][0]
    linhas = list(
        csv.reader(
            io.StringIO((reconciliacao.GOLDEN / recurso["file"]).read_text(encoding="utf-8"))
        )
    )
    if mutacao == "cabecalho_reordenado":
        linhas = [[linha[1], linha[0], *linha[2:]] for linha in linhas]
    else:
        campo, valor = {
            "ano_invalido": ("criacaoano", "MMXX"),
            "grupo_invalido": ("grupouc", "XX"),
        }[mutacao]
        linhas[-1][linhas[0].index(campo)] = valor
    saida = io.StringIO(newline="")
    csv.writer(saida, lineterminator="\n").writerows(linhas)
    return saida.getvalue().encode("utf-8")


@pytest.mark.parametrize(
    ("mutacao", "problema"),
    [
        ("cabecalho_reordenado", "Colunas ausentes, duplicadas ou sem decisão"),
        ("ano_invalido", "Registros incompatíveis: [347]"),
        ("grupo_invalido", "Registros incompatíveis: [347]"),
    ],
    ids=["cabecalho_reordenado", "ano_invalido", "grupo_invalido"],
)
def test_n1_icmbio_csv_acusa_deriva(mutacao, problema):
    resultado = reconciliacao.compare_csv(csv_derivado(mutacao), MANIFESTO["resources"][0])
    assert resultado["status"] == "mismatch"
    assert resultado["problems"] == [problema]


@pytest.mark.parametrize(
    ("mutacao", "problema"),
    [
        ("raiz", "Documento não é um schema XSD"),
        ("namespace", "Namespace diferente da camada capturada"),
    ],
    ids=["raiz", "namespace"],
)
def test_n1_icmbio_schema_acusa_deriva(mutacao, problema):
    esquema = MANIFESTO["schema"]
    raiz = ET.fromstring((reconciliacao.GOLDEN / esquema["file"]).read_bytes())
    if mutacao == "raiz":
        raiz.tag = "{http://www.w3.org/2001/XMLSchema}element"
    else:
        raiz.set("targetNamespace", "https://example.test/outra")
    resultado = reconciliacao.compare_schema(ET.tostring(raiz), esquema)
    assert resultado["status"] == "mismatch"
    assert resultado["problems"] == [problema]
