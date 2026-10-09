from __future__ import annotations

import hashlib
import json
import warnings
from pathlib import Path
from typing import Any

import pandas as pd

from agrobr import embrapa_solos
from agrobr.embrapa_solos import parser
from tests.helpers import install_embrapa_solos_wfs, sem_excecao

PERFIS_TEXTO = Path(__file__).parents[1] / "golden_data" / "embrapa_solos" / "perfis_texto_20260926"
AVISO = (
    "Texto publicado pela Embrapa com dupla codificação (UTF-8 lido como Latin-1), reparado por "
    "coluna: municipio 1, uso_atual 1, titulo 1, autor 1, responsave 1, material_o 1, descricao_ 1, "
    "grau_con_1 1, plasticida 1"
)


def _corpo(nome: str) -> dict[str, Any]:
    recibo = json.loads((PERFIS_TEXTO / "manifest.json").read_bytes())["arquivos"][nome]
    corpo = (PERFIS_TEXTO / nome).read_bytes()
    digest = hashlib.sha256(corpo).hexdigest()
    assert (digest, len(corpo)) == (recibo["sha256"], recibo["bytes"])
    return json.loads(corpo)


async def test_perfis_reparam_o_texto_publicado_com_dupla_codificacao(monkeypatch):
    install_embrapa_solos_wfs(monkeypatch, _corpo("fid256.json")["features"])
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame, meta = await embrapa_solos.perfis(max_registros=None, return_meta=True)
    [linha] = frame.to_dict("records")
    assert linha["feature_id"] == "perfis_pronasolos_2020.256"
    assert linha["municipio"] == "Brasília"
    assert "Solos e Aptidão Agrícola das Terras de Parte da Região" in linha["titulo"]
    assert linha["uso_atual"] == "Pastagem de capim-jaraguá e cultura de milho."
    assert linha["grau_con_1"] == "Friável"
    assert [aviso for aviso in meta.validation_warnings if "dupla codificação" in aviso] == [AVISO]
    assert meta.parser_version == 3


def test_reparo_so_desfaz_a_dupla_codificacao():
    sp = {feature["id"]: feature["properties"] for feature in _corpo("sp5.json")["features"]}
    analandia = sp["perfis_pronasolos_2020.1757"]
    frame = pd.DataFrame(
        {
            "municipio": pd.Series(
                [
                    analandia["municipio"],
                    sp["perfis_pronasolos_2020.1759"]["municipio"],
                    "SÃO JOSÉ DO RIO PRETO",
                    "Ð¿",
                    None,
                ],
                dtype="string",
            ),
            "uso_atual": pd.Series(
                [analandia["uso_atual"], "SÃ£o — com travessão", "Rosana", "Ã", None],
                dtype="string",
            ),
            "fid": pd.Series([1757, 1759, 1, 2, 3], dtype="Int64"),
        }
    )
    reparos, sem_reparo = parser.reparar_texto(frame)
    assert frame["municipio"].tolist() == [
        "Analândia",
        "São Carlos",
        "SÃO JOSÉ DO RIO PRETO",
        "Ð¿",
        pd.NA,
    ]
    assert frame["uso_atual"].tolist() == [
        "Área urbana.",
        "SÃ£o — com travessão",
        "Rosana",
        "Ã",
        pd.NA,
    ]
    assert reparos == {"municipio": 2, "uso_atual": 1}
    assert sem_reparo == {"uso_atual": 1}
    assert str(frame["municipio"].dtype) == "string"


def test_reparo_conta_o_que_a_fonte_publicou_sem_volta():
    titulos = [
        "Solos e AptidÃ£o AgrÃ",
        "Solos e AptidÃ£o \ufffd",
        "Solos e AptidÃ£o Ã€ vista",
        "Solos e AptidÃ£o",
    ]
    frame = pd.DataFrame({"titulo": pd.Series(titulos, dtype="string")})
    reparos, sem_reparo = parser.reparar_texto(frame)
    assert frame["titulo"].tolist() == [*titulos[:3], "Solos e Aptidão"]
    assert (reparos, sem_reparo) == ({"titulo": 1}, {"titulo": 3})


async def test_aviso_traz_as_2_contagens(monkeypatch):
    [feature] = _corpo("fid256.json")["features"]
    publicado = json.loads(json.dumps(feature))
    publicado["id"] = "perfis_pronasolos_2020.257"
    publicado["properties"]["fid"] = 257
    publicado["properties"]["municipio"] = "BrasÃ\u00adlia \ufffd"
    install_embrapa_solos_wfs(monkeypatch, [feature, publicado])
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame, meta = await embrapa_solos.perfis(max_registros=None, return_meta=True)
    assert frame["municipio"].tolist() == ["Brasília", "BrasÃ\u00adlia \ufffd"]
    [aviso] = [aviso for aviso in meta.validation_warnings if "dupla codificação" in aviso]
    assert aviso.endswith("; com a assinatura e sem reparo por coluna: municipio 1")
    assert "reparado por coluna: municipio 1, uso_atual 2, titulo 2," in aviso
