from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.sfb import parser
from scripts import reconciliar_sfb as reconciliacao

CNFP_DF = Path(__file__).parent / "golden_data/sfb/oficial_20260923/cnfp_df_01.json"


def test_comparar_cnfp_confere_o_texto_publicado_do_ano_de_criacao():
    pagina = CNFP_DF.read_bytes()
    feicoes = json.loads(pagina)["features"]
    publicado = parser.parse_layer_tabular([pagina], layer_key="cnfp")
    campos = reconciliacao.CAMPOS_CNFP

    assert reconciliacao.comparar(publicado, feicoes, campos, "anocriacao")["problems"] == []
    publicado.loc[0, "ano_criacao_texto"] = "01/01/1900"
    resultado = reconciliacao.comparar(publicado, feicoes, campos, "anocriacao")
    assert (resultado["status"], resultado["problems"]) == ("mismatch", ["1 linhas divergentes"])


@pytest.mark.parametrize(
    "campo,valor",
    [
        ("lote", "lote de outra linha"),
        ("codigo_lote", 999),
        ("ciclo", "2"),
        ("municipio", "outro município"),
        ("id", 1),
    ],
)
def test_comparar_ifn_detecta_valor_incorreto_com_o_mesmo_total(campo, valor):
    golden = Path(__file__).parent / "golden_data/sfb/ifn_migracao_20261002"
    esperado = json.loads((golden / "expected.json").read_bytes())["first"]
    publicado = pd.DataFrame([esperado])
    pontos = json.loads((golden / "pontos_df_tab.json").read_bytes())["features"][:1]
    lotes = json.loads((golden / "lotes_df.json").read_bytes())["features"]

    assert reconciliacao.comparar_ifn(publicado, pontos, lotes)["status"] == "ok"
    publicado.loc[0, campo] = valor
    divergente = reconciliacao.comparar_ifn(publicado, pontos, lotes)
    assert divergente["status"] == "mismatch"
    assert divergente["linhas"] == 1
    assert divergente["problems"]
