from __future__ import annotations

import json
from pathlib import Path

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
