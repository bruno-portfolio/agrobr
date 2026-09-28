from __future__ import annotations

import json
from pathlib import Path

from scripts import reconciliar_embrapa_solos as reconciliation

FID256 = Path(__file__).parent / "golden_data/embrapa_solos/r39_20260926/fid256.json"


def test_esperado_repara_so_o_texto_com_dupla_codificacao():
    feature = json.loads(FID256.read_bytes())["features"][0]
    linha = {
        nome: "" if valor is None else str(valor) for nome, valor in feature["properties"].items()
    }
    esperado = reconciliation.esperado(
        "perfis", {**linha, "FID": feature["id"]}, ["municipio", "plasticida", "uf"]
    )
    assert (esperado["municipio"], esperado["plasticida"]) == ("Brasília", "Plástica")
    for legitimo in ["SÃO JOSÉ DO RIO PRETO", "Ð¿", "SÃ£o — com travessão", "Rosana"]:
        assert reconciliation.texto(legitimo) == legitimo
