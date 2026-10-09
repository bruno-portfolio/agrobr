from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from scripts import reconciliar_embrapa_solos as reconciliation

FID256 = Path(__file__).parent / "golden_data/embrapa_solos/perfis_texto_20260926/fid256.json"


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


def test_esperado_tipa_calendario_publicado_e_preserva_laboratorio():
    pagina = (
        Path(__file__).parent
        / "golden_data/embrapa_solos/oficial_20260923/perfis_prefixo/pagina_0.json"
    )
    features = json.loads(pagina.read_bytes())["features"]
    for fid, ano, data in ((1, 2006, datetime(2006, 2, 8)), (2, None, datetime(2005, 7, 7))):
        feature = next(f for f in features if f["properties"]["fid"] == fid)
        bruto = feature["properties"]
        linha = {nome: "" if valor is None else str(valor) for nome, valor in bruto.items()}
        esperado = reconciliation.esperado(
            "perfis",
            {**linha, "FID": feature["id"]},
            ["ano", "data_colet", "ph_h2o", "fosforo_as", "uf"],
        )
        assert (esperado["ano"], esperado["data_colet"]) == (ano, data)
        assert esperado["ph_h2o"] == bruto["ph_h2o"] == "4.400000095367432"
        assert esperado["fosforo"] == bruto["fosforo_as"]
