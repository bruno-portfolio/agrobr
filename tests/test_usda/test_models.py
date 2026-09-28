from __future__ import annotations

import hashlib
import json

from agrobr.exceptions import InvalidParameterError
from agrobr.usda import models
from tests.helpers import levanta_exatamente, sem_excecao

from .conftest import GOLDEN, corpo

PRODUTOS_OFICIAIS = {
    "soja": ("2222000", "Oilseed, Soybean"),
    "milho": ("0440000", "Corn"),
    "trigo": ("0410000", "Wheat"),
    "arroz": ("0422110", "Rice, Milled"),
    "algodao": ("2631000", "Cotton"),
    "acucar": ("0612000", "Sugar, Centrifugal"),
    "farelo_soja": ("0813100", "Meal, Soybean"),
    "oleo_soja": ("4232000", "Oil, Soybean"),
    "cafe": ("0711100", "Coffee, Green"),
}

ATRIBUTOS_OFICIAIS = {
    4: ("area_colhida", "Area Harvested"),
    20: ("estoque_inicial", "Beginning Stocks"),
    28: ("producao", "Production"),
    57: ("importacao", "Imports"),
    86: ("oferta_total", "Total Supply"),
    88: ("exportacao", "Exports"),
    176: ("estoque_final", "Ending Stocks"),
    178: ("distribuicao_total", "Total Distribution"),
    184: ("produtividade", "Yield"),
}

PAISES_OFICIAIS = {
    "brasil": ("BR", "Brazil"),
    "eua": ("US", "United States"),
    "china": ("CH", "China"),
    "argentina": ("AR", "Argentina"),
    "india": ("IN", "India"),
    "indonesia": ("ID", "Indonesia"),
    "mexico": ("MX", "Mexico"),
    "ue": ("E4", "European Union"),
}


def test_catalogos_do_pacote_sao_os_bytes_oficiais():
    dados = json.loads((GOLDEN / "manifest.json").read_bytes())
    catalogos = {entrada["arquivo"]: entrada for entrada in dados["catalogos_do_pacote"]}
    assert sorted(p.name for p in models.CATALOGOS.iterdir()) == [
        "commodities.json",
        "commodityAttributes.json",
        "countries.json",
        "unitsOfMeasure.json",
    ]
    for arquivo in models.CATALOGOS.iterdir():
        entrada = catalogos[f"agrobr/usda/catalogos/{arquivo.name}"]
        assert hashlib.sha256(arquivo.read_bytes()).hexdigest() == entrada["sha256"]
        assert entrada["url"] == f"https://api.fas.usda.gov/api/psd/{arquivo.stem}"


def test_cadastro_de_produtos_bate_com_o_catalogo():
    oficiais = models.nomes_de_produto()
    for nome, (codigo, oficial) in PRODUTOS_OFICIAIS.items():
        assert models.resolve_commodity_code(nome) == codigo
        assert oficiais[codigo] == oficial
        assert models.commodity_name(codigo) == nome
    assert oficiais["4233000"] == "Oil, Cottonseed"
    assert [models.commodity_name(c) for c in ("0430000", "9999999")] == ["Barley", "9999999"]
    assert set(models.PSD_COMMODITIES.values()) == {c for c, _ in PRODUTOS_OFICIAIS.values()}


def test_cadastro_de_atributos_bate_com_o_catalogo():
    oficiais = models.nomes_de_atributo()
    for atributo, (rotulo, nome) in ATRIBUTOS_OFICIAIS.items():
        assert oficiais[atributo] == nome
        assert models.attribute_br("2222000", atributo) == rotulo
    assert set(models.PSD_ATTRIBUTES) == set(ATRIBUTOS_OFICIAIS)
    assert oficiais[125] == "Domestic Consumption"
    assert oficiais[models.CONSUMO_DOMESTICO["0612000"]] == "Total Disappearance"
    assert oficiais[models.CONSUMO_DOMESTICO["2631000"]] == "Domestic Use"
    assert oficiais[models.PERDAS["2631000"]] == "Loss"


def test_paises_do_cadastro_batem_com_o_catalogo():
    oficiais = models.nomes_de_pais()
    for nome, (codigo, oficial) in PAISES_OFICIAIS.items():
        with sem_excecao():
            resolvido = models.resolve_country_code(nome)
        assert resolvido == codigo
        assert oficiais[codigo] == oficial
    assert (oficiais["E2"], oficiais["E3"]) == ("EU-15", "EU-25")
    assert models.MUNDO not in oficiais
    regioes = {r["regionCode"]: r["regionName"] for r in json.loads(corpo("cat_regioes.json"))}
    assert regioes["R00"] == models.NOME_MUNDO


def test_resolve_commodity_code():
    with sem_excecao():
        resolvidos = [
            models.resolve_commodity_code(n) for n in (" Soja ", "soybean_meal", "0430000")
        ]
    assert resolvidos == ["2222000", "0813100", "0430000"]
    for invalido in ("9999999", "feijao", "4233"):
        with levanta_exatamente(InvalidParameterError, "Commodity desconhecida"):
            models.resolve_commodity_code(invalido)
    with levanta_exatamente(InvalidParameterError, "string"):
        models.resolve_commodity_code(2222000)  # type: ignore[arg-type]


def test_resolve_country_code():
    with sem_excecao():
        resolvidos = [models.resolve_country_code(n) for n in ("ar", " E4 ")]
    assert resolvidos == ["AR", "E4"]
    for invalido in ("AB", "00", "brasill"):
        with levanta_exatamente(InvalidParameterError, "País desconhecido"):
            models.resolve_country_code(invalido)
    with levanta_exatamente(InvalidParameterError, "string"):
        models.resolve_country_code(None)  # type: ignore[arg-type]


def test_resolve_attributes():
    assert models.resolve_attributes(None) is None
    assert models.resolve_attributes([" Production", "CONSUMO_DOMESTICO", "perdas", "Crush"]) == [
        "production",
        "consumo_domestico",
        "perdas",
        "crush",
    ]
    with levanta_exatamente(InvalidParameterError, r"\['produção'\]"):
        models.resolve_attributes(["producao", "produção"])
    for invalido in ("Production", ["Production", 28]):
        with levanta_exatamente(InvalidParameterError, "lista de strings"):
            models.resolve_attributes(invalido)  # type: ignore[arg-type]
