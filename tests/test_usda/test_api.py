from __future__ import annotations

import csv
import hashlib
import io
import json

import pytest

from agrobr import usda
from agrobr.exceptions import InvalidParameterError
from agrobr.usda import parser
from tests.helpers import conferir_corpo, levanta_exatamente, sem_excecao

from .conftest import GOLDEN, corpo, manifesto

PAISES_R24 = {"BR": "BR", "US": "US", "world": "world"}
ONDE_R23 = {"BR": "BR", "mundo": "world"}


def _oraculo_r24() -> list[dict[str, str]]:
    texto = corpo("oraculo_r24_72_valores.csv").decode("utf-8")
    return list(csv.DictReader(io.StringIO(texto)))


def _corte(linha: dict[str, str]) -> tuple[str, str, str]:
    return linha["commodity_query"], linha["country_query"], linha["marketYear"]


async def test_oraculo_r24_pela_api_publica(gateway):
    linhas = _oraculo_r24()
    assert len(linhas) == 72
    cortes = sorted({_corte(linha) for linha in linhas})
    assert len(cortes) == 24
    for produto, pais, ano in cortes:
        arquivo = f"{produto}_{pais}_{ano}.json"
        entrada = gateway.servir(arquivo)
        with sem_excecao():
            df, meta = await usda.psd(
                produto, country=PAISES_R24[pais], market_year=int(ano), return_meta=True
            )
        assert meta.source_url == entrada["url"]
        conferir_corpo(meta, corpo(arquivo))
        assert meta.parser_version == parser.PARSER_VERSION
        for linha in (linha for linha in linhas if _corte(linha) == (produto, pais, ano)):
            achada = df[df["attribute_id"] == int(linha["attributeId"])]
            assert len(achada) == 1, (arquivo, linha["attributeId"])
            assert achada.iloc[0][
                [
                    "commodity_code",
                    "country_code",
                    "country",
                    "market_year",
                    "attribute",
                    "value",
                    "unit",
                    "unit_id",
                ]
            ].tolist() == [
                linha["commodityCode"],
                linha["countryCode"],
                linha["countryName"] or "World",
                int(linha["marketYear"]),
                linha["attributeName"],
                float(linha["value"]),
                linha["unitDescription_literal"].strip(),
                int(linha["unitId"]),
            ], arquivo
            assert (achada.iloc[0]["last_update_year"], achada.iloc[0]["last_update_month"]) == (
                int(linha["calendarYear"]),
                int(linha["month"]),
            )
            assert (
                achada.iloc[0]["attribute_br"]
                == {
                    "28": "producao",
                    "88": "exportacao",
                    "176": "estoque_final",
                }[linha["attributeId"]]
            )


async def test_oraculo_r23_pela_api_publica(gateway):
    oraculo = json.loads(corpo("oraculo_r23_p2a_usda.json"))
    valores = oraculo["valores"]
    assert len(valores) == 48
    for produto, dados in oraculo["codigos_catalogo"].items():
        for onde, pais in ONDE_R23.items():
            gateway.servir(f"{produto}_{onde}_2024.json")
            with sem_excecao():
                df = await usda.psd(produto, country=pais, market_year=2024)
            assert set(df["commodity"]) == {produto}
            assert set(df["commodity_code"]) == {dados["commodityCode"]}
            for valor in (v for v in valores if (v["produto"], v["onde"]) == (produto, onde)):
                achada = df[df["attribute_id"] == valor["attributeId"]]
                assert achada[["attribute", "unit", "value"]].values.tolist() == [
                    [valor["atributo"], valor["unidade"], valor["value"]]
                ], (produto, onde, valor["attributeId"])


async def test_filtro_e_pivot_pela_api(gateway):
    gateway.servir("cafe_BR_2024.json")
    with sem_excecao():
        df = await usda.psd(
            "cafe", market_year=2024, attributes=["producao", "Exports", "consumo_domestico"]
        )
        largo = await usda.psd("cafe", market_year=2024, pivot=True)
    assert sorted(df["attribute_id"]) == [28, 88, 125]
    assert set(df["unit"]) == {"(1000 60 KG BAGS)"}
    assert {"producao", "Arabica Production"} <= set(largo.columns)
    assert largo.loc[0, "producao"] == df.loc[df["attribute_id"] == 28, "value"].item()


async def test_todos_os_paises_pela_api(gateway):
    entrada = gateway.servir("soja_all_2024.json")
    with sem_excecao():
        df, meta = await usda.psd("soja", country=" ALL ", market_year=2024, return_meta=True)
    assert meta.source_url == entrada["url"]
    assert (len(df), df["country_code"].nunique()) == (871, 67)
    assert set(df["commodity"]) == {"soja"}


async def test_ano_anterior_ao_historico_recusado_antes_da_rede(gateway):
    with levanta_exatamente(InvalidParameterError):
        await usda.psd("soja", market_year=1950, return_meta=True)
    assert gateway.pedidos == []


@pytest.mark.parametrize(
    "argumentos",
    [
        {"produto": "9999999"},
        {"produto": "soja", "country": "AB"},
        {"produto": "soja", "attributes": ["produção"]},
    ],
)
async def test_parametro_invalido_para_antes_da_rede(gateway, argumentos):
    with levanta_exatamente(InvalidParameterError):
        await usda.psd(**argumentos, market_year=2024)
    assert gateway.pedidos == []


def test_golden_sao_os_bytes_capturados_sem_cabecalho_nem_chave():
    entradas = manifesto()
    assert sorted(entradas) == sorted(p.name for p in GOLDEN.iterdir() if p.name != "manifest.json")
    for arquivo, entrada in entradas.items():
        assert hashlib.sha256(corpo(arquivo)).hexdigest() == entrada["sha256"], arquivo
        assert len(corpo(arquivo)) == entrada["bytes"], arquivo
        assert "headers" not in entrada and "api_key" not in json.dumps(entrada).lower()
        assert "?" not in entrada.get("url", "")
