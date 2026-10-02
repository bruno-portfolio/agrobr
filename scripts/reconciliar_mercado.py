from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx

from agrobr import constants
from agrobr.bcb import api as sicor_api
from agrobr.bcb import client as sicor_client
from agrobr.bcb import models as sicor_models
from agrobr.cftc import models as cftc_models
from agrobr.conab.ceasa import client as ceasa_client
from agrobr.conab.ceasa import models as ceasa_models
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import retry_on_status
from agrobr.http.user_agents import UserAgentRotator
from agrobr.normalize.dates import INICIO_SAFRA_MES

ROOT = Path(__file__).resolve().parents[1]
PTAX_ROUTES = ("CotacaoMoedaDia", "CotacaoMoedaPeriodo", "Moedas")
CFTC_METADATA_URL = "https://publicreporting.cftc.gov/api/views/72hh-3qpy.json"
SGS_PROBE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados/ultimos/1?formato=json"
B3_AJUSTES_PROBE = constants.URLS[constants.Fonte.B3]["ajustes_zip"]
CEASA_DATE = re.compile(r"\s*\(\d{2}/\d{2}/\d{4}\).*$")
EDM = "{http://docs.oasis-open.org/odata/ns/edm}"
_FOCUS_COMMON = {
    "Indicador": "Edm.String",
    "Data": "Edm.String",
    "DataReferencia": "Edm.String",
    "Media": "Edm.Decimal",
    "Mediana": "Edm.Decimal",
    "DesvioPadrao": "Edm.Decimal",
    "Minimo": "Edm.Decimal",
    "Maximo": "Edm.Decimal",
    "numeroRespondentes": "Edm.Int32",
    "baseCalculo": "Edm.Int32",
}
SICOR_MUNICIPIOS = "CusteioInvestimentoComercialIndustrialSemFiltros"
_SICOR_TOTAIS = {
    "cdEstado": "Edm.String",
    "nomeUF": "Edm.String",
    "MesEmissao": "Edm.String",
    "AnoEmissao": "Edm.String",
    "cdPrograma": "Edm.String",
    "cdSubPrograma": "Edm.String",
    "cdFonteRecurso": "Edm.String",
    "Atividade": "Edm.String",
    **{
        campo: "Edm.Int32" if campo.startswith("Qtd") else "Edm.Decimal"
        for par in sicor_models.SICOR_TOTAL_FINALIDADES.values()
        for campo in par
    },
}
ODATA_PROPERTIES: dict[str, dict[str, dict[str, str]]] = {
    "ptax": {
        "TipoCotacaoMoeda": {
            "paridadeCompra": "Edm.Decimal",
            "paridadeVenda": "Edm.Decimal",
            "cotacaoCompra": "Edm.Decimal",
            "cotacaoVenda": "Edm.Decimal",
            "dataHoraCotacao": "Edm.String",
            "tipoBoletim": "Edm.String",
        },
        "TipoMoeda": {
            "simbolo": "Edm.String",
            "nomeFormatado": "Edm.String",
            "tipoMoeda": "Edm.String",
        },
    },
    "sicor": {
        "CusteioRegiaoUFProduto": {
            "nomeProduto": "Edm.String",
            "nomeRegiao": "Edm.String",
            "nomeUF": "Edm.String",
            "MesEmissao": "Edm.String",
            "AnoEmissao": "Edm.String",
            "cdPrograma": "Edm.String",
            "cdSubPrograma": "Edm.String",
            "cdFonteRecurso": "Edm.String",
            "cdTipoSeguro": "Edm.String",
            "QtdCusteio": "Edm.Int32",
            "VlCusteio": "Edm.Decimal",
            "Atividade": "Edm.String",
            "cdModalidade": "Edm.String",
            "AreaCusteio": "Edm.Decimal",
        },
        "RegiaoUF": {
            "cdRegiao": "Edm.String",
            "nomeRegiao": "Edm.String",
            **_SICOR_TOTAIS,
        },
        SICOR_MUNICIPIOS: {
            "cdMunicipio": "Edm.String",
            "Municipio": "Edm.String",
            "codMunicIbge": "Edm.String",
            "AreaCusteio": "Edm.Decimal",
            "AreaInvestimento": "Edm.Decimal",
            **_SICOR_TOTAIS,
        },
    },
    "focus": {
        "ExpectativasMercadoAnuais": {**_FOCUS_COMMON, "IndicadorDetalhe": "Edm.String"},
        "ExpectativaMercadoMensais": dict(_FOCUS_COMMON),
    },
}


def compare_entity_sets(published: list[str], expected: list[str]) -> dict[str, Any]:
    missing = [name for name in expected if name not in published]
    return {
        "status": "mismatch" if missing else "ok",
        "problems": [f"entidade OData ausente no serviço: {name}" for name in missing],
        "publicadas": len(published),
    }


def odata_schema(metadata_xml: bytes) -> dict[str, dict[str, Any]]:
    root = ET.fromstring(metadata_xml)
    types: dict[str, dict[str, str]] = {}
    for kind in ("EntityType", "ComplexType"):
        for element in root.iter(EDM + kind):
            types[element.get("Name", "")] = {
                prop.get("Name", ""): prop.get("Type", "")
                for prop in element.findall(EDM + "Property")
            }
    sets = {
        element.get("Name", ""): element.get("EntityType", "").rsplit(".", 1)[-1]
        for element in root.iter(EDM + "EntitySet")
    }
    return {"types": types, "sets": sets}


def compare_odata_properties(
    schema: dict[str, dict[str, Any]], expected: dict[str, dict[str, str]]
) -> dict[str, Any]:
    problems = []
    for name, properties in expected.items():
        type_name = schema["sets"].get(name, name)
        published = schema["types"].get(type_name)
        if published is None:
            problems.append(f"tipo OData ausente no $metadata: {name}")
            continue
        for prop, edm_type in properties.items():
            if prop not in published:
                problems.append(f"{name}.{prop} ausente")
            elif published[prop] != edm_type:
                problems.append(f"{name}.{prop} mudou de {edm_type} para {published[prop]}")
        unknown = sorted(set(published) - set(properties))
        if unknown:
            problems.append(f"{name}: propriedades sem decisão: {unknown}")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "tipos": len(schema["types"]),
    }


def compare_socrata_columns(columns: list[dict[str, Any]], expected: list[str]) -> dict[str, Any]:
    published = {column.get("fieldName") for column in columns}
    missing = [name for name in expected if name not in published]
    return {
        "status": "mismatch" if missing else "ok",
        "problems": [f"campo Socrata ausente: {name}" for name in missing],
        "publicadas": len(published),
    }


def compare_sgs_fields(body: Any) -> dict[str, Any]:
    problems = []
    if not isinstance(body, list) or not body:
        problems.append("corpo SGS vazio ou fora do formato de lista")
    else:
        fields = set(body[0])
        if fields != {"data", "valor"}:
            problems.append(f"campos SGS diferentes de data/valor: {sorted(fields)}")
    return {"status": "mismatch" if problems else "ok", "problems": problems}


def ceasa_header_name(col_name: str) -> str:
    stripped = CEASA_DATE.sub("", col_name).replace("\r", " - ")
    return re.sub(r"\s+", " ", stripped).strip()


def compare_ceasa_alignment(precos: dict[str, Any], ceasas: dict[str, Any]) -> dict[str, Any]:
    headers = [column.get("colName", "") for column in precos.get("metadata", [])[1:]]
    names = [row[1] for row in ceasas.get("resultset", []) if len(row) > 1]
    problems = []
    if not headers or not names or not precos.get("resultset"):
        problems.append("documento CEASA sem colunas de preço, sem catálogo ou sem linhas")
    if len(headers) != len(names):
        problems.append(f"{len(headers)} colunas de preço para {len(names)} CEASAs")
    for index, (header, name) in enumerate(zip(headers, names, strict=False), start=1):
        if ceasa_header_name(header) != name:
            problems.append(f"coluna {index}: cabeçalho {header!r} não corresponde a {name!r}")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "colunas": len(headers),
        "ceasas": len(names),
        "linhas": len(precos.get("resultset", [])),
    }


def compare_ceasa_catalog(precos: dict[str, Any]) -> dict[str, Any]:
    publicados = {
        ceasa_models.parse_produto_unidade(row[0])[0]
        for row in precos.get("resultset", [])
        if row and isinstance(row[0], str)
    }
    novos = sorted(publicados - set(ceasa_models.PRODUTO_PARA_CATEGORIA))
    problems = [f"produtos fora da tabela de categorias do agrobr: {novos}"] if novos else []
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "produtos": len(publicados),
    }


def somar_sicor(registros: list[dict[str, Any]]) -> dict[tuple[str, str], tuple[int, Decimal]]:
    somas: dict[tuple[str, str], tuple[int, Decimal]] = {}
    for registro in registros:
        for finalidade, (campo_qtd, campo_valor) in sicor_models.SICOR_TOTAL_FINALIDADES.items():
            qtd, valor = int(registro[campo_qtd]), Decimal(str(registro[campo_valor]))
            if qtd == 0 and valor == 0:
                continue
            chave = (str(registro["nomeUF"]).strip().upper(), finalidade)
            total_qtd, total_valor = somas.get(chave, (0, Decimal(0)))
            somas[chave] = (total_qtd + qtd, total_valor + valor)
    return somas


def compare_sicor_totais(
    esperado: dict[tuple[str, str], tuple[int, Decimal]],
    obtido: dict[tuple[str, str], tuple[int, Decimal]],
    rotulos: tuple[str, str],
) -> dict[str, Any]:
    problems = [
        f"{uf} {finalidade}: {rotulos[0]} {esperado.get((uf, finalidade))} × "
        f"{rotulos[1]} {obtido.get((uf, finalidade))}"
        for uf, finalidade in sorted(set(esperado) | set(obtido))
        if esperado.get((uf, finalidade)) != obtido.get((uf, finalidade))
    ]
    if not esperado:
        problems.append(f"{rotulos[0]} sem nenhum par UF × finalidade")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "pares": len(esperado),
    }


def meses_consultados(hoje: date) -> tuple[tuple[int, int], list[tuple[int, int]]]:
    """Último mês fechado e os meses da safra dele até o mês corrente, que a fonte ainda não
    publica mas o total do agrobr para a safra também lê."""
    fechado = (hoje.year, hoje.month - 1) if hoje.month > 1 else (hoje.year - 1, 12)
    inicio = fechado[0] if fechado[1] >= INICIO_SAFRA_MES else fechado[0] - 1
    safra = [
        (inicio + (mes < INICIO_SAFRA_MES), mes)
        for mes in (*range(INICIO_SAFRA_MES, 13), *range(1, INICIO_SAFRA_MES))
    ]
    return fechado, [mes for mes in safra if mes <= (hoje.year, hoje.month)]


async def _sicor_mes(
    http: httpx.AsyncClient, base: str, entidade: str, ano: int, mes: int
) -> list[dict[str, Any]]:
    filtro = quote(f"AnoEmissao eq '{ano}' and MesEmissao eq '{mes:02d}'", safe="'")
    url = f"{base}/{entidade}?$format=json&$top={sicor_client.SICOR_RECORD_LIMIT}&$filter={filtro}"
    response = await retry_on_status(
        lambda: http.get(url), source="bcb", max_attempts=sicor_client.BCB_MAX_RETRIES
    )
    response.raise_for_status()
    registros: list[dict[str, Any]] = json.loads(response.content, parse_float=Decimal)["value"]
    if len(registros) == sicor_client.SICOR_RECORD_LIMIT:
        raise SourceUnavailableError(source="bcb", url=url, last_error="volume no teto da Olinda")
    return registros


SicorColeta = tuple[dict[tuple[int, int], list[dict[str, Any]]], list[dict[str, Any]], Any]


async def _coletar_sicor(
    http: httpx.AsyncClient,
    base: str,
    meses: list[tuple[int, int]],
    ano: int,
    mes: int,
    inicio: int,
) -> SicorColeta:
    regiao_uf = {
        periodo: await _sicor_mes(http, base, sicor_client.TOTAL_ENDPOINT, *periodo)
        for periodo in meses
    }
    municipios = await _sicor_mes(http, base, SICOR_MUNICIPIOS, ano, mes)
    frame = await sicor_api.credito_rural_total(safra=f"{inicio}/{inicio + 1}")
    return regiao_uf, municipios, frame


def _casos_sicor(
    coleta: SicorColeta, casos: tuple[str, str], meses: list[tuple[int, int]], ano: int, mes: int
) -> list[dict[str, Any]]:
    regiao_uf, municipios, frame = coleta
    inicio = meses[0][0]
    agrobr = {
        (str(uf), str(finalidade)): (int(qtd), Decimal(str(valor)))
        for uf, finalidade, qtd, valor in zip(
            frame["uf"], frame["finalidade"], frame["qtd_contratos"], frame["valor"], strict=True
        )
    }
    todos = [registro for registros in regiao_uf.values() for registro in registros]
    mensal = compare_sicor_totais(
        somar_sicor(regiao_uf[(ano, mes)]), somar_sicor(municipios), ("RegiaoUF", "SemFiltros")
    )
    if not regiao_uf[(ano, mes)] and not municipios:
        mensal["status"] = "pendente"
        mensal["problems"] = [f"Nenhum dado publicado em {ano}-{mes:02d} nas duas entidades"]
    return [
        {
            "case": casos[0],
            "safra": f"{inicio}/{inicio + 1}",
            "meses": [f"{a}-{m:02d}" for a, m in meses],
            **compare_sicor_totais(somar_sicor(todos), agrobr, ("RegiaoUF", "agrobr")),
        },
        {
            "case": casos[1],
            "mes": f"{ano}-{mes:02d}",
            **mensal,
        },
    ]


def _mesma_publicacao(primeira: SicorColeta, segunda: SicorColeta) -> bool:
    return (
        primeira[0] == segunda[0] and primeira[1] == segunda[1] and primeira[2].equals(segunda[2])
    )


async def n2_sicor(http: httpx.AsyncClient, base: str, hoje: date) -> list[dict[str, Any]]:
    """Diferença na 1ª leitura só vira ``mismatch`` se uma 2ª leitura dos dois lados, com a
    publicação igual à da 1ª, mantiver a diferença; a Olinda publica o mês aos poucos."""
    (ano, mes), meses = meses_consultados(hoje)
    inicio = meses[0][0]
    casos = ("sicor_total_agrobr_x_regiaouf", "sicor_regiaouf_x_semfiltros")
    try:
        primeira = await _coletar_sicor(http, base, meses, ano, mes, inicio)
    except (SourceUnavailableError, httpx.HTTPError) as erro:
        return [
            {"case": caso, "status": "indisponivel", "problems": [f"{type(erro).__name__}: {erro}"]}
            for caso in casos
        ]
    resultado = _casos_sicor(primeira, casos, meses, ano, mes)
    if all(caso["status"] != "mismatch" for caso in resultado):
        return resultado
    try:
        segunda = await _coletar_sicor(http, base, meses, ano, mes, inicio)
    except (SourceUnavailableError, httpx.HTTPError) as erro:
        return [
            {
                **caso,
                "status": "indisponivel",
                "problems": [
                    *caso["problems"],
                    f"2ª leitura falhou ({type(erro).__name__}: {erro}); diferença não confirmada",
                ],
            }
            if caso["status"] == "mismatch"
            else caso
            for caso in resultado
        ]
    estavel = _mesma_publicacao(primeira, segunda)
    final = []
    for antes, depois in zip(resultado, _casos_sicor(segunda, casos, meses, ano, mes), strict=True):
        if antes["status"] != "mismatch":
            final.append(antes)
        elif depois["status"] != "mismatch":
            final.append({**depois, "segunda_leitura": "a diferença da 1ª leitura sumiu"})
        elif estavel:
            final.append(
                {**depois, "segunda_leitura": "diferença confirmada com a publicação igual"}
            )
        else:
            final.append(
                {
                    **depois,
                    "status": "indisponivel",
                    "problems": [
                        *depois["problems"],
                        "a publicação mudou entre as 2 leituras; diferença não confirmada",
                    ],
                }
            )
    return final


async def _get(http: httpx.AsyncClient, url: str) -> httpx.Response:
    response = await http.get(url)
    response.raise_for_status()
    return response


async def run(output: Path) -> int:
    report: dict[str, Any] = {"fetched_at": datetime.now(UTC).isoformat(), "structure": []}
    bcb = constants.URLS[constants.Fonte.BCB]
    services = {"ptax": bcb["ptax"], "sicor": bcb["base"], "focus": bcb["focus"]}
    expected_sets = {
        "ptax": list(PTAX_ROUTES),
        "sicor": [
            *sicor_client.ENDPOINT_MAP.values(),
            sicor_client.TOTAL_ENDPOINT,
            SICOR_MUNICIPIOS,
        ],
        "focus": list(constants.BCB_FOCUS_ENTITIES.values()),
    }
    async with httpx.AsyncClient(
        timeout=120, follow_redirects=True, headers=UserAgentRotator.get_bot_headers()
    ) as http:
        for name, base in services.items():
            document = (await _get(http, f"{base}/?$format=json")).json()
            published = [entry.get("name", "") for entry in document.get("value", [])]
            report["structure"].append(
                {
                    "case": f"bcb_{name}_entidades",
                    **compare_entity_sets(published, expected_sets[name]),
                }
            )
            metadata = (await _get(http, f"{base}/$metadata")).content
            report["structure"].append(
                {
                    "case": f"bcb_{name}_propriedades",
                    **compare_odata_properties(odata_schema(metadata), ODATA_PROPERTIES[name]),
                }
            )
        report["structure"].extend(await n2_sicor(http, bcb["base"], datetime.now(UTC).date()))
        sgs = (await _get(http, SGS_PROBE_URL)).json()
        report["structure"].append({"case": "bcb_sgs_campos", **compare_sgs_fields(sgs)})
        metadata_json = (await _get(http, CFTC_METADATA_URL)).json()
        report["structure"].append(
            {
                "case": "cftc_campos_socrata",
                **compare_socrata_columns(
                    metadata_json.get("columns", []), list(cftc_models.COLUMN_MAP)
                ),
            }
        )
        head = await http.head(f"{B3_AJUSTES_PROBE}?filelist=PR{datetime.now(UTC):%y%m%d}.zip")
        available = head.status_code in (200, 404)
        report["structure"].append(
            {
                "case": "b3_ajustes_disponibilidade",
                "status": "ok" if available else "mismatch",
                "problems": [] if available else [f"HTTP {head.status_code} na consulta do ZIP"],
                "http": head.status_code,
                "nota": "disponibilidade do endpoint; 404 significa pregão ainda não publicado",
            }
        )
    (precos, _), (ceasas, _) = await asyncio.gather(
        ceasa_client.fetch_precos(), ceasa_client.fetch_ceasas()
    )
    report["structure"].append(
        {"case": "ceasa_alinhamento_colunas", **compare_ceasa_alignment(precos, ceasas)}
    )
    report["structure"].append(
        {"case": "ceasa_catalogo_categorias", **compare_ceasa_catalog(precos)}
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
    mismatches = [entry for entry in report["structure"] if entry["status"] == "mismatch"]
    unavailable = [entry for entry in report["structure"] if entry["status"] == "indisponivel"]
    pending = [entry for entry in report["structure"] if entry["status"] == "pendente"]
    for entry in report["structure"]:
        print(entry["status"], entry["case"], "; ".join(entry["problems"]))
    print(
        f"{len(report['structure']) - len(mismatches) - len(unavailable) - len(pending)} ok / "
        f"{len(mismatches)} mismatch / {len(unavailable)} indisponível / "
        f"{len(pending)} pendente -> {output}"
    )
    return 1 if mismatches else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inventário N1 live de mercado (BCB PTAX/SICOR/Focus/SGS, CFTC, CEASA, B3)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / f"reports/reconciliacao_mercado_{datetime.now(UTC):%Y%m%d}.json",
    )
    arguments = parser.parse_args()
    return asyncio.run(run(arguments.output))


if __name__ == "__main__":
    sys.exit(main())
