from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pandas as pd
import sidrapy
import structlog

from agrobr import constants
from agrobr.exceptions import SourceUnavailableError
from agrobr.http.rate_limiter import RateLimiter

logger = structlog.get_logger()

SIDRA_FETCH_TIMEOUT = 120.0

TABELAS = {
    "pam_temporarias": "1612",
    "pam_permanentes": "1613",
    "pam_nova": "5457",
    "lspa": "6588",
    "lspa_safra": "1618",
    "ppm_rebanho": "3939",
    "ppm_producao": "74",
}

VARIAVEIS = {
    "area_plantada": "8331",
    "area_colhida": "216",
    "producao": "214",
    "rendimento": "112",
    "valor_producao": "215",
    "area_plantada_1612": "109",
    "area_colhida_1612": "1000109",
    "producao_1612": "214",
    "rendimento_1612": "112",
    "valor_1612": "215",
    "area_lspa": "109",
    "producao_lspa": "216",
    "rendimento_lspa": "112",
}

PRODUTOS_PAM = {
    "soja": "40124",
    "milho": "40122",
    "arroz": "40102",
    "feijao": "40112",
    "trigo": "40127",
    "algodao": "40099",
    "cafe": "40139",
    "cana": "40106",
    "mandioca": "40119",
    "laranja": "40151",
    "cacau": "40138",
}

VARIAVEIS_PPM = {
    "efetivo": "105",
    "producao": "106",
    "valor_producao": "215",
    "vacas_ordenhadas": "107",
}

REBANHOS_PPM = {
    "bovino": "2670",
    "bubalino": "2675",
    "equino": "2672",
    "suino_total": "32794",
    "suino_matrizes": "32795",
    "caprino": "2681",
    "ovino": "2677",
    "galinaceos_total": "32796",
    "galinhas_poedeiras": "32793",
    "codornas": "2680",
}

PRODUTOS_ORIGEM_ANIMAL = {
    "leite": "2682",
    "ovos_galinha": "2685",
    "ovos_codorna": "2686",
    "mel": "2687",
    "casulos": "2683",
    "la": "2684",
}

UNIDADES_PPM: dict[str, str] = {
    "bovino": "cabeças",
    "bubalino": "cabeças",
    "equino": "cabeças",
    "suino_total": "cabeças",
    "suino_matrizes": "cabeças",
    "caprino": "cabeças",
    "ovino": "cabeças",
    "galinaceos_total": "cabeças",
    "galinhas_poedeiras": "cabeças",
    "codornas": "cabeças",
    "leite": "mil litros",
    "ovos_galinha": "mil dúzias",
    "ovos_codorna": "mil dúzias",
    "mel": "kg",
    "casulos": "kg",
    "la": "kg",
}

TABELAS_ABATE = {
    "bovino": "1092",
    "suino": "1093",
    "frango": "1094",
}

VARIAVEIS_ABATE = {
    "animais_abatidos": "284",
    "peso_carcacas": "285",
}

ESPECIES_ABATE = ["bovino", "suino", "frango"]

TABELAS_CENSO_AGRO: dict[str, dict[str, str]] = {
    "efetivo_rebanho": {"1995": "323", "2017": "6907"},
    "uso_terra": {"1995": "316", "2017": "6881"},
    "lavoura_temporaria": {"1995": "497", "2017": "6957"},
    "lavoura_permanente": {"1995": "509", "2017": "6956"},
    "preparo_solo": {"2006": "791", "2017": "6855"},
    "adubacao": {"2006": "1249", "2017": "6848"},
    "calagem": {"2006": "1245", "2017": "6849"},
    "agrotoxicos": {"2006": "1459", "2017": "6851"},
    "praticas_agricolas": {"2006": "837", "2017": "8561"},
    "irrigacao": {"2006": "855", "2017": "6857"},
    "despesa_adubos": {"2017": "6899"},
}

VARIAVEIS_CENSO_AGRO: dict[str, dict[str, dict[str, str]]] = {
    "efetivo_rebanho": {
        "1995": {"cabecas": "105"},
        "2017": {"estabelecimentos": "10010", "cabecas": "2209"},
    },
    "uso_terra": {
        "1995": {"area": "184", "estabelecimentos": "183"},
        "2017": {"estabelecimentos": "9587", "area": "184"},
    },
    "lavoura_temporaria": {
        "1995": {"producao": "214", "estabelecimentos": "151", "area_colhida": "216"},
        "2017": {"estabelecimentos": "10084", "producao": "10085", "area_colhida": "10089"},
    },
    "lavoura_permanente": {
        "1995": {"producao": "214", "estabelecimentos": "151", "area_colhida": "216"},
        "2017": {"estabelecimentos": "9504", "producao": "9506", "area_colhida": "10078"},
    },
    "preparo_solo": {
        "2006": {"estabelecimentos": "183"},
        "2017": {
            "nao_utiliza": "9562",
            "utiliza": "9563",
            "convencional": "9564",
            "cultivo_minimo": "9565",
            "plantio_direto": "2016",
            "area_plantio_direto": "2018",
        },
    },
    "adubacao": {
        "2006": {"estabelecimentos": "183"},
        "2017": {"estabelecimentos": "183", "area": "184"},
    },
    "calagem": {
        "2006": {"estabelecimentos": "183"},
        "2017": {"estabelecimentos": "183", "area": "184"},
    },
    "agrotoxicos": {
        "2006": {"estabelecimentos": "183"},
        "2017": {"estabelecimentos": "183", "area": "184"},
    },
    "praticas_agricolas": {
        "2006": {"estabelecimentos": "183"},
        "2017": {"estabelecimentos": "183", "area": "184"},
    },
    "irrigacao": {
        "2006": {"estabelecimentos": "183"},
        "2017": {"estabelecimentos": "183", "area": "184"},
    },
    "despesa_adubos": {
        "2017": {"estabelecimentos": "2", "valor_mil_reais": "1996"},
    },
}

TEMAS_CENSO_AGRO: list[str] = list(TABELAS_CENSO_AGRO.keys())

TABELAS_CENSO_HISTORICO: dict[str, str] = {
    "estabelecimentos_area": "263",
    "uso_terra": "264",
    "pessoal_tratores": "265",
    "condicao_produtor": "280",
    "efetivo_animais": "281",
    "producao_animal": "282",
    "producao_vegetal": "283",
    "lavoura_permanente": "1730",
    "lavoura_temporaria": "1731",
}

PERIODOS_CENSO_HISTORICO: dict[str, list[int]] = {
    "estabelecimentos_area": [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006],
    "uso_terra": [1970, 1975, 1980, 1985, 1995, 2006],
    "pessoal_tratores": [1970, 1975, 1980, 1985, 1995, 2006],
    "condicao_produtor": [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006],
    "efetivo_animais": [1970, 1975, 1980, 1985, 1995, 2006],
    "producao_animal": [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006],
    "producao_vegetal": [1920, 1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006],
    "lavoura_permanente": [1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006],
    "lavoura_temporaria": [1940, 1950, 1960, 1970, 1975, 1980, 1985, 1995, 2006],
}

VARIAVEIS_CENSO_HISTORICO: dict[str, dict[str, str]] = {
    "estabelecimentos_area": {
        "estabelecimentos": "183",
        "estabelecimentos_pct": "1000183",
        "area": "184",
        "area_pct": "1000184",
    },
    "uso_terra": {
        "area": "184",
        "area_pct": "1000184",
    },
    "pessoal_tratores": {
        "pessoal_ocupado": "185",
        "tratores": "1862",
    },
    "condicao_produtor": {
        "estabelecimentos": "183",
        "estabelecimentos_pct": "1000183",
        "area": "184",
        "area_pct": "1000184",
    },
    "efetivo_animais": {
        "efetivo": "1863",
    },
    "producao_animal": {
        "producao": "1864",
    },
    "producao_vegetal": {
        "producao": "1865",
        "area_colhida": "216",
    },
    "lavoura_permanente": {
        "quantidade_produzida": "214",
    },
    "lavoura_temporaria": {
        "quantidade_produzida": "214",
    },
}

CLASSIFICACOES_CENSO_HISTORICO: dict[str, dict[str, str]] = {
    "estabelecimentos_area": {"220": "all"},
    "uso_terra": {"222": "all"},
    "pessoal_tratores": {},
    "condicao_produtor": {"12441": "all"},
    "efetivo_animais": {"12443": "all"},
    "producao_animal": {"12444": "all"},
    "producao_vegetal": {"12445": "all"},
    "lavoura_permanente": {"227": "all"},
    "lavoura_temporaria": {"226": "all"},
}

NIVEIS_CENSO_HISTORICO: dict[str, list[str]] = {
    "estabelecimentos_area": ["brasil", "regiao", "uf"],
    "uso_terra": ["brasil", "regiao", "uf"],
    "pessoal_tratores": ["brasil", "regiao", "uf"],
    "condicao_produtor": ["brasil", "regiao", "uf"],
    "efetivo_animais": ["brasil", "regiao", "uf"],
    "producao_animal": ["brasil", "regiao", "uf"],
    "producao_vegetal": ["brasil", "regiao", "uf"],
    "lavoura_permanente": ["brasil", "regiao", "uf"],
    "lavoura_temporaria": ["brasil", "regiao", "uf"],
}

CATEGORIAS_CENSO_HISTORICO: dict[str, dict[str, str]] = {
    "estabelecimentos_area": {
        "110085": "total",
        "110049": "menos_100ha",
        "110045": "menos_10ha",
        "110046": "10_a_100ha",
        "110047": "100_a_1000ha",
        "110048": "1000ha_mais",
    },
    "uso_terra": {
        "110087": "total",
        "4812": "lavouras_permanentes",
        "4813": "lavouras_temporarias",
        "4815": "pastagens_naturais",
        "4816": "pastagens_plantadas",
        "110050": "matas_naturais",
        "110051": "matas_plantadas",
    },
    "condicao_produtor": {
        "110086": "total",
        "110052": "proprietario",
        "110053": "arrendatario_parceiro",
        "110054": "administrador",
        "110055": "ocupante",
    },
    "efetivo_animais": {
        "110056": "bovinos",
        "110057": "bubalinos",
        "110058": "equinos",
        "110059": "asininos",
        "110060": "muares",
        "110061": "caprinos",
        "110062": "ovinos",
        "110063": "suinos",
        "110064": "aves",
    },
    "producao_animal": {
        "110067": "leite_vaca",
        "110070": "leite_cabra",
        "110069": "la",
        "110068": "ovos_galinha",
    },
    "producao_vegetal": {
        "110072": "cafe",
        "110071": "cacau",
        "110073": "laranja",
        "110074": "uva",
        "110075": "algodao",
        "110076": "arroz",
        "110077": "cana",
        "110078": "feijao",
        "110079": "fumo",
        "110081": "mandioca",
        "110080": "milho",
        "110082": "soja",
        "110083": "trigo",
    },
    "lavoura_permanente": {
        "4921": "abacate",
        "4925": "algodao_arboreo",
        "4930": "banana",
        "4933": "cacau",
        "4934": "cafe",
        "4945": "coco",
        "4952": "goiaba",
        "4961": "laranja",
        "4963": "limao",
        "4968": "manga",
        "4967": "mamao",
        "4979": "pimenta_do_reino",
        "4982": "tangerina",
        "96607": "uva",
    },
    "lavoura_temporaria": {
        "4844": "abacaxi",
        "4845": "abobora",
        "4846": "algodao_herbaceo",
        "96608": "amendoim",
        "4851": "arroz",
        "4853": "batata_doce",
        "96609": "batata_inglesa",
        "4857": "cana",
        "4860": "cebola",
        "4865": "fava",
        "96610": "feijao",
        "4870": "fumo",
        "4884": "mamona",
        "4885": "mandioca",
        "4886": "melancia",
        "4887": "melao",
        "4888": "milho",
        "4896": "soja",
        "4899": "tomate",
        "4901": "trigo",
    },
}

UNIDADES_VARIAVEIS_CENSO_HISTORICO: dict[str, str] = {
    "183": "Unidades",
    "1000183": "%",
    "184": "Hectares",
    "1000184": "%",
    "185": "Pessoas",
    "1862": "Unidades",
    "216": "Hectares",
}

UNIDADES_CATEGORIAS_CENSO_HISTORICO: dict[str, str] = {
    "110056": "Cabeças",
    "110057": "Cabeças",
    "110058": "Cabeças",
    "110059": "Cabeças",
    "110060": "Cabeças",
    "110061": "Cabeças",
    "110062": "Cabeças",
    "110063": "Cabeças",
    "110064": "Mil cabeças",
    "110067": "Mil litros",
    "110070": "Mil litros",
    "110069": "Toneladas",
    "110068": "Mil dúzias",
    "110072": "Toneladas",
    "110071": "Toneladas",
    "110073": "Mil frutos",
    "110074": "Toneladas",
    "110075": "Toneladas",
    "110076": "Toneladas",
    "110077": "Toneladas",
    "110078": "Toneladas",
    "110079": "Toneladas",
    "110081": "Toneladas",
    "110080": "Toneladas",
    "110082": "Toneladas",
    "110083": "Toneladas",
    "4921": "Mil frutos",
    "4925": "Toneladas",
    "4930": "Mil cachos",
    "4933": "Toneladas",
    "4934": "Toneladas",
    "4945": "Mil frutos",
    "4952": "Mil frutos",
    "4961": "Mil frutos",
    "4963": "Mil frutos",
    "4968": "Mil frutos",
    "4967": "Mil frutos",
    "4979": "Toneladas",
    "4982": "Mil frutos",
    "96607": "Toneladas",
    "4844": "Mil frutos",
    "4845": "Mil frutos",
    "4846": "Toneladas",
    "96608": "Toneladas",
    "4851": "Toneladas",
    "4853": "Toneladas",
    "96609": "Toneladas",
    "4857": "Toneladas",
    "4860": "Toneladas",
    "4865": "Toneladas",
    "96610": "Toneladas",
    "4870": "Toneladas",
    "4884": "Toneladas",
    "4885": "Toneladas",
    "4886": "Mil frutos",
    "4887": "Mil frutos",
    "4888": "Toneladas",
    "4896": "Toneladas",
    "4899": "Toneladas",
    "4901": "Toneladas",
}

TEMAS_CENSO_HISTORICO: list[str] = list(TABELAS_CENSO_HISTORICO.keys())

TABELAS_PEVS = {
    "silvicultura_producao": "291",
    "silvicultura_area": "5930",
    "extracao_vegetal": "289",
}

VARIAVEIS_SILVICULTURA = {
    "quantidade_produzida": "142",
    "valor_producao": "143",
}

VARIAVEIS_SILVICULTURA_AREA = {
    "area_total": "6549",
}

PRODUTOS_SILVICULTURA: dict[str, str] = {
    "carvao": "3455",
    "carvao_eucalipto": "33247",
    "carvao_pinus": "33248",
    "carvao_outras": "33249",
    "lenha": "3456",
    "lenha_eucalipto": "33250",
    "lenha_pinus": "33251",
    "lenha_outras": "33252",
    "madeira_tora": "3457",
    "madeira_celulose": "3458",
    "madeira_outras_finalidades": "3459",
    "acacia_negra": "3461",
    "eucalipto_folha": "3462",
    "resina": "3463",
}

ESPECIES_SILVICULTURA_AREA: dict[str, str] = {
    "eucalipto": "39326",
    "pinus": "39327",
    "outras": "39328",
}

UNIDADES_SILVICULTURA: dict[str, str] = {
    "carvao": "Toneladas",
    "carvao_eucalipto": "Toneladas",
    "carvao_pinus": "Toneladas",
    "carvao_outras": "Toneladas",
    "lenha": "Metros cúbicos",
    "lenha_eucalipto": "Metros cúbicos",
    "lenha_pinus": "Metros cúbicos",
    "lenha_outras": "Metros cúbicos",
    "madeira_tora": "Metros cúbicos",
    "madeira_celulose": "Metros cúbicos",
    "madeira_outras_finalidades": "Metros cúbicos",
    "acacia_negra": "Toneladas",
    "eucalipto_folha": "Toneladas",
    "resina": "Toneladas",
}

VARIAVEIS_EXTRACAO_VEGETAL = {
    "quantidade_produzida": "144",
    "valor_producao": "145",
}

PRODUTOS_EXTRACAO_VEGETAL: dict[str, str] = {
    "acai": "3403",
    "castanha_caju": "3404",
    "castanha_para": "3405",
    "erva_mate": "3406",
    "mangaba": "3407",
    "palmito": "3408",
    "pequi_fruto": "39409",
    "pinhao": "3409",
    "umbu": "3410",
    "hevea_coagulado": "3418",
    "hevea_liquido": "3419",
    "carnauba_cera": "3421",
    "carnauba_po": "3422",
    "piacava": "3426",
    "carvao": "3433",
    "lenha": "3434",
    "madeira_tora": "3435",
    "babacu": "3439",
    "copaiba": "3440",
    "cumaru": "3441",
    "pequi_amendoa": "3444",
}

UNIDADES_EXTRACAO_VEGETAL: dict[str, str] = {
    "acai": "Toneladas",
    "castanha_caju": "Toneladas",
    "castanha_para": "Toneladas",
    "erva_mate": "Toneladas",
    "mangaba": "Toneladas",
    "palmito": "Toneladas",
    "pequi_fruto": "Toneladas",
    "pinhao": "Toneladas",
    "umbu": "Toneladas",
    "hevea_coagulado": "Toneladas",
    "hevea_liquido": "Toneladas",
    "carnauba_cera": "Toneladas",
    "carnauba_po": "Toneladas",
    "piacava": "Toneladas",
    "carvao": "Toneladas",
    "lenha": "Metros cúbicos",
    "madeira_tora": "Metros cúbicos",
    "babacu": "Toneladas",
    "copaiba": "Toneladas",
    "cumaru": "Toneladas",
    "pequi_amendoa": "Toneladas",
}

TABELAS_LEITE = {"leite_trimestral": "1086"}

VARIAVEIS_LEITE = {
    "leite_adquirido": "282",
    "leite_industrializado": "283",
    "preco_medio": "2522",
}

TABELAS_PIB = {
    "pib_corrente": "1846",
    "pib_real": "6612",
}

VARIAVEIS_PIB = {
    "corrente": "585",
    "real_1995": "9318",
}

SETORES_PIB: dict[str, str] = {
    "agropecuaria": "90687",
    "industria": "90691",
    "servicos": "90696",
    "pib_total": "90707",
}

PRODUTOS_LSPA = {
    "soja": "39443",
    "milho_1": "39441",
    "milho_2": "39442",
    "arroz": "39432",
    "feijao_1": "39436",
    "feijao_2": "39437",
    "feijao_3": "39438",
    "trigo": "39447",
    "algodao": "39433",
    "cafe": "109194",
    "amendoim_1": "109180",
    "amendoim_2": "109181",
    "aveia": "109179",
    "batata_1": "39434",
    "batata_2": "39435",
    "cevada": "109182",
    "mamona": "109183",
    "sorgo": "109184",
    "triticale": "109185",
}


async def fetch_sidra(
    table_code: str,
    territorial_level: str = "1",
    ibge_territorial_code: str = "all",
    variable: str | list[str] | None = None,
    period: str | list[str] | None = None,
    classifications: dict[str, str | list[str]] | None = None,
    header: str = "n",
) -> pd.DataFrame:
    logger.info(
        "ibge_fetch_start",
        table=table_code,
        level=territorial_level,
        period=period,
    )

    async with RateLimiter.acquire(constants.Fonte.IBGE):
        kwargs: dict[str, Any] = {
            "table_code": table_code,
            "territorial_level": territorial_level,
            "ibge_territorial_code": ibge_territorial_code,
            "header": header,
        }

        if variable:
            if isinstance(variable, list):
                kwargs["variable"] = ",".join(variable)
            else:
                kwargs["variable"] = variable

        if period:
            if isinstance(period, list):
                kwargs["period"] = ",".join(period)
            else:
                kwargs["period"] = period

        if classifications:
            kwargs["classifications"] = classifications

        import requests

        from agrobr.http.retry import retry_async

        sidra_url = f"{constants.URLS[constants.Fonte.IBGE]['api']}/values/t/{table_code}"

        async def _do_fetch() -> pd.DataFrame:
            try:
                df = await asyncio.wait_for(
                    asyncio.to_thread(sidrapy.get_table, **kwargs),
                    timeout=SIDRA_FETCH_TIMEOUT,
                )
            except ValueError as exc:
                raise SourceUnavailableError(
                    source="ibge",
                    url=sidra_url,
                    last_error=str(exc)[:300],
                ) from exc
            return pd.DataFrame(df)

        df = await retry_async(
            _do_fetch,
            retriable_exceptions=(
                httpx.TimeoutException,
                httpx.NetworkError,
                httpx.RemoteProtocolError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                TimeoutError,
                SourceUnavailableError,
            ),
        )

        logger.info(
            "ibge_fetch_success",
            table=table_code,
            rows=len(df),
        )

        return df


def parse_sidra_response(
    df: pd.DataFrame,
    rename_columns: dict[str, str] | None = None,
) -> pd.DataFrame:
    default_rename = {
        "NC": "nivel_territorial_cod",
        "NN": "nivel_territorial",
        "MC": "localidade_cod",
        "MN": "localidade",
        "V": "valor",
        "D1C": "ano_cod",
        "D1N": "ano",
        "D2C": "variavel_cod",
        "D2N": "variavel",
        "D3C": "produto_cod",
        "D3N": "produto",
        "D4C": "classificacao_cod",
        "D4N": "classificacao",
    }

    if rename_columns:
        default_rename.update(rename_columns)

    rename_map = {k: v for k, v in default_rename.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    return df


_UF_CODES: dict[str, str] = {
    "RO": "11",
    "AC": "12",
    "AM": "13",
    "RR": "14",
    "PA": "15",
    "AP": "16",
    "TO": "17",
    "MA": "21",
    "PI": "22",
    "CE": "23",
    "RN": "24",
    "PB": "25",
    "PE": "26",
    "AL": "27",
    "SE": "28",
    "BA": "29",
    "MG": "31",
    "ES": "32",
    "RJ": "33",
    "SP": "35",
    "PR": "41",
    "SC": "42",
    "RS": "43",
    "MS": "50",
    "MT": "51",
    "GO": "52",
    "DF": "53",
}


def get_uf_codes() -> dict[str, str]:
    return _UF_CODES


def uf_to_ibge_code(uf: str) -> str:
    return _UF_CODES.get(uf.upper(), uf)
