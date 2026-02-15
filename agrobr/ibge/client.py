"""Cliente para API SIDRA do IBGE usando sidrapy."""

from __future__ import annotations

from typing import Any

import pandas as pd
import sidrapy
import structlog

from agrobr import constants
from agrobr.http.rate_limiter import RateLimiter

logger = structlog.get_logger()


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

NIVEIS_TERRITORIAIS = {
    "brasil": "1",
    "regiao": "2",
    "uf": "3",
    "mesorregiao": "7",
    "microrregiao": "8",
    "municipio": "6",
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

CATEGORIAS_ABATE = {
    "ref_trimestre": {"12716": "115236"},
    "tipo_rebanho_bovino": {"18": "992"},
    "tipo_inspecao": {"12529": "118225"},
}

ESPECIES_ABATE = ["bovino", "suino", "frango"]

TABELAS_CENSO_AGRO = {
    "efetivo_rebanho": "6907",
    "uso_terra": "6881",
    "lavoura_temporaria": "6957",
    "lavoura_permanente": "6956",
}

VARIAVEIS_CENSO_AGRO: dict[str, dict[str, str]] = {
    "efetivo_rebanho": {"estabelecimentos": "10010", "cabecas": "2209"},
    "uso_terra": {"estabelecimentos": "9587", "area": "184"},
    "lavoura_temporaria": {
        "estabelecimentos": "10084",
        "producao": "10085",
        "area_colhida": "10089",
    },
    "lavoura_permanente": {
        "estabelecimentos": "9504",
        "producao": "9506",
        "area_colhida": "10078",
    },
}

TEMAS_CENSO_AGRO = list(TABELAS_CENSO_AGRO.keys())

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
    """
    Busca dados do SIDRA/IBGE usando sidrapy.

    Args:
        table_code: Código da tabela SIDRA
        territorial_level: Nível territorial (1=Brasil, 3=UF, 6=Município)
        ibge_territorial_code: Código territorial IBGE ou "all"
        variable: Código(s) da variável
        period: Período (ex: "2023", "last 5", "2019-2023")
        classifications: Classificações/filtros adicionais
        header: "n" para header numérico, "y" para descritivo

    Returns:
        DataFrame com dados do SIDRA
    """
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

        from agrobr.http.retry import retry_async

        async def _do_fetch() -> pd.DataFrame:
            df = sidrapy.get_table(**kwargs)
            if header == "n" and len(df) > 1:
                df = df.iloc[1:].reset_index(drop=True)
            return pd.DataFrame(df)

        df = await retry_async(
            _do_fetch,
            retriable_exceptions=(Exception,),
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
    """
    Processa resposta do SIDRA para formato mais legível.

    Args:
        df: DataFrame retornado pelo sidrapy
        rename_columns: Mapeamento de renomeação de colunas

    Returns:
        DataFrame processado
    """
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


def get_uf_codes() -> dict[str, str]:
    """Retorna mapeamento de sigla UF para código IBGE."""
    return {
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


def uf_to_ibge_code(uf: str) -> str:
    """Converte sigla UF para código IBGE."""
    codes = get_uf_codes()
    return codes.get(uf.upper(), uf)
