"""Cliente para API SIDRA do IBGE usando sidrapy."""

from __future__ import annotations

from typing import Any

import pandas as pd
import sidrapy
import structlog

from agrobr import constants
from agrobr.http.rate_limiter import RateLimiter

logger = structlog.get_logger()


# Códigos das tabelas SIDRA
TABELAS = {
    # PAM - Produção Agrícola Municipal
    "pam_temporarias": "1612",  # Lavouras temporárias (1974-2018)
    "pam_permanentes": "1613",  # Lavouras permanentes (1974-2018)
    "pam_nova": "5457",         # Nova série PAM (2018+)

    # LSPA - Levantamento Sistemático da Produção Agrícola
    "lspa": "6588",             # Série mensal (2006+)
    "lspa_safra": "1618",       # Por ano de safra
}

# Variáveis disponíveis
VARIAVEIS = {
    # PAM 5457
    "area_plantada": "214",
    "area_colhida": "215",
    "producao": "216",
    "rendimento": "112",
    "valor_producao": "215",

    # PAM 1612 (lavouras temporárias)
    "area_plantada_1612": "109",
    "area_colhida_1612": "1000109",
    "producao_1612": "214",
    "rendimento_1612": "112",
    "valor_1612": "215",

    # LSPA 6588
    "area_lspa": "109",
    "producao_lspa": "216",
    "rendimento_lspa": "112",
}

# Níveis territoriais
NIVEIS_TERRITORIAIS = {
    "brasil": "1",
    "regiao": "2",
    "uf": "3",
    "mesorregiao": "7",
    "microrregiao": "8",
    "municipio": "6",
}

# Códigos de produtos agrícolas (classificação 782 para tabela 5457)
PRODUTOS_PAM = {
    "soja": "40124",
    "milho": "40126",
    "arroz": "40117",
    "feijao": "40120",
    "trigo": "40145",
    "algodao": "40109",
    "cafe": "40112",
    "cana": "40114",
    "mandioca": "40127",
    "laranja": "40125",
}

# Códigos para LSPA (classificação 48 para tabela 6588)
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
        # sidrapy é síncrono, então apenas chamamos diretamente
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

        try:
            df = sidrapy.get_table(**kwargs)

            # Remove primeira linha que é o header descritivo
            if header == "n" and len(df) > 1:
                df = df.iloc[1:].reset_index(drop=True)

            logger.info(
                "ibge_fetch_success",
                table=table_code,
                rows=len(df),
            )

            return df

        except Exception as e:
            logger.error(
                "ibge_fetch_error",
                table=table_code,
                error=str(e),
            )
            raise


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
    # Mapeamento padrão de colunas SIDRA
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

    # Renomeia apenas colunas que existem
    rename_map = {k: v for k, v in default_rename.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Converte valor para numérico
    if "valor" in df.columns:
        df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    return df


def get_uf_codes() -> dict[str, str]:
    """Retorna mapeamento de sigla UF para código IBGE."""
    return {
        "RO": "11", "AC": "12", "AM": "13", "RR": "14", "PA": "15",
        "AP": "16", "TO": "17", "MA": "21", "PI": "22", "CE": "23",
        "RN": "24", "PB": "25", "PE": "26", "AL": "27", "SE": "28",
        "BA": "29", "MG": "31", "ES": "32", "RJ": "33", "SP": "35",
        "PR": "41", "SC": "42", "RS": "43", "MS": "50", "MT": "51",
        "GO": "52", "DF": "53",
    }


def uf_to_ibge_code(uf: str) -> str:
    """Converte sigla UF para código IBGE."""
    codes = get_uf_codes()
    return codes.get(uf.upper(), uf)
