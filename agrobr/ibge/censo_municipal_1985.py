from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal, overload

import pandas as pd
import structlog

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.models import MetaInfo
from agrobr.utils.result import build_source_meta, finalize_result

logger = structlog.get_logger()

_DATA_DIR = Path(__file__).parent.parent / "data" / "censo_1985"
_PACOTE = _DATA_DIR / "censo_agro_municipal_1985.parquet"
_COBERTURA = _DATA_DIR / "cobertura.parquet"
_MANIFESTO = _DATA_DIR / "manifesto.json"
_CATALOGO_URL = "https://biblioteca.ibge.gov.br/index.php/biblioteca-catalogo?view=detalhes&id=768"

TABELAS_CENSO_MUNICIPAL_1985: dict[int, str] = {
    67: "propriedade_terras",
    68: "condicao_legal_terras",
    69: "classe_atividade_economica",
    70: "condicao_produtor",
    71: "residencia_produtor",
    72: "forma_administracao",
    73: "cooperativas",
    74: "servicos_empreitada",
    75: "uso_forca_trabalho",
    76: "assistencia_tecnica",
    77: "fertilizantes_defensivos",
    78: "conservacao_solo",
    79: "irrigacao",
    80: "inseminacao_ordenha",
    81: "terras_fora_area",
    82: "parcelas",
    83: "terras_proprias_terceiros",
    84: "grupos_area_total",
    85: "grupos_area_lavouras",
    86: "utilizacao_terras",
    87: "pessoal_ocupado",
    88: "grupos_pessoal_ocupado",
    89: "empregados_temporarios",
    90: "silos_forragens",
    91: "depositos_producao",
    92: "maquinas_instrumentos",
    93: "meios_transporte",
    94: "combustiveis_energia",
    95: "valor_bens_invest_financ",
    96: "despesas_receitas",
    97: "efetivo_bovinos",
    98: "efetivo_bubalinos",
    99: "efetivo_equinos",
    100: "efetivo_asininos",
    101: "efetivo_muares",
    102: "efetivo_suinos",
    103: "efetivo_ovinos",
    104: "efetivo_caprinos",
    105: "efetivo_coelhos",
    106: "efetivo_aves",
    107: "compra_venda_aves_ovos",
    108: "producao_leite",
    109: "producao_ovos",
    110: "producao_la_casulos_mel",
    111: "colheita_lav_temporaria",
    112: "lavoura_permanente",
    113: "horticultura",
    114: "produtos_extrativos",
    115: "silvicultura",
    116: "efetivo_silvicultura",
    117: "transformacao_beneficiamento",
    118: "animais_pessoal_residente",
    119: "producao_particular",
}

TITULOS_CENSO_MUNICIPAL_1985: dict[int, str] = {
    67: "Propriedade das terras",
    68: "Condição legal das terras",
    69: "Classe da atividade econômica",
    70: "Condição do produtor",
    71: "Residência do produtor",
    72: "Forma de administração",
    73: "Produtores associados a cooperativas",
    74: "Serviços de empreitada",
    75: "Uso e procedência da força utilizada nos trabalhos agrários",
    76: "Utilização de assistência técnica no estabelecimento",
    77: "Uso de fertilizantes e defensivos",
    78: "Uso de práticas de conservação do solo",
    79: "Uso de irrigação e área irrigada",
    80: "Uso de inseminação artificial e ordenha mecânica",
    81: "Estabelecimentos que utilizaram terras fora de sua área",
    82: "Número de parcelas que constituem os estabelecimentos",
    83: "Terras próprias e de terceiros",
    84: "Grupos de área total",
    85: "Grupos de área de lavouras",
    86: "Utilização das terras",
    87: (
        "Pessoal ocupado, distribuído por categoria e sexo, e pessoal ocupado residente nos "
        "estabelecimentos"
    ),
    88: "Grupos de pessoal ocupado",
    89: "Empregados temporários por meses de emprego do pessoal da categoria",
    90: "Silos para forragens",
    91: "Depósitos para produção",
    92: "Máquinas e instrumentos agrícolas",
    93: "Meios de transporte",
    94: "Consumo de combustíveis, lubrificantes e energia elétrica",
    95: "Valor dos bens, investimentos e financiamentos",
    96: "Despesas, valor da produção e receitas",
    97: "Efetivo de bovinos e número de nascidos, vitimados, comprados, vendidos e abatidos",
    98: "Efetivo de bubalinos e número de nascidos, vitimados, comprados, vendidos e abatidos",
    99: "Efetivo de eqüinos e número de nascidos, vitimados, comprados e vendidos",
    100: "Efetivo de asininos e número de nascidos, vitimados, comprados e vendidos",
    101: "Efetivo de muares e número de nascidos, vitimados, comprados e vendidos",
    102: "Efetivo de suínos e número de nascidos, vitimados, comprados, vendidos e abatidos",
    103: "Efetivo de ovinos e número de nascidos, vitimados, comprados, vendidos e abatidos",
    104: "Efetivo de caprinos e número de nascidos, vitimados, comprados, vendidos e abatidos",
    105: "Efetivo de coelhos e número de nascidos, vitimados, comprados, vendidos e abatidos",
    106: (
        "Efetivos e número de vitimados e abatidos de galinhas, galos, frangas e frangos, e "
        "efetivos de patos, gansos, marrecos, perus e codornas"
    ),
    107: (
        "Compra de ovos para incubação, pintos de 1 dia, galinhas, galos, frangas e frangos, e "
        "venda de pintos de 1 dia, galinhas, galos, frangas e frangos"
    ),
    108: "Produção de leite de vaca, búfala e cabra",
    109: "Produção de ovos de galinha, de codorna e de outras aves",
    110: "Produção de lã, casulos do bicho-da-seda e mel e cera de abelha",
    111: "Colheita e área dos produtos da lavoura temporária",
    112: "Produção, área e efetivo das plantações dos produtos da lavoura permanente",
    113: "Produção de produtos da horticultura",
    114: "Produção de produtos extrativos",
    115: "Produção de produtos da silvicultura",
    116: "Efetivo da silvicultura",
    117: "Transformação ou beneficiamento de produtos agropecuários",
    118: "Animais pertencentes ao pessoal residente nos estabelecimentos",
    119: "Produção particular do pessoal residente nos estabelecimentos",
}

TEMAS_CENSO_MUNICIPAL_1985: dict[str, int] = {v: k for k, v in TABELAS_CENSO_MUNICIPAL_1985.items()}

TEMAS_DISPONIVEIS: list[str] = sorted(TEMAS_CENSO_MUNICIPAL_1985)

NIVEIS: tuple[str, ...] = ("uf", "mesorregiao", "microrregiao", "municipio")

STATUS_CONFIRMADOS: tuple[str, ...] = (
    "confirmado_soma_exata",
    "confirmado_soma_arredondada_2a_compativel",
)

COLUNAS: list[str] = [
    "ano",
    "uf",
    "volume",
    "tabela",
    "tema",
    "pagina_pdf",
    "pagina_impressa",
    "linha",
    "coluna",
    "nivel",
    "localidade",
    "coluna_nome",
    "coluna_nome_lido",
    "coluna_nome_status",
    "variavel",
    "unidade",
    "unidade_lida",
    "valor",
    "valor_lido",
    "marcador",
    "status",
    "reparado",
]

_INTEIROS_NULOS = ("pagina_impressa", "valor", "valor_lido")
_INTEIROS = ("ano", "tabela", "pagina_pdf", "linha", "coluna")
_TEXTOS = tuple(c for c in COLUNAS if c not in (*_INTEIROS_NULOS, *_INTEIROS, "reparado"))


def _consultar(sql: str, parametros: dict[str, Any]) -> pd.DataFrame:
    import duckdb

    conexao = duckdb.connect()
    try:
        conexao.execute("SET threads = 2; SET memory_limit = '512MB'")
        return conexao.execute(sql, parametros).df()
    finally:
        conexao.close()


@lru_cache(maxsize=1)
def _manifesto() -> dict[str, Any]:
    dados: dict[str, Any] = json.loads(_MANIFESTO.read_text(encoding="utf-8"))
    return dados


@lru_cache(maxsize=1)
def _sha_pacote() -> str:
    resumo = hashlib.sha256()
    with _PACOTE.open("rb") as arquivo:
        while bloco := arquivo.read(1 << 20):
            resumo.update(bloco)
    return resumo.hexdigest()


def _tipar(df: pd.DataFrame) -> pd.DataFrame:
    for coluna in _INTEIROS_NULOS:
        df[coluna] = df[coluna].astype("Int64")
    for coluna in _INTEIROS:
        df[coluna] = df[coluna].astype("int64")
    for coluna in _TEXTOS:
        df[coluna] = df[coluna].astype("string")
    df["reparado"] = df["reparado"].astype(bool)
    return df


def _validar(tema: str, uf: str | None, nivel: str | None) -> tuple[int, str | None, str | None]:
    from agrobr.ibge.client import get_uf_codes

    tema_normalizado = tema.lower().strip() if isinstance(tema, str) else ""
    if tema_normalizado not in TEMAS_CENSO_MUNICIPAL_1985:
        raise InvalidParameterError(
            f"Tema '{tema}' inválido. Temas disponíveis: {TEMAS_DISPONIVEIS}"
        )
    uf_normalizada = uf.upper().strip() if uf is not None else None
    if uf_normalizada is not None and uf_normalizada not in get_uf_codes():
        raise InvalidParameterError(f"UF '{uf}' inválida.")
    nivel_normalizado = nivel.lower().strip() if nivel is not None else None
    if nivel_normalizado is not None and nivel_normalizado not in NIVEIS:
        raise InvalidParameterError(f"Nível '{nivel}' inválido. Opções: {list(NIVEIS)}")
    return TEMAS_CENSO_MUNICIPAL_1985[tema_normalizado], uf_normalizada, nivel_normalizado


def _cobertura_da_consulta(tabela: int, uf: str | None) -> dict[str, int]:
    filtro = "tabela = $tabela" + (" AND uf = $uf" if uf else "")
    parametros: dict[str, Any] = {"caminho": str(_COBERTURA), "tabela": tabela}
    if uf:
        parametros["uf"] = uf
    linhas = _consultar(
        f"SELECT motivo, count(*) AS paginas FROM read_parquet($caminho) WHERE {filtro} GROUP BY motivo",
        parametros,
    )
    return {str(m): int(n) for m, n in zip(linhas["motivo"], linhas["paginas"], strict=True)}


def _meta(df: pd.DataFrame, tabela: int, uf: str | None, parse_ms: int) -> MetaInfo:
    usados = set(df["volume"].unique())
    volumes = [v for v in _manifesto()["volumes"] if v["volume"] in usados]
    if len(volumes) == 1:
        source_url = volumes[0]["url"]
        raw_hash = volumes[0]["sha256"]
    else:
        source_url = _CATALOGO_URL
        linhas = "\n".join(sorted(f"{v['arquivo']} {v['sha256']}" for v in volumes))
        raw_hash = hashlib.sha256(linhas.encode("utf-8")).hexdigest()
    meta = build_source_meta(
        "ibge.censo_agro_municipal_1985",
        source_url,
        "pacote",
        0,
        parse_ms,
        df,
        constants.IBGE_CENSO_MUNICIPAL_1985_PARSER_VERSION,
        schema_version="2.0",
        raw_content_hash=raw_hash,
        raw_content_size=sum(int(v["bytes"]) for v in volumes),
        source_details={
            "tabela": tabela,
            "titulo": TITULOS_CENSO_MUNICIPAL_1985[tabela],
            "volumes": [
                {k: v[k] for k in ("volume", "uf", "arquivo", "url", "sha256", "bytes")}
                for v in volumes
            ],
            "pacote": {"arquivo": _PACOTE.name, "sha256": _sha_pacote()},
            "cobertura_paginas": _cobertura_da_consulta(tabela, uf),
            "hash": "SHA-256 do PDF; com mais de 1 volume, SHA-256 da lista 'arquivo sha256'",
        },
    )
    conferencias = [datetime.fromisoformat(v["conferido_em"]) for v in volumes]
    meta.fetch_timestamp = max(conferencias) if conferencias else None
    meta.from_cache = False
    return meta


@overload
async def censo_agro_municipal_1985(
    tema: str,
    *,
    uf: str | None = ...,
    nivel: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[False] = ...,
) -> pd.DataFrame: ...


@overload
async def censo_agro_municipal_1985(
    tema: str,
    *,
    uf: str | None = ...,
    nivel: str | None = ...,
    as_polars: bool = ...,
    return_meta: Literal[True],
) -> tuple[pd.DataFrame, MetaInfo]: ...


async def censo_agro_municipal_1985(
    tema: str,
    *,
    uf: str | None = None,
    nivel: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]:
    """Censo Agropecuário 1985 municipal, 1 linha por casa do PDF (a do número e as sem leitura, incertas e
    fora de coluna), com o `status` de cada uma. `valor` só vem na casa confirmada pelas somas impressas;
    `valor_lido` traz a leitura sempre. Lê o pacote do agrobr, sem rede.

    Raises:
        InvalidParameterError: tema, UF ou nível inválidos, ou UF cujo volume não tem a tabela (o IBGE
            omite a tabela que não se aplica ao estado).
    """
    t0 = time.perf_counter()
    tabela, uf_normalizada, nivel_normalizado = _validar(tema, uf, nivel)
    filtros = ["tabela = $tabela"]
    parametros: dict[str, Any] = {"caminho": str(_PACOTE), "tabela": tabela}
    if uf_normalizada:
        filtros.append("uf = $uf")
        parametros["uf"] = uf_normalizada
    if nivel_normalizado:
        filtros.append("nivel = $nivel")
        parametros["nivel"] = nivel_normalizado
    colunas = ", ".join("1985::INTEGER AS ano" if c == "ano" else c for c in COLUNAS)
    df = _consultar(
        f"SELECT {colunas} FROM read_parquet($caminho) WHERE {' AND '.join(filtros)} "
        "ORDER BY uf, volume, pagina_pdf, linha, coluna",
        parametros,
    )
    if df.empty and uf_normalizada:
        paginas = _cobertura_da_consulta(tabela, uf_normalizada)
        if not paginas:
            ufs = (await cobertura_censo_agro_municipal_1985())[
                TABELAS_CENSO_MUNICIPAL_1985[tabela]
            ]
            raise InvalidParameterError(
                f"A tabela {tabela} ({TITULOS_CENSO_MUNICIPAL_1985[tabela]}) não está no volume de "
                f"{uf_normalizada}: o IBGE omite a tabela que não se aplica ao estado. UFs com a tabela: "
                f"{', '.join(ufs)}"
            )
        if not paginas.keys() & {"com_coluna", "sem_grade"}:
            raise ParseError(
                "ibge.censo_agro_municipal_1985",
                constants.IBGE_CENSO_MUNICIPAL_1985_PARSER_VERSION,
                f"A tabela {tabela} ({TITULOS_CENSO_MUNICIPAL_1985[tabela]}) está no volume de {uf_normalizada}, "
                f"mas a extração não leu nenhuma casa ({sum(paginas.values())} página(s): "
                f"{', '.join(sorted(paginas))})",
            )
    df = _tipar(df)
    parse_ms = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "censo_municipal_1985_loaded",
        tema=TABELAS_CENSO_MUNICIPAL_1985[tabela],
        uf=uf_normalizada,
        nivel=nivel_normalizado,
        rows=len(df),
    )
    meta = _meta(df, tabela, uf_normalizada, parse_ms)
    return finalize_result(
        df, meta, as_polars=as_polars, return_meta=return_meta, string_columns=_TEXTOS
    )


async def temas_censo_agro_municipal_1985() -> list[str]:
    return list(TEMAS_DISPONIVEIS)


@lru_cache(maxsize=1)
def _cobertura() -> dict[str, list[str]]:
    linhas = _consultar(
        "SELECT tabela, list(DISTINCT uf ORDER BY uf) AS ufs FROM read_parquet($caminho) GROUP BY tabela",
        {"caminho": str(_PACOTE)},
    )
    return {
        TABELAS_CENSO_MUNICIPAL_1985[int(t)]: list(u)
        for t, u in zip(linhas["tabela"], linhas["ufs"], strict=True)
    }


async def cobertura_censo_agro_municipal_1985() -> dict[str, list[str]]:
    """UFs cujo volume tem a tabela de cada tema no pacote (o IBGE omite a tabela que não se aplica)."""
    return {tema: ufs.copy() for tema, ufs in _cobertura().items()}
