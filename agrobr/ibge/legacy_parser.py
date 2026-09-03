from __future__ import annotations

import io
from typing import Any

import pandas as pd
import structlog

from agrobr.normalize.numeric import safe_float

logger = structlog.get_logger()

PARSER_VERSION = 1

_TEMA_COLS: dict[str, dict[int, tuple[str, str, str]]] = {
    "tecnologia": {
        1: ("assistencia_tecnica", "estabelecimentos", "estabelecimentos"),
        2: ("irrigacao", "estabelecimentos", "estabelecimentos"),
        3: ("adubos_corretivos", "estabelecimentos", "estabelecimentos"),
        4: ("controle_pragas", "estabelecimentos", "estabelecimentos"),
        5: ("conservacao_solo", "estabelecimentos", "estabelecimentos"),
        6: ("energia_eletrica", "estabelecimentos", "estabelecimentos"),
    },
    "pessoal_ocupado": {
        1: ("total", "quantidade", "pessoas"),
        2: ("familiar", "quantidade", "pessoas"),
        3: ("permanentes", "quantidade", "pessoas"),
        4: ("temporarios", "quantidade", "pessoas"),
        5: ("parceiros_outra", "quantidade", "pessoas"),
    },
    "maquinas": {
        1: ("total_tratores", "quantidade", "unidades"),
        2: ("menos_10cv", "quantidade", "unidades"),
        3: ("10_50cv", "quantidade", "unidades"),
        4: ("50_100cv", "quantidade", "unidades"),
        5: ("mais_100cv", "quantidade", "unidades"),
    },
    "producao_animal": {
        1: ("leite_vaca", "producao", "litros"),
        2: ("leite_cabra", "producao", "litros"),
        3: ("la", "producao", "kg"),
        4: ("ovos_galinha", "producao", "duzias"),
    },
    "valor_producao": {
        1: ("total", "valor", "R$"),
        2: ("vegetal", "valor", "R$"),
        3: ("vegetal_subtipo", "valor", "R$"),
        4: ("animal", "valor", "R$"),
        5: ("animal_subtipo", "valor", "R$"),
    },
    "financeiro": {
        1: ("investimentos", "valor", "R$"),
        2: ("financiamentos", "valor", "R$"),
        3: ("despesas", "valor", "R$"),
        4: ("receitas", "valor", "R$"),
    },
}

TEMAS_LEGADO: list[str] = list(_TEMA_COLS.keys())

_OUTPUT_COLS = [
    "ano",
    "localidade",
    "localidade_cod",
    "tema",
    "categoria",
    "variavel",
    "valor",
    "unidade",
    "fonte",
    "nivel_geo",
]


def _detect_nivel_geo(raw_loc: str) -> str:
    leading = len(raw_loc) - len(raw_loc.lstrip(" "))
    if leading >= 10:
        return "totais"
    if leading >= 6:
        return "municipio"
    if leading >= 3:
        return "microrregiao"
    return "mesorregiao"


def _empty_legacy_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_OUTPUT_COLS)


def parse_legacy_xls(data: bytes, tema: str, filename: str = "") -> pd.DataFrame:
    if tema not in _TEMA_COLS:
        from agrobr.exceptions import ParseError

        raise ParseError(
            source="ibge_censo_agro_legado",
            parser_version=PARSER_VERSION,
            reason=f"Tema não suportado: {tema}. Disponíveis: {TEMAS_LEGADO}",
        )

    config = _TEMA_COLS[tema]

    try:
        df_raw = pd.read_excel(io.BytesIO(data), header=None, engine="xlrd")
    except Exception as exc:
        from agrobr.exceptions import ParseError

        raise ParseError(
            source="ibge_censo_agro_legado",
            parser_version=PARSER_VERSION,
            reason=f"Falha ao ler XLS ({filename}): {exc}",
        ) from exc

    records = _extract_from_sheet(df_raw, tema, config)

    if not records:
        logger.warning(
            "ibge_legacy_parse_empty",
            tema=tema,
            filename=filename,
        )
        return _empty_legacy_df()

    result = pd.DataFrame(records, columns=_OUTPUT_COLS)
    result["valor"] = pd.to_numeric(result["valor"], errors="coerce")
    result["localidade_cod"] = result["localidade_cod"].astype("Int64")

    logger.info(
        "ibge_legacy_parse_ok",
        tema=tema,
        filename=filename,
        records=len(result),
    )
    return result


def _extract_from_sheet(
    df: pd.DataFrame,
    tema: str,
    config: dict[int, tuple[str, str, str]],
) -> list[list[Any]]:
    records: list[list[Any]] = []

    for idx in range(len(df)):
        row = df.iloc[idx]

        raw_loc = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        if not raw_loc.strip() or raw_loc.strip() == "nan":
            continue

        localidade = raw_loc.strip()
        nivel_geo = _detect_nivel_geo(raw_loc)

        for col_idx, (categoria, variavel, unidade) in config.items():
            if col_idx > len(row) - 1:
                continue

            val = row.iloc[col_idx]
            valor = safe_float(val)

            records.append(
                [
                    1995,
                    localidade,
                    pd.NA,
                    tema,
                    categoria,
                    variavel,
                    valor,
                    unidade,
                    "ibge_censo_agro_legado",
                    nivel_geo,
                ]
            )

    return records
