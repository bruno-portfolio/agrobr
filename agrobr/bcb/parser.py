from __future__ import annotations

import math
from typing import Any

import pandas as pd

from agrobr import _log, contracts
from agrobr.contracts import bcb_sicor
from agrobr.exceptions import ParseError
from agrobr.utils.result import ATRIBUTO_AVISOS
from agrobr.utils.warnings import warn_once

logger = _log.get_logger(__name__)

PARSER_VERSION = 2

COLUNAS_MAP: dict[str, str] = {
    "AnoEmissao": "ano_emissao",
    "MesEmissao": "mes_emissao",
    "nomeUF": "uf",
    "nomeRegiao": "regiao",
    "nomeProduto": "produto",
    "VlCusteio": "valor",
    "AreaCusteio": "area_financiada",
    "QtdCusteio": "qtd_contratos",
    "VlInvest": "valor",
    "QtdInvest": "qtd_contratos",
    "VlComerc": "valor",
    "QtdComerc": "qtd_contratos",
    "cdPrograma": "cd_programa",
    "cdSubPrograma": "cd_sub_programa",
    "cdFonteRecurso": "cd_fonte_recurso",
    "cdTipoSeguro": "cd_tipo_seguro",
    "cdModalidade": "cd_modalidade",
    "Atividade": "cd_atividade",
}

ENRIQUECIMENTO_MAP: dict[str, tuple[str, str]] = {
    "cd_programa": ("programa", "programa"),
    "cd_tipo_seguro": ("tipo_seguro", "tipo_seguro"),
}
ENRIQUECIMENTO_REGISTRO_MAP: dict[str, tuple[str, str]] = {
    "cd_fonte_recurso": ("fonte_recurso", "fonte_recurso"),
    "cd_modalidade": ("modalidade", "modalidade"),
    "cd_atividade": ("atividade", "atividade"),
}


def _enriquecer_dimensoes(
    df: pd.DataFrame, mapa: dict[str, tuple[str, str]] = ENRIQUECIMENTO_MAP
) -> pd.DataFrame:
    from agrobr.bcb import models

    resolve_fns = {
        "programa": models.resolve_programa,
        "fonte_recurso": models.resolve_fonte_recurso,
        "tipo_seguro": models.resolve_tipo_seguro,
        "modalidade": models.resolve_modalidade,
        "atividade": models.resolve_atividade,
    }

    for cd_col, (nome_col, dominio) in mapa.items():
        if cd_col in df.columns:
            fn = resolve_fns[dominio]
            df[nome_col] = df[cd_col].map(str, na_action="ignore").map(fn, na_action="ignore")

    return df


def nomear_dimensoes_do_registro(df: pd.DataFrame) -> pd.DataFrame:
    return _enriquecer_dimensoes(df.copy(), ENRIQUECIMENTO_REGISTRO_MAP)


def parse_credito_rural(
    dados: list[dict[str, Any]],
    finalidade: str = "custeio",
) -> pd.DataFrame:
    if not dados:
        warn_once(
            f"sicor_empty:{finalidade}",
            "Nenhum registro para produto/safra/UF/finalidade; em investimento os produtos "
            "são itens de investimento, ex.: BOVINOS, CAFÉ, CANA-DE-AÇUCAR, BANANA e tratores.",
        )
        return contracts.get_contract("credito_rural").empty_frame()

    df = pd.DataFrame(dados)

    rename = {k: v for k, v in COLUNAS_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)

    for col in ("valor", "area_financiada"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    for col in ("ano_emissao", "mes_emissao", "qtd_contratos"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "ano_emissao" in df.columns:
        from agrobr.normalize.dates import INICIO_SAFRA_MES, anos_para_safra

        ano = df["ano_emissao"]
        if "mes_emissao" in df.columns:
            inicio = ano.where(df["mes_emissao"] >= INICIO_SAFRA_MES, ano - 1)
        else:
            inicio = ano
        mask = inicio.notna()
        df["safra"] = pd.Series(None, index=df.index, dtype=pd.Series([""]).dtype)
        df.loc[mask, "safra"] = inicio[mask].astype(int).map(anos_para_safra)

    if "produto" in df.columns:
        df["produto"] = df["produto"].str.strip().str.strip('"').str.lower().str.strip()

    if "uf" in df.columns:
        df["uf"] = df["uf"].str.upper().str.strip()

    df["finalidade"] = finalidade.lower()

    df = _enriquecer_dimensoes(df)

    sort_cols = [c for c in ("safra", "uf", "municipio", "produto") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)

    logger.info(
        "bcb_parsed",
        records=len(df),
        columns=df.columns.tolist(),
    )

    return df


def parse_credito_rural_total(
    dados: list[dict[str, Any]], finalidades: tuple[str, ...], agregacao: str
) -> pd.DataFrame:
    """Uma linha por safra, UF e finalidade, e também por programa com agregacao='programa'.
    Na linha larga da RegiaoUF, a fonte preenche com 0 as finalidades sem operação: o par
    qtd = 0 e valor = 0 não vira linha."""
    from agrobr.bcb import models
    from agrobr.normalize.dates import INICIO_SAFRA_MES, anos_para_safra

    somas: dict[tuple[str, str, str, str | None], tuple[int, list[float]]] = {}
    for registro in dados:
        faltando = [campo for campo in models.SICOR_TOTAL_CAMPOS if registro.get(campo) is None]
        if faltando:
            raise ParseError(
                source="bcb",
                parser_version=PARSER_VERSION,
                reason=f"RegiaoUF sem {faltando}: {registro}",
            )
        ano, mes = int(registro["AnoEmissao"]), int(registro["MesEmissao"])
        inicio = ano if mes >= INICIO_SAFRA_MES else ano - 1
        programa = str(registro["cdPrograma"]).strip() if agregacao == "programa" else None
        for finalidade in finalidades:
            campo_qtd, campo_valor = models.SICOR_TOTAL_FINALIDADES[finalidade]
            qtd, valor = registro[campo_qtd], registro[campo_valor]
            if qtd == 0 and valor == 0:
                continue
            chave = (
                anos_para_safra(inicio),
                str(registro["nomeUF"]).strip().upper(),
                finalidade,
                programa,
            )
            contagem, valores = somas.get(chave, (0, []))
            valores.append(valor)
            somas[chave] = (contagem + qtd, valores)
    frame = pd.DataFrame(
        [
            {
                "safra": safra,
                "uf": uf,
                "finalidade": finalidade,
                "agregacao": agregacao,
                "programa": models.resolve_programa(codigo) if codigo else None,
                "cd_programa": codigo,
                "qtd_contratos": contagem,
                "valor": round(math.fsum(valores), 2),
                "fonte": "bcb_odata",
            }
            for (safra, uf, finalidade, codigo), (contagem, valores) in sorted(
                somas.items(), key=lambda item: (*item[0][:3], item[0][3] or "")
            )
        ],
        columns=bcb_sicor.BCB_CREDITO_RURAL_TOTAL_V1.list_columns(),
    )
    return frame.astype(bcb_sicor.BCB_CREDITO_RURAL_TOTAL_V1.empty_frame().dtypes.to_dict())


def _soma_preservando_nulos(values: pd.Series) -> Any:
    return values.sum(min_count=len(values))


def _avisos_de_parte_nula(df: pd.DataFrame, group_cols: list[str]) -> list[str]:
    avisos = []
    for medida in ("valor", "area_financiada", "qtd_contratos"):
        if medida not in df.columns:
            continue
        nulos = df[medida].isna().groupby([df[c] for c in group_cols], dropna=False)
        mistos = int((nulos.any() & ~nulos.all()).sum())
        if mistos:
            avisos.append(
                f"credito_rural: {medida} sai nulo em {mistos} grupo(s) com registro sem o valor; "
                "a soma das partes conhecidas não é o total"
            )
    return avisos


def agregar_por_uf(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = [c for c in ("safra", "uf", "produto", "finalidade") if c in df.columns]
    if not group_cols:
        return df

    agg_dict: dict[str, Any] = {}
    if "valor" in df.columns:
        agg_dict["valor"] = _soma_preservando_nulos
    if "area_financiada" in df.columns:
        agg_dict["area_financiada"] = _soma_preservando_nulos
    if "qtd_contratos" in df.columns:
        agg_dict["qtd_contratos"] = _soma_preservando_nulos

    if not agg_dict:
        return df

    result = df.groupby(group_cols, as_index=False, dropna=False).agg(agg_dict)
    result = result.sort_values(group_cols).reset_index(drop=True)
    avisos = _avisos_de_parte_nula(df, group_cols)
    if avisos:
        result.attrs[ATRIBUTO_AVISOS] = avisos
    return result


def agregar_por_programa(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_cols = [
        c
        for c in ("safra", "uf", "produto", "finalidade", "programa", "cd_programa")
        if c in df.columns
    ]
    if not group_cols:
        return df

    agg_dict: dict[str, Any] = {}
    if "valor" in df.columns:
        agg_dict["valor"] = _soma_preservando_nulos
    if "area_financiada" in df.columns:
        agg_dict["area_financiada"] = _soma_preservando_nulos
    if "qtd_contratos" in df.columns:
        agg_dict["qtd_contratos"] = _soma_preservando_nulos

    if not agg_dict:
        return df

    result = df.groupby(group_cols, as_index=False, dropna=False).agg(agg_dict)
    result = result.sort_values(group_cols).reset_index(drop=True)
    avisos = _avisos_de_parte_nula(df, group_cols)
    if avisos:
        result.attrs[ATRIBUTO_AVISOS] = avisos
    return result
