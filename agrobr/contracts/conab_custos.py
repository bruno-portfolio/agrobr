from __future__ import annotations

import pandas as pd

from agrobr import contracts
from agrobr.contracts import Column, ColumnType, Contract

TEXTO = pd.Series([""]).dtype

COLUNAS = (
    "cultura",
    "uf",
    "safra",
    "tecnologia",
    "categoria",
    "item",
    "unidade",
    "quantidade_ha",
    "preco_unitario",
    "valor_ha",
    "participacao_pct",
    "local",
    "ano_referencia",
    "referencia",
    "data_referencia",
    "planilha",
    "aba",
    "sistema",
    "linha",
    "tipo_linha",
    "secao",
    "unidade_produto",
    "valor_unidade_produto",
    "participacao_cv_pct",
    "participacao_ct_pct",
)
MEDIDAS = frozenset(
    {
        "quantidade_ha",
        "preco_unitario",
        "valor_ha",
        "participacao_pct",
        "valor_unidade_produto",
        "participacao_cv_pct",
        "participacao_ct_pct",
    }
)
NULAS = MEDIDAS | {"safra", "tecnologia", "data_referencia", "secao", "unidade_produto"}
DESCRICOES = {
    "cultura": "Cultura canônica do recurso oficial selecionado.",
    "uf": "UF publicada no contexto da aba; não inferida pelo nome do arquivo.",
    "safra": "Token de safra publicado, inclusive grafias anômalas; nunca derivado do ano da referência.",
    "tecnologia": "Qualificação alta/média/baixa quando explícita no sistema; nula se ausente.",
    "categoria": (
        "Classificação auxiliar pela seção publicada (IV e V: custos_fixos; II, III e VI: outros), "
        "no custeio pelo rótulo normalizado e, nas linhas de total, pelo rótulo do total; item e seção "
        "conservam os rótulos publicados."
    ),
    "item": "Descrição literal da linha, inclusive espaços, receitas e totais.",
    "unidade": "Cabeçalho literal da coluna de custo por hectare.",
    "quantidade_ha": "Coeficiente físico por hectare, nulo quando não publicado; nunca derivado de custo.",
    "preco_unitario": "Preço unitário publicado, nulo quando ausente; nunca custo por unidade de produção.",
    "valor_ha": "Valor publicado por hectare; admite zero, receitas negativas e ausência.",
    "participacao_pct": "Participação genérica somente quando publicada sem base CV/CT específica.",
    "local": "Local literal identificado na aba; não restrito a município IBGE.",
    "ano_referencia": "Ano extraído da referência de preços publicada; distinto de safra.",
    "referencia": "Referência textual publicada ou ISO da célula Excel datada.",
    "data_referencia": "Data somente quando a célula Excel publica data; mês/ano não inventa dia.",
    "planilha": "Identificador exato entre candidatos do catálogo oficial.",
    "aba": "Nome literal da aba selecionada de forma única.",
    "sistema": "Descrição publicada do sistema de produção.",
    "linha": "Número físico da linha na aba, base 1.",
    "tipo_linha": "item, subtotal ou total publicado; somar indiscriminadamente duplica componentes.",
    "secao": (
        "Último cabeçalho romano de seção publicado, quando identificado; nulo nas linhas de total "
        "(CUSTO ...), que somam várias seções."
    ),
    "unidade_produto": "Cabeçalho literal da unidade de produção, por exemplo CUSTO /  60 kg, R$/1 kg ou (R$/t); sem equivalência presumida.",
    "valor_unidade_produto": "Custo pela unidade de produção publicada, não preço unitário de insumo.",
    "participacao_cv_pct": "Participação publicada na base custo variável; CV não é COE.",
    "participacao_ct_pct": "Participação publicada na base custo total.",
}
UNIDADES = {
    "valor_ha": "BRL/ha",
    "participacao_pct": "%",
    "participacao_cv_pct": "% CV",
    "participacao_ct_pct": "% CT",
}


def _dtype(name: str) -> ColumnType:
    if name in MEDIDAS:
        return ColumnType.FLOAT
    if name in {"linha", "ano_referencia"}:
        return ColumnType.INTEGER
    if name == "data_referencia":
        return ColumnType.DATETIME
    return ColumnType.STRING


def _strict(df: pd.DataFrame) -> list[str]:
    errors = []
    if list(df.columns) != list(COLUNAS):
        errors.append("Ordem/projeção de custos CONAB difere do contrato 3.0")
    for name in COLUNAS:
        if name not in df:
            continue
        expected = (
            "float64"
            if name in MEDIDAS
            else "Int64"
            if name in {"linha", "ano_referencia"}
            else "datetime64[ns]"
            if name == "data_referencia"
            else str(TEXTO)
        )
        if str(df[name].dtype) != expected:
            errors.append(f"dtype inválido: {name}")
    if "tipo_linha" in df and not df["tipo_linha"].isin(["item", "subtotal", "total"]).all():
        errors.append("Tipo de linha não reconhecido")
    if "unidade" in df:
        units = df["unidade"].astype("string").str.upper().str.split().str.join(" ")
        if not units.isin(["CUSTO POR HA", "(R$/HA)", "R$/HA", "CUSTO/HA"]).all():
            errors.append("Unidade publicada de custo por hectare não reconhecida")
    return errors


class CustosContract(Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        _, errors = super().validate(df)
        if df.columns.duplicated().any():
            return False, errors
        errors.extend(_strict(df))
        return not errors, errors

    def empty_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                name: pd.Series(
                    dtype="float64"
                    if name in MEDIDAS
                    else "Int64"
                    if name in {"linha", "ano_referencia"}
                    else "datetime64[ns]"
                    if name == "data_referencia"
                    else TEXTO
                )
                for name in COLUNAS
            }
        )


CONAB_CUSTOS_V3 = CustosContract(
    name="conab.custo_producao",
    version="3.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[
        Column(
            name=name,
            type=_dtype(name),
            nullable=name in NULAS,
            stable=True,
            description=DESCRICOES[name],
            unit=UNIDADES.get(name),
        )
        for name in COLUNAS
    ],
)

contracts.register_contract("custo_producao", CONAB_CUSTOS_V3)

SOCIOBIO_COLUMNS = (
    "produto",
    "local",
    "uf",
    "ano",
    "safra_publicada",
    "sistema",
    "tipo_relatorio",
    "mes_ano_referencia",
    "data_precos",
    "produtividade",
    "unidade_produtividade",
    "secao",
    "item",
    "tipo_linha",
    "linha",
    "valor",
    "unidade_valor",
    "valor_unidade_produto",
    "unidade_produto",
    "participacao_pct",
    "participacao_ct_pct",
    "planilha",
    "aba",
)
SOCIOBIO_MEASURES = frozenset(
    {"produtividade", "valor", "valor_unidade_produto", "participacao_pct", "participacao_ct_pct"}
)
SOCIOBIO_NULLABLE = SOCIOBIO_MEASURES | {
    "safra_publicada",
    "tipo_relatorio",
    "mes_ano_referencia",
    "data_precos",
    "unidade_produtividade",
    "secao",
    "unidade_produto",
}
SOCIOBIO_DESCRIPTIONS = {
    "produto": "Produto canônico identificado no catálogo oficial da família sociobiodiversidade.",
    "local": "Local publicado no contexto da aba.",
    "uf": "UF publicada no contexto; se ausente no formato reconhecido, UF do nome da aba com proveniência.",
    "ano": "Primeiro ano da safra publicada no contexto; nunca inferido do nome da aba ou da data de preços.",
    "safra_publicada": "Token literal da safra com dois anos, como 2018/19; nulo quando há um ano único.",
    "sistema": "Descrição publicada do sistema de produção ou extrativismo.",
    "tipo_relatorio": "Tipo de relatório publicado; nulo quando ausente.",
    "mes_ano_referencia": "Referência textual publicada, sem inventar um dia.",
    "data_precos": "Data publicada em célula datada de preços; nula quando só há referência textual.",
    "produtividade": "Produtividade publicada, sem conversão entre bases.",
    "unidade_produtividade": "Unidade literal publicada da produtividade, sem enum ou conversão.",
    "secao": "Cabeçalho publicado da seção, romano ou gestão da propriedade familiar.",
    "item": "Rótulo publicado da linha, preservando espaços e sinais.",
    "tipo_linha": "item, total ou secao; somar linhas indiscriminadamente duplica componentes.",
    "linha": "Número físico da linha na aba, base 1.",
    "valor": "Valor da primeira coluna monetária na base publicada; sem conversão.",
    "unidade_valor": "Cabeçalho literal da primeira coluna monetária, com espaços colapsados; consumidor filtra por esta coluna.",
    "valor_unidade_produto": "Valor da segunda coluna monetária na unidade publicada, quando presente.",
    "unidade_produto": "Cabeçalho literal da segunda coluna monetária, com espaços colapsados.",
    "participacao_pct": "Participação publicada; no layout novo a base é CV, no antigo a base é a publicada no cabeçalho genérico.",
    "participacao_ct_pct": "Participação publicada na base custo total, quando presente.",
    "planilha": "Identificador exato do recurso selecionado no catálogo.",
    "aba": "Nome literal da aba de origem.",
}


def _sociobio_dtype(name: str) -> str:
    if name in SOCIOBIO_MEASURES:
        return "float64"
    if name in {"ano", "linha"}:
        return "Int64"
    return "datetime64[ns]" if name == "data_precos" else str(TEXTO)


class SociobioContract(Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        _, errors = super().validate(df)
        if df.columns.duplicated().any():
            return False, errors
        if list(df.columns) != list(SOCIOBIO_COLUMNS):
            errors.append("Ordem/projeção de sociobiodiversidade difere do contrato 1.0")
        for name in SOCIOBIO_COLUMNS:
            if name not in df:
                continue
            if str(df[name].dtype) != _sociobio_dtype(name):
                errors.append(f"dtype inválido: {name}")
        if "tipo_linha" in df and not df["tipo_linha"].isin(["item", "total", "secao"]).all():
            errors.append("Tipo de linha não reconhecido")
        return not errors, errors

    def empty_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            {name: pd.Series(dtype=_sociobio_dtype(name)) for name in SOCIOBIO_COLUMNS}
        )


CONAB_SOCIOBIO_V1 = SociobioContract(
    name="conab.custo_sociobiodiversidade",
    version="1.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[
        Column(
            name=name,
            type=ColumnType.FLOAT
            if name in SOCIOBIO_MEASURES
            else ColumnType.INTEGER
            if name in {"ano", "linha"}
            else ColumnType.DATETIME
            if name == "data_precos"
            else ColumnType.STRING,
            nullable=name in SOCIOBIO_NULLABLE,
            stable=True,
            description=SOCIOBIO_DESCRIPTIONS[name],
            unit="%" if name.startswith("participacao") else None,
        )
        for name in SOCIOBIO_COLUMNS
    ],
)

contracts.register_contract("custo_sociobiodiversidade", CONAB_SOCIOBIO_V1)
