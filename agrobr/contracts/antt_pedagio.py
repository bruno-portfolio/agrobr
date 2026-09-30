from __future__ import annotations

import pandas as pd

from agrobr import constants
from agrobr.contracts import Column, ColumnType, Contract
from agrobr.normalize.regions import UFS_VALIDAS


class FluxoContract(Contract):
    def empty_frame(self) -> pd.DataFrame:
        frame = super().empty_frame()
        for column in self.columns:
            if column.type == ColumnType.STRING:
                frame[column.name] = frame[column.name].astype("string[python]")
        return frame

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not valid:
            return False, errors
        if df.columns.tolist() != list(constants.ANTT_FLUXO_COLUMNS):
            errors.append("Colunas e ordem devem corresponder ao contrato ANTT 3.0")
        if str(df["data"].dtype) != "datetime64[ns]":
            errors.append("data deve usar datetime64[ns] civil sem fuso")
        for name in ("volume", "n_eixos"):
            if str(df[name].dtype) != "Int64":
                errors.append(f"{name} deve usar Int64 exato")
        for column in self.columns:
            if column.type == ColumnType.STRING:
                dtype = df[column.name].dtype
                if not isinstance(dtype, pd.StringDtype) or dtype.storage != "python":
                    errors.append(f"{column.name} deve usar string[python]")
        categories = df["categoria_eixo"].astype("string[python]").str.strip()
        explicit = categories.str.fullmatch(constants.ANTT_EXPLICIT_AXLES_PATTERN, na=False)
        counts = categories.where(explicit).str.extract(
            constants.ANTT_EXPLICIT_AXLES_PATTERN, expand=False
        )
        for actual, raw in zip(df["n_eixos"], counts, strict=True):
            value = None
            if pd.notna(raw):
                digits = raw.lstrip("0") or "0"
                if len(digits) > 19 or (
                    len(digits) == 19 and digits > str(constants.ANTT_INT64_MAX)
                ):
                    errors.append("Contagem explícita de eixos fora de Int64")
                    break
                value = int(digits)
            if (pd.isna(actual) != pd.isna(value)) or (
                pd.notna(actual) and pd.notna(value) and actual != value
            ):
                errors.append("n_eixos diverge da contagem explícita em categoria_eixo")
                break
        if not df["frequencia"].isin(("mensal", "diaria")).all():
            errors.append("frequencia deve ser mensal ou diaria")
        if pd.api.types.is_datetime64_ns_dtype(df["data"]):
            if df["data"].dt.tz is not None or (df["data"] != df["data"].dt.normalize()).any():
                errors.append("data deve conter datas civis sem horário/fuso")
            if (df.loc[df["frequencia"].eq("mensal"), "data"].dt.day != 1).any():
                errors.append("Referência mensal deve usar o primeiro dia do mês")
        for name in ("concessionaria", "praca"):
            if df[name].str.strip().eq("").any():
                errors.append(f"{name} não admite texto vazio")
        if not df["uf"].dropna().isin(UFS_VALIDAS).all():
            errors.append("uf inválida no enriquecimento")
        return not errors, errors


ANTT_PEDAGIO_FLUXO_V3 = FluxoContract(
    name="antt_pedagio.fluxo",
    version="3.0",
    effective_from="2.0.0",
    primary_key=[
        "data",
        "concessionaria",
        "praca",
        "sentido",
        "tipo_veiculo",
        "categoria_eixo",
        "tipo_cobranca",
        "frequencia",
    ],
    columns=[
        Column(
            "data", ColumnType.DATE, description="Data civil diária ou referência mensal no dia 1"
        ),
        Column(
            "concessionaria", ColumnType.STRING, description="Texto publicado, sem alterar espaços"
        ),
        Column("praca", ColumnType.STRING, description="Texto publicado, sem alterar espaços"),
        Column("sentido", ColumnType.STRING, nullable=True),
        Column(
            "n_eixos",
            ColumnType.INTEGER,
            nullable=True,
            min_value=1,
            description="Somente contagem explícita em texto; categoria tarifária/número isolado permanece nulo",
        ),
        Column(
            "tipo_veiculo",
            ColumnType.STRING,
            nullable=True,
            description="Tipo explícito da fonte ou texto inequívoco da categoria",
        ),
        Column(
            "volume",
            ColumnType.INTEGER,
            min_value=0,
            max_value=constants.ANTT_INT64_MAX,
            description="Contagem exata no período e nas dimensões publicadas",
        ),
        Column(
            "rodovia",
            ColumnType.STRING,
            nullable=True,
            description="Cadastro com correspondência literal e enriquecimento único",
        ),
        Column("uf", ColumnType.STRING, nullable=True),
        Column("municipio", ColumnType.STRING, nullable=True),
        Column(
            "categoria_eixo",
            ColumnType.STRING,
            nullable=True,
            description="Categoria literal, sem converter códigos tarifários em eixos",
        ),
        Column(
            "tipo_cobranca",
            ColumnType.STRING,
            nullable=True,
            description="Modalidade literal, incluindo N/I quando publicado",
        ),
        Column(
            "frequencia",
            ColumnType.STRING,
            description="mensal ou diaria; sem troca automática entre frequências",
        ),
    ],
    guarantees=[
        "Todas as ocorrências dos recursos completos são validadas antes do retorno, inclusive fora dos filtros",
        "Datas diárias preservam o dia publicado; referências mensais usam o dia 1",
        "Cobranças, tipos e categorias distintos permanecem em grupos separados",
        "Volumes são inteiros exatos não negativos, sem converter valores inválidos ou ausentes em zero",
        "Número isolado em categoria_eixo não comprova contagem de eixos",
        "Recursos, revisões literais, hashes, tentativas e cobertura constam na proveniência",
        "Enriquecimento ambíguo ou sem correspondência permanece nulo; não multiplica linhas",
        "Texto em string[python], e não no dtype padrão do pandas: o limite de memória conta cada texto retido pela identidade do objeto",
    ],
)
