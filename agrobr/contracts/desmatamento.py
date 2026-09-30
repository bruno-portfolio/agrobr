from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr import constants, contracts
from agrobr.normalize import regions

TEXTO = pd.Series([""]).dtype


class DesmatamentoContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for column in self.columns:
            if column.name not in df:
                continue
            dtype = df[column.name].dtype
            if column.type == contracts.ColumnType.STRING:
                if dtype != TEXTO:
                    errors.append(f"Column '{column.name}' must use the default pandas text dtype")
            else:
                expected = {
                    contracts.ColumnType.INTEGER: "Int64",
                    contracts.ColumnType.FLOAT: "float64",
                    contracts.ColumnType.DATE: "datetime64[ns]",
                }.get(column.type)
                if expected is not None and str(dtype) != expected:
                    errors.append(f"Column '{column.name}' must use {expected} dtype")
        for name, allowed in (("uf", regions.UFS_VALIDAS), ("bioma", regions.BIOMAS_VALIDOS)):
            if name in df and not df[name].dropna().isin(allowed).all():
                errors.append(f"Column '{name}' contains an unrecognized normalized value")
        if "feature_id" in df and any(
            not isinstance(value, str) or not value.strip() for value in df["feature_id"]
        ):
            errors.append("Column 'feature_id' requires non-empty published strings")
        if errors:
            return False, errors
        for column in self.columns:
            if column.type == contracts.ColumnType.DATE:
                values = df[column.name].dropna()
                if not values.eq(values.dt.normalize()).all():
                    errors.append(f"Column '{column.name}' must contain civil dates at midnight")
        if (
            "scene_id" in df
            and not df["scene_id"].dropna().str.fullmatch(constants.JSON_NUMBER_PATTERN).all()
        ):
            errors.append("scene_id must preserve a numeric JSON lexeme")
        if self.primary_key and not df["classe"].str.strip().ne("").all():
            errors.append("Aggregate classe must contain nonblank text")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        schema["constraints"].update(
            string_dtype="pandas default text dtype",
            integer_dtype="Int64",
            float_dtype="float64",
            date_dtype="datetime64[ns]",
            date_semantics="civil_midnight_without_timezone",
            uf_domain=sorted(regions.UFS_VALIDAS),
            biome_domain=sorted(regions.BIOMAS_VALIDOS),
            feature_identity="no_unique_feature_key"
            if not self.primary_key
            else "aggregate_group_key_only",
        )
        if "scene_id" in schema["required_columns"]:
            schema["constraints"]["scene_id_pattern"] = constants.JSON_NUMBER_PATTERN
        if not self.primary_key:
            schema["constraints"]["feature_id"] = "required_nonblank_published_string_not_unique"
        else:
            schema["constraints"]["aggregate_classe"] = "nonblank"
        return schema


PRODES_FEICOES_V2 = DesmatamentoContract(
    name="desmatamento.prodes_feicoes",
    version="2.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[
        contracts.Column("ano", contracts.ColumnType.INTEGER, nullable=True),
        contracts.Column("uf", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("classe", contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            "area_km2", contracts.ColumnType.FLOAT, nullable=True, unit="km2", min_value=0
        ),
        contracts.Column("satelite", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("sensor", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("bioma", contracts.ColumnType.STRING),
        contracts.Column("feature_id", contracts.ColumnType.STRING),
        contracts.Column("uuid", contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            "fid",
            contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=-(2**31),
            max_value=2**31 - 1,
        ),
        contracts.Column("estado_original", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("path_row", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("class_name", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("def_cloud", contracts.ColumnType.FLOAT, nullable=True),
        contracts.Column("julian_day", contracts.ColumnType.FLOAT, nullable=True),
        contracts.Column("image_date", contracts.ColumnType.DATE, nullable=True),
        contracts.Column(
            "scene_id",
            contracts.ColumnType.STRING,
            nullable=True,
            description="Lexema numérico JSON original, sem conversão para float ou garantia de identidade",
        ),
        contracts.Column("publish_year", contracts.ColumnType.DATE, nullable=True),
        contracts.Column("source", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("pub_date", contracts.ColumnType.DATE, nullable=True),
    ],
    guarantees=[
        "Todas as ocorrências recebidas são preservadas, inclusive identificadores repetidos",
        "Feature.id é string não vazia no escopo suportado, sem garantia de unicidade",
        "Atributo ausente do layout é identificado nos metadados, distinto de null publicado",
        "Ano sai Int64; ano decimal não é truncado: a fonte recusa com ParseError",
        "Área é o atributo publicado da feição, não taxa oficial de desmatamento",
        "Datas de imagem, publicação e aquisição permanecem distintas",
    ],
)

DETER_FEICOES_V2 = DesmatamentoContract(
    name="desmatamento.deter_feicoes",
    version="2.0",
    effective_from="2.0.0",
    primary_key=[],
    columns=[
        contracts.Column("data", contracts.ColumnType.DATE, nullable=True),
        contracts.Column("classe", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("uf", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("municipio", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("municipio_id", contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            "area_km2", contracts.ColumnType.FLOAT, nullable=True, unit="km2", min_value=0
        ),
        contracts.Column("satelite", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("sensor", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("bioma", contracts.ColumnType.STRING),
        contracts.Column("feature_id", contracts.ColumnType.STRING),
        contracts.Column("gid", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("uf_original", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("quadrant", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("path_row", contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            "areauckm", contracts.ColumnType.FLOAT, nullable=True, unit="km2", min_value=0
        ),
        contracts.Column("uc", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("publish_month", contracts.ColumnType.DATE, nullable=True),
        contracts.Column("created_date", contracts.ColumnType.DATE, nullable=True),
        contracts.Column(
            "areatotalkm", contracts.ColumnType.FLOAT, nullable=True, unit="km2", min_value=0
        ),
    ],
    guarantees=[
        "Gid e Feature.id podem repetir com propriedades diferentes; não são chave primária",
        "Código municipal é texto literal e não é fabricado nos layouts que não o publicam",
        "Área municipal, área UC e área original são atributos distintos",
        "Null, string vazia e zero não são intercambiáveis",
        "Data das imagens não é o instante do evento nem o instante de aquisição",
    ],
)

DESMATAMENTO_PRODES_V2 = DesmatamentoContract(
    name="desmatamento.prodes",
    version="2.0",
    effective_from="2.0.0",
    primary_key=["ano", "uf", "classe", "bioma"],
    columns=[
        contracts.Column(
            name="ano", type=contracts.ColumnType.INTEGER, min_value=1, max_value=9999
        ),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING),
        contracts.Column(name="classe", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="area_km2", type=contracts.ColumnType.FLOAT, nullable=True, unit="km2", min_value=0
        ),
        contracts.Column(name="satelite", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="sensor", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="bioma", type=contracts.ColumnType.STRING),
    ],
    guarantees=[
        "Chave primária descreve grupos agregados, não identidade das feições",
        "A soma contém somente áreas da seleção quantitativamente reconciliada",
        "Qualquer área ausente no grupo produz total ausente, não soma parcial",
        "Ano, UF ou classe incompatível impede agregação em vez de descartar ocorrências",
        "Soma de áreas de feições não é uma taxa oficial PRODES",
    ],
)

DESMATAMENTO_DETER_V2 = DesmatamentoContract(
    name="desmatamento.deter",
    version="2.1",
    effective_from="2.0.0",
    primary_key=["data", "classe", "uf", "municipio", "municipio_id", "bioma"],
    columns=[
        contracts.Column(name="data", type=contracts.ColumnType.DATE),
        contracts.Column(name="classe", type=contracts.ColumnType.STRING),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING),
        contracts.Column(name="municipio", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="municipio_id", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            name="area_km2", type=contracts.ColumnType.FLOAT, nullable=True, unit="km2", min_value=0
        ),
        contracts.Column(name="satelite", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="sensor", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="bioma", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="cod_municipio",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Código municipal participa da chave do grupo e permanece textual",
        "Ocorrências com ID repetido permanecem na população de entrada da agregação",
        "Qualquer área municipal ausente no grupo produz total ausente",
        "Área UC e área original nunca substituem ou se somam à área municipal",
        "Seleção cortada por limite local não é publicada como agregado integral",
        "Reconciliação quantitativa não representa snapshot transacional",
    ],
)

contracts.register_contract("desmatamento_prodes_feicoes", PRODES_FEICOES_V2)
contracts.register_contract("desmatamento_deter_feicoes", DETER_FEICOES_V2)
contracts.register_contract("desmatamento_prodes", DESMATAMENTO_PRODES_V2)
contracts.register_contract("desmatamento_deter", DESMATAMENTO_DETER_V2)
