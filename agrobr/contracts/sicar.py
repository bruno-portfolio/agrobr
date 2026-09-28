from __future__ import annotations

from typing import Any

import pandas as pd

from agrobr import constants, contracts
from agrobr.normalize import regions


class SicarContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        frame = super().empty_frame()
        for column in self.columns:
            if column.type == contracts.ColumnType.DATETIME:
                frame[column.name] = pd.Series(dtype="datetime64[ns, UTC]")
        return frame

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        for column in self.columns:
            if column.type != contracts.ColumnType.DATETIME or column.name not in df:
                continue
            dtype = df[column.name].dtype
            if not isinstance(dtype, pd.DatetimeTZDtype) or str(dtype.tz) != "UTC":
                errors.append(f"Column '{column.name}' must use a UTC datetime dtype")
        for name, allowed in (
            ("status", constants.SICAR_STATUS_VALIDOS),
            ("tipo", constants.SICAR_TIPO_VALIDOS),
            ("uf", regions.UFS_VALIDAS),
        ):
            if name in df and not df[name].isin(allowed).all():
                errors.append(f"Column '{name}' contains an invalid SICAR value")
        for name in ("cod_imovel", "municipio"):
            if name in df and not all(
                isinstance(value, str) and value.strip() for value in df[name]
            ):
                errors.append(f"Column '{name}' must contain non-empty strings")
        return not errors, errors

    def to_dict(self) -> dict[str, Any]:
        schema = super().to_dict()
        for column in schema["columns"]:
            if column["type"] == contracts.ColumnType.DATETIME.value:
                column["timezone"] = "UTC"
                schema["constraints"][f"{column['name']}_timezone"] = "UTC"
        schema["constraints"].update(
            status_allowed=sorted(constants.SICAR_STATUS_VALIDOS),
            tipo_allowed=sorted(constants.SICAR_TIPO_VALIDOS),
            uf_allowed=sorted(regions.UFS_VALIDAS),
            non_empty_strings=["cod_imovel", "municipio"],
        )
        return schema


SICAR_IMOVEIS_V2 = SicarContract(
    name="sicar.imoveis",
    version="2.1",
    effective_from="2.0.0",
    primary_key=["cod_imovel"],
    columns=[
        contracts.Column(name="cod_imovel", type=contracts.ColumnType.STRING),
        contracts.Column(name="status", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="data_criacao",
            type=contracts.ColumnType.DATETIME,
            nullable=True,
            description="Instante UTC informado pelo GeoJSON; datetime64[ns, UTC], inclusive nulos",
        ),
        contracts.Column(
            name="data_atualizacao",
            type=contracts.ColumnType.DATETIME,
            nullable=True,
            description="Instante UTC informado pelo GeoJSON; datetime64[ns, UTC], inclusive nulos",
        ),
        contracts.Column(name="area_ha", type=contracts.ColumnType.FLOAT, unit="ha", min_value=0),
        contracts.Column(name="condicao", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING),
        contracts.Column(name="municipio", type=contracts.ColumnType.STRING),
        contracts.Column(name="cod_municipio_ibge", type=contracts.ColumnType.INTEGER),
        contracts.Column(name="modulos_fiscais", type=contracts.ColumnType.FLOAT, min_value=0),
        contracts.Column(name="tipo", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="cod_municipio",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            stable=False,
            description="Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo onde a linha não é de município.",
        ),
    ],
    guarantees=[
        "Column names never change (additions only)",
        "'cod_imovel' is always non-empty",
        "'status' is always AT, PE, SU, or CA",
        "'tipo' is always IRU, AST, or PCT",
        "'area_ha' is always >= 0",
        "'uf' is always a valid Brazilian state code",
        "Datas de criação e atualização são instantes UTC, inclusive em retornos vazios",
        "Data de atualização ausente na camada permanece nula, sem atribuir outro instante",
        "Filtros incrementais não reconstituem versões históricas do cadastro",
    ],
)

contracts.register_contract("sicar_imoveis", SICAR_IMOVEIS_V2)
contracts.register_contract("cadastro_rural", SICAR_IMOVEIS_V2)
