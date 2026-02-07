"""Modelos Pydantic v2 para dados INMET."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


class Estacao(BaseModel):
    """Metadados de uma estação meteorológica INMET."""

    codigo: str = Field(..., alias="CD_ESTACAO")
    nome: str = Field(..., alias="DC_NOME")
    uf: str = Field(..., alias="SG_ESTADO")
    situacao: str = Field(..., alias="CD_SITUACAO")
    tipo: str = Field(..., alias="TP_ESTACAO")
    latitude: float = Field(..., alias="VL_LATITUDE")
    longitude: float = Field(..., alias="VL_LONGITUDE")
    altitude: float = Field(..., alias="VL_ALTITUDE")
    inicio_operacao: date | None = Field(None, alias="DT_INICIO_OPERACAO")
    fim_operacao: date | None = Field(None, alias="DT_FIM_OPERACAO")

    model_config = {"populate_by_name": True}

    @field_validator("latitude", "longitude", "altitude", mode="before")
    @classmethod
    def parse_float_strings(cls, v: Any) -> float:
        if isinstance(v, str):
            return float(v)
        return v

    @field_validator("inicio_operacao", "fim_operacao", mode="before")
    @classmethod
    def parse_date_strings(cls, v: Any) -> date | None:
        if v is None or v == "":
            return None
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v

    @property
    def operante(self) -> bool:
        return self.situacao == "Operante"


class ObservacaoHoraria(BaseModel):
    """Observação horária de estação automática INMET."""

    estacao: str = Field(..., alias="CD_ESTACAO")
    data: date = Field(..., alias="DT_MEDICAO")
    hora: str = Field(..., alias="HR_MEDICAO")
    uf: str = Field("", alias="UF")

    temperatura: float | None = Field(None, alias="TEM_INS")
    temperatura_max: float | None = Field(None, alias="TEM_MAX")
    temperatura_min: float | None = Field(None, alias="TEM_MIN")
    umidade: float | None = Field(None, alias="UMD_INS")
    umidade_max: float | None = Field(None, alias="UMD_MAX")
    umidade_min: float | None = Field(None, alias="UMD_MIN")
    pressao: float | None = Field(None, alias="PRE_INS")
    pressao_max: float | None = Field(None, alias="PRE_MAX")
    pressao_min: float | None = Field(None, alias="PRE_MIN")
    vento_velocidade: float | None = Field(None, alias="VEN_VEL")
    vento_direcao: float | None = Field(None, alias="VEN_DIR")
    vento_rajada: float | None = Field(None, alias="VEN_RAJ")
    radiacao_global: float | None = Field(None, alias="RAD_GLO")
    ponto_orvalho: float | None = Field(None, alias="PTO_INS")
    ponto_orvalho_max: float | None = Field(None, alias="PTO_MAX")
    ponto_orvalho_min: float | None = Field(None, alias="PTO_MIN")
    precipitacao: float | None = Field(None, alias="CHUVA")

    model_config = {"populate_by_name": True}

    SENTINEL: float = -9999.0

    @field_validator(
        "temperatura",
        "temperatura_max",
        "temperatura_min",
        "umidade",
        "umidade_max",
        "umidade_min",
        "pressao",
        "pressao_max",
        "pressao_min",
        "vento_velocidade",
        "vento_direcao",
        "vento_rajada",
        "radiacao_global",
        "ponto_orvalho",
        "ponto_orvalho_max",
        "ponto_orvalho_min",
        "precipitacao",
        mode="before",
    )
    @classmethod
    def parse_numeric(cls, v: Any) -> float | None:
        if v is None or v == "" or v == "null":
            return None
        if isinstance(v, str):
            try:
                val = float(v)
            except ValueError:
                return None
            if val == -9999.0:
                return None
            return val
        if isinstance(v, (int, float)):
            if v == -9999.0:
                return None
            return float(v)
        return None

    @field_validator("data", mode="before")
    @classmethod
    def parse_date(cls, v: Any) -> date:
        if isinstance(v, str):
            return date.fromisoformat(v)
        return v

    @property
    def datetime_utc(self) -> datetime:
        hora_str = self.hora.replace(" UTC", "").strip()
        hora_int = int(hora_str[:2])
        minuto_int = int(hora_str[2:4]) if len(hora_str) >= 4 else 0
        return datetime(
            self.data.year,
            self.data.month,
            self.data.day,
            hora_int,
            minuto_int,
        )
