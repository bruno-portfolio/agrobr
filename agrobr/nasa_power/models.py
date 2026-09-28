from __future__ import annotations

from typing import Annotated

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

from agrobr.constants import NASA_POWER_PARAMETER_DEFINITIONS
from agrobr.exceptions import InvalidParameterError

UF_COORDS: dict[str, tuple[float, float]] = {
    "AC": (-9.0, -70.8),
    "AL": (-9.6, -36.8),
    "AM": (-3.4, -65.0),
    "AP": (1.4, -51.8),
    "BA": (-12.6, -41.7),
    "CE": (-5.5, -39.3),
    "DF": (-15.8, -47.9),
    "ES": (-19.6, -40.5),
    "GO": (-15.9, -49.3),
    "MA": (-5.4, -45.3),
    "MG": (-18.5, -44.0),
    "MS": (-20.5, -54.6),
    "MT": (-12.6, -56.1),
    "PA": (-3.8, -52.3),
    "PB": (-7.1, -36.8),
    "PE": (-8.3, -37.9),
    "PI": (-7.7, -42.7),
    "PR": (-24.5, -51.5),
    "RJ": (-22.2, -42.7),
    "RN": (-5.8, -36.4),
    "RO": (-10.9, -62.8),
    "RR": (2.1, -61.4),
    "RS": (-29.8, -53.3),
    "SC": (-27.6, -50.4),
    "SE": (-10.6, -37.4),
    "SP": (-22.3, -49.1),
    "TO": (-10.2, -48.3),
}

PARAMS_AG: list[str] = [
    "T2M",
    "T2M_MAX",
    "T2M_MIN",
    "PRECTOTCORR",
    "RH2M",
    "ALLSKY_SFC_SW_DWN",
    "WS2M",
]

COLUNAS_MAP: dict[str, str] = {
    "T2M": "temp_media",
    "T2M_MAX": "temp_max",
    "T2M_MIN": "temp_min",
    "PRECTOTCORR": "precip_mm",
    "RH2M": "umidade_rel",
    "ALLSKY_SFC_SW_DWN": "radiacao_mj",
    "WS2M": "vento_ms",
}

SENTINEL: float = -999.0


class Parametro(BaseModel):
    model_config = ConfigDict(frozen=True)

    codigo: str
    coluna: str
    unidade: str
    coluna_mensal: str
    unidade_mensal: str
    agregacao_mensal: str = "mean_available_daily_values"
    comunidade: str = "AG"
    frequencia_origem: str = "diario"


CATALOGO = tuple(
    Parametro(
        codigo=code,
        coluna=column,
        unidade=unit,
        coluna_mensal=monthly,
        unidade_mensal=monthly_unit,
        agregacao_mensal="sum_available_daily_values"
        if code == "PRECTOTCORR"
        else "mean_available_daily_values",
    )
    for code, column, unit, monthly, monthly_unit in NASA_POWER_PARAMETER_DEFINITIONS
)


def validate_parameters(parameters: list[str] | None) -> list[str]:
    if parameters is None:
        return list(PARAMS_AG)
    if not isinstance(parameters, list) or not 1 <= len(parameters) <= 20:
        raise InvalidParameterError("parameters deve ser lista com 1 a 20 códigos NASA POWER")
    supported = {item.codigo for item in CATALOGO}
    if any(not isinstance(code, str) or code not in supported for code in parameters):
        raise InvalidParameterError(f"parameters contém código não suportado: {sorted(supported)}")
    if len(set(parameters)) != len(parameters):
        raise InvalidParameterError("parameters não aceita códigos duplicados")
    return list(parameters)


def _reject_boolean(value: object) -> object:
    if isinstance(value, bool):
        raise ValueError("valor climático não pode ser booleano")
    return value


DailyValue = Annotated[float, BeforeValidator(_reject_boolean), Field(allow_inf_nan=False)]


class DailyProperties(BaseModel):
    parameter: dict[str, dict[str, DailyValue | None]] = Field(min_length=1)


class ParameterMetadata(BaseModel):
    units: str
    longname: str = ""


class DailyHeader(BaseModel):
    fill_value: DailyValue
    time_standard: str
    sources: list[str] = Field(default_factory=list)


class DailyResponse(BaseModel):
    properties: DailyProperties
    parameters: dict[str, ParameterMetadata]
    header: DailyHeader
