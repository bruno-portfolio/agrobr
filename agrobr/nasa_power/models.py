"""Modelos e constantes para dados NASA POWER."""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel, Field, field_validator

# Coordenadas centrais aproximadas de cada UF brasileira (lat, lon).
# Usadas como ponto representativo para consultas NASA POWER.
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

# Parametros agroclimáticos da API NASA POWER (comunidade AG).
PARAMS_AG: list[str] = [
    "T2M",  # Temperatura media a 2m (C)
    "T2M_MAX",  # Temperatura maxima a 2m (C)
    "T2M_MIN",  # Temperatura minima a 2m (C)
    "PRECTOTCORR",  # Precipitacao corrigida (mm/dia)
    "RH2M",  # Umidade relativa a 2m (%)
    "ALLSKY_SFC_SW_DWN",  # Radiacao solar incidente (MJ/m2/dia)
    "WS2M",  # Velocidade do vento a 2m (m/s)
]

# Mapeamento parametro NASA -> nome normalizado agrobr.
COLUNAS_MAP: dict[str, str] = {
    "T2M": "temp_media",
    "T2M_MAX": "temp_max",
    "T2M_MIN": "temp_min",
    "PRECTOTCORR": "precip_mm",
    "RH2M": "umidade_rel",
    "ALLSKY_SFC_SW_DWN": "radiacao_mj",
    "WS2M": "vento_ms",
}

# Valor sentinela da NASA POWER para dados indisponiveis.
SENTINEL: float = -999.0


class ClimaObservacao(BaseModel):
    """Observacao climatica diaria de um ponto NASA POWER."""

    data: date
    lat: float
    lon: float
    uf: str = Field("", max_length=2)

    temp_media: float | None = None
    temp_max: float | None = None
    temp_min: float | None = None
    precip_mm: float | None = None
    umidade_rel: float | None = None
    radiacao_mj: float | None = None
    vento_ms: float | None = None

    model_config = {"populate_by_name": True}

    @field_validator(
        "temp_media",
        "temp_max",
        "temp_min",
        "precip_mm",
        "umidade_rel",
        "radiacao_mj",
        "vento_ms",
        mode="before",
    )
    @classmethod
    def convert_sentinel(cls, v: Any) -> float | None:
        if v is None:
            return None
        try:
            val = float(v)
        except (ValueError, TypeError):
            return None
        if val == SENTINEL:
            return None
        return val
