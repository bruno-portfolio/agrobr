"""ANA/SNIRH — Dados hidrologicos, irrigacao e massas d'agua.
Fonte: portal1.snirh.gov.br e www.snirh.gov.br (ArcGIS REST MapServer, sem auth).
Licenca: Livre (dados publicos governo federal).
"""

from agrobr.ana.api import (
    demanda_irrigacao,
    demanda_irrigacao_geo,
    disponibilidade_hidrica,
    disponibilidade_hidrica_geo,
    hidrografia,
    hidrografia_geo,
    massas_dagua,
    massas_dagua_geo,
    pivos_irrigacao,
    pivos_irrigacao_geo,
)

__all__ = [
    "demanda_irrigacao",
    "demanda_irrigacao_geo",
    "disponibilidade_hidrica",
    "disponibilidade_hidrica_geo",
    "hidrografia",
    "hidrografia_geo",
    "massas_dagua",
    "massas_dagua_geo",
    "pivos_irrigacao",
    "pivos_irrigacao_geo",
]
