"""FUNAI — Terras Indigenas do Brasil.
Fonte: geoserver.funai.gov.br (WFS OGC, sem auth).
Licenca: termo da FUNAI para geoprocessamento e mapas — reproducao com citacao da fonte.
"""

from agrobr.funai.api import terras_indigenas, terras_indigenas_geo

__all__ = ["terras_indigenas", "terras_indigenas_geo"]
