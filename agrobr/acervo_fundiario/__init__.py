"""Acervo Fundiario — Certificacao fundiaria e assentamentos (INCRA).

Fonte: certificacao.incra.gov.br/csv_shp/zip/ (download de shapefile estatico).
Licenca: dados publicos federais — classificacao livre.
"""

from agrobr.acervo_fundiario.api import (
    assentamentos,
    assentamentos_geo,
    sigef,
    sigef_geo,
    snci,
    snci_geo,
)

__all__ = [
    "assentamentos",
    "assentamentos_geo",
    "sigef",
    "sigef_geo",
    "snci",
    "snci_geo",
]
