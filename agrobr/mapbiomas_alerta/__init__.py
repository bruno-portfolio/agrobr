"""MapBiomas Alerta — Alertas de desmatamento.
Fonte: plataforma.alerta.mapbiomas.org (GraphQL, exige token).
Licença: CC BY-SA 3.0 BR, com atribuição e CompartilhaIgual nas adaptações.
"""

from agrobr.mapbiomas_alerta.api import alerta_info, alertas, alertas_geo

__all__ = ["alertas", "alertas_geo", "alerta_info"]
