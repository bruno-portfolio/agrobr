"""Módulo CEPEA - Indicadores de preços agrícolas.

Dados CEPEA/ESALQ licenciados sob CC BY-NC 4.0.
Uso comercial requer autorização do CEPEA (cepea@usp.br).
Ref: https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx
"""

from __future__ import annotations

from agrobr.cepea.api import indicador, pracas, produtos, ultimo

__all__ = ["indicador", "produtos", "pracas", "ultimo"]
