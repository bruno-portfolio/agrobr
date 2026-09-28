"""USDA — United States Department of Agriculture.

Dados PSD (Production, Supply, Distribution) para commodities agrícolas.
Fonte: gateway da API PSD do USDA FAS (https://api.fas.usda.gov/api/psd).

Requer API key gratuita: https://api.data.gov/signup/
"""

from agrobr.usda.api import psd

__all__ = ["psd"]
