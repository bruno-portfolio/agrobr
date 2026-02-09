"""USDA — United States Department of Agriculture.

Dados PSD (Production, Supply, Distribution) para commodities agrícolas.
Fonte: USDA FAS OpenData API v2 (https://apps.fas.usda.gov/OpenData/api).

Requer API key gratuita: https://api.data.gov/signup/
"""

from agrobr.usda.api import psd

__all__ = ["psd"]
