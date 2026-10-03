"""Módulo para coleta de dados do Notícias Agrícolas (fonte alternativa CEPEA).

AVISO: fonte privada com classificação zona_cinza, sem licença própria de
reutilização das cotações localizada. O fallback automático emite aviso na
primeira chamada.
Dados originários do CEPEA estão sujeitos a CC BY-NC 4.0.
"""

from agrobr.noticias_agricolas.client import fetch_indicador_page
from agrobr.noticias_agricolas.parser import parse_indicador

__all__ = ["fetch_indicador_page", "parse_indicador"]
