"""Módulo para coleta de dados do Notícias Agrícolas (fonte alternativa CEPEA).

AVISO: Fallback temporário para contornar Cloudflare no CEPEA.
Pendente deprecação em favor de acesso direto ao CEPEA ou fontes
primárias (DERAL, etc.). Notícias Agrícolas é empresa privada sem
termos de uso públicos — todos os direitos reservados (Lei 9.610/98).
Dados originários do CEPEA estão sujeitos a CC BY-NC 4.0.
"""

from agrobr.noticias_agricolas.client import fetch_indicador_page
from agrobr.noticias_agricolas.parser import parse_indicador

__all__ = ["fetch_indicador_page", "parse_indicador"]
