"""Lista Suja — Cadastro de empregadores que submeteram trabalhadores a condicoes de escravidao.
Fonte: gov.br/trabalho-e-emprego (CSV do cadastro, sem auth; PDF com formato='pdf' ou como
fallback quando o CSV nao e anunciado ou falha).
Licenca: Livre (Lei de Acesso a Informacao).
"""

from agrobr.lista_suja.api import empregadores

__all__ = ["empregadores"]
