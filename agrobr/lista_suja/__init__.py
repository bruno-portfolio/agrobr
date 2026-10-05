"""Lista Suja — Cadastro de empregadores que submeteram trabalhadores a condicoes de escravidao.
Fonte: gov.br/trabalho-e-emprego (CSV do cadastro, sem auth; PDF com formato='pdf' ou como
fallback quando o CSV nao e anunciado ou falha).
Licença: CC BY-ND 3.0 no portal do MTE, sem licença individual dos arquivos; uso com atribuição,
sem distribuir adaptação.
"""

from agrobr.lista_suja.api import empregadores

__all__ = ["empregadores"]
