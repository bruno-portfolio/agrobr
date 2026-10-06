from agrobr.alt.anp_diesel import parser
from agrobr.exceptions import ParseError
from tests import helpers


def test_vendas_recusa_regiao_publicada_desconhecida():
    content = (
        b"ANO;MES;GRANDE REGIAO;UNIDADE DA FEDERACAO;PRODUTO;VENDAS\n"
        b"2024;JAN;REGIAO ATLANTIDA;SAO PAULO;OLEO DIESEL;800000\n"
    )
    with helpers.levanta_exatamente(ParseError, "Região publicada inválida"):
        parser.parse_vendas(content)
