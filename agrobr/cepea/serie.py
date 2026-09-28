from __future__ import annotations

import io
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import structlog

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.models import Indicador
from agrobr.normalize import dates
from agrobr.utils import io as io_utils

from .parsers import v1

logger = structlog.get_logger()

PARSER_VERSION = constants.CEPEA_SERIE_PARSER_VERSION
_LINHA_DO_CABECALHO = 3


def _erro(motivo: str) -> ParseError:
    return ParseError(source="cepea", parser_version=PARSER_VERSION, reason=motivo)


def ler_planilha(conteudo: bytes) -> pd.DataFrame:
    """Lê a 1ª aba da série; o `xlrd` com `ignore_workbook_corruption` só entra se o `calamine` falhar.

    Os XLS do CEPEA trazem uma irregularidade no documento composto que o `xlrd` recusa por padrão.
    Ignorá-la não pode esconder arquivo truncado: no recuo, as linhas lidas têm de fechar com as
    declaradas no registro DIMENSIONS, que o CEPEA grava com 1 linha a mais.
    """
    io_utils.check_xlsx_expansion(conteudo, source="cepea")
    try:
        return pd.read_excel(io.BytesIO(conteudo), engine="calamine", sheet_name=0, header=None)
    except Exception as primario:
        logger.warning("cepea_serie_recuo_xlrd", erro=str(primario))
    livro = pd.ExcelFile(
        io.BytesIO(conteudo),
        engine="xlrd",
        engine_kwargs={"ignore_workbook_corruption": True, "logfile": io.StringIO()},
    )
    aba = livro.book.sheet_by_index(0)
    if aba.nrows < aba._dimnrows - 1:
        raise _erro(f"série truncada: {aba.nrows} linhas lidas de {aba._dimnrows - 1} declaradas")
    return livro.parse(0, header=None)


def _conferir(tabela: pd.DataFrame, titulo: str, cabecalho: list[str]) -> pd.DataFrame:
    publicado = str(tabela.iat[0, 0]).strip() if len(tabela) > _LINHA_DO_CABECALHO else ""
    if not re.search(titulo, publicado.upper()):
        raise _erro(f"título da série {publicado!r} não corresponde a {titulo!r}")
    lido = [str(valor).strip() for valor in tabela.iloc[_LINHA_DO_CABECALHO] if pd.notna(valor)]
    if lido != cabecalho:
        raise _erro(f"cabeçalho da série {lido} diverge de {cabecalho}")
    return tabela.iloc[_LINHA_DO_CABECALHO + 1 :].dropna(how="all")


def _dia(texto: Any, linha: int) -> date:
    try:
        return datetime.strptime(str(texto).strip(), "%d/%m/%Y").date()
    except ValueError:
        raise _erro(f"data {texto!r} fora do padrão dd/mm/aaaa na linha {linha}") from None


def _numero(valor: Any, linha: int) -> float | None:
    """Valor da célula; vazio ou não positivo sai ``None``, como na página (`_parse_decimal`)."""
    if pd.isna(valor):
        return None
    try:
        numero = float(valor)
    except (TypeError, ValueError):
        raise _erro(f"valor {valor!r} não numérico na linha {linha}") from None
    return numero if numero > 0 else None


def _indicador(produto: str, praca: str, dia: date, valor: float, usd: float | None) -> Indicador:
    return Indicador(
        fonte=constants.Fonte.CEPEA,
        produto=produto,
        praca=praca,
        data=dia,
        valor=Decimal(str(valor)),
        unidade=v1.CepeaParserV1()._detect_unidade(produto, []),
        metodologia="indicador_esalq",
        revisao=0,
        meta={} if usd is None else {"valor_usd": usd},
        parser_version=PARSER_VERSION,
    )


def _linhas(dados: pd.DataFrame) -> Any:
    return (
        (posicao + _LINHA_DO_CABECALHO + 2, linha)
        for posicao, linha in enumerate(dados.itertuples(index=False))
    )


def _leite(dados: pd.DataFrame) -> list[Indicador]:
    indicadores = []
    for numero, (ano, mes, estado, preco) in _linhas(dados):
        mes_numero = dates.month_to_number(str(mes))
        valor = _numero(preco, numero)
        if mes_numero is None or pd.isna(ano):
            raise _erro(f"mês {ano!r}/{mes!r} fora do padrão na linha {numero}")
        if valor is not None:
            dia = date(int(ano), mes_numero, 1)
            indicadores.append(_indicador("leite", str(estado).strip(), dia, valor, None))
    return indicadores


def _suino(dados: pd.DataFrame, pracas: dict[str, str]) -> list[Indicador]:
    indicadores = []
    for numero, (texto, *valores) in _linhas(dados):
        dia = _dia(texto, numero)
        for rotulo, bruto in zip(pracas.values(), valores, strict=True):
            valor = _numero(bruto, numero)
            if valor is not None:
                indicadores.append(_indicador("suino", rotulo, dia, valor, None))
    return indicadores


def parse_serie(conteudo: bytes, produto: str, papel: str = "") -> list[Indicador]:
    """Indicadores da planilha de série histórica de ``produto``.

    ``papel`` é a praça da série quando o produto tem uma série por praça (trigo); vazio usa a praça
    da página. O cabeçalho esperado depende do produto: leite mensal, suíno por UF, os demais diários.
    """
    tabela = ler_planilha(conteudo)
    titulo = (
        constants.CEPEA_TABELAS_POR_PRACA.get(produto, {}).get(papel)
        or constants.CEPEA_TITULOS[produto]
    )
    if produto == "leite":
        return _leite(_conferir(tabela, titulo, ["Ano", "Mês", "Estado", "Preço médio"]))
    if produto == "suino":
        pracas = {
            rotulo.split(" ")[0]: rotulo for rotulo in constants.CEPEA_PRACAS_REGIONAIS["suino"]
        }
        return _suino(_conferir(tabela, titulo, ["Data", *pracas]), pracas)
    publicado = [
        str(valor).strip() for valor in tabela.iloc[_LINHA_DO_CABECALHO] if pd.notna(valor)
    ]
    prazo = publicado[1].removesuffix(" R$") if len(publicado) == 3 else "À vista"
    dados = _conferir(tabela, titulo, ["Data", f"{prazo} R$", f"{prazo} US$"])
    praca = papel or v1.PRACAS[produto]
    indicadores = []
    for numero, (texto, reais, dolares) in _linhas(dados):
        valor = _numero(reais, numero)
        if valor is not None:
            dia = _dia(texto, numero)
            indicadores.append(_indicador(produto, praca, dia, valor, _numero(dolares, numero)))
    return indicadores


def parse_peso(conteudo: bytes) -> dict[date, float]:
    dados = _conferir(
        ler_planilha(conteudo), constants.CEPEA_SERIE_TITULO_PESO, ["Data", "Peso Médio"]
    )
    pesos = {}
    for numero, (texto, bruto) in _linhas(dados):
        peso = _numero(bruto, numero)
        if peso is not None:
            pesos[_dia(texto, numero)] = peso
    return pesos
