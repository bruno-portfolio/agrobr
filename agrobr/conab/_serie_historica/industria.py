from __future__ import annotations

from typing import Any

import openpyxl
import pandas as pd
import pydantic
import xlrd

from agrobr import _log, constants
from agrobr.contracts import producao_acucar_etanol as contrato
from agrobr.exceptions import ParseError
from agrobr.utils.result import ATRIBUTO_AVISOS
from agrobr.utils.warnings import warn_once

from . import parser
from .models import SafraIndustrial

logger = _log.get_logger(__name__)

PRODUTO = "cana_industria"
FONTE = "conab_cana_industria"
PUBLICACAO = "série histórica industrial"
PARSER_VERSION = 1

ABAS: dict[str, tuple[str, str, str]] = {
    "acucar": ("acucar_mil_ton", "em mil toneladas", "producao de acucar"),
    "etanol anidro": (
        "etanol_anidro_cana_mil_l",
        "em mil litros",
        "etanol anidro de cana-de-acucar",
    ),
    "etanol hidratado": (
        "etanol_hidratado_cana_mil_l",
        "em mil litros",
        "etanol hidratado de cana-de-acucar",
    ),
    "etanol anidro (milho)": (
        "etanol_anidro_milho_mil_l",
        "em mil litros",
        "etanol anidro de milho",
    ),
    "etanol hidratado (milho)": (
        "etanol_hidratado_milho_mil_l",
        "em mil litros",
        "etanol hidratado de milho",
    ),
    "etanol total (cana e milho)": (
        "etanol_total_mil_l",
        "em mil litros",
        "etanol total de cana-de-acucar e milho",
    ),
    "atr medio": ("atr_kg_t", "kg/t cana", "atr medio"),
}
COMPONENTES_ETANOL = (
    "etanol_anidro_cana_mil_l",
    "etanol_hidratado_cana_mil_l",
    "etanol_anidro_milho_mil_l",
    "etanol_hidratado_milho_mil_l",
)
METRICAS = tuple(campo for campo, _, _ in ABAS.values())
_MEDIAS = frozenset({"atr_kg_t"})
_TEXTO_SEM_VALOR = frozenset({"", "-", "–", "—"})

Pendencia = tuple[str, str, str, str]


def _erro(motivo: str) -> ParseError:
    return ParseError(source=FONTE, parser_version=PARSER_VERSION, reason=motivo)


def _avisar(chave: str, aviso: str, avisos: list[str]) -> None:
    warn_once(f"conab_cana_industria:{chave}", aviso)
    avisos.append(aviso)


def resolver_abas(nomes: list[str]) -> dict[str, tuple[str, str, str]]:
    """(campo, unidade, título) de cada aba pelo nome normalizado exato: aba ausente, desconhecida
    ou repetida é mudança de layout, nunca coluna vazia ou métrica trocada."""
    abas: dict[str, tuple[str, str, str]] = {}
    vistas: dict[str, str] = {}
    for nome in nomes:
        normalizado = parser._normalize_sheet_name(nome)
        if normalizado in vistas:
            raise _erro(f"abas com o mesmo nome normalizado: {vistas[normalizado]!r} e {nome!r}")
        if normalizado not in ABAS:
            raise _erro(f"aba desconhecida {nome!r}")
        vistas[normalizado] = nome
        abas[nome] = ABAS[normalizado]
    ausentes = sorted(set(ABAS) - set(vistas))
    if ausentes:
        raise _erro(f"abas ausentes: {', '.join(ausentes)}")
    return abas


def _celulas_com_erro(xls_file: pd.ExcelFile, aba: str) -> set[tuple[int, int]]:
    livro = xls_file.book
    if isinstance(livro, xlrd.Book):
        folha = livro.sheet_by_name(aba)
        return {
            (linha, coluna)
            for linha in range(folha.nrows)
            for coluna in range(folha.ncols)
            if folha.cell_type(linha, coluna) == xlrd.XL_CELL_ERROR
        }
    if isinstance(livro, openpyxl.Workbook):
        return {
            (celula.row - 1, celula.column - 1)
            for linha in livro[aba].iter_rows()
            for celula in linha
            if celula.data_type == "e"
        }
    raise _erro(f"planilha aberta por motor sem leitura de erro de célula: {type(livro).__name__}")


def _conferir_titulo_e_unidade(
    df_raw: pd.DataFrame, cabecalho: int, aba: str, unidade: str, titulo: str
) -> None:
    acima = [parser._normalize_sheet_name(str(v)) for v in df_raw.iloc[:cabecalho, 0].dropna()]
    if not any(titulo in texto for texto in acima):
        raise _erro(f"aba {aba!r}: título sem {titulo!r} (publicado: {acima})")
    publicada = df_raw.iat[cabecalho - 1, 0] if cabecalho > 0 else None
    if pd.isna(publicada) or parser._normalize_sheet_name(str(publicada)) != unidade:
        raise _erro(f"aba {aba!r}: unidade publicada {publicada!r}, esperada {unidade!r}")


def _safras(df_raw: pd.DataFrame, cabecalho: int, aba: str) -> list[tuple[int, str]]:
    """Colunas de safra fechada. Só a última coluna pode ser estimativa, e o rodapé que anuncia
    estimativa exige a coluna marcada: nota em safra fechada não a descarta calada."""
    rotulos = [str(v) if pd.notna(v) else "" for v in df_raw.iloc[cabecalho]]
    periodos = parser.resolve_period_columns(rotulos)
    safras = [(coluna, p.safra) for coluna, p in periodos.items() if p.estado == "mapeada"]
    marcadas = [coluna for coluna, p in periodos.items() if p.estado == "ignorada"]
    if len({safra for _, safra in safras}) != len(safras):
        raise _erro(f"aba {aba!r}: safra repetida no cabeçalho {rotulos}")
    ultima_coluna = max((coluna for coluna, _ in safras), default=-1)
    if len(marcadas) > 1 or any(coluna < ultima_coluna for coluna in marcadas):
        raise _erro(f"aba {aba!r}: coluna marcada antes da última safra: {rotulos}")
    rodape = df_raw.iloc[cabecalho + 1 :, 0].dropna().astype(str)
    if not marcadas and any(parser._REFERENCIA_RE.search(texto) for texto in rodape):
        raise _erro(f"aba {aba!r}: rodapé anuncia estimativa sem coluna marcada: {rotulos}")
    return safras


def _linhas_uf(df_raw: pd.DataFrame, cabecalho: int, aba: str) -> dict[int, str]:
    linhas: dict[int, str] = {}
    for linha in range(cabecalho + 1, len(df_raw)):
        rotulo = df_raw.iat[linha, 0]
        tipo, _, uf = parser._classify_row(str(rotulo)) if pd.notna(rotulo) else ("", None, None)
        if tipo == "uf" and uf is not None:
            linhas[linha] = uf
    if sorted(linhas.values()) != sorted(constants.CONAB_UFS):
        raise _erro(f"aba {aba!r}: linhas de UF {sorted(linhas.values())} ≠ as 27 UFs")
    return linhas


def _conferir_celulas(
    df_raw: pd.DataFrame,
    cabecalho: int,
    aba: str,
    safras: list[tuple[int, str]],
    erros: set[tuple[int, int]],
) -> list[Pendencia]:
    """Recusa texto no lugar de número e devolve os erros do Excel nas células de UF, por (safra,
    UF), para avisar só no recorte pedido."""
    linhas_uf = _linhas_uf(df_raw, cabecalho, aba)
    pendencias: list[Pendencia] = []
    for coluna, safra in safras:
        for linha in range(cabecalho + 1, len(df_raw)):
            valor = df_raw.iat[linha, coluna]
            if isinstance(valor, str) and valor.strip() not in _TEXTO_SEM_VALOR:
                raise _erro(
                    f"aba {aba!r}, {df_raw.iat[linha, 0]} {safra}: texto {valor!r} no lugar de número"
                )
        for linha, uf in linhas_uf.items():
            if (linha, coluna) in erros:
                pendencias.append(
                    (
                        safra,
                        uf,
                        f"erro:{aba}:{safra}:{uf}",
                        f"CONAB: em {PRODUTO} {safra} {uf} ({PUBLICACAO}), a aba {aba!r} publica "
                        "erro do Excel no lugar do número; o agrobr devolve NaN",
                    )
                )
    return pendencias


def _ler_aba(
    xls_file: pd.ExcelFile,
    aba: str,
    layout: tuple[str, str, str],
    inicio: int | None,
    fim: int | None,
) -> tuple[
    list[tuple[str, str | None, str, float | None]], list[str], list[Pendencia], pd.DataFrame
]:
    campo, unidade, titulo = layout
    try:
        df_raw = parser._read_selected_sheet(xls_file, aba, PRODUTO)
        celulas = parser.celulas_por_uf(
            df_raw, PRODUTO, campo, inicio, fim, aba, so_levantadas=False
        )
        cabecalho = parser._find_header_row(df_raw)
        _conferir_titulo_e_unidade(df_raw, cabecalho, aba, unidade, titulo)
        safras = _safras(df_raw, cabecalho, aba)
        pendencias = _conferir_celulas(
            df_raw, cabecalho, aba, safras, _celulas_com_erro(xls_file, aba)
        )
    except ParseError as exc:
        raise _erro(f"layout inválido na aba {aba!r}: {exc.reason}") from exc
    return celulas, [safra for _, safra in safras], pendencias, df_raw


def _ler_abas(
    raw: bytes, inicio: int | None, fim: int | None
) -> tuple[
    dict[tuple[str, str], dict[str, Any]], dict[tuple[str, str], float], str, list[Pendencia]
]:
    xls_file = parser._excel_file(raw)
    linhas: dict[tuple[str, str], dict[str, Any]] = {}
    brasil: dict[tuple[str, str], float] = {}
    referencia: list[str] = []
    pendencias: list[Pendencia] = []
    for aba, layout in resolver_abas(list(map(str, xls_file.sheet_names))).items():
        celulas, safras, pendencias_aba, df_raw = _ler_aba(xls_file, aba, layout, inicio, fim)
        referencia = referencia or safras
        if safras != referencia:
            raise _erro(f"aba {aba!r} com safras {safras}, diferentes de {referencia}")
        pendencias.extend(pendencias_aba)
        campo = layout[0]
        for uf, regiao, safra, valor in celulas:
            linha = linhas.setdefault((safra, uf), {"safra": safra, "regiao": regiao, "uf": uf})
            linha[campo] = valor
        if campo not in _MEDIAS:
            brasil.update(
                {(safra, campo): valor for safra, valor in parser.brasil_por_safra(df_raw).items()}
            )
    return linhas, brasil, max(referencia), pendencias


def _avisar_total_etanol(frame: pd.DataFrame) -> list[str]:
    componentes = frame[list(COMPONENTES_ETANOL)]
    soma = componentes.fillna(0.0).sum(axis=1)
    totais = frame["etanol_total_mil_l"]
    diferenca = totais.fillna(0.0) - soma
    tolerancia = constants.CONAB_ARREDONDAMENTO * (len(COMPONENTES_ETANOL) + 1)
    avisos: list[str] = []
    for indice in frame.index[diferenca.abs() > tolerancia]:
        safra, uf = frame.at[indice, "safra"], frame.at[indice, "uf"]
        total = totais[indice]
        publicado = f"{total:.1f} mil l" if pd.notna(total) else "sem número"
        _avisar(
            f"total:{safra}:{uf}",
            f"CONAB: em {PRODUTO} {safra} {uf} ({PUBLICACAO}), o etanol total publicado "
            f"({publicado}) difere da soma do anidro e do hidratado de cana e de milho "
            f"({soma[indice]:.1f} mil l); o agrobr repassa os números publicados",
            avisos,
        )
    return avisos


def _avisar_revisao(frame: pd.DataFrame, ultima: str) -> list[str]:
    avisos: list[str] = []
    if ultima in set(frame["safra"]):
        _avisar(
            f"revisao:{ultima}",
            f"Safra {ultima} de açúcar e etanol: é a mais recente fechada na série histórica da "
            "CONAB, que pode revisá-la nos levantamentos quadrimestrais da safra de cana; o agrobr "
            "repassa os números publicados.",
            avisos,
        )
    return avisos


def _avisar_celulas(
    pendencias: list[Pendencia], inicio: int | None, fim: int | None, uf: str | None
) -> list[str]:
    avisos: list[str] = []
    for safra, uf_celula, chave, aviso in pendencias:
        ano = int(safra[:4])
        if (
            (inicio is None or ano >= inicio)
            and (fim is None or ano <= fim)
            and (uf is None or uf_celula == uf.upper())
        ):
            _avisar(chave, aviso, avisos)
    return avisos


def parse_cana_industria(
    raw: bytes, inicio: int | None = None, fim: int | None = None, uf: str | None = None
) -> pd.DataFrame:
    """Uma linha por (safra, UF), nas unidades publicadas. Traço, vazio e erro do Excel saem NaN;
    zero publicado sai 0.0, inclusive em safra zerada em todas as UFs. Divergências da própria
    planilha (etanol total × componentes, soma das UFs × BRASIL), erro de célula e a safra fechada
    mais recente viram aviso em `attrs`, sem alterar os números; layout diferente do medido
    levanta `ParseError`."""
    linhas, brasil, ultima, pendencias = _ler_abas(raw, inicio, fim)
    selecionadas = [linha for linha in linhas.values() if uf is None or linha["uf"] == uf.upper()]
    try:
        registros = [SafraIndustrial(**linha).model_dump() for linha in selecionadas]
    except pydantic.ValidationError as exc:
        raise _erro(f"valor fora do domínio: {exc}") from exc
    frame = (
        pd.DataFrame(registros, columns=list(SafraIndustrial.model_fields))
        .astype(dict.fromkeys(METRICAS, "float64"))
        .sort_values(["safra", "uf"], ignore_index=True)
        if registros
        else contrato.PRODUCAO_ACUCAR_ETANOL_V1.empty_frame()
    )
    volumes = dict.fromkeys(set(METRICAS) - _MEDIAS, 0.0)
    frame.attrs[ATRIBUTO_AVISOS] = [
        *_avisar_celulas(pendencias, inicio, fim, uf),
        *parser.avisar_soma_das_ufs(PRODUTO, PUBLICACAO, frame.fillna(volumes), brasil),
        *_avisar_total_etanol(frame),
        *_avisar_revisao(frame, ultima),
    ]
    logger.info(
        "conab_cana_industria_parsed",
        records=len(frame),
        safras=frame["safra"].nunique(),
        ufs=frame["uf"].nunique(),
        avisos=len(frame.attrs[ATRIBUTO_AVISOS]),
    )
    return frame
