"""
Padronização de regiões, UFs e municípios brasileiros.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Literal

UF = Literal[
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]

Regiao = Literal["Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"]

UFS: dict[str, dict] = {
    "AC": {"nome": "Acre", "regiao": "Norte", "ibge": 12},
    "AL": {"nome": "Alagoas", "regiao": "Nordeste", "ibge": 27},
    "AP": {"nome": "Amapá", "regiao": "Norte", "ibge": 16},
    "AM": {"nome": "Amazonas", "regiao": "Norte", "ibge": 13},
    "BA": {"nome": "Bahia", "regiao": "Nordeste", "ibge": 29},
    "CE": {"nome": "Ceará", "regiao": "Nordeste", "ibge": 23},
    "DF": {"nome": "Distrito Federal", "regiao": "Centro-Oeste", "ibge": 53},
    "ES": {"nome": "Espírito Santo", "regiao": "Sudeste", "ibge": 32},
    "GO": {"nome": "Goiás", "regiao": "Centro-Oeste", "ibge": 52},
    "MA": {"nome": "Maranhão", "regiao": "Nordeste", "ibge": 21},
    "MT": {"nome": "Mato Grosso", "regiao": "Centro-Oeste", "ibge": 51},
    "MS": {"nome": "Mato Grosso do Sul", "regiao": "Centro-Oeste", "ibge": 50},
    "MG": {"nome": "Minas Gerais", "regiao": "Sudeste", "ibge": 31},
    "PA": {"nome": "Pará", "regiao": "Norte", "ibge": 15},
    "PB": {"nome": "Paraíba", "regiao": "Nordeste", "ibge": 25},
    "PR": {"nome": "Paraná", "regiao": "Sul", "ibge": 41},
    "PE": {"nome": "Pernambuco", "regiao": "Nordeste", "ibge": 26},
    "PI": {"nome": "Piauí", "regiao": "Nordeste", "ibge": 22},
    "RJ": {"nome": "Rio de Janeiro", "regiao": "Sudeste", "ibge": 33},
    "RN": {"nome": "Rio Grande do Norte", "regiao": "Nordeste", "ibge": 24},
    "RS": {"nome": "Rio Grande do Sul", "regiao": "Sul", "ibge": 43},
    "RO": {"nome": "Rondônia", "regiao": "Norte", "ibge": 11},
    "RR": {"nome": "Roraima", "regiao": "Norte", "ibge": 14},
    "SC": {"nome": "Santa Catarina", "regiao": "Sul", "ibge": 42},
    "SP": {"nome": "São Paulo", "regiao": "Sudeste", "ibge": 35},
    "SE": {"nome": "Sergipe", "regiao": "Nordeste", "ibge": 28},
    "TO": {"nome": "Tocantins", "regiao": "Norte", "ibge": 17},
}

REGIOES: dict[str, list[str]] = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}


def remover_acentos(texto: str) -> str:
    """Remove acentos de uma string."""
    nfkd = unicodedata.normalize("NFKD", texto)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


NOMES_PARA_UF: dict[str, str] = {
    remover_acentos(info["nome"].lower()): uf for uf, info in UFS.items()
} | {uf.lower(): uf for uf in UFS}


def normalizar_uf(entrada: str) -> str | None:
    """
    Normaliza entrada para sigla UF.

    Args:
        entrada: Sigla ou nome do estado

    Returns:
        Sigla UF ou None se não encontrado

    Examples:
        >>> normalizar_uf('mato grosso')
        'MT'
        >>> normalizar_uf('SP')
        'SP'
        >>> normalizar_uf('são paulo')
        'SP'
    """
    entrada_norm = remover_acentos(entrada.strip().lower())

    if entrada_norm.upper() in UFS:
        return entrada_norm.upper()

    if entrada_norm in NOMES_PARA_UF:
        return NOMES_PARA_UF[entrada_norm]

    for nome, uf in NOMES_PARA_UF.items():
        if nome in entrada_norm or entrada_norm in nome:
            return uf

    return None


def uf_para_nome(uf: str) -> str:
    """
    Retorna nome completo da UF.

    Args:
        uf: Sigla da UF

    Returns:
        Nome completo

    Raises:
        KeyError: Se UF inválida
    """
    return UFS[uf.upper()]["nome"]


def uf_para_regiao(uf: str) -> str:
    """
    Retorna região da UF.

    Args:
        uf: Sigla da UF

    Returns:
        Nome da região
    """
    return UFS[uf.upper()]["regiao"]


def uf_para_ibge(uf: str) -> int:
    """
    Retorna código IBGE da UF.

    Args:
        uf: Sigla da UF

    Returns:
        Código IBGE (2 dígitos)
    """
    return UFS[uf.upper()]["ibge"]


def ibge_para_uf(codigo: int) -> str:
    """
    Converte código IBGE para sigla UF.

    Args:
        codigo: Código IBGE

    Returns:
        Sigla UF
    """
    for uf, info in UFS.items():
        if info["ibge"] == codigo:
            return uf
    raise ValueError(f"Código IBGE inválido: {codigo}")


def listar_ufs(regiao: str | None = None) -> list[str]:
    """
    Lista UFs, opcionalmente filtradas por região.

    Args:
        regiao: Filtrar por região

    Returns:
        Lista de siglas UF
    """
    if regiao:
        return REGIOES.get(regiao, [])
    return list(UFS.keys())


def listar_regioes() -> list[str]:
    """Retorna lista de regiões."""
    return list(REGIOES.keys())


def normalizar_municipio(nome: str) -> str:
    """
    Normaliza nome de município.

    Remove acentos, converte para title case, trata casos especiais.

    Args:
        nome: Nome do município

    Returns:
        Nome normalizado
    """
    nome = nome.strip()

    nome = re.sub(r"\s+", " ", nome)

    palavras_minusculas = {"de", "da", "do", "das", "dos", "e"}

    partes = nome.lower().split()
    resultado = []

    for i, parte in enumerate(partes):
        if i == 0 or parte not in palavras_minusculas:
            resultado.append(parte.capitalize())
        else:
            resultado.append(parte)

    return " ".join(resultado)


def extrair_uf_municipio(texto: str) -> tuple[str | None, str | None]:
    """
    Extrai UF e município de texto no formato "Município - UF" ou "Município/UF".

    Args:
        texto: Texto com município e UF

    Returns:
        Tupla (uf, municipio) ou (None, None) se não identificado
    """
    padrao = re.compile(r"^(.+?)[\s]*[-/][\s]*([A-Za-z]{2})$")
    match = padrao.match(texto.strip())

    if match:
        municipio = normalizar_municipio(match.group(1))
        uf = normalizar_uf(match.group(2))
        return uf, municipio

    return None, None


def validar_uf(uf: str) -> bool:
    """Verifica se UF é válida."""
    return uf.upper() in UFS


def validar_regiao(regiao: str) -> bool:
    """Verifica se região é válida."""
    return regiao in REGIOES


PRACAS_CEPEA: dict[str, dict[str, str]] = {
    "soja": {
        "paranagua": "PR",
        "rio grande": "RS",
        "santos": "SP",
    },
    "milho": {
        "campinas": "SP",
        "cascavel": "PR",
        "rio verde": "GO",
    },
    "boi_gordo": {
        "sao paulo": "SP",
        "araçatuba": "SP",
        "presidente prudente": "SP",
    },
    "cafe": {
        "sao paulo": "SP",
        "mogiana": "SP",
        "sul de minas": "MG",
    },
}


def normalizar_praca(praca: str, produto: str | None = None) -> str:
    """
    Normaliza nome de praça de comercialização.

    Args:
        praca: Nome da praça
        produto: Produto (para contexto)

    Returns:
        Nome normalizado
    """
    praca_norm = remover_acentos(praca.lower().strip())

    if produto and produto.lower() in PRACAS_CEPEA:
        pracas_produto = PRACAS_CEPEA[produto.lower()]
        for praca_padrao in pracas_produto:
            if praca_padrao in praca_norm or praca_norm in praca_padrao:
                return praca_padrao.title()

    return normalizar_municipio(praca)
