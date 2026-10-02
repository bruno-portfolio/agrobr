from __future__ import annotations

import re
import unicodedata
from typing import Literal

import pandas as pd

from agrobr.exceptions import InvalidParameterError, UnknownNameError

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

UFS: dict[str, dict[str, str | int]] = {
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

UFS_VALIDAS: frozenset[str] = frozenset(UFS)

REGIOES: dict[str, list[str]] = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}


def _texto(valor: object, parametro: str) -> str:
    if not isinstance(valor, str):
        raise InvalidParameterError(f"{parametro} deve ser texto, recebeu {valor!r}")
    return valor


def remover_acentos(texto: str) -> str:
    nfkd = unicodedata.normalize("NFKD", _texto(texto, "texto"))
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def slugificar_praca(praca: str) -> str:
    sem_uf = re.sub(r"\s*/\s*[A-Za-z]{2}\s*$", "", _texto(praca, "praca").strip())
    sem_acentos = remover_acentos(sem_uf).lower()
    return re.sub(r"[^a-z0-9]+", "_", sem_acentos).strip("_")


NOMES_PARA_UF: dict[str, str] = {
    remover_acentos(str(info["nome"]).lower()): uf for uf, info in UFS.items()
} | {uf.lower(): uf for uf in UFS}


def normalizar_uf(entrada: str) -> str | None:
    entrada_norm = remover_acentos(_texto(entrada, "entrada").strip().lower())

    if entrada_norm.upper() in UFS:
        return entrada_norm.upper()

    if entrada_norm in NOMES_PARA_UF:
        return NOMES_PARA_UF[entrada_norm]

    for nome in sorted(NOMES_PARA_UF, key=len, reverse=True):
        if re.search(rf"(?<!\S){re.escape(nome)}(?!\S)", entrada_norm):
            return NOMES_PARA_UF[nome]

    return None


def sigla_uf(uf: str) -> str:
    sigla = uf.strip().upper() if isinstance(uf, str) else ""
    if sigla not in UFS:
        raise UnknownNameError(f"UF inválida: {uf!r}. Valores válidos: {', '.join(sorted(UFS))}")
    return sigla


def uf_para_nome(uf: str) -> str:
    return str(UFS[sigla_uf(uf)]["nome"])


def uf_para_regiao(uf: str) -> str:
    return str(UFS[sigla_uf(uf)]["regiao"])


def uf_para_ibge(uf: str) -> int:
    return int(UFS[sigla_uf(uf)]["ibge"])


def ibge_para_uf(codigo: int) -> str:
    for uf, info in UFS.items():
        if info["ibge"] == codigo:
            return uf
    raise InvalidParameterError(f"Código IBGE inválido: {codigo}")


def cod_municipio(codigos: pd.Series) -> pd.Series:
    """Código IBGE de município em ``Int64``: 7 dígitos com o prefixo de uma UF.

    Número ou texto; o que não for código de município (UF, Brasil, marcador ou ausente) vira nulo.
    """
    numeros = pd.to_numeric(codigos, errors="coerce")
    prefixos = {int(info["ibge"]) for info in UFS.values()}
    municipal = numeros.mod(1).eq(0) & numeros.floordiv(100_000).isin(prefixos)
    return numeros.where(municipal.fillna(False).astype(bool)).astype("Int64")


def listar_ufs(regiao: str | None = None) -> list[str]:
    if regiao is None:
        return list(UFS.keys())
    if regiao not in REGIOES:
        raise InvalidParameterError(
            f"Região inválida: {regiao!r}. Valores válidos: {', '.join(REGIOES)}"
        )
    return list(REGIOES[regiao])


def listar_regioes() -> list[str]:
    return list(REGIOES.keys())


def normalizar_municipio(nome: str) -> str:
    nome = _texto(nome, "nome").strip()

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


def validar_uf(uf: str) -> bool:
    return _texto(uf, "uf").upper() in UFS


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


BIOMAS: dict[str, str] = {
    "amazonia": "Amazônia",
    "amazônia": "Amazônia",
    "cerrado": "Cerrado",
    "mata atlantica": "Mata Atlântica",
    "mata atlântica": "Mata Atlântica",
    "caatinga": "Caatinga",
    "pampa": "Pampa",
    "pantanal": "Pantanal",
}

BIOMAS_VALIDOS: frozenset[str] = frozenset(BIOMAS.values())


def normalizar_bioma(bioma: str) -> str:
    key = _texto(bioma, "bioma").strip().lower()
    return BIOMAS.get(key, bioma.strip())


def normalizar_praca(praca: str, produto: str | None = None) -> str:
    praca_norm = remover_acentos(_texto(praca, "praca").lower().strip())
    produto_norm = None if produto is None else _texto(produto, "produto").lower()

    if produto_norm and produto_norm in PRACAS_CEPEA:
        pracas_produto = PRACAS_CEPEA[produto_norm]
        for praca_padrao in pracas_produto:
            if praca_padrao in praca_norm or praca_norm in praca_padrao:
                return praca_padrao.title()

    return normalizar_municipio(praca)
