from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, FiniteFloat, field_validator

from agrobr.constants import URLS, Fonte
from agrobr.normalize.regions import UFS

MAPSERVER: str = URLS[Fonte.CNUC]["mapserver"]
MAPFILE = "/var/www/storage/app/mapfiles/ucs.map"
WFS_VERSION = "2.0.0"
TYPENAME = "ms:ucs_selected"
GEOM_COLUMN = "msGeometry"
CRS_URN_4326 = "urn:ogc:def:crs:EPSG::4326"
LIMITE_UC = "uc"
MAX_FEATURES_TABULAR = 10_000
MAX_FEATURES_GEO = 600

NS_WFS = "http://www.opengis.net/wfs/2.0"
NS_FES = "http://www.opengis.net/fes/2.0"
NS_GML = "http://www.opengis.net/gml/3.2"
NS_MS = "http://mapserver.gis.umn.edu/mapserver"

COLUNAS_BIOMA: dict[str, str] = {
    "amazonia": "Amazônia",
    "caatinga": "Caatinga",
    "cerrado": "Cerrado",
    "matlantica": "Mata Atlântica",
    "pampa": "Pampa",
    "pantanal": "Pantanal",
}

PROPERTY_NAMES = [
    "cd_cnuc",
    "nome_uc",
    "esfera",
    "categoria",
    "grupo",
    "cat_iucn",
    "uf",
    "municipio",
    "org_gestor",
    "ha_total",
    "cria_ano",
    "cria_ato",
    "quali_pol",
    "wdpa_pid",
    *COLUNAS_BIOMA,
    "limite",
]

ESFERAS: dict[str, str] = {
    "federal": "Federal",
    "estadual": "Estadual",
    "municipal": "Municipal",
}

GRUPOS: dict[str, str] = {
    "PI": "Proteção Integral",
    "US": "Uso Sustentável",
}

CATEGORIAS = (
    "Área de Proteção Ambiental",
    "Área de Relevante Interesse Ecológico",
    "Estação Ecológica",
    "Floresta",
    "Monumento Natural",
    "Parque",
    "Refúgio de Vida Silvestre",
    "Reserva Biológica",
    "Reserva de Desenvolvimento Sustentável",
    "Reserva de Fauna",
    "Reserva Extrativista",
    "Reserva Particular do Patrimônio Natural",
)

UF_POR_NOME: dict[str, str] = {str(info["nome"]).upper(): sigla for sigla, info in UFS.items()}

COLUNAS_SAIDA = [
    "codigo",
    "nome",
    "esfera",
    "categoria",
    "grupo",
    "categoria_iucn",
    "uf",
    "municipios",
    "bioma",
    "area_ha",
    "data_criacao",
    "ato_criacao",
    "orgao_gestor",
    "qualidade_poligono",
    "wdpa_id",
]

COLUNAS_SAIDA_GEO = [*COLUNAS_SAIDA, "geometry"]


def _texto_ou_nulo(value: Any) -> Any:
    if isinstance(value, str) and not value.strip():
        return None
    return value


class UnidadeConservacao(BaseModel):
    cd_cnuc: str = Field(min_length=1)
    nome_uc: str = Field(min_length=1)
    esfera: str
    categoria: str
    grupo: str
    cat_iucn: str | None = None
    uf: str
    municipio: str = Field(min_length=1)
    org_gestor: str | None = None
    ha_total: FiniteFloat | None = None
    cria_ano: datetime | None = None
    cria_ato: str | None = None
    quali_pol: str | None = None
    wdpa_pid: str | None = None
    amazonia: FiniteFloat | None = Field(default=None, ge=0)
    caatinga: FiniteFloat | None = Field(default=None, ge=0)
    cerrado: FiniteFloat | None = Field(default=None, ge=0)
    matlantica: FiniteFloat | None = Field(default=None, ge=0)
    pampa: FiniteFloat | None = Field(default=None, ge=0)
    pantanal: FiniteFloat | None = Field(default=None, ge=0)
    limite: str

    @field_validator("*", mode="before")
    @classmethod
    def vazio_vira_nulo(cls, value: Any) -> Any:
        return _texto_ou_nulo(value)

    @field_validator("esfera")
    @classmethod
    def esfera_publicada(cls, value: str) -> str:
        esferas = {publicada: chave for chave, publicada in ESFERAS.items()}
        if value not in esferas:
            raise ValueError(f"esfera fora do domínio publicado: {value!r}")
        return esferas[value]

    @field_validator("categoria")
    @classmethod
    def categoria_publicada(cls, value: str) -> str:
        if value not in CATEGORIAS:
            raise ValueError(f"categoria fora do domínio publicado: {value!r}")
        return value

    @field_validator("grupo")
    @classmethod
    def grupo_publicado(cls, value: str) -> str:
        grupos = {publicado: sigla for sigla, publicado in GRUPOS.items()}
        if value not in grupos:
            raise ValueError(f"grupo fora do domínio publicado: {value!r}")
        return grupos[value]

    @field_validator("uf")
    @classmethod
    def siglas_da_uf(cls, value: str) -> str:
        nomes = [nome.strip() for nome in value.split(",")]
        desconhecidos = [nome for nome in nomes if nome not in UF_POR_NOME]
        if desconhecidos:
            raise ValueError(f"UF fora do cadastro: {desconhecidos}")
        return "/".join(sorted({UF_POR_NOME[nome] for nome in nomes}))

    @field_validator("cria_ano", mode="before")
    @classmethod
    def data_dia_mes_ano(cls, value: Any) -> Any:
        value = _texto_ou_nulo(value)
        if value is None:
            return None
        return datetime.strptime(value, "%d-%m-%Y")

    @field_validator("limite")
    @classmethod
    def limite_da_uc(cls, value: str) -> str:
        if value != LIMITE_UC:
            raise ValueError(f"feição sem limite de UC: {value!r}")
        return value

    def bioma(self) -> str | None:
        biomas = [nome for campo, nome in COLUNAS_BIOMA.items() if (getattr(self, campo) or 0) > 0]
        return "/".join(biomas) or None

    def saida(self) -> dict[str, Any]:
        return {
            "codigo": self.cd_cnuc,
            "nome": self.nome_uc,
            "esfera": self.esfera,
            "categoria": self.categoria,
            "grupo": self.grupo,
            "categoria_iucn": self.cat_iucn,
            "uf": self.uf,
            "municipios": self.municipio,
            "bioma": self.bioma(),
            "area_ha": self.ha_total,
            "data_criacao": self.cria_ano,
            "ato_criacao": self.cria_ato,
            "orgao_gestor": self.org_gestor,
            "qualidade_poligono": self.quali_pol,
            "wdpa_id": self.wdpa_pid,
        }


class FeatureCount(BaseModel):
    number_matched: int = Field(ge=0)

    @field_validator("number_matched", mode="before")
    @classmethod
    def decimal_count(cls, value: Any) -> int:
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError("numberMatched deve ser um inteiro decimal não negativo")
        return int(value)
