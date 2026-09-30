from __future__ import annotations

import re
from typing import Annotated, Any
from urllib.parse import urlsplit

from pydantic import BaseModel, BeforeValidator, ConfigDict, field_validator, model_validator

from agrobr import constants
from agrobr.constants import URLS, Fonte
from agrobr.exceptions import ParseError
from agrobr.normalize import regions

CKAN_API = URLS[Fonte.ZARC]["ckan_api"]
DATASET_SLUG = "tabua-de-risco-zoneamento-agricola-de-risco-climatico"

_CULTURAS_ANUAIS: dict[str, str] = {
    "Algodão Herbáceo": "algodao",
    "Amendoim": "amendoim",
    "Arroz": "arroz",
    "Aveia": "aveia",
    "Cevada Cervejeira": "cevada_cervejeira",
    "Cevada Grãos": "cevada_graos",
    "Feijão": "feijao",
    "Feijão 2ª Safra": "feijao_2",
    "Feijão Caupi": "feijao_caupi",
    "Girassol": "girassol",
    "Mamona": "mamona",
    "Milho 1ª Safra": "milho_1",
    "Milho 1ª Safra consorciado com Braquiária": "milho_1_braquiaria",
    "Milho 2ª Safra": "milho_2",
    "Milho 2ª Safra consorciado com Braquiária": "milho_2_braquiaria",
    "Soja": "soja",
    "Sorgo Forrageiro": "sorgo_forrageiro",
    "Sorgo Forrageiro 2ª Safra": "sorgo_forrageiro_2",
    "Sorgo Granífero": "sorgo",
    "Sorgo Granífero 2ª Safra": "sorgo_2",
    "Trigo": "trigo",
    "Trigo - Duplo Propósito": "trigo_duplo_proposito",
    "Milho": "milho",
    "Arroz Irrigado": "arroz_irrigado",
    "Feijão 1ª Safra": "feijao_1",
    "Trigo Irrigado": "trigo_irrigado",
    "Mamona Semi-árido Sequeiro": "mamona_semiarido_sequeiro",
    "Cevada Grãos Irrigada": "cevada_graos_irrigada",
    "Cevada Grãos Sequeiro": "cevada_graos_sequeiro",
    "Aveia Sequeiro": "aveia_sequeiro",
    "Aveia Irrigada": "aveia_irrigada",
}

_CULTURAS_PERENES: dict[str, str] = {
    "Abacaxi": "abacaxi",
    "Alho Nobre": "alho_nobre",
    "Arroz": "arroz",
    "Açaí Implatação": "acai_implantacao",
    "Açaí Produção": "acai",
    "Banana Cavendish Implantação": "banana_cavendish_implantacao",
    "Banana Cevendish Produção": "banana_cavendish",
    "Banana Maçã Implantação": "banana_maca_implantacao",
    "Banana Maçã Produção": "banana_maca",
    "Banana Prata Implantação": "banana_prata_implantacao",
    "Banana Prata Produção": "banana_prata",
    "Batata Indústria": "batata_industria",
    "Batata Mesa": "batata_mesa",
    "Cacau Implantação": "cacau_implantacao",
    "Cacau Produção": "cacau",
    "Cacau SAF Implantação": "cacau_saf_implantacao",
    "Cacau SAF Produção": "cacau_saf",
    "Café Arábica Implantação": "cafe_arabica_implantacao",
    "Café Arábica Produção": "cafe_arabica",
    "Café Canéfora Implantação": "cafe_canefora_implantacao",
    "Café Canéfora Produção": "cafe_canefora",
    "Caju Anão Implantação": "caju_anao_implantacao",
    "Caju Anão Produção\xa0": "caju_anao",
    "Cana-de-Açúcar (açúcar e álcool)": "cana",
    "Cana-de-Açúcar (outros fins)": "cana_outros_fins",
    "Canola": "canola",
    "Cebola": "cebola",
    "Cebola Plantio com Bulbinho": "cebola_bulbinho",
    "Centeio": "centeio",
    "Dendê": "dende",
    "Forrageira Pecuária": "forrageira_pecuaria",
    "Gergelim": "gergelim",
    "Grão de Bico": "grao_de_bico",
    "Laranja Implantação": "laranja_implantacao",
    "Laranja Produção": "laranja",
    "Lima Implantação": "lima_implantacao",
    "Lima Produção": "lima",
    "Limão Implantação": "limao_implantacao",
    "Limão Produção": "limao",
    "Macaúba (Acrocomia aculeata)  Implantação": "macauba_aculeata_implantacao",
    "Macaúba (Acrocomia aculeata) Produção": "macauba_aculeata",
    "Macaúba (Acrocomia intumescens)  Implantação": "macauba_intumescens_implantacao",
    "Macaúba (Acrocomia intumescens)  Produção": "macauba_intumescens",
    "Macaúba (Acrocomia totai) Implantação": "macauba_totai_implantacao",
    "Macaúba (Acrocomia totai) Produção": "macauba_totai",
    "Mamão": "mamao",
    "Mandioca (aipim, macaxeira)": "mandioca",
    "Manga": "manga",
    "Maracujá": "maracuja",
    "Maçã Implantação": "maca_implantacao",
    "Maçã Produção": "maca",
    "Melancia": "melancia",
    "Milheto": "milheto",
    "Nectarina Implantação Indústria": "nectarina_industria_implantacao",
    "Nectarina Implantação Mesa": "nectarina_mesa_implantacao",
    "Nectarina Produção Indústria": "nectarina_industria",
    "Nectarina Produção Mesa": "nectarina_mesa",
    "Oliva (Azeitona)": "oliva",
    "Palma Forrageira": "palma_forrageira",
    "Pomelo Implantação": "pomelo_implantacao",
    "Pomelo Produção": "pomelo",
    "Pêssego Implantação Indústria": "pessego_industria_implantacao",
    "Pêssego Implantação Mesa": "pessego_mesa_implantacao",
    "Pêssego Produção Indústria": "pessego_industria",
    "Pêssego Produção Mesa": "pessego_mesa",
    "Sisal": "sisal",
    "Tangerina Implantação": "tangerina_implantacao",
    "Tangerina Produção": "tangerina",
    "Toranja Implantação": "toranja_implantacao",
    "Toranja Produção": "toranja",
    "Triticale": "triticale",
    "Uva Industrial": "uva_industrial",
    "Uva Industrial 2ª Safra": "uva_industrial_2",
    "Uva Mesa": "uva_mesa",
    "Uva Mesa 2ª Safra": "uva_mesa_2",
}

_CULTURAS_LEGADAS: dict[str, str] = {
    "Arroz Sequeiro": "arroz_sequeiro",
    "Trigo Sequeiro": "trigo_sequeiro",
}

CULTURAS_ZARC: dict[str, str] = {
    **_CULTURAS_ANUAIS,
    **_CULTURAS_PERENES,
    **_CULTURAS_LEGADAS,
}
CULTURAS_CANONICAS: frozenset[str] = frozenset(CULTURAS_ZARC.values())
CULTURAS_PERENES: frozenset[str] = frozenset(_CULTURAS_PERENES.values()) - frozenset(
    _CULTURAS_ANUAIS.values()
)
SAFRAS_POR_CULTURA: dict[str, tuple[str, str]] = {
    "algodao": ("2016/2017", "2026/2027"),
    "amendoim": ("2018/2019", "2026/2027"),
    "arroz": ("2024/2025", "2026/2027"),
    "arroz_irrigado": ("2018/2019", "2023/2024"),
    "arroz_sequeiro": ("2016/2017", "2023/2024"),
    "aveia": ("2024/2025", "2025/2026"),
    "aveia_irrigada": ("2020/2021", "2023/2024"),
    "aveia_sequeiro": ("2020/2021", "2023/2024"),
    "canola": ("2023/2024", "2023/2024"),
    "cevada_cervejeira": ("2023/2024", "2025/2026"),
    "cevada_graos": ("2024/2025", "2025/2026"),
    "cevada_graos_irrigada": ("2020/2021", "2023/2024"),
    "cevada_graos_sequeiro": ("2020/2021", "2023/2024"),
    "feijao": ("2024/2025", "2026/2027"),
    "feijao_1": ("2019/2020", "2023/2024"),
    "feijao_2": ("2018/2019", "2026/2027"),
    "feijao_caupi": ("2016/2017", "2026/2027"),
    "girassol": ("2021/2022", "2026/2027"),
    "mamona": ("2020/2021", "2026/2027"),
    "mamona_semiarido_sequeiro": ("2020/2021", "2023/2024"),
    "milho": ("2017/2018", "2023/2024"),
    "milho_1": ("2024/2025", "2026/2027"),
    "milho_1_braquiaria": ("2019/2020", "2026/2027"),
    "milho_2": ("2016/2017", "2026/2027"),
    "milho_2_braquiaria": ("2019/2020", "2026/2027"),
    "soja": ("2017/2018", "2026/2027"),
    "sorgo": ("2020/2021", "2026/2027"),
    "sorgo_2": ("2021/2022", "2026/2027"),
    "sorgo_forrageiro": ("2021/2022", "2026/2027"),
    "sorgo_forrageiro_2": ("2021/2022", "2026/2027"),
    "trigo": ("2024/2025", "2025/2026"),
    "trigo_duplo_proposito": ("2018/2019", "2025/2026"),
    "trigo_irrigado": ("2019/2020", "2023/2024"),
    "trigo_sequeiro": ("2016/2017", "2023/2024"),
}
RENOMEACOES_2024_2025: dict[str, tuple[str, str, str]] = {
    "milho": ("milho_1", "cultura_codigo", "12015080000011"),
    "feijao_1": ("feijao", "cultura_codigo", "12013560000011"),
    "arroz_irrigado": ("arroz", "manejo", "Irrigado"),
    "arroz_sequeiro": ("arroz", "manejo", "Sequeiro"),
    "aveia_irrigada": ("aveia", "manejo", "Irrigado"),
    "aveia_sequeiro": ("aveia", "manejo", "Sequeiro"),
    "cevada_graos_irrigada": ("cevada_graos", "manejo", "Irrigado"),
    "cevada_graos_sequeiro": ("cevada_graos", "manejo", "Sequeiro"),
    "mamona_semiarido_sequeiro": ("mamona", "cultura_codigo", "12014720000011"),
    "trigo_irrigado": ("trigo", "manejo", "Irrigado"),
    "trigo_sequeiro": ("trigo", "manejo", "Sequeiro"),
}

DEC_COLS: list[str] = list(constants.ZARC_RISK_COLUMNS)
COLUNAS_SAIDA: list[str] = list(constants.ZARC_OUTPUT_COLUMNS)
_SAFRA_RE = re.compile(r"(?<![0-9])([0-9]{4})/([0-9]{4})(?![0-9])")
_CULTURAS_NORMALIZADAS = {
    " ".join(regions.remover_acentos(label).casefold().split()): canonical
    for label, canonical in CULTURAS_ZARC.items()
}


def normalize_cultura(value: object) -> str:
    if not isinstance(value, str):
        return ""
    normalized = " ".join(regions.remover_acentos(value).casefold().split())
    return _CULTURAS_NORMALIZADAS.get(normalized, normalized.replace(" ", "_"))


def _integer(value: Any) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]+", value.strip()):
        raise ValueError("esperado lexema inteiro decimal ASCII")
    normalized = value.strip()
    if len(normalized) > 19 or int(normalized) > 2**63 - 1:
        raise ValueError("inteiro fora de Int64")
    return int(normalized)


def _risk(value: Any) -> int | None:
    if value == "":
        return None
    number = _integer(value)
    if number not in constants.ZARC_RISK_VALUES:
        raise ValueError("valor de decêndio não homologado")
    return number


RiskValue = Annotated[int | None, BeforeValidator(_risk)]
IntegerValue = Annotated[int, BeforeValidator(_integer)]


class ZarcRecord(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    cultura_original: str
    safra_inicio: str
    safra_fim: str
    cultura_codigo: str
    ciclo_codigo: IntegerValue
    solo_codigo: IntegerValue
    geocodigo: str
    uf: regions.UF
    municipio: str
    clima_codigo: str
    clima: str
    manejo_codigo: str
    manejo: str
    produtividade_texto: str
    nm_codigo: str
    municipio_sicor_codigo: str
    mesorregiao_codigo: str
    microrregiao_codigo: str
    portaria: str
    riscos: tuple[RiskValue, ...]

    @field_validator("cultura_original", "cultura_codigo", "portaria")
    @classmethod
    def required_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("texto obrigatório vazio")
        return value

    @field_validator(*constants.ZARC_TEXT_CODE_COLUMNS)
    @classmethod
    def textual_code(cls, value: str) -> str:
        if value and not re.fullmatch(r"[0-9]+", value):
            raise ValueError("código publicado deve conter dígitos ASCII ou ser vazio")
        return value

    @field_validator("geocodigo")
    @classmethod
    def geographic_code(cls, value: str) -> str:
        if not re.fullmatch(r"[0-9]{7}", value):
            raise ValueError("geocódigo deve conter sete dígitos ASCII")
        return value

    @field_validator("solo_codigo")
    @classmethod
    def soil(cls, value: int) -> int:
        if value not in constants.ZARC_SOIL_CODES:
            raise ValueError("código de solo não homologado")
        return value

    @field_validator("ciclo_codigo")
    @classmethod
    def cycle(cls, value: int) -> int:
        if value not in constants.ZARC_CYCLE_CODES:
            raise ValueError("código de ciclo não homologado")
        return value

    @field_validator("riscos")
    @classmethod
    def complete_risks(cls, value: tuple[int | None, ...]) -> tuple[int | None, ...]:
        if len(value) != 36:
            raise ValueError("vetor deve conter 36 decêndios")
        return value

    @model_validator(mode="after")
    def season(self) -> ZarcRecord:
        derive_safra(self.safra_inicio, self.safra_fim)
        return self


def derive_safra(start: str, end: str) -> str:
    if start == "" and end in constants.ZARC_NON_ANNUAL_SEASONS:
        return constants.ZARC_NON_ANNUAL_SEASONS[end]
    if not re.fullmatch(r"[0-9]{4}", start) or not re.fullmatch(r"[0-9]{4}", end):
        raise ValueError("SafraIni/Fin incompatíveis com safra anual ou modalidade publicada")
    if int(end) != int(start) + 1:
        raise ValueError("safra anual exige dois anos consecutivos")
    return f"{start}/{end}"


def build_ckan_package_url(slug: str) -> str:
    return f"{CKAN_API}/package_show?id={slug}"


def _csv_resources(resources: list[dict[str, str]]) -> list[dict[str, str]]:
    return [
        resource
        for resource in resources
        if resource.get("format", "").strip().upper() == "CSV"
        or (
            not resource.get("format", "").strip()
            and urlsplit(resource.get("url", "")).path.lower().endswith(".csv")
        )
    ]


def _resource_seasons(resource: dict[str, str]) -> list[str]:
    name = resource.get("name", "")
    seasons = [
        f"{start}/{end}" for start, end in _SAFRA_RE.findall(name) if int(end) == int(start) + 1
    ]
    if "perene" in name.casefold():
        seasons.append("perene")
    return seasons


def match_safra_resource(resources: list[dict[str, str]], safra: str) -> str | None:
    matches = [r for r in _csv_resources(resources) if safra.casefold() in _resource_seasons(r)]
    if len(matches) > 1 or any(len(_resource_seasons(r)) != 1 for r in matches):
        raise ParseError(source="zarc", parser_version=2, reason="Catálogo com safra ambígua")
    return matches[0].get("url") if matches else None


def extract_safras(resources: list[dict[str, str]]) -> list[str]:
    seasons = {season for r in _csv_resources(resources) for season in _resource_seasons(r)}
    for season in seasons:
        match_safra_resource(resources, season)
    return sorted(seasons - {"perene"}) + (["perene"] if "perene" in seasons else [])
