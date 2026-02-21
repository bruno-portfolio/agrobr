from __future__ import annotations

from pydantic import BaseModel, field_validator

PSD_COMMODITIES: dict[str, str] = {
    "soja": "2222000",
    "soybeans": "2222000",
    "milho": "0440000",
    "corn": "0440000",
    "trigo": "0410000",
    "wheat": "0410000",
    "arroz": "0422110",
    "rice": "0422110",
    "algodao": "2631000",
    "cotton": "2631000",
    "acucar": "0612000",
    "sugar": "0612000",
    "farelo_soja": "4233000",
    "soybean_meal": "4233000",
    "oleo_soja": "4232000",
    "soybean_oil": "4232000",
}

_COMMODITY_NAMES: dict[str, str] = {
    "2222000": "soja",
    "0440000": "milho",
    "0410000": "trigo",
    "0422110": "arroz",
    "2631000": "algodao",
    "0612000": "acucar",
    "4233000": "farelo_soja",
    "4232000": "oleo_soja",
}

PSD_ATTRIBUTES: dict[int, str] = {
    4: "area_colhida",
    28: "estoque_inicial",
    57: "consumo_domestico",
    84: "estoque_final",
    88: "exportacao",
    125: "producao",
    130: "importacao",
    176: "oferta_total",
    184: "produtividade",
}

PSD_COUNTRIES: dict[str, str] = {
    "brasil": "BR",
    "brazil": "BR",
    "br": "BR",
    "eua": "US",
    "usa": "US",
    "us": "US",
    "china": "CH",
    "argentina": "AR",
    "india": "IN",
    "indonesia": "ID",
    "mexico": "MX",
    "ue": "E2",
    "eu": "E2",
}

PSD_COLUMNS_MAP: dict[str, str] = {
    "CommodityCode": "commodity_code",
    "CommodityDescription": "commodity",
    "CountryCode": "country_code",
    "CountryName": "country",
    "MarketYear": "market_year",
    "CalendarYear": "calendar_year",
    "Month": "month",
    "AttributeId": "attribute_id",
    "AttributeDescription": "attribute",
    "UnitId": "unit_id",
    "UnitDescription": "unit",
    "Value": "value",
}


def resolve_commodity_code(nome: str) -> str:
    key = nome.strip().lower()
    if key in PSD_COMMODITIES:
        return PSD_COMMODITIES[key]
    if len(key) == 7 and key.isdigit():
        return key
    raise ValueError(
        f"Commodity desconhecida: '{nome}'. Opções: {list(dict.fromkeys(PSD_COMMODITIES.values()))}"
    )


def resolve_country_code(nome: str) -> str:
    key = nome.strip().lower()
    if key in PSD_COUNTRIES:
        return PSD_COUNTRIES[key]
    if len(key) <= 3:
        return key.upper()
    raise ValueError(
        f"País desconhecido: '{nome}'. Opções: {list(dict.fromkeys(PSD_COUNTRIES.values()))}"
    )


def commodity_name(code: str) -> str:
    return _COMMODITY_NAMES.get(code, code)


class PSDRecord(BaseModel):
    commodity_code: str
    commodity: str
    country_code: str
    country: str
    market_year: int
    attribute: str
    value: float | None = None
    unit: str = ""

    @field_validator("commodity", mode="before")
    @classmethod
    def normalize_commodity(cls, v: str) -> str:
        return v.strip().lower()

    @field_validator("country", mode="before")
    @classmethod
    def normalize_country(cls, v: str) -> str:
        return v.strip()

    @field_validator("attribute", mode="before")
    @classmethod
    def normalize_attribute(cls, v: str) -> str:
        return v.strip().lower()
