from __future__ import annotations

import re
from datetime import datetime
from typing import Literal, Self

import pydantic

from agrobr import constants
from agrobr.exceptions import InvalidParameterError

COMTRADE_PAISES: dict[str, int] = {
    "br": 76,
    "bra": 76,
    "brasil": 76,
    "brazil": 76,
    "cn": 156,
    "chn": 156,
    "china": 156,
    "us": 842,
    "usa": 842,
    "eua": 842,
    "ar": 32,
    "arg": 32,
    "argentina": 32,
    "eu": 97,
    "ue": 97,
    "jp": 392,
    "jpn": 392,
    "japao": 392,
    "kr": 410,
    "kor": 410,
    "coreia": 410,
    "in": 356,
    "ind": 356,
    "india": 356,
    "mx": 484,
    "mex": 484,
    "mexico": 484,
    "cl": 152,
    "chl": 152,
    "chile": 152,
    "co": 170,
    "col": 170,
    "colombia": 170,
    "pe": 604,
    "per": 604,
    "peru": 604,
    "py": 600,
    "pry": 600,
    "paraguai": 600,
    "uy": 858,
    "ury": 858,
    "uruguai": 858,
    "world": 0,
    "mundo": 0,
    "eg": 818,
    "egy": 818,
    "egito": 818,
    "id": 360,
    "idn": 360,
    "indonesia": 360,
    "th": 764,
    "tha": 764,
    "tailandia": 764,
    "ir": 364,
    "irn": 364,
    "ira": 364,
    "sa": 682,
    "sau": 682,
    "arabia_saudita": 682,
    "tr": 792,
    "tur": 792,
    "turquia": 792,
    "ru": 643,
    "rus": 643,
    "russia": 643,
}

COMTRADE_PAISES_INV: dict[int, str] = {
    76: "BRA",
    156: "CHN",
    842: "USA",
    32: "ARG",
    97: "EU",
    392: "JPN",
    410: "KOR",
    356: "IND",
    484: "MEX",
    152: "CHL",
    170: "COL",
    604: "PER",
    600: "PRY",
    858: "URY",
    0: "WLD",
    818: "EGY",
    360: "IDN",
    764: "THA",
    364: "IRN",
    682: "SAU",
    792: "TUR",
    643: "RUS",
}

HS_PRODUTOS_AGRO: dict[str, list[str]] = {
    "soja": ["120100", "120190"],
    "complexo_soja": ["120100", "120190", "1507", "2304"],
    "farelo_soja": ["2304"],
    "oleo_soja": ["1507"],
    "milho": ["1005"],
    "arroz": ["1006"],
    "trigo": ["1001"],
    "cafe": ["090111", "090112", "090121", "090122"],
    "acucar": ["1701"],
    "etanol": ["2207"],
    "algodao": ["5201", "5203"],
    "carne_bovina": ["0201", "0202"],
    "carne_frango": ["020711", "020712", "020713", "020714"],
    "carne_suina": ["0203"],
    "celulose": ["4703"],
    "tabaco": ["2401"],
    "suco_laranja": ["200911", "200912", "200919"],
}

HS_VIGENCIA_DO_CODIGO: dict[str, tuple[str, ...]] = {
    "120100": ("H0", "H1", "H2", "H3"),
    "120190": ("H4", "H5", "H6"),
    "020711": ("H1", "H2", "H3", "H4", "H5", "H6"),
    "020712": ("H1", "H2", "H3", "H4", "H5", "H6"),
    "020713": ("H1", "H2", "H3", "H4", "H5", "H6"),
    "020714": ("H1", "H2", "H3", "H4", "H5", "H6"),
    "200912": ("H2", "H3", "H4", "H5", "H6"),
}

CLASSIFICACAO_REPORTADA_BR: dict[int, str] = {
    ano: classificacao
    for inicio, fim, classificacao in (
        (1996, 1996, "H0"),
        (1997, 2001, "H1"),
        (2002, 2006, "H2"),
        (2007, 2011, "H3"),
        (2012, 2016, "H4"),
        (2017, 2021, "H5"),
        (2022, 2024, "H6"),
    )
    for ano in range(inicio, fim + 1)
}

DICA_ALIAS_FORA_DA_CLASSIFICACAO: dict[tuple[str, str], str] = {
    ("carne_frango", "H0"): (
        "Na H0, só 020721 (frango inteiro congelado) e 020741 (cortes e miudezas congelados, sem "
        "fígado) são só de galinha: um subconjunto do alias, e o 020741 vira pato na HS 2012. "
        "Peça-os explicitamente (produto='020721,020741')."
    ),
}

COLUNAS_SAIDA: list[str] = [
    "periodo",
    "ano",
    "mes",
    "reporter_code",
    "reporter_iso",
    "reporter",
    "partner_code",
    "partner_iso",
    "partner",
    "fluxo_code",
    "fluxo",
    "hs_code",
    "produto_desc",
    "nivel_hs",
    "peso_liquido_kg",
    "peso_bruto_kg",
    "volume_ton",
    "valor_fob_usd",
    "valor_cif_usd",
    "valor_primario_usd",
    "quantidade",
    "unidade_qtd",
    "classificacao",
    "classificacao_original",
    "peso_liquido_estimado",
    "peso_bruto_estimado",
    "quantidade_estimada",
]

COLUNAS_MIRROR: list[str] = [
    "periodo",
    "ano",
    "mes",
    "hs_code",
    "produto_desc",
    "reporter_iso",
    "partner_iso",
    "peso_liquido_kg_reporter",
    "valor_fob_usd_reporter",
    "volume_ton_reporter",
    "peso_liquido_kg_partner",
    "valor_fob_usd_partner",
    "valor_cif_usd_partner",
    "volume_ton_partner",
    "diff_peso_kg",
    "diff_valor_fob_usd",
    "ratio_valor",
    "ratio_peso",
    "classificacao_reporter",
    "classificacao_partner",
    "classificacao_original_reporter",
    "classificacao_original_partner",
    "reporter_code",
    "partner_code",
]


def resolve_pais(nome: str) -> int:
    if not isinstance(nome, str):
        raise InvalidParameterError("país deve ser uma string")
    key = nome.strip().lower()
    if key in COMTRADE_PAISES:
        return COMTRADE_PAISES[key]
    if key.isdigit():
        return int(key)
    raise InvalidParameterError(
        f"Pais desconhecido: '{nome}'. Opcoes: {sorted(set(COMTRADE_PAISES_INV.values()))}"
    )


def resolve_hs(produto: str) -> list[str]:
    if not isinstance(produto, str):
        raise InvalidParameterError("produto deve ser uma string")
    key = produto.strip().lower()
    if key in HS_PRODUTOS_AGRO:
        return list(HS_PRODUTOS_AGRO[key])
    codes = [value.strip() for value in key.split(",")]
    if all(re.fullmatch(constants.COMTRADE_HS_PATTERN, value) for value in codes):
        return list(dict.fromkeys(codes))
    raise InvalidParameterError(
        f"Produto desconhecido: '{produto}'. Opcoes: {list(HS_PRODUTOS_AGRO.keys())}"
    )


def validate_hs_periods(produto: str, periods: list[str], reporter: int) -> None:
    alias = produto.strip().lower()
    if alias == "carne_frango" and any(int(p[:4]) < 1996 for p in periods):
        raise InvalidParameterError(
            "carne_frango só a partir de 1996: na HS 1992 a Comtrade não separa a galinha fresca "
            "das outras aves, e o código dos cortes de galinha congelados (020741) virou pato na "
            "HS 2012. Antes de 1996, peça os códigos da HS 1992 explicitamente (020721, 020741)."
        )
    codigos = HS_PRODUTOS_AGRO.get(alias)
    if codigos is None or reporter != COMTRADE_PAISES["br"]:
        return
    for ano in sorted({int(p[:4]) for p in periods}):
        classificacao = CLASSIFICACAO_REPORTADA_BR.get(ano)
        if classificacao is None or any(
            classificacao in HS_VIGENCIA_DO_CODIGO.get(codigo, (classificacao,))
            for codigo in codigos
        ):
            continue
        dica = DICA_ALIAS_FORA_DA_CLASSIFICACAO.get((alias, classificacao))
        raise InvalidParameterError(
            f"{alias} em {ano}: o Brasil reportou na {classificacao}, onde nenhum código do alias "
            f"existe ({', '.join(codigos)})." + (f" {dica}" if dica else "")
        )


class TradeRecord(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(
        strict=True,
        populate_by_name=True,
        extra="allow",
        allow_inf_nan=False,
        revalidate_instances="always",
    )

    type_code: Literal["C"] = pydantic.Field(alias="typeCode")
    freq_code: Literal["A", "M"] = pydantic.Field(alias="freqCode")
    ref_period_id: int = pydantic.Field(alias="refPeriodId")
    ref_year: int = pydantic.Field(alias="refYear", ge=1, le=9999)
    ref_month: int = pydantic.Field(alias="refMonth")
    period: str
    reporter_code: int = pydantic.Field(alias="reporterCode", gt=0, le=2**63 - 1)
    reporter_iso: str | None = pydantic.Field(default=None, alias="reporterISO")
    reporter_desc: str | None = pydantic.Field(default=None, alias="reporterDesc")
    flow_code: Literal["X", "M"] = pydantic.Field(alias="flowCode")
    flow_desc: str | None = pydantic.Field(default=None, alias="flowDesc")
    partner_code: int = pydantic.Field(alias="partnerCode", ge=0, le=2**63 - 1)
    partner_iso: str | None = pydantic.Field(default=None, alias="partnerISO")
    partner_desc: str | None = pydantic.Field(default=None, alias="partnerDesc")
    partner2_code: int = pydantic.Field(alias="partner2Code", ge=0, le=0)
    partner2_iso: str | None = pydantic.Field(default=None, alias="partner2ISO")
    partner2_desc: str | None = pydantic.Field(default=None, alias="partner2Desc")
    classification_code: str = pydantic.Field(alias="classificationCode")
    classification_search_code: str = pydantic.Field(alias="classificationSearchCode")
    is_original_classification: bool | None = pydantic.Field(alias="isOriginalClassification")
    cmd_code: str = pydantic.Field(alias="cmdCode")
    cmd_desc: str | None = pydantic.Field(default=None, alias="cmdDesc")
    aggr_level: int = pydantic.Field(alias="aggrLevel")
    is_leaf: bool | None = pydantic.Field(default=None, alias="isLeaf")
    customs_code: Literal["C00"] = pydantic.Field(alias="customsCode")
    customs_desc: str | None = pydantic.Field(default=None, alias="customsDesc")
    mos_code: Literal["0"] = pydantic.Field(alias="mosCode")
    mot_code: int = pydantic.Field(alias="motCode", ge=0, le=0)
    mot_desc: str | None = pydantic.Field(default=None, alias="motDesc")
    qty_unit_code: int | None = pydantic.Field(default=None, alias="qtyUnitCode")
    qty_unit_abbr: str | None = pydantic.Field(default=None, alias="qtyUnitAbbr")
    qty: float | None = pydantic.Field(default=None, ge=0)
    is_qty_estimated: bool | None = pydantic.Field(default=None, alias="isQtyEstimated")
    alt_qty_unit_code: int | None = pydantic.Field(default=None, alias="altQtyUnitCode")
    alt_qty_unit_abbr: str | None = pydantic.Field(default=None, alias="altQtyUnitAbbr")
    alt_qty: float | None = pydantic.Field(default=None, alias="altQty", ge=0)
    is_alt_qty_estimated: bool | None = pydantic.Field(default=None, alias="isAltQtyEstimated")
    net_wgt: float | None = pydantic.Field(default=None, alias="netWgt", ge=0)
    is_net_wgt_estimated: bool | None = pydantic.Field(default=None, alias="isNetWgtEstimated")
    gross_wgt: float | None = pydantic.Field(default=None, alias="grossWgt", ge=0)
    is_gross_wgt_estimated: bool | None = pydantic.Field(default=None, alias="isGrossWgtEstimated")
    cifvalue: float | None = pydantic.Field(default=None, ge=0)
    fobvalue: float | None = pydantic.Field(default=None, ge=0)
    primary_value: float | None = pydantic.Field(default=None, alias="primaryValue", ge=0)
    legacy_estimation_flag: int | None = pydantic.Field(default=None, alias="legacyEstimationFlag")
    is_reported: bool | None = pydantic.Field(default=None, alias="isReported")
    is_aggregate: bool | None = pydantic.Field(default=None, alias="isAggregate")

    @pydantic.field_validator("cmd_code")
    @classmethod
    def valid_hs(cls, value: str) -> str:
        if not re.fullmatch(constants.COMTRADE_HS_PATTERN, value):
            raise ValueError("cmdCode deve conter código HS de 2, 4 ou 6 dígitos ASCII")
        return value

    @pydantic.field_validator("classification_code")
    @classmethod
    def valid_classification(cls, value: str) -> str:
        if not re.fullmatch(constants.COMTRADE_CLASSIFICATION_PATTERN, value):
            raise ValueError("classificationCode deve identificar uma edição HS")
        return value

    @pydantic.model_validator(mode="after")
    def consistent_dimensions(self) -> Self:
        width = 4 if self.freq_code == "A" else 6
        if len(self.period) != width or not self.period.isascii() or not self.period.isdigit():
            raise ValueError("period incompatível com freqCode")
        year = int(self.period[:4])
        month = int(self.period[4:]) if width == 6 else 1
        datetime(year, month, 1)
        expected_month = month if width == 6 else 52
        if self.ref_year != year or self.ref_month != expected_month:
            raise ValueError("refYear/refMonth incompatíveis com period")
        if self.ref_period_id != year * 10000 + month * 100 + 1:
            raise ValueError("refPeriodId incompatível com period")
        if self.aggr_level != len(self.cmd_code):
            raise ValueError("aggrLevel incompatível com cmdCode")
        if self.classification_search_code not in {"HS", self.classification_code}:
            raise ValueError("classificationSearchCode incompatível com classificação")
        return self
