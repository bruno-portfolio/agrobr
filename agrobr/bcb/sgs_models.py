from __future__ import annotations

from datetime import date

import pydantic

SGS_SERIES: dict[str, int] = {
    "selic": 432,
    "ipca": 433,
    "ipca_alimentacao": 1635,
    "ipa_agricola": 7460,
    "pib_agropecuaria": 22083,
    "credito_rural_concessoes_pf": 20701,
    "credito_rural_saldo_pf": 20609,
    "dolar_ptax_venda": 1,
    "dolar_ptax_compra": 10813,
    "cambio_mensal_compra": 3697,
    "cambio_mensal_venda": 3698,
    "igpm": 189,
    "igpdi": 190,
    "inpc": 188,
    "cdi": 4392,
    "tjlp": 256,
    "tr": 226,
}

SGS_ALIASES_DEPRECIADOS: dict[str, tuple[str, str]] = {
    "ipa_agropecuario": (
        "ipa_agricola",
        "a série 7460 é o IPA-DI por origem de produtos agrícolas, sem os pecuários",
    ),
}

COLUNAS_SAIDA: list[str] = ["data", "valor", "codigo", "nome_serie"]

PARSER_VERSION: int = 2


class SGSObservation(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid", allow_inf_nan=False)

    data: date
    valor: float | None
    data_fim: date | None = None


class SGSParsedBlock(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(strict=True, extra="forbid")

    records: list[SGSObservation]
    source_rows: int = pydantic.Field(ge=0)
    layout_fingerprint: str | None
    parser_version: int = PARSER_VERSION
    warnings: list[str] = pydantic.Field(default_factory=list)
