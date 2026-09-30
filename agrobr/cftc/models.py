from __future__ import annotations

from agrobr.exceptions import InvalidParameterError
from agrobr.normalize.crops import normalizar_cultura

CFTC_CONTRACTS: dict[str, str] = {
    "005602": "soja",
    "026603": "farelo_soja",
    "007601": "oleo_soja",
    "002602": "milho",
    "001602": "trigo",
    "080732": "acucar",
    "083731": "cafe",
    "033661": "algodao",
    "057642": "boi",
    "054642": "suino",
    "040701": "laranja",
    "039601": "arroz",
}

COLUMN_MAP: dict[str, str] = {
    "report_date_as_yyyy_mm_dd": "data",
    "market_and_exchange_names": "contrato",
    "cftc_contract_market_code": "codigo_cftc",
    "open_interest_all": "open_interest",
    "m_money_positions_long_all": "managed_money_long",
    "m_money_positions_short_all": "managed_money_short",
    "m_money_positions_spread": "managed_money_spread",
    "prod_merc_positions_long": "producer_long",
    "prod_merc_positions_short": "producer_short",
    "swap_positions_long_all": "swap_long",
    "swap__positions_short_all": "swap_short",
    "swap__positions_spread_all": "swap_spread",
    "other_rept_positions_long": "other_long",
    "other_rept_positions_short": "other_short",
    "other_rept_positions_spread": "other_spread",
    "nonrept_positions_long_all": "nonreportable_long",
    "nonrept_positions_short_all": "nonreportable_short",
    "change_in_m_money_long_all": "change_managed_money_long",
    "change_in_m_money_short_all": "change_managed_money_short",
    "change_in_open_interest_all": "change_open_interest",
}

POSITION_COLUMNS: list[str] = [
    "open_interest",
    "managed_money_long",
    "managed_money_short",
    "managed_money_spread",
    "producer_long",
    "producer_short",
    "swap_long",
    "swap_short",
    "swap_spread",
    "other_long",
    "other_short",
    "other_spread",
    "nonreportable_long",
    "nonreportable_short",
]

CHANGE_COLUMNS: list[str] = [
    "change_managed_money_long",
    "change_managed_money_short",
    "change_open_interest",
]

COLUNAS_SAIDA: list[str] = [
    "data",
    "commodity",
    "contrato",
    "codigo_cftc",
    "open_interest",
    "managed_money_long",
    "managed_money_short",
    "managed_money_spread",
    "managed_money_net",
    "producer_long",
    "producer_short",
    "swap_long",
    "swap_short",
    "swap_spread",
    "other_long",
    "other_short",
    "other_spread",
    "nonreportable_long",
    "nonreportable_short",
    "change_managed_money_long",
    "change_managed_money_short",
    "change_open_interest",
]

PARSER_VERSION: int = 1
SCHEMA_VERSION: str = "1.1"


def resolve_contract_codes(commodity: str | None) -> list[str]:
    """Resolve commodity (canônica PT, alias EN ou código CFTC) para códigos de contrato.

    None retorna todos os contratos agro mapeados.
    """
    if commodity is None:
        return list(CFTC_CONTRACTS)

    raw = commodity.strip() if isinstance(commodity, str) else ""
    if raw in CFTC_CONTRACTS:
        return [raw]

    canonico = normalizar_cultura(raw) if raw else ""
    codes = [code for code, canon in CFTC_CONTRACTS.items() if canon == canonico]
    if not codes:
        disponiveis = ", ".join(sorted(set(CFTC_CONTRACTS.values())))
        raise InvalidParameterError(
            f"produto {commodity!r} sem contrato CFTC mapeado. Valores válidos: {disponiveis}"
        )
    return codes
