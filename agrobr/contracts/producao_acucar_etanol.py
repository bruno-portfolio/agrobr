from __future__ import annotations

from agrobr import contracts
from agrobr.contracts import Column, ColumnType, Contract


def _metrica(nome: str, unidade: str, descricao: str) -> Column:
    return Column(
        name=nome,
        type=ColumnType.FLOAT,
        nullable=True,
        unit=unidade,
        min_value=0,
        description=descricao,
    )


PRODUCAO_ACUCAR_ETANOL_V1 = Contract(
    name="conab.producao_acucar_etanol",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["safra", "uf"],
    columns=[
        Column(
            name="safra",
            type=ColumnType.STRING,
            description="Safra da cana como publicada (AAAA/AA)",
        ),
        Column(
            name="regiao",
            type=ColumnType.STRING,
            description="Grande região da UF: NORTE, NORDESTE, CENTRO-OESTE, SUDESTE ou SUL",
        ),
        Column(name="uf", type=ColumnType.STRING, description="Sigla da UF"),
        _metrica("acucar_mil_ton", "mil t", "Produção de açúcar"),
        _metrica("etanol_anidro_cana_mil_l", "mil litros", "Etanol anidro de cana-de-açúcar"),
        _metrica("etanol_hidratado_cana_mil_l", "mil litros", "Etanol hidratado de cana-de-açúcar"),
        _metrica("etanol_anidro_milho_mil_l", "mil litros", "Etanol anidro de milho"),
        _metrica("etanol_hidratado_milho_mil_l", "mil litros", "Etanol hidratado de milho"),
        _metrica(
            "etanol_total_mil_l",
            "mil litros",
            "Etanol total publicado, de cana e de milho; não é o etanol de cana",
        ),
        _metrica(
            "atr_kg_t",
            "kg/t de cana",
            "ATR médio; 0 publicado acompanha UF sem açúcar nem etanol de cana na safra",
        ),
    ],
    guarantees=[
        "PK única por safra + uf",
        "Só linhas de UF, as 27 em toda safra fechada; regiões, NORTE/NORDESTE, CENTRO-SUL e "
        "BRASIL ficam fora",
        "Safras fechadas desde 2005/06; a coluna da estimativa (marcada com nota) fica fora",
        "Unidades da fonte, sem conversão: açúcar em mil t, etanol em mil litros, ATR em kg/t de cana",
        "Zero publicado sai 0.0; traço, célula vazia e erro do Excel saem NaN, nunca zero",
        "etanol_total_mil_l é o publicado e inclui o etanol de milho; total diferente da soma das 4 "
        "parcelas (vazio conta 0) gera aviso em validation_warnings e UserWarning, sem alterar número",
        "Soma das UFs diferente do BRASIL publicado nas colunas de volume gera aviso, sem alterar número",
        "Métricas >= 0 quando presentes",
        "Aba, título, unidade, UFs ou colunas de safra diferentes do layout medido levantam ParseError",
    ],
)

contracts.register_contract("producao_acucar_etanol", PRODUCAO_ACUCAR_ETANOL_V1)
