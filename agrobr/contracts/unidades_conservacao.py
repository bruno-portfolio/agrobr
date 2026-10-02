from __future__ import annotations

from agrobr import contracts

UNIDADES_CONSERVACAO_V1 = contracts.Contract(
    name="unidades_conservacao",
    version="1.0",
    effective_from="2.0.0",
    columns=[
        contracts.Column(
            "codigo", contracts.ColumnType.STRING, description="Código CNUC publicado"
        ),
        contracts.Column("nome", contracts.ColumnType.STRING),
        contracts.Column(
            "esfera", contracts.ColumnType.STRING, description="federal, estadual ou municipal"
        ),
        contracts.Column(
            "categoria", contracts.ColumnType.STRING, description="Categoria de manejo do SNUC"
        ),
        contracts.Column(
            "grupo",
            contracts.ColumnType.STRING,
            description="PI (proteção integral) ou US (uso sustentável)",
        ),
        contracts.Column("categoria_iucn", contracts.ColumnType.STRING, nullable=True),
        contracts.Column(
            "uf",
            contracts.ColumnType.STRING,
            description="Siglas em ordem alfabética, separadas por /",
        ),
        contracts.Column(
            "municipios",
            contracts.ColumnType.STRING,
            description="Lista publicada; a fonte corta textos longos com ...",
        ),
        contracts.Column(
            "bioma",
            contracts.ColumnType.STRING,
            nullable=True,
            description="Biomas com área publicada na UC, separados por /",
        ),
        contracts.Column(
            "area_ha", contracts.ColumnType.FLOAT, nullable=True, unit="ha", min_value=0
        ),
        contracts.Column("data_criacao", contracts.ColumnType.DATE, nullable=True),
        contracts.Column("ato_criacao", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("orgao_gestor", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("qualidade_poligono", contracts.ColumnType.STRING, nullable=True),
        contracts.Column("wdpa_id", contracts.ColumnType.STRING, nullable=True),
    ],
    primary_key=["codigo"],
    guarantees=[
        "UCs federais, estaduais e municipais, inclusive RPPNs, com limite cadastrado no CNUC",
        "UCs sem polígono no CNUC e zonas de amortecimento não entram",
        "Código CNUC único por linha",
        "Área publicada em hectares, sem recálculo geométrico",
        "Data de criação é atributo da UC, não edição da camada",
        "Contagem do serviço conciliada com o retorno; não há snapshot transacional",
        "Seleção acima do limite de aquisição é recusada, sem resultado parcial",
        "Geometria na API de fonte cnuc.ucs_geo, com dependência geo separada",
    ],
)

contracts.register_contract("unidades_conservacao", UNIDADES_CONSERVACAO_V1)
