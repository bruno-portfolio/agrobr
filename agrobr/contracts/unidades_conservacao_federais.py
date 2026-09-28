from __future__ import annotations

import pandas as pd

from agrobr import contracts


class UnidadesConservacaoFederaisContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        frame = super().empty_frame()
        for column in self.columns:
            if column.type == contracts.ColumnType.STRING:
                frame[column.name] = pd.Series(dtype=pd.StringDtype(storage="python"))
            elif column.type == contracts.ColumnType.FLOAT:
                frame[column.name] = pd.Series(dtype="float64")
        return frame


UNIDADES_CONSERVACAO_FEDERAIS_V1 = UnidadesConservacaoFederaisContract(
    name="unidades_conservacao_federais",
    version="1.0",
    effective_from="2.0.0",
    columns=[
        contracts.Column(
            "codigo", contracts.ColumnType.STRING, description="Código CNUC publicado"
        ),
        contracts.Column("nome", contracts.ColumnType.STRING),
        contracts.Column("categoria", contracts.ColumnType.STRING),
        contracts.Column("grupo", contracts.ColumnType.STRING),
        contracts.Column("uf", contracts.ColumnType.STRING, description="Pode conter várias UFs"),
        contracts.Column(
            "bioma", contracts.ColumnType.STRING, description="Pode conter vários biomas"
        ),
        contracts.Column("area_ha", contracts.ColumnType.FLOAT, nullable=True, unit="ha"),
        contracts.Column("ano_criacao", contracts.ColumnType.INTEGER, nullable=True),
        contracts.Column("ato_criacao", contracts.ColumnType.STRING),
    ],
    primary_key=[],
    guarantees=[
        "Cadastro corrente da camada ICMBio:limiteucsfederais_a, não todas as esferas do CNUC",
        "Código CNUC textual e atributos publicados preservados; sem deduplicação por código",
        "Área publicada em hectares, sem recálculo geométrico",
        "Ano e ato de criação não representam edição ou histórico dos perímetros",
        "Contagem do serviço conciliada com o retorno; não há snapshot transacional",
        "Seleção acima do limite de aquisição é recusada, sem resultado parcial",
        "Geometria permanece na API de fonte icmbio.ucs_geo, com dependência geo separada",
    ],
)

contracts.register_contract("unidades_conservacao_federais", UNIDADES_CONSERVACAO_FEDERAIS_V1)
