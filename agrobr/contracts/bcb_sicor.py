from __future__ import annotations

from agrobr import contracts

BCB_CREDITO_RURAL_TOTAL_V1 = contracts.Contract(
    name="bcb.credito_rural_total",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["safra", "uf", "finalidade", "programa"],
    columns=[
        contracts.Column(
            name="safra",
            type=contracts.ColumnType.STRING,
            description="Safra de julho a junho do mês de emissão, no formato AAAA/AA (2023/24) da camada; o SICOR não publica safra",
        ),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING),
        contracts.Column(name="finalidade", type=contracts.ColumnType.STRING),
        contracts.Column(name="agregacao", type=contracts.ColumnType.STRING),
        contracts.Column(name="programa", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="cd_programa", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="qtd_contratos", type=contracts.ColumnType.INTEGER, min_value=0),
        contracts.Column(name="valor", type=contracts.ColumnType.FLOAT, unit="BRL", min_value=0),
        contracts.Column(name="fonte", type=contracts.ColumnType.STRING),
    ],
    guarantees=[
        "Nove colunas estáveis, inclusive em resultado vazio",
        "Uma linha por safra, UF e finalidade (e programa, com agregacao='programa')",
        "As quatro finalidades da entidade RegiaoUF, inclusive a industrialização",
        "Finalidade sem operação fica ausente; o zero de preenchimento da fonte não vira linha",
        "Sem linha Brasil: o total do país é a soma das UFs",
        "valor em BRL somado ao centavo; qtd_contratos inteiro",
    ],
)

contracts.register_contract("bcb_credito_rural_total", BCB_CREDITO_RURAL_TOTAL_V1)

_CODIGO = "Código da tabela de domínio do SICOR, como a fonte publica"
_NOME = "Descrição da tabela de domínio do BCB; nulo quando o código não está na tabela"

BCB_CREDITO_RURAL_REGISTRO_V1 = contracts.Contract(
    name="bcb.credito_rural_registro",
    version="1.0",
    effective_from="2.0.0",
    primary_key=[
        "ano_emissao",
        "mes_emissao",
        "uf",
        "produto",
        "finalidade",
        "cd_programa",
        "cd_sub_programa",
        "cd_fonte_recurso",
        "cd_tipo_seguro",
        "cd_atividade",
        "cd_modalidade",
    ],
    columns=[
        contracts.Column(
            name="safra",
            type=contracts.ColumnType.STRING,
            description="Safra de julho a junho do mês de emissão, no formato AAAA/AA (2023/24) da camada; o SICOR não publica safra",
        ),
        contracts.Column(name="ano_emissao", type=contracts.ColumnType.INTEGER, min_value=2013),
        contracts.Column(
            name="mes_emissao", type=contracts.ColumnType.INTEGER, min_value=1, max_value=12
        ),
        contracts.Column(name="produto", type=contracts.ColumnType.STRING),
        contracts.Column(name="regiao", type=contracts.ColumnType.STRING, nullable=True),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING),
        contracts.Column(name="finalidade", type=contracts.ColumnType.STRING),
        contracts.Column(name="agregacao", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="programa",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description="Nome da tabela de domínio do BCB: o trecho da descrição antes do primeiro ' - '; código fora da tabela sai 'Desconhecido (<código>)'",
        ),
        contracts.Column(
            name="cd_programa", type=contracts.ColumnType.STRING, nullable=True, description=_CODIGO
        ),
        contracts.Column(
            name="cd_sub_programa",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description="Código do subprograma, como a fonte publica, sem coluna de nome, como na 1.1.0; o nome depende do programa na tabela de domínio Subprograma do BCB",
        ),
        contracts.Column(
            name="fonte_recurso", type=contracts.ColumnType.STRING, nullable=True, description=_NOME
        ),
        contracts.Column(
            name="cd_fonte_recurso",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description=_CODIGO,
        ),
        contracts.Column(
            name="tipo_seguro",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description="Descrição da tabela de domínio do BCB; código fora da tabela sai 'Desconhecido (<código>)'",
        ),
        contracts.Column(
            name="cd_tipo_seguro",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description=_CODIGO,
        ),
        contracts.Column(
            name="modalidade", type=contracts.ColumnType.STRING, nullable=True, description=_NOME
        ),
        contracts.Column(
            name="cd_modalidade",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description=_CODIGO,
        ),
        contracts.Column(
            name="atividade", type=contracts.ColumnType.STRING, nullable=True, description=_NOME
        ),
        contracts.Column(
            name="cd_atividade",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description=_CODIGO,
        ),
        contracts.Column(
            name="qtd_contratos", type=contracts.ColumnType.INTEGER, nullable=True, min_value=0
        ),
        contracts.Column(
            name="valor", type=contracts.ColumnType.FLOAT, nullable=True, unit="BRL", min_value=0
        ),
        contracts.Column(
            name="area_financiada",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="ha",
            min_value=0,
        ),
        contracts.Column(name="fonte", type=contracts.ColumnType.STRING),
    ],
    guarantees=[
        "23 colunas estáveis, inclusive em resultado vazio",
        "Uma linha por registro das entidades *RegiaoUFProduto do SICOR, sem agregar: mês de emissão, UF, produto, finalidade, programa, subprograma, fonte de recursos, tipo de seguro, atividade e modalidade",
        "Somado por safra, UF, produto e finalidade, é igual ao agregacao='uf' da mesma chamada",
        "Nomes de programa, fonte de recursos, tipo de seguro, modalidade e atividade pelas tabelas de domínio do BCB",
        "Só o OData: sem fallback BigQuery",
    ],
)

contracts.register_contract("bcb_credito_rural_registro", BCB_CREDITO_RURAL_REGISTRO_V1)
