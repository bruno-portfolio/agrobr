from __future__ import annotations

from dataclasses import replace

import pandas as pd

from agrobr import contracts


class PrecosDieselContract(contracts.Contract):
    def empty_frame(self) -> pd.DataFrame:
        frame = super().empty_frame()
        for column in self.columns:
            if column.type == contracts.ColumnType.STRING:
                frame[column.name] = pd.Series(dtype=str)
            elif column.type == contracts.ColumnType.FLOAT:
                frame[column.name] = pd.Series(dtype="float64")
        return frame

    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not valid or df.empty:
            return valid, errors
        for name, allowed in {
            "nivel": {"brasil", "uf", "municipio"},
            "produto": {"DIESEL", "DIESEL S10"},
            "unidade": {"BRL/litro"},
            "agregacao": {"semanal", "mensal"},
        }.items():
            if not df[name].isin(allowed).all():
                errors.append(f"{name}: valores fora do domínio {sorted(allowed)}")
        weekly = df["agregacao"].eq("semanal")
        monthly = df["agregacao"].eq("mensal")
        if df.loc[weekly, "n_postos"].isna().any() or df.loc[monthly, "n_postos"].notna().any():
            errors.append("n_postos é contagem publicada somente na frequência semanal")
        if (
            df.loc[weekly, "n_postos_media"].notna().any()
            or df.loc[monthly, "n_postos_media"].isna().any()
        ):
            errors.append("n_postos_media é média de contagens somente na frequência mensal")
        if not df.loc[weekly, "n_semanas"].eq(1).all():
            errors.append("n_semanas deve ser 1 nos registros semanais")
        data = pd.to_datetime(df["data"])
        start, end = pd.to_datetime(df["periodo_inicio"]), pd.to_datetime(df["periodo_fim"])
        if (start > end).any() or not data[weekly].eq(start[weekly]).all():
            errors.append("Período inválido ou início semanal diferente de data")
        month_starts = pd.Series(
            start[monthly].dt.to_period("M").dt.to_timestamp(), index=start[monthly].index
        )
        if not data[monthly].eq(month_starts).all():
            errors.append("data mensal deve ser o primeiro dia do mês de início das semanas")
        brasil = df["nivel"].eq("brasil")
        municipality = df["nivel"].eq("municipio")
        if df.loc[brasil, "uf"].ne("").any() or df.loc[~brasil, "uf"].eq("").any():
            errors.append("UF incompatível com o nível territorial")
        if (
            df.loc[~municipality, "municipio"].ne("").any()
            or df.loc[municipality, "municipio"].eq("").any()
        ):
            errors.append("Município incompatível com o nível territorial")
        return not errors, errors


ANP_DIESEL_PRECOS_V2 = PrecosDieselContract(
    name="anp_diesel.precos",
    version="2.0",
    effective_from="2.0.0",
    columns=[
        contracts.Column(
            "data",
            contracts.ColumnType.DATE,
            description="Início semanal ou primeiro dia do mês de início das semanas",
        ),
        contracts.Column("uf", contracts.ColumnType.STRING, description="Vazia para Brasil"),
        contracts.Column(
            "municipio", contracts.ColumnType.STRING, description="Vazio para Brasil e UF"
        ),
        contracts.Column(
            "produto",
            contracts.ColumnType.STRING,
            description="DIESEL é o óleo diesel B S500 comum; DIESEL S10 é o S10",
        ),
        contracts.Column(
            "preco_venda",
            contracts.ColumnType.FLOAT,
            min_value=0,
            unit="BRL/litro",
            description="Município: média simples dos postos; UF e Brasil: média ponderada pelas vendas das "
            "distribuidoras (ANP, desde 31/10/2004)",
        ),
        contracts.Column(
            "preco_compra", contracts.ColumnType.FLOAT, nullable=True, min_value=0, unit="BRL/litro"
        ),
        contracts.Column(
            "n_postos",
            contracts.ColumnType.INTEGER,
            nullable=True,
            min_value=0,
            description="Postos da amostra, publicado no semanal e nulo no mensal; não é o peso da média da UF "
            "e do Brasil",
        ),
        contracts.Column(
            "margem",
            contracts.ColumnType.FLOAT,
            nullable=True,
            unit="BRL/litro",
            description="Diferença derivada entre as médias de revenda e distribuição",
        ),
        contracts.Column(
            "periodo_inicio",
            contracts.ColumnType.DATE,
            description="Menor início das semanas selecionadas",
        ),
        contracts.Column(
            "periodo_fim",
            contracts.ColumnType.DATE,
            description="Maior término publicado das semanas selecionadas",
        ),
        contracts.Column("nivel", contracts.ColumnType.STRING),
        contracts.Column("unidade", contracts.ColumnType.STRING),
        contracts.Column("agregacao", contracts.ColumnType.STRING),
        contracts.Column("n_semanas", contracts.ColumnType.INTEGER, min_value=1),
        contracts.Column(
            "n_postos_media",
            contracts.ColumnType.FLOAT,
            nullable=True,
            min_value=0,
            description="Média das contagens semanais; não são postos únicos",
        ),
    ],
    primary_key=[],
    guarantees=[
        "Unidade R$/litro verificada no arquivo, valores monetários float64",
        "Período semanal publicado preservado; filtro inclusivo pela data de início da semana",
        "Município selecionado por igualdade após normalização de acentos e espaços",
        "Ausência de distribuição preservada; margem é diferença de médias, não lucro líquido",
        "Mensal derivado por média aritmética das médias semanais disponíveis, sem ponderação por postos nem rateio diário",
        "Mensal usa somente as semanas selecionadas e não garante cobertura do mês completo",
        "Linhas semanais idênticas são deduplicadas com aviso; valores conflitantes na mesma semana são recusados",
        "Arquivos correntes com observações históricas; ano do filtro não seleciona edição de publicação",
        "preco_venda da UF e do Brasil é o publicado, ponderado pelas vendas; não é a média dos níveis de "
        "baixo por n_postos",
    ],
)

PRECOS_DIESEL_V1 = replace(ANP_DIESEL_PRECOS_V2, name="precos_diesel", version="1.0")

contracts.register_contract("anp_diesel_precos", ANP_DIESEL_PRECOS_V2)
contracts.register_contract("precos_diesel", PRECOS_DIESEL_V1)
