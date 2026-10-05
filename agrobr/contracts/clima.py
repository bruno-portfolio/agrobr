from __future__ import annotations

import pandas as pd

from agrobr import contracts


class ClimaHorarioContract(contracts.Contract):
    def validate(self, df: pd.DataFrame) -> tuple[bool, list[str]]:
        valid, errors = super().validate(df)
        if not df.columns.is_unique:
            return valid, errors
        if (
            "hora_utc" in df
            and not df["hora_utc"]
            .map(
                lambda value: (
                    isinstance(value, str)
                    and len(value) == 4
                    and value.isascii()
                    and value.isdigit()
                    and value.endswith("00")
                    and 0 <= int(value[:2]) <= 23
                )
            )
            .all()
        ):
            errors.append("hora_utc deve representar uma hora UTC entre 0000 e 2300")
        return not errors, errors


CLIMA_V3 = contracts.Contract(
    name="datasets.clima",
    version="3.1",
    effective_from="2.0.0",
    primary_key=["mes", "uf"],
    columns=[
        contracts.Column(name="mes", type=contracts.ColumnType.DATE),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="precip_acum_mm",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="mm",
            min_value=0,
        ),
        contracts.Column(
            name="temp_media", type=contracts.ColumnType.FLOAT, nullable=True, unit="°C"
        ),
        contracts.Column(
            name="temp_max_media", type=contracts.ColumnType.FLOAT, nullable=True, unit="°C"
        ),
        contracts.Column(
            name="temp_min_media", type=contracts.ColumnType.FLOAT, nullable=True, unit="°C"
        ),
        contracts.Column(
            name="num_estacoes", type=contracts.ColumnType.INTEGER, nullable=True, min_value=0
        ),
        contracts.Column(
            name="umidade_media",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="%",
            min_value=0,
            max_value=100,
        ),
        contracts.Column(
            name="radiacao_media_mj",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="MJ/m²/dia",
            min_value=0,
        ),
        contracts.Column(
            name="vento_medio_ms",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="m/s",
            min_value=0,
        ),
        contracts.Column(name="fonte", type=contracts.ColumnType.STRING),
        contracts.Column(
            name="lat",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="graus",
            description="Latitude do ponto NASA; nula para o agregado de estações INMET",
            stable=False,
            min_value=-90,
            max_value=90,
        ),
        contracts.Column(
            name="lon",
            type=contracts.ColumnType.FLOAT,
            nullable=True,
            unit="graus",
            description="Longitude do ponto NASA; nula para o agregado de estações INMET",
            stable=False,
            min_value=-180,
            max_value=180,
        ),
        contracts.Column(
            name="agregacao_espacial",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description="estacoes ou ponto_grade; fórmulas por variável em MetaInfo.source_details",
            stable=False,
        ),
        contracts.Column(
            name="base_tempo",
            type=contracts.ColumnType.STRING,
            nullable=True,
            description="UTC no INMET; LST na consulta diária padrão NASA POWER",
            stable=False,
        ),
        contracts.Column(
            name="estacoes_chuva",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            description="Estações INMET com chuva válida em todos os dias do mês; só elas entram na média",
            stable=False,
            min_value=0,
        ),
        contracts.Column(
            name="estacoes_chuva_parciais",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            description="Estações INMET com chuva válida em parte dos dias do mês; ficam fora da média",
            stable=False,
            min_value=0,
        ),
        contracts.Column(
            name="dias",
            type=contracts.ColumnType.INTEGER,
            nullable=True,
            description="Dias do mês com ao menos um valor diário válido",
            stable=False,
            min_value=0,
            max_value=31,
        ),
        contracts.Column(
            name="data_inicio",
            type=contracts.ColumnType.DATE,
            nullable=True,
            description="Primeiro dia do mês com valor diário válido",
            stable=False,
        ),
        contracts.Column(
            name="data_fim",
            type=contracts.ColumnType.DATE,
            nullable=True,
            description="Último dia do mês com valor diário válido",
            stable=False,
        ),
    ],
    guarantees=[
        "PK unica por combinacao mes + uf",
        "'uf' sempre uppercase 2 letras",
        "'fonte' sempre 'inmet' ou 'nasa_power'",
        "'num_estacoes', 'estacoes_chuva' e 'estacoes_chuva_parciais' presentes apenas quando fonte='inmet'",
        "'umidade_media', 'radiacao_media_mj', 'vento_medio_ms' presentes apenas quando fonte='nasa_power'",
        "Agregacao mensal: 'mes' sempre primeiro dia do mes",
        "Temperaturas ausentes permanecem nulas; ausência não é zero",
        "Precipitação INMET: média dos acumulados das estações com chuva válida em todos os dias do mês; "
        "sem nenhuma, nula e com aviso",
        "'dias', 'data_inicio' e 'data_fim' dão a cobertura diária do mês; mês parcial não é extrapolado",
        "Temperatura INMET: média dos registros diários válidos, sem pesos iguais por estação",
        "Coordenadas NASA identificam um ponto; não representam média territorial da UF",
    ],
)

CLIMA_ESTACAO_HORARIA_V1 = ClimaHorarioContract(
    name="datasets.clima_estacao_horaria",
    version="1.0",
    effective_from="2.0.0",
    primary_key=["data", "hora_utc", "estacao"],
    columns=[
        contracts.Column(name="data", type=contracts.ColumnType.DATE, nullable=False),
        contracts.Column(name="hora_utc", type=contracts.ColumnType.STRING, nullable=False),
        contracts.Column(name="estacao", type=contracts.ColumnType.STRING, nullable=False),
        contracts.Column(name="uf", type=contracts.ColumnType.STRING, nullable=True),
        *[
            contracts.Column(name=name, type=contracts.ColumnType.FLOAT, nullable=True, unit=unit)
            for name, unit in (
                ("temperatura", "°C"),
                ("temperatura_max", "°C"),
                ("temperatura_min", "°C"),
                ("umidade", "%"),
                ("umidade_max", "%"),
                ("umidade_min", "%"),
                ("precipitacao_mm", "mm"),
                ("pressao_hpa", "hPa"),
                ("vento_ms", "m/s"),
                ("vento_dir", "graus"),
                ("vento_rajada_ms", "m/s"),
                ("radiacao_kj_m2", "kJ/m²"),
                ("ponto_orvalho", "°C"),
            )
        ],
    ],
    guarantees=[
        "PK única por data + hora_utc + estação",
        "data identifica o dia UTC; hora_utc contém HH00",
        "Medições ausentes permanecem nulas, sem interpolação",
        "Dados de automáticas são observações brutas, não uma série consistida",
    ],
)

contracts.register_contract("clima", CLIMA_V3)
contracts.register_contract("clima_estacao_horaria", CLIMA_ESTACAO_HORARIA_V1)
