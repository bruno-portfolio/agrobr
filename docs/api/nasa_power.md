# API NASA POWER

O módulo NASA POWER fornece dados climatologicos gridded globais da NASA — temperatura, precipitação, radiação, umidade e vento. Alternativa ao INMET que não requer token.

## Funções

### `clima_ponto`

Dados climatologicos para um ponto geográfico (latitude/longitude).

```python
async def clima_ponto(
    lat: float,
    lon: float,
    inicio: str | date,
    fim: str | date,
    agregacao: str = "diario",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `lat` | `float` | Latitude (-90 a 90) |
| `lon` | `float` | Longitude (-180 a 180) |
| `inicio` | `str \| date` | Data inicial (YYYY-MM-DD), de 1981-01-01 até hoje no calendário de Brasília; no futuro, `InvalidParameterError` antes da rede |
| `fim` | `str \| date` | Data final (YYYY-MM-DD); depois de hoje, a série sai até o último dia publicado |
| `agregacao` | `str` | `"diario"` (default) ou `"mensal"` |
| `as_polars` | `bool` | Retorna polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |
| `parameters` | `list[str] \| None` | Códigos NASA POWER a pedir (1 a 20, sem repetir), do catálogo de `parametros()`; `None` pede os 7 padrão (`T2M`, `T2M_MAX`, `T2M_MIN`, `PRECTOTCORR`, `RH2M`, `ALLSKY_SFC_SW_DWN`, `WS2M`); os outros códigos do catálogo só vêm quando listados |

**Retorno:**

DataFrame com colunas (diário): `data`, `lat`, `lon`, `uf` (vazia em `clima_ponto`), `temp_media`, `temp_max`, `temp_min`, `precip_mm`, `umidade_rel`, `radiacao_mj`, `vento_ms`

Com `agregacao="mensal"`, as colunas agregadas são renomeadas: `mes` (timestamp), `precip_acum_mm`, `temp_media`, `temp_max_media`, `temp_min_media`, `umidade_media`, `radiacao_media_mj`, `vento_medio_ms` (mais `lat`/`lon`). `dias`, `data_inicio` e `data_fim` dão os dias do mês com algum parâmetro válido. O mês cortado pelo período pedido sai parcial e não é extrapolado: de 15/01 a 05/02/2025, fevereiro sai com `dias=5` e 17,81 mm, contra 28 dias e 52,33 mm do mês inteiro (schema 1.2).

**Exemplo:**

```python
from agrobr import nasa_power

# Clima diario para Sorriso-MT
df = await nasa_power.clima_ponto(
    lat=-12.55, lon=-55.72,
    inicio="2024-01-01", fim="2024-03-31"
)

# Clima mensal
df = await nasa_power.clima_ponto(
    lat=-12.55, lon=-55.72,
    inicio="2023-01-01", fim="2023-12-31",
    agregacao="mensal"
)
```

---

### `clima_uf`

Dados climatologicos de um ponto representativo fixo configurado para a UF.

```python
async def clima_uf(
    uf: str,
    ano: int,
    agregacao: str = "mensal",
    *,
    as_polars: bool = False,
    return_meta: bool = False,
    parameters: list[str] | None = None,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parametros:**

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | `str` | Sigla UF (ex: "MT", "SP") |
| `ano` | `int` | Ano de referencia, de 1981 ao ano corrente; ano seguinte levanta `InvalidParameterError` antes da rede |
| `agregacao` | `str` | `"diario"` ou `"mensal"` (default) |
| `as_polars` | `bool` | Retorna polars.DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |
| `parameters` | `list[str] \| None` | Códigos NASA POWER a pedir (1 a 20, sem repetir), do catálogo de `parametros()`; `None` pede os 7 padrão (`T2M`, `T2M_MAX`, `T2M_MIN`, `PRECTOTCORR`, `RH2M`, `ALLSKY_SFC_SW_DWN`, `WS2M`); os outros códigos do catálogo só vêm quando listados |

**Exemplo:**

```python
from agrobr import nasa_power

df = await nasa_power.clima_uf("MT", 2024)
```

`as_polars`, `return_meta` e `parameters` só por nome.

---

### `parametros`

Catálogo dos códigos diários da comunidade AG que o agrobr lê, sem rede.

```python
def parametros() -> pd.DataFrame
```

Colunas: `codigo` (nome do parâmetro na NASA POWER, o valor aceito em `parameters`), `coluna` e `unidade` (saída diária),
`coluna_mensal`, `unidade_mensal` e `agregacao_mensal` (saída mensal), `comunidade` e `frequencia_origem`.

```python
from agrobr import nasa_power

nasa_power.parametros()[["codigo", "coluna", "unidade"]]
df = await nasa_power.clima_ponto(-12.55, -55.72, "2024-01-01", "2024-01-31", parameters=["T2M", "PRECTOTCORR"])
```

## Versão Síncrona

```python
from agrobr.sync import nasa_power

df = nasa_power.clima_ponto(lat=-12.55, lon=-55.72, inicio="2024-01-01", fim="2024-03-31")
df = nasa_power.clima_uf("MT", 2024)
```

## Notas

- Dados da [NASA POWER](https://power.larc.nasa.gov/) — licença livre
- Usa coordenadas representativas fixas para `clima_uf()` — para analises precisas, use `clima_ponto()` com coordenadas especificas
- Alternativa ao INMET para quem não tem token

## Agregação e ausência de medições

Precipitação é acumulada no tempo por estação. O INMET calcula o valor mensal da UF pela média simples dos acumulados das estações com chuva válida em todos os dias do mês, não pela soma das estações; `estacoes_chuva` e `estacoes_chuva_parciais` contam as que entraram e as que ficaram fora. `num_estacoes` conta estações presentes. O NASA POWER usa um ponto representativo fixo da UF. Suas coordenadas são preservadas pelo dataset; isso não é uma média territorial ou centroide comprovado, nem demonstra uma célula espacial comum a todas as variáveis. O dia padrão NASA usa [LST](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards), enquanto o INMET usa UTC; o dataset registra essa distinção em `base_tempo`.

Grupos inteiramente sem medições permanecem nulos: ausência não significa 0 mm. Somatórios usam somente as medições disponíveis, sem completar ou extrapolar horas/dias faltantes; `dias`, `data_inicio` e `data_fim` dão a cobertura de cada mês; compare-os com o calendário antes de comparar totais. A mesma preservação de ausência vale para radiação diária INMET. O contrato mensal `clima` 3.1 permite precipitação e temperaturas nulas; `clima_estacao` diário e `clima_estacao_horaria` horário são ambos 1.0. No mensal, `lat`/`lon` preservam o ponto NASA, e `agregacao_espacial` distingue `ponto_grade` de `estacoes` INMET.
