# Queimadas/INPE - BDQueimadas

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | INPE — Instituto Nacional de Pesquisas Espaciais |
| **Website** | [queimadas.dgi.inpe.br](https://queimadas.dgi.inpe.br) |
| **Acesso agrobr** | Direto (CSV publicos) |

## Origem dos Dados

### Fonte

- **URL**: `https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/`
- **Formato**: CSV (latin-1 ou UTF-8), ZIP para dados historicos
- **Acesso**: Publico, sem autenticacao
- **Granularidade**: Diario (`focos_diario_br_YYYYMMDD.csv`) e mensal (fallback em cascata)

## Dados Disponiveis

### Focos de Calor

Deteccao por satelite de pontos de calor (hot spots) no territorio brasileiro:

- Coordenadas geograficas (lat/lon)
- Data e hora GMT da deteccao
- Satelite detector (13 satelites)
- Municipio e estado
- Bioma (6 biomas brasileiros)
- Indicadores: dias sem chuva, precipitacao, risco de fogo, FRP

### Cobertura

- **Temporal**: Desde 2003 (dados anuais); mensal desde 2023; CSV direto desde 2024
- **Espacial**: Todo o territorio brasileiro
- **Frequencia**: Diaria (atualizacao varias vezes ao dia)

### Fallback em cascata (mensal)

O servidor INPE mudou a organizacao dos dados historicos. O client tenta em ordem:

| Periodo | Formato | URL |
|---------|---------|-----|
| 2024+ | `.csv` mensal | `mensal/Brasil/focos_mensal_br_YYYYMM.csv` |
| 2023 | `.zip` mensal | `mensal/Brasil/focos_mensal_br_YYYYMM.zip` |
| 2003-2022 | `.zip` anual | `anual/Brasil_todos_sats/focos_br_todos-sats_YYYY.zip` |

Para dados anuais, o CSV completo do ano e baixado e filtrado pelo mes solicitado.

## Uso

### Focos Mensais

```python
import asyncio
from agrobr import queimadas

async def main():
    # Todos os focos de setembro/2024
    df = await queimadas.focos(ano=2024, mes=9)
    print(f"{len(df)} focos detectados")

    # Filtrar por UF
    df = await queimadas.focos(ano=2024, mes=9, uf="MT")

    # Filtrar por bioma
    df = await queimadas.focos(ano=2024, mes=9, bioma="Cerrado")

    # Com metadados
    df, meta = await queimadas.focos(ano=2024, mes=9, return_meta=True)
    print(meta.source, meta.records_count)

asyncio.run(main())
```

### Focos Diarios

```python
# Focos de um dia especifico
df = await queimadas.focos(ano=2024, mes=9, dia=15)
```

### Filtros Combinados

```python
# Focos em Mato Grosso na Amazonia por satelite de referencia
df = await queimadas.focos(
    ano=2024, mes=9,
    uf="MT",
    bioma="Amazonia",
    satelite="AQUA_M-T",
)
```

## Schema

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `data` | date | Data da deteccao |
| `hora_gmt` | str | Horario GMT (HH:MM) |
| `lat` | float | Latitude (-35 a 6) |
| `lon` | float | Longitude (-74 a -30) |
| `satelite` | str | Nome do satelite |
| `municipio` | str | Nome do municipio |
| `municipio_id` | Int64 | Codigo IBGE |
| `estado` | str | Nome do estado |
| `uf` | str | Sigla UF (2 caracteres) |
| `bioma` | str | Bioma brasileiro |
| `numero_dias_sem_chuva` | float | Dias sem precipitacao |
| `precipitacao` | float | Precipitacao (mm) |
| `risco_fogo` | float | Indice de risco (0-1) |
| `frp` | float | Fire Radiative Power (MW) |

## Satelites

O INPE monitora focos de calor com 13 satelites. O satelite de referencia e o
AQUA_M-T (MODIS), utilizado nas estatisticas oficiais por ter serie temporal
mais longa e consistente.

## Cache

| Aspecto | Valor |
|---------|-------|
| **TTL** | 12 horas |
| **Politica** | TTL fixo |

## Atualizacao

| Aspecto | Valor |
|---------|-------|
| **Frequencia** | Diaria |
| **Satelite referencia** | AQUA_M-T passagens ~13h e ~01h30 UTC |
