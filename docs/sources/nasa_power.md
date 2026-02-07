# NASA POWER - Dados Climaticos Globais

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | NASA / LaRC |
| **Website** | [power.larc.nasa.gov](https://power.larc.nasa.gov) |
| **Acesso agrobr** | REST API (JSON), sem autenticacao |
| **Substitui** | INMET (API fora do ar desde jan/2026) |

## Origem dos Dados

### Fonte

- **URL**: `https://power.larc.nasa.gov/api/temporal/daily/point`
- **Formato**: JSON
- **Acesso**: Publico, sem restricoes de autenticacao
- **Cobertura**: Global, grid 0.5 grau, desde 1981
- **Comunidade**: AG (Agroclimatology)

## Parametros Disponveis

| Parametro NASA | Nome agrobr | Unidade | Descricao |
|----------------|-------------|---------|-----------|
| `T2M` | `temp_media` | C | Temperatura media a 2m |
| `T2M_MAX` | `temp_max` | C | Temperatura maxima a 2m |
| `T2M_MIN` | `temp_min` | C | Temperatura minima a 2m |
| `PRECTOTCORR` | `precip_mm` | mm/dia | Precipitacao corrigida |
| `RH2M` | `umidade_rel` | % | Umidade relativa a 2m |
| `ALLSKY_SFC_SW_DWN` | `radiacao_mj` | MJ/m2/dia | Radiacao solar incidente |
| `WS2M` | `vento_ms` | m/s | Velocidade do vento a 2m |

## Uso

### Dados por ponto (lat/lon)

```python
import asyncio
from agrobr import nasa_power

async def main():
    # Dados diarios de Sorriso-MT
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-01-31"
    )
    print(df)

    # Agregacao mensal
    df = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-12-31",
        agregacao="mensal"
    )

    # Com metadados
    df, meta = await nasa_power.clima_ponto(
        lat=-12.6, lon=-56.1,
        inicio="2024-01-01", fim="2024-01-31",
        return_meta=True
    )

asyncio.run(main())
```

### Dados por UF

Usa coordenadas centrais da UF como ponto representativo.

```python
# Clima mensal de MT em 2024
df = await nasa_power.clima_uf("MT", ano=2024)

# Diario
df = await nasa_power.clima_uf("MT", ano=2024, agregacao="diario")

# Com metadados
df, meta = await nasa_power.clima_uf("MT", ano=2024, return_meta=True)
```

## Schema - Diario

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `data` | datetime | Data da observacao |
| `lat` | float | Latitude do ponto |
| `lon` | float | Longitude do ponto |
| `uf` | str | Sigla da UF (quando usado clima_uf) |
| `temp_media` | float | Temperatura media (C) |
| `temp_max` | float | Temperatura maxima (C) |
| `temp_min` | float | Temperatura minima (C) |
| `precip_mm` | float | Precipitacao (mm/dia) |
| `umidade_rel` | float | Umidade relativa (%) |
| `radiacao_mj` | float | Radiacao solar (MJ/m2/dia) |
| `vento_ms` | float | Velocidade do vento (m/s) |

## Schema - Mensal

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `mes` | datetime | Primeiro dia do mes |
| `uf` | str | Sigla da UF |
| `precip_acum_mm` | float | Precipitacao acumulada (mm) |
| `temp_media` | float | Temperatura media (C) |
| `temp_max_media` | float | Media das maximas (C) |
| `temp_min_media` | float | Media das minimas (C) |
| `umidade_media` | float | Umidade relativa media (%) |
| `radiacao_media_mj` | float | Radiacao media (MJ/m2/dia) |
| `vento_medio_ms` | float | Vento medio (m/s) |
| `lat` | float | Latitude do ponto |
| `lon` | float | Longitude do ponto |

## UFs Disponiveis

Todas as 27 UFs brasileiras possuem coordenadas centrais mapeadas.
Para analises precisas, usar `clima_ponto()` com coordenadas exatas.

## Nota sobre Resolucao Espacial

NASA POWER fornece dados em grid de 0.5 grau (~55km). Para UFs grandes
como MT ou PA, o ponto central pode nao representar bem toda a variabilidade
climatica do estado. Para analises regionais detalhadas, consultar multiplos
pontos com `clima_ponto()`.

## Cache

| Aspecto | Valor |
|---------|-------|
| **TTL** | 24 horas |
| **Stale maximo** | 30 dias |
| **Politica** | TTL fixo |

## Atualizacao

| Aspecto | Valor |
|---------|-------|
| **Frequencia** | Dados com ~2 dias de lag |
| **Historico** | Desde 1981 |
| **Resolucao** | Diaria |
