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
from datetime import date, timedelta

# Focos de um dia específico (ontem): o INPE só mantém o arquivo diário dos últimos dias
ontem = date.today() - timedelta(days=1)
df = await queimadas.focos(ano=ontem.year, mes=ontem.month, dia=ontem.day)
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
| `cod_municipio` | Int64 | Código IBGE de 7 dígitos, do `municipio_id`; nulo fora de município |

A fonte publica, em alguns meses, focos fora do contrato: a cópia igual sai uma vez; a chave repetida que difere só no FRP
sai em 1 linha com o `frp` nulo; a que difere em outra coluna sai do resultado; e o FRP negativo sai nulo. Cada caso vem com
aviso e a contagem. Os 7 meses de 2023–2025 em que isso acontecia e as regras estão no
[contrato](../contracts/queimadas.md#frp-negativo-e-foco-repetido).

## Satelites

O INPE monitora focos de calor com 13 satelites. O satelite de referencia e o
AQUA_M-T (MODIS), utilizado nas estatisticas oficiais por ter serie temporal
mais longa e consistente.

Sem `satelite=`, `focos()` devolve os focos de todos os satélites, e a contagem soma as detecções de cada um: em agosto de
2025, foram 594.309 focos no total e 18.451 do AQUA_M-T. As estatísticas do INPE por estado usam só o satélite de
referência; para comparar com elas, passe `satelite="AQUA_M-T"`.

## Cache

Não há cache local: cada chamada baixa os dados do INPE.

## Atualizacao

| Aspecto | Valor |
|---------|-------|
| **Frequencia** | Diaria |
| **Satelite referencia** | AQUA_M-T, com passagens por volta das 13h30 e da 01h30, hora local nominal; `hora_gmt` vem em GMT |

O arquivo mensal do mês corrente e o diário do dia corrente são parciais e mudam durante o período: `focos()` avisa e diz no
`source_details` até qual foco o arquivo vai e o `Last-Modified` dele. Veja o
[contrato](../contracts/queimadas.md#mes-corrente-parcial).

## Arquivos históricos

Os CSVs legados com `latitude`, `longitude` e `data_pas` são normalizados para a mesma saída dos arquivos atuais. Quando não existe arquivo mensal, `focos()` usa o ZIP anual. Em 2020, são cerca de 81 MB de download e 584 MB de CSV descompactado; considere também a memória necessária para processar o ano completo.
