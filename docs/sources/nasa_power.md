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
- **Cobertura**: Global, consulta por ponto, desde 1981
- **Comunidade**: AG (Agroclimatology)

## Parâmetros padrão

| Parametro NASA | Nome agrobr | Unidade | Descricao |
|----------------|-------------|---------|-----------|
| `T2M` | `temp_media` | C | Temperatura media a 2m |
| `T2M_MAX` | `temp_max` | C | Temperatura maxima a 2m |
| `T2M_MIN` | `temp_min` | C | Temperatura minima a 2m |
| `PRECTOTCORR` | `precip_mm` | mm/dia | Precipitacao corrigida |
| `RH2M` | `umidade_rel` | % | Umidade relativa a 2m |
| `ALLSKY_SFC_SW_DWN` | `radiacao_mj` | MJ/m2/dia | Radiacao solar incidente |
| `WS2M` | `vento_ms` | m/s | Velocidade do vento a 2m |

Também aceitos em `parameters=`: `PS` (`ps_kpa`, kPa), `WS10M` (`vento_10m_ms`, m/s), `T2MDEW` (`ponto_orvalho`, C), `GWETROOT` (`umidade_solo_raiz`, 1) e `GWETTOP` (`umidade_solo_superficie`, 1). A lista completa sai de `nasa_power.parametros()`.

## Uso

Consultas longas são divididas em blocos. Se um bloco falhar após os retries, a chamada inteira levanta `SourceUnavailableError`; blocos anteriores não são devolvidos como uma série completa. `clima_ponto` e `clima_uf` aceitam somente `agregacao="diario"` ou `"mensal"`, com validação antes da rede. `inicio` vai de 1981-01-01 até hoje, no calendário de Brasília, e o `ano` de `clima_uf`, de 1981 ao ano corrente: início no futuro levanta `InvalidParameterError` antes da rede (a fonte responde sem nenhum dia). O período que começa no passado e passa de hoje sai até o último dia publicado. Isso não implica que todas as variáveis tenham medições em todos os dias: ausências publicadas pela fonte continuam nulas.

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

Usa um ponto representativo fixo por UF, escolhido pelo agrobr (`UF_COORDS`). Não é o centroide oficial da UF.

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
| `uf` | str | Sigla da UF (só em `clima_uf`; ausente no mensal de `clima_ponto`) |
| `precip_acum_mm` | float | Precipitacao acumulada (mm) |
| `temp_media` | float | Temperatura media (C) |
| `temp_max_media` | float | Media das maximas (C) |
| `temp_min_media` | float | Media das minimas (C) |
| `umidade_media` | float | Umidade relativa media (%) |
| `radiacao_media_mj` | float | Radiacao media (MJ/m2/dia) |
| `vento_medio_ms` | float | Vento medio (m/s) |
| `dias` | int | Dias do mês com algum parâmetro válido |
| `data_inicio` | datetime | Primeiro desses dias |
| `data_fim` | datetime | Último desses dias |
| `lat` | float | Latitude do ponto |
| `lon` | float | Longitude do ponto |

## UFs Disponiveis

Todas as 27 UFs brasileiras têm um ponto representativo fixo configurado.
Para analises precisas, usar `clima_ponto()` com coordenadas exatas.

## Nota sobre Resolucao Espacial

NASA POWER combina produtos com características espaciais próprias; a consulta por ponto não estabelece uma resolução única para todas as variáveis. Para UFs grandes
como MT ou PA, o ponto central pode nao representar bem toda a variabilidade
climatica do estado. Para analises regionais detalhadas, consultar multiplos
pontos com `clima_ponto()`.

## Cache

Não há cache local: cada chamada baixa os dados da NASA POWER.

## Atualizacao

| Aspecto | Valor |
|---------|-------|
| **Frequencia** | Dados com ~2 dias de lag |
| **Historico** | Desde 1981 |
| **Resolucao** | Diaria |

## Agregação e ausência de medições

Precipitação é acumulada no tempo por estação. O INMET calcula o valor mensal da UF pela média simples dos acumulados das estações com chuva válida em todos os dias do mês, não pela soma das estações; `estacoes_chuva` e `estacoes_chuva_parciais` contam as que entraram e as que ficaram fora. `num_estacoes` conta estações presentes. O NASA POWER usa um ponto representativo fixo da UF. Suas coordenadas são preservadas pelo dataset; isso não é uma média territorial ou centroide comprovado, nem demonstra uma célula espacial comum a todas as variáveis. O dia padrão NASA usa [LST](https://power.larc.nasa.gov/docs/services/api/temporal/daily/#time-standards), enquanto o INMET usa UTC; o dataset registra essa distinção em `base_tempo`.

O dado do NASA POWER é a reanálise MERRA-2 em ponto de grade, e não uma estação, até o mês anterior; no mês corrente é o GEOS-IT, de baixa latência, que a NASA substitui pelo MERRA-2 depois, e a radiação recente vem do FLASHFlux, que o SYN1deg substitui ([fontes da NASA POWER](https://power.larc.nasa.gov/docs/methodology/data/sources/); a NASA recomenda parar a análise de tendência 2 meses antes). O `source_details` traz as fontes que o cabeçalho de cada consulta declara (`fontes` e `fontes_por_bloco`) e o trecho de baixa latência (`periodos_baixa_latencia`, com a `origem` e se é `exato`), e o `validation_warnings`, um aviso por fonte. O cabeçalho só dá as fontes da janela: num bloco com as 2, o início do GEOS-IT sai da regra da NASA (o MERRA-2 fecha por mês) quando há um só dia 1 no bloco; com mais de um, o aviso cobre o bloco e diz que o cabeçalho não separa por dia. Comparado ao INMET em 2025 (conferência de 26/09/2026), a chuva anual saiu 41% abaixo no DF e 27% abaixo em MT, e a temperatura média mensal, até 2,9 °C acima. No `datasets.clima` com `fonte=None`, a troca de rota muda a natureza do dado: veja o [contrato `clima`](../contracts/clima.md).

Grupos inteiramente sem medições permanecem nulos: ausência não significa 0 mm. Somatórios usam somente as medições disponíveis, sem completar ou extrapolar horas/dias faltantes; `dias`, `data_inicio` e `data_fim` dão a cobertura de cada mês; compare-os com o calendário antes de comparar totais. A mesma preservação de ausência vale para radiação diária INMET. O contrato mensal `clima` 3.1 permite precipitação e temperaturas nulas; `clima_estacao` diário e `clima_estacao_horaria` horário são ambos 1.0. No mensal, `lat`/`lon` preservam o ponto NASA, e `agregacao_espacial` distingue `ponto_grade` de `estacoes` INMET.
