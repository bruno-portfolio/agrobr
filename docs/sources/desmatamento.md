# Desmatamento (PRODES/DETER)

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Provedor** | INPE — Instituto Nacional de Pesquisas Espaciais |
| **Programas** | PRODES (anual) e DETER (alertas diarios) |
| **Acesso** | API WFS publica (TerraBrasilis GeoServer) |
| **Formato** | GeoJSON do WFS 2.0 (`application/json`) nos dois modos |
| **Autenticacao** | Nenhuma |
| **Licenca** | CC BY-SA 4.0 (INPE), com atribuição e CompartilhaIgual nas adaptações |
| **Serie Historica** | PRODES: 2000+, DETER: 2016+ (Amazonia), 2020+ (Cerrado) |

## Origem dos Dados

O INPE opera dois sistemas complementares de monitoramento do desmatamento:

- **PRODES**: Mapeamento anual consolidado do desmatamento por corte raso. Usa imagens Landsat (30m) para gerar poligonos de desmatamento com area minima de 6.25 hectares. Resultado oficial usado pelo governo federal.

- **DETER**: Sistema de alertas diarios para acoes de fiscalizacao. Usa imagens de sensores como CBERS-4, AMAZONIA-1 e Landsat com resolucao variavel. Detecta desmatamento, degradacao, mineracao e cicatrizes de queimada.

## Acesso via TerraBrasilis

Os dados são acessados via GeoServer WFS 2.0.0 do TerraBrasilis em JSON (`outputFormat=application/json`), paginados por `startIndex`/`count`, com filtros via `CQL_FILTER`.

### PRODES — Workspaces por Bioma

| Bioma | Workspace | Layer |
|-------|-----------|-------|
| Amazonia | prodes-amazon-nb | yearly_deforestation_biome |
| Cerrado | prodes-cerrado-nb | yearly_deforestation |
| Caatinga | prodes-caatinga-nb | yearly_deforestation |
| Mata Atlantica | prodes-mata-atlantica-nb | yearly_deforestation |
| Pantanal | prodes-pantanal-nb | yearly_deforestation |
| Pampa | prodes-pampa-nb | yearly_deforestation |

### DETER — Workspaces por Bioma

| Bioma | Workspace | Layer |
|-------|-----------|-------|
| Amazonia | deter-amz | deter_amz |
| Cerrado | deter-cerrado-nb | deter_cerrado |

## Geometria (prodes_geo)

A funcao `prodes_geo()` retorna desmatamento PRODES consolidado com poligonos de geometria como GeoDataFrame.

| Campo | Valor |
|-------|-------|
| **Coluna de geometria** | `geom` (uniforme em todos os 6 biomas) |
| **Formato** | MultiPolygon EPSG:4326 |
| **max_registros (padrão)** | 10.000 (tabular: 50.000) |
| **outputFormat** | `application/json` (GeoJSON) |

## Geometria (deter_geo)

A funcao `deter_geo()` retorna alertas DETER com poligonos de geometria como GeoDataFrame.

| Campo | Valor |
|-------|-------|
| **Coluna de geometria (AMZ)** | `geom` |
| **Coluna de geometria (Cerrado)** | `st_multi` |
| **Formato** | MultiPolygon EPSG:4326 |
| **Volume por feature** | ~1.1 KB com geometria |
| **max_registros (padrão)** | 10.000 (tabular: 50.000) |
| **outputFormat** | `application/json` (GeoJSON) |

A coluna de geometria e bioma-especifica no GeoServer. O parser normaliza ambas para `geometry` no GeoDataFrame de saida.

## Normalizacao de Bioma

O parametro `bioma` aceita variantes com/sem acento e case insensitive:

- `"amazonia"` ou `"amazônia"` → `"Amazônia"`
- `"cerrado"` → `"Cerrado"`
- `"mata atlantica"` ou `"mata atlântica"` → `"Mata Atlântica"`

A normalizacao e aplicada automaticamente em `prodes()`, `prodes_geo()`, `deter()` e `deter_geo()`.

## Exemplo de Uso

```python
import agrobr

# PRODES — desmatamento anual consolidado
df_prodes = await agrobr.desmatamento.prodes(
    bioma="Cerrado",
    ano=2022,
    uf="MT",
)

# DETER — alertas em tempo real
df_deter = await agrobr.desmatamento.deter(
    bioma="Amazônia",
    uf="PA",
    inicio="2024-01-01",
    fim="2024-06-30",
)

# Com metadados
df, meta = await agrobr.desmatamento.prodes(
    bioma="Cerrado", ano=2022, return_meta=True
)
print(meta.records_count, meta.fetch_duration_ms)

# PRODES com geometria (requer pip install agrobr[geo])
gdf_prodes = await agrobr.desmatamento.prodes_geo(
    bioma="Cerrado",
    ano=2022,
    uf="MT",
)

# DETER com geometria (requer pip install agrobr[geo])
gdf = await agrobr.desmatamento.deter_geo(
    bioma="Amazônia",
    uf="PA",
    inicio="2024-01-01",
    fim="2024-06-30",
)
```

## Limitacoes

- DETER so disponivel para Amazonia e Cerrado
- Na Amazônia, o PRODES do agrobr é o recorte do bioma (`yearly_deforestation_biome`), não o da Amazônia Legal, onde o INPE publica a taxa de destaque. Em 2024, a nota técnica do INPE dá cerca de 6.288 km² para a Amazônia Legal, e a soma das UFs do bioma no agrobr dá 6.068,9 km²
- O agrobr pagina o WFS: `tamanho_pagina` feições por página (500; 100 nas `_geo`; até 2.000 e 500), com 2 s entre as requisições, até `max_registros` (50.000; 10.000 nas `_geo`). Além do limite, sai o prefixo em ordem de `fid` (PRODES) ou `gid` (DETER), com `UserWarning`. Filtre por `ano`, `uf` ou datas, que vão ao servidor, ou use `max_registros=None` ([guia de migração, §84](../guides/migracao-2.md#84-desmatamento-paginacao-corte-e-custo-da-chamada-padrao))
- Source API (`agrobr.desmatamento.*`) retorna poligonos individuais (granularidade fina); o dataset `datasets.desmatamento` entrega agregados conforme o contrato: anual por uf/classe/bioma no PRODES e diário por uf/município/classe/bioma no DETER
- Pos-migracao BiomasBR (03/2026), os layers PRODES de Amazonia, Pantanal, Caatinga e Mata Atlantica estao temporariamente quebrados no GeoServer do INPE (ServiceException para qualquer cliente); Cerrado e Pampa operacionais
- DETER e sistema de alerta, nao de consolidacao — pode haver sobreposicao
- No DETER Cerrado, `municipio_id` é sempre nulo porque a camada da fonte não fornece esse identificador.
- `prodes_geo()` e `deter_geo()` retornam geometria (~10x mais volume que tabular) — usar filtros para reduzir dados

## Cache e Atualizacao

- Não há cache local: cada chamada baixa os dados do TerraBrasilis.
- O PRODES publica dados consolidados anuais, atualizados aproximadamente uma vez por ano.
- O DETER publica alertas diários, atualizados frequentemente.
- Recomenda-se usar filtros de estado e ano para reduzir o volume de dados.

## Links

- [TerraBrasilis](https://terrabrasilis.dpi.inpe.br)
- [PRODES](https://www.obt.inpe.br/OBT/assuntos/programas/amazonia/prodes)
- [DETER](https://www.obt.inpe.br/OBT/assuntos/programas/amazonia/deter)

## Intervalo PRODES

`prodes()` e `prodes_geo()` aceitam `ano` somente como inteiro ou `None`; ano posterior ao corrente levanta `InvalidParameterError` antes da rede. Ano sem feição no WFS (ainda não publicado ou fora da cobertura da camada) devolve resultado vazio, com `UserWarning` e aviso em `meta.validation_warnings`. A cobertura dos polígonos não deve ser confundida com o início das séries históricas de taxas de desmatamento.
