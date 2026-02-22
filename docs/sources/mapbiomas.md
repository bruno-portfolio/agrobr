# MapBiomas

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Provedor** | Projeto MapBiomas — Rede multi-institucional |
| **Dados** | Cobertura e uso da terra, transicoes entre classes |
| **Acesso** | Download XLSX publico (data.mapbiomas.org Dataverse) |
| **Formato** | XLSX (openpyxl) |
| **Autenticacao** | Nenhuma |
| **Licenca** | Dados publicos — livre com citacao |
| **Serie Historica** | 1985-2024 (Colecao 10) |

## Origem dos Dados

O MapBiomas e um projeto colaborativo multi-institucional que produz mapas anuais de cobertura e uso da terra do Brasil a partir de imagens de satelite Landsat (30m de resolucao). Os dados sao gerados via classificacao automatica usando Google Earth Engine.

O agrobr acessa as **estatisticas tabulares** (areas por classe, bioma e estado) disponibilizadas como planilhas XLSX na pagina de estatisticas do projeto. Os dados geoespaciais (rasters) nao sao incluidos nesta versao.

## Colecoes

O MapBiomas publica colecoes anuais com melhorias metodologicas:

| Colecao | Data | Periodo |
|---------|------|---------|
| 10 (atual) | Agosto 2025 | 1985-2024 |

O agrobr usa a colecao mais recente por padrao, com opcao de selecionar versoes anteriores via parametro `colecao`.

## Estrutura dos Dados

### Cobertura (COVERAGE_10)

Dados em formato wide: uma coluna por ano (1985-2024) com area em hectares para cada combinacao bioma x estado x classe.

Apos parsing, o agrobr converte para formato long: uma linha por combinacao bioma x estado x classe x ano.

### Transicao (TRANSITION_10)

Area em hectares de transicao entre pares de classes para cada periodo temporal. Inclui periodos anuais (ex: 2019-2020), quinquenais e o total 1985-2024.

## Exemplo de Uso

```python
import agrobr

# Cobertura do Cerrado em 2020
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020)

# Pastagem (classe 15) em Goias
df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", estado="GO", classe_id=15)

# Transicao floresta→pastagem no Cerrado
df = await agrobr.mapbiomas.transicao(
    bioma="Cerrado",
    classe_de_id=3,   # Formacao Florestal
    classe_para_id=15, # Pastagem
    periodo="2019-2020",
)

# Com metadados
df, meta = await agrobr.mapbiomas.cobertura(
    bioma="Cerrado", ano=2020, return_meta=True
)
print(meta.records_count, meta.fetch_duration_ms)
```

## Limitacoes

- Apenas dados tabulares (estatisticas). Dados geoespaciais (rasters/GEE) ficam para versao futura
- O XLSX estadual (~23 MB) e baixado inteiro na primeira chamada (o parsing seleciona os filtros)
- Nivel municipal (~660 MB) disponivel via `fetch_biome_state_municipality` mas nao exposto na API publica por questao de performance — sera adicionado em versao futura com cache local
- Nomes de classes seguem a legenda oficial do MapBiomas (em portugues)

## Cache e Atualizacao

- **TTL**: 7 dias (colecoes sao publicadas anualmente)
- MapBiomas publica uma nova colecao por ano com dados retroativos recalculados
- Recomendado: especificar filtros para reduzir volume de dados no DataFrame

## Links

- [MapBiomas Brasil](https://brasil.mapbiomas.org)
- [Estatisticas](https://brasil.mapbiomas.org/estatisticas/)
- [Legenda](https://brasil.mapbiomas.org/codigos-de-legenda/)
- [Citacao](https://brasil.mapbiomas.org/termos-de-uso/)
