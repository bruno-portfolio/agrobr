# MapBiomas (Cobertura e Uso da Terra)

Dados tabulares do Projeto MapBiomas — area (ha) por classe de cobertura e uso da terra, bioma e estado, com serie historica anual desde 1985.

## `mapbiomas.cobertura()`

Area por classe de cobertura e uso da terra x bioma x estado x ano.

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020, uf="GO")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Nao | Bioma: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pampa", "Pantanal". Se None, todos |
| `uf` | `str` | Nao | Sigla ou nome completo da UF (ex: `"MT"`, `"Mato Grosso"`). Caixa e acentos são opcionais; valor inválido levanta `InvalidParameterError` com as siglas válidas, antes do download |
| `ano` | `int` | Nao | Ano: 1985-2025 na coleção 11; 1985-2024 na coleção 10. Se None, todos os anos |
| `classe_id` | `int` | Nao | Codigo de classe MapBiomas (ex: 15 para Pastagem). Código fora das classes publicadas na coleção levanta `InvalidParameterError` com a lista, depois do download |
| `nivel` | `str` | Nao | `"estado"` (default) ou `"municipio"`. O arquivo municipal é baixado inteiro antes dos filtros |
| `municipio` | `str` ou `int` | Nao | Nome inteiro do município (sem diferenciar caixa e acento; com `uf` para desambiguar) ou código territorial de sete dígitos (`int` ou texto). Pedaço de nome, nome de mais de um município sem `uf` e código fora do recurso levantam `InvalidParameterError`. Requer `nivel="municipio"` |
| `colecao` | `int` | Nao | `10` ou `11`; `None` usa a coleção atual (11). Outras coleções levantam `ValueError` antes do download |
| `as_polars` | `bool` | Nao | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Seleção da coleção

Cada coleção possui arquivos e revisão histórica próprios. Fixe `colecao` e preserve o recurso e seu hash para reproduzir uma análise; selecionar a coleção não congela seus bytes. A coleção 11 revisa também os anos anteriores a 2025.

```python
df, meta = await agrobr.mapbiomas.cobertura(
    uf="MT", ano=2025, colecao=11, return_meta=True
)
anterior = await agrobr.mapbiomas.cobertura(uf="MT", ano=2024, colecao=10)
print(meta.data_sources, meta.source_url)
```

`meta.data_sources` identifica `mapbiomas_colecao_11` ou `mapbiomas_colecao_10`; `meta.source_url` registra o arquivo consultado. `colecao=10, ano=2025` é inválido. O dataset `datasets.uso_do_solo()` também aceita e encaminha `colecao`.

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `bioma` | str | Nome do bioma |
| `uf` | str | Sigla da UF (ex: "MT") |
| `municipio` | str | Nome do municipio (apenas quando `nivel="municipio"`) |
| `classe_id` | int | Codigo da classe MapBiomas |
| `classe` | str | Rótulo normalizado pelo SDK conforme a coleção; não é transcrição literal da aba de legenda |
| `nivel_0` | str | Texto publicado, como `Natural`, `Antropic`, `Natural/Antropic` e `Undefined` |
| `ano` | int | Ano de referencia |
| `area_ha` | float | Area em hectares |
| `geocodigo` | str | Apenas cobertura municipal: identificador territorial publicado pelo MapBiomas |
| `id_registro` | Int64 | Apenas cobertura municipal: `ID` numérico original da linha, local à coleção e recurso |
| `cod_municipio` | Int64 | Apenas cobertura municipal: código IBGE tirado do `geocodigo`; nulo sem prefixo de UF |

### Cobertura municipal da Coleção 11

O retorno municipal tem onze colunas, na ordem acima: as oito anteriores, `geocodigo`, `id_registro` e `cod_municipio` (`Int64`, código IBGE tirado do `geocodigo`, nulo sem prefixo de UF). `classe_id`, `ano` e `id_registro` usam pandas `Int64`, `area_ha` usa `float64` e os textos usam o dtype de texto padrão do pandas instalado (`str` no pandas 3, `object` no 2), inclusive em um recorte vazio. O contrato `mapbiomas.cobertura_municipal` 1.1 verifica a chave `(bioma, uf, geocodigo, classe_id, id_registro, ano)` dentro de uma coleção e recurso. O parser municipal tem versão 2; os contratos e o parser estaduais permanecem próprios.

`geocodigo` preserva a coluna `geocode`; não garante pertencimento ao catálogo municipal atual do IBGE. O recurso inclui Lagoa Mirim e Lagoa dos Patos, e um mesmo código pode ocorrer em mais de uma UF. Os cruzamentos territoriais publicados permanecem separados, sem corrigir UF ou somar áreas automaticamente.

O filtro `municipio` sempre seleciona pelo `geocodigo`. Um nome passa por `normalize.resolver_municipio`, que compara o nome inteiro com o cadastro do IBGE e devolve o código: `"Santa Rita"` não casa com `"Santa Rita do Sapucaí"`, e um nome de vários municípios exige a `uf`. Um código de sete dígitos segue direto, porque o recurso também publica geocódigos fora do cadastro (as lagoas); o código que não aparece no recurso da coleção levanta `InvalidParameterError` depois do download. Para achar o nome de um município por pedaço, use `normalize.buscar_municipios`.

O arquivo inteiro é baixado, sem cache integrado. O parser percorre todas as linhas e os 41 anos de 1985–2025 antes de concluir, validando identidade, unicidade e áreas também fora dos filtros. Área ausente, negativa, não finita ou incompatível causa `ParseError`; zero é preservado. Somente linhas inteiramente vazias são ignoradas e contabilizadas. A emissão segue a ordem dos anos e das linhas da planilha, sem materializar um DataFrame nacional largo antes do recorte.

Com `return_meta=True`, `source_details` registra o fingerprint de layout, contagens da população e do retorno, estatísticas anuais e `geocodes_with_multiple_states`. `source_details["acquisition"]` distingue recurso HTTP, eventual confirmação de download e membro XLSX extraído. `raw_content_hash` e `raw_content_size` descrevem o corpo HTTP baixado; quando ele é ZIP, o hash do XLSX está em `acquisition["member"]`. Essas evidências não certificam acurácia científica nem equivalência entre revisões.

Na Coleção 10, o recurso usa 40 anos de 1985–2024 e pode publicar múltiplas linhas com a mesma combinação bioma/UF/geocódigo/classe e áreas distintas. `id_registro` preserva o `ID` original para manter essas linhas separadas, sem somá-las ou deduplicá-las. O valor zero é válido; o ID não é gerado pelo parser, não é filtro público e não tem estabilidade prometida entre arquivos, revisões ou coleções. A unicidade do ID publicado é verificada em toda a planilha antes dos filtros. Ambas as coleções retornam o mesmo esquema municipal.

A legenda também depende da coleção: no recurso municipal 10, classe 13 corresponde a **Outras Formações não Florestais**; na 11, a **Mosaico Herbáceo-Arbustivo**. Ambas publicam classe 0 como não observado. Não interprete a igualdade do código entre coleções como equivalência de significado; dentro de uma mesma coleção, os recortes estadual e municipal usam a mesma legenda.

### Classes MapBiomas (principais)

| Codigo | Classe | Nivel 0 |
|--------|--------|---------|
| 3 | Formação Florestal | Natural |
| 4 | Formação Savânica | Natural |
| 12 | Formação Campestre | Natural |
| 15 | Pastagem | Antropic |
| 18 | Agricultura | Antropic |
| 39 | Soja | Antropic |
| 20 | Cana | Antropic |
| 40 | Arroz | Antropic |
| 9 | Silvicultura | Antropic |
| 21 | Mosaico de Usos | Antropic |
| 24 | Área Urbanizada | Antropic |
| 33 | Rio, Lago e Oceano | Natural |

A coluna `classe` traz o rótulo exatamente como acima; para filtrar, prefira `classe_id`.

---

## `mapbiomas.transicao()`

Area de transicao entre classes de uso da terra por bioma x estado x periodo.

```python
import agrobr

df = await agrobr.mapbiomas.transicao(bioma="Cerrado", periodo="2019-2020")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Nao | Filtrar por bioma. Se None, todos |
| `uf` | `str` | Nao | Sigla ou nome completo da UF. Caixa e acentos são opcionais; valor inválido levanta `InvalidParameterError` com as siglas válidas, antes do download |
| `periodo` | `str` | Nao | Período da coleção selecionada (ex: `"2019-2020"`, `"1985-2025"` na coleção 11) |
| `classe_de_id` | `int` | Nao | Codigo da classe de origem. Código fora das classes publicadas levanta `InvalidParameterError` com a lista, depois do download |
| `classe_para_id` | `int` | Nao | Codigo da classe de destino, com a mesma conferência |
| `colecao` | `int` | Nao | `10` ou `11`; `None` usa a coleção atual (11). O arquivo de transições pertence à coleção selecionada |
| `as_polars` | `bool` | Nao | Retornar como polars.DataFrame |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `bioma` | str | Nome do bioma |
| `uf` | str | Sigla da UF |
| `classe_de_id` | int | Codigo da classe de origem |
| `classe_de` | str | Nome da classe de origem |
| `classe_para_id` | int | Codigo da classe de destino |
| `classe_para` | str | Nome da classe de destino |
| `periodo` | str | Periodo no formato "YYYY-YYYY" |
| `area_ha` | float | Area em hectares |

### Periodos Disponiveis

Na coleção 11, os períodos anuais vão de `1985-1986` a `2024-2025`, os quinquenais de `1985-1990` a `2020-2025`, e o período total é `1985-2025`. A planilha inclui também intervalos decenais e especiais. Na coleção 10, a série termina em 2024.

Consulte `df.periodo.unique()` sem o filtro `periodo` para obter os intervalos efetivamente publicados na coleção selecionada.

---

## Uso Sincrono

```python
from agrobr import sync

df = sync.mapbiomas.cobertura(bioma="Cerrado", ano=2020)
df_trans = sync.mapbiomas.transicao(bioma="Amazonia", periodo="2019-2020")
```

## Exemplos

### Desmatamento no Cerrado (perda de vegetacao nativa)

```python
import agrobr

# Transicao de Formacao Florestal (3) para Pastagem (15) no Cerrado
df = await agrobr.mapbiomas.transicao(
    bioma="Cerrado",
    classe_de_id=3,
    classe_para_id=15,
    periodo="2019-2020",
)
print(f"Area convertida: {df['area_ha'].sum():,.0f} ha")
```

### Cobertura municipal (Belem, PA)

Os filtros são aplicados após o download do arquivo municipal da coleção selecionada.

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(
    nivel="municipio", uf="PA", municipio="Belém", ano=2020
)
print(df[["municipio", "classe", "area_ha"]].head())
```

Pelo código territorial de sete dígitos:

```python
df, meta = await agrobr.mapbiomas.cobertura(
    nivel="municipio", municipio=5107925, classe_id=39,
    ano=2025, colecao=11, return_meta=True,
)
print(df[["bioma", "uf", "municipio", "geocodigo", "area_ha"]])
```

### Evolucao da soja no Brasil

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(classe_id=39)  # Soja
pivot = df.groupby("ano")["area_ha"].sum()
print(pivot)
```

## Fonte dos Dados

- **Projeto:** MapBiomas — Mapeamento Anual de Cobertura e Uso da Terra no Brasil
- **Coleção padrão:** 11 (agosto de 2026); coleção 10 disponível explicitamente
- **Série histórica:** 1985-2025 na coleção 11; 1985-2024 na coleção 10
- **Resolucao:** 30m (Landsat)
- **Provedor:** Rede colaborativa multi-institucional
- **Dados:** [Cobertura e uso da terra — MapBiomas 30m](https://brasil.mapbiomas.org/iniciativas-e-produtos/cobertura-e-uso-da-terra/cobertura-30m/cobertura/)
- **Licenca:** Dados publicos — livre para uso com citacao ao Projeto MapBiomas
