# Contrato: uso_do_solo

Cobertura e uso da terra (MapBiomas) — cobertura anual e transições entre classes.

## Modos

| `tipo=` / `nivel=` | Contrato | Fonte |
|---------|----------|-------|
| `"cobertura"` / `"estado"` (padrões) | `MAPBIOMAS_COBERTURA_V2` | MapBiomas |
| `"cobertura"` / `"municipio"` | `MAPBIOMAS_COBERTURA_MUNICIPAL_V1` | MapBiomas |
| `"transicao"` / `"estado"` | `MAPBIOMAS_TRANSICAO_V2` | MapBiomas |

## Schema: Cobertura

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `bioma` | STRING | Não | — | Bioma válido |
| `uf` | STRING | Não | — | Sigla da UF |
| `classe_id` | INTEGER | Não | — | Código LULC MapBiomas |
| `classe` | STRING | Sim | — | Nulo só para código fora da legenda |
| `nivel_0` | STRING | Sim | — | — |
| `ano` | INTEGER | Não | — | ≥ 1985 |
| `area_ha` | FLOAT | Não | ha | ≥ 0 |

**PK:** `(bioma, uf, classe_id, ano)`

## Schema: Transição

| Coluna | Tipo | Nullable | Unidade | Restrições |
|--------|------|----------|---------|------------|
| `bioma` | STRING | Não | — | Bioma válido |
| `uf` | STRING | Não | — | Sigla da UF |
| `classe_de_id` | INTEGER | Não | — | Código LULC |
| `classe_de` | STRING | Sim | — | Nulo só para código fora da legenda |
| `classe_para_id` | INTEGER | Não | — | Código LULC |
| `classe_para` | STRING | Sim | — | Nulo só para código fora da legenda |
| `periodo` | STRING | Não | — | Formato YYYY-YYYY |
| `area_ha` | FLOAT | Não | ha | ≥ 0 |

**PK:** `(bioma, uf, classe_de_id, classe_para_id, periodo)`

## Nível municipal

Cobertura com `nivel="municipio"` valida o contrato próprio `mapbiomas.cobertura_municipal` 1.1. A validação de chave não é mais ignorada neste modo. O esquema estadual continua separado.

| Coluna | Tipo físico pandas | Nulo | Significado |
|--------|--------------------|------|-------------|
| `bioma` | str | Não | Bioma publicado |
| `uf` | str | Não | Sigla da UF do cruzamento publicado |
| `municipio` | str | Não | Nome territorial publicado, sem espaços externos |
| `classe_id` | Int64 | Não | Código da classe |
| `classe` | str | Sim | Rótulo normalizado pelo SDK conforme a coleção; não é transcrição literal da legenda; nulo só para código fora da legenda |
| `nivel_0` | str | Não | Categoria textual publicada, incluindo `Undefined` |
| `ano` | Int64 | Não | Ano de referência |
| `area_ha` | float64 | Não | Área finita e não negativa em hectares; zero preservado |
| `geocodigo` | str | Não | `geocode` publicado, sete dígitos ASCII como texto |
| `cod_municipio` | Int64 | Sim | Código IBGE do município tirado do `geocodigo`; nulo quando o código não tem o prefixo de uma UF |
| `id_registro` | Int64 | Não | `ID` original não negativo, local à coleção e recurso |

O texto sai no dtype padrão do pandas instalado: `str` no pandas 3 e `object` no pandas 2.

A chave é `(bioma, uf, geocodigo, classe_id, id_registro, ano)`, dentro de uma coleção e recurso. Um código pode pertencer a cruzamentos em múltiplas UFs; isso não autoriza remover a UF da chave. `geocodigo` também inclui entidades como lagoas, sem promessa de catálogo municipal atual do IBGE. O `id_registro` é copiado do `ID` original, incluindo zero; não é ordinal gerado nem uma identidade estável entre publicações.

Todos os 41 anos de 1985–2025 e todas as linhas identificadas da Coleção 11 são validados antes de concluir, inclusive dados fora dos filtros. A consulta só acumula as linhas selecionadas. Ausência de área, tipo incompatível, valor não finito/negativo ou repetição da chave completa causam erro; não há imputação nem deduplicação silenciosa. Recortes sem correspondência retornam um DataFrame vazio com os mesmos tipos.

`municipio` aceita o nome inteiro (sem diferenciar caixa e acento, com `uf` para desambiguar) ou o código de sete dígitos, e seleciona pelo `geocodigo`; exige `nivel="municipio"`. Pedaço de nome, nome de mais de um município sem `uf` e código ausente do recurso levantam `InvalidParameterError`. Um `classe_id` fora das classes publicadas na coleção também levanta, com a lista. Veja [parâmetros e proveniência da fonte](../api/mapbiomas.md#cobertura-municipal-da-colecao-11).

A Coleção 10 usa o mesmo esquema municipal de dez colunas, com 40 anos de 1985–2024. Seu recurso contém linhas com a mesma combinação territorial e áreas distintas, preservadas por seus IDs originais. Não há soma ou deduplicação automática; o ID publicado repetido causa erro antes dos filtros. `source_details["territorial_keys"]` descreve as repetições territoriais sem classificá-las como erro geográfico.

`classe_id` deve ser interpretado junto da coleção: classe 13 municipal significa **Outras Formações não Florestais** na 10 e **Mosaico Herbáceo-Arbustivo** na 11. Classe 0 corresponde a não observado em ambas. A identidade contratual não estabelece equivalência semântica entre coleções. Dentro de uma coleção, a legenda é a mesma nos recortes estadual e municipal: a classe 0 sai como `Não observado` nos dois. Um código publicado fora da legenda conhecida sai com o rótulo nulo (`classe`, ou `classe_de`/`classe_para` na transição), o `classe_id` publicado e um aviso (`UserWarning` e `meta.validation_warnings`) com os códigos. Até a 1.1.0, o estadual devolvia `Classe {id}`. Célula de classe sem código inteiro é defeito da planilha: a leitura falha com `ParseError`, com a linha e o valor publicado.

A soma das classes de um município é a área mapeada pelo MapBiomas, não a área territorial do IBGE. Nos 93 municípios costeiros com baía ou ilha, ela fica abaixo da área do IBGE, que inclui as águas internas (em Florianópolis, 35% abaixo). Fernando de Noronha não aparece.

Em 5 UFs amazônicas de fronteira, a soma dos municípios publicada passa do estadual publicado, na mesma coleção, ano e classe, com a mesma diferença em 1985, 2000 e 2025 (Coleção 11):

| UF | Σ municípios − estadual, todas as classes | Formação Florestal (classe 3), 2025 |
|---|---:|---:|
| RR | +20.059 ha | +19.589 ha (0,14%) |
| AM | +20.316 ha | +18.615 ha (0,015%) |
| PA | +9.671 ha | +8.976 ha (0,011%) |
| AP | +5.851 ha | +5.695 ha (0,057%) |
| AC | +4.078 ha | +3.996 ha (0,029%) |

Nas outras 22 UFs, a diferença fica abaixo de 1 ha (no RS, com as lagoas, +0,985 ha). O agrobr repassa as 2 publicações como vêm, e o `READ_ME` delas não trata da diferença. Para o total da UF, use `nivel="estado"`, e não a soma dos municípios.

## Coleções

`colecao=11` seleciona a coleção 11 (1985–2025); `colecao=10` identifica a coleção 10 (1985–2024). O padrão `None` usa a coleção 11. O dataset encaminha a seleção para a fonte, sem misturar coleções. Os esquemas municipal, estadual e de transição têm contratos próprios.

Cada coleção revisa o histórico completo. Para reprodução, fixe `colecao` e preserve os bytes e hashes identificados por `meta.source_details["acquisition"]`, além de `meta.source_url`, `meta.fetched_at` e `meta.data_sources`. A coleção não congela uma revisão e não há cache local integrado.

## Seleção e erros

O dataset usa somente MapBiomas, sem fallback para outra instituição. Cobertura aceita os filtros `bioma`, `uf`, `ano`, `classe_id`, `nivel`, `municipio` e `colecao`. Transição aceita `bioma`, `uf`, `periodo`, `classe_de_id`, `classe_para_id` e `colecao`, apenas no nível estadual. Filtros do outro modo e opções desconhecidas são rejeitados; não são ignorados.

`as_polars` e `return_meta` exigem booleanos. A conversão para Polars ocorre após a validação do contrato. Não há `produto` nem `use_cache`. O contexto `deterministic` é recusado antes da aquisição, pois não existe seleção de snapshot histórico arbitrário. Erros de aquisição ou parsing podem chegar pela camada de datasets como `SourceUnavailableError`, com detalhes em `errors`; não há retorno parcial de uma planilha inválida.

## Exemplo

```python
from agrobr import datasets

# Cobertura por estado
df = await datasets.uso_do_solo(tipo="cobertura", bioma="Cerrado", ano=2022)

# Cobertura por município
df = await datasets.uso_do_solo(
    tipo="cobertura", nivel="municipio", municipio="Sorriso", uf="MT",
    ano=2025, colecao=11,
)

# Transições entre classes
df = await datasets.uso_do_solo(tipo="transicao", periodo="2020-2021")

# Com metadados
df, meta = await datasets.uso_do_solo(tipo="cobertura", return_meta=True)
```
