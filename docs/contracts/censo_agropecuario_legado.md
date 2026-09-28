# censo_agropecuario_legado v2.1

Dados do Censo Agropecuário 1995/96 — seis temas publicados em ZIPs com tabelas XLS ou HTML.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | IBGE FTP | Censo Agropecuário 1995/96, tabelas nacionais e estaduais |

## Temas

`tecnologia`, `pessoal_ocupado`, `maquinas`, `producao_animal`, `valor_producao`, `financeiro`

As categorias nacionais são os rótulos de atividade publicados nas linhas da tabela, como `Total` e `Arroz`. Nas tabelas estaduais e municipais, a categoria é `Total`. `variavel` preserva a hierarquia do cabeçalho oficial, separada por ` / `; não use os antigos nomes inferidos pela posição da coluna.

Os números das tabelas variam entre os diretórios Brasil e UF. No tema `financeiro`, Brasil combina a tabela 11 (despesas) com a 12 (receitas); a tabela 11 estadual inclui investimentos, financiamentos, despesas e receitas.

O tema `maquinas` não tem o Pará: o IBGE publicou a Tabela 6 (pessoal ocupado) no lugar da 7 em `Para/Tab_7Mn.zip`. A consulta sem `uf` devolve 26 UFs, com o aviso em `validation_warnings`; `uf='PA'` levanta `SourceUnavailableError`.

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `ano` | int | N | Sempre 1995 |
| `localidade` | str | S | Brasil, nome da UF ou nome histórico do município |
| `localidade_cod` | int | S | Código de Brasil/UF; nulo para municípios sem código na tabela |
| `cod_municipio` | int | S | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município (e nos municípios sem código na tabela) |
| `uf` | str | S | Sigla do diretório/cabeçalho oficial; nula para Brasil |
| `tema` | str | N | Tema do censo |
| `categoria` | str | N | Categoria dentro do tema |
| `variavel` | str | N | Nome da variavel |
| `valor` | float64 | S | Valor da variavel |
| `unidade` | str | N | Unidade de medida |
| `fonte` | str | N | Sempre 'ibge_censo_agro_legado' |

## Primary Key

`[ano, tema, categoria, variavel, localidade, uf]`

A UF distingue municípios homônimos sem alterar seus nomes ou atribuir códigos atuais a localidades históricas.

## Garantias

- Ano sempre 1995 (Censo 1995/96)
- Valores numericos sempre >= 0
- Fonte sempre 'ibge_censo_agro_legado'
- Dados estaticos (update_frequency = never)

`valor` está na unidade indicada em cada linha. O parser interpreta o cabeçalho e a escala numérica da célula Excel: por exemplo, valores monetários armazenados em reais e exibidos em mil reais são divididos por 1.000. Contagens de informantes continuam em unidades. A precisão decimal disponível é preservada.

Nos HTMLs do Censo 1995/96, `-` é interpretado como zero conforme a legenda oficial, preservando a distinção entre ausência do fenômeno e dado não interpretável.

## Exemplo

```python
from agrobr import ibge

# Despesas e receitas nacionais por atividade
df = await ibge.censo_agro_legado('financeiro', nivel='brasil')

# Pessoal ocupado em Sao Paulo
df = await ibge.censo_agro_legado('pessoal_ocupado', uf='SP')

# Máquinas nos municípios de Goiás
df = await ibge.censo_agro_legado('maquinas', uf='GO', nivel='municipio')

# Com metadados
df, meta = await ibge.censo_agro_legado('tecnologia', return_meta=True)
```

Pelo dataset, com o `MetaInfo` da camada semântica:

```python
from agrobr import datasets

df, meta = await datasets.censo_agropecuario_legado("pessoal_ocupado", uf="SP", return_meta=True)
```

## Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Tabelas nacionais, incluindo as categorias de atividade; incompatível com filtro `uf` |
| `uf` | Totais estaduais reais (padrão); sem `uf`, consulta os 27 diretórios estaduais |
| `municipio` | Municípios do filtro `uf`; sem filtro, consulta todas as UFs |

O contrato 2.0 corrige geografia, categorias e variáveis e acrescenta `uf` à chave. Mesorregiões e microrregiões não são devolvidas como se fossem UFs.
