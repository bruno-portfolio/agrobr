# BCB/SICOR — Credito Rural

Dados de credito rural do Sistema de Operacoes do Credito Rural (SICOR),
disponibilizados via API OData do Banco Central.

## API

```python
from agrobr import bcb

# Credito de custeio para soja, safra 2024/25
df = await bcb.credito_rural(produto="soja", safra="2024/25", finalidade="custeio")

# Filtrar por UF
df = await bcb.credito_rural(produto="soja", safra="2024/25", uf="MT")

# Agregacao por UF (soma municipios)
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="uf")

# Agregacao por programa
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="programa")

# Filtrar por programa
df = await bcb.credito_rural(produto="soja", safra="2024/25", programa="Pronamp")

# Filtrar por tipo de seguro
df = await bcb.credito_rural(produto="soja", safra="2024/25", tipo_seguro="Proagro")
```

## Colunas — `credito_rural`

| Coluna | Tipo | Descricao |
|---|---|---|
| `safra` | str | Safra no formato "2024/2025" |
| `ano_emissao` | int | Ano de emissao do contrato |
| `mes_emissao` | int | Mes de emissao |
| `uf` | str | UF do municipio |
| `municipio` | str | Nome do municipio |
| `produto` | str | Produto financiado |
| `finalidade` | str | Finalidade (custeio, investimento, comercializacao) |
| `valor` | float | Valor financiado (R$) |
| `area_financiada` | float | Area financiada (ha) |
| `qtd_contratos` | int | Quantidade de contratos |
| `cd_programa` | str | Codigo do programa SICOR |
| `programa` | str | Nome do programa (Pronamp, Pronaf, etc.) |
| `cd_sub_programa` | str | Codigo do sub-programa |
| `cd_fonte_recurso` | str | Codigo da fonte de recurso |
| `fonte_recurso` | str | Nome da fonte (LCA, FNE, Poupanca rural, etc.) |
| `cd_tipo_seguro` | str | Codigo do tipo de seguro |
| `tipo_seguro` | str | Nome do seguro (Proagro, Seguro privado, etc.) |
| `cd_modalidade` | str | Codigo da modalidade |
| `modalidade` | str | Nome (Individual, Coletiva) |
| `cd_atividade` | str | Codigo da atividade |
| `atividade` | str | Nome (Agricola, Pecuaria) |
| `regiao` | str | Regiao (SUL, CENTRO-OESTE, etc.) |

## Dimensoes SICOR

A API retorna codigos de dimensao (`cdPrograma`, `cdFonteRecurso`, etc.)
que o parser enriquece automaticamente com nomes legiveis via dicionarios
hardcoded. Codigos desconhecidos geram `"Desconhecido ({code})"` com log warning.

## Finalidades

- `custeio` — financiamento da producao
- `investimento` — aquisicao de maquinas, infraestrutura
- `comercializacao` — financiamento da comercializacao

> **Nota:** `industrializacao` foi removida — endpoint nao existe mais na API reestruturada.

## Produtos

Soja, milho, cafe, algodao, arroz, trigo, feijao, cana-de-acucar, mandioca,
sorgo, aveia, cevada, entre outros. Use o nome canonico do agrobr.

## MetaInfo

```python
df, meta = await bcb.credito_rural(produto="soja", safra="2024/25", return_meta=True)
print(meta.source)          # "bcb"
print(meta.schema_version)  # "1.1"
```

## Status (fev/2026)

A API SICOR foi reestruturada (~2024). Endpoints antigos (`CusteioMunicipio`,
`InvestimentoMunicipio`) foram substituidos por `CusteioRegiaoUFProduto`,
`InvestRegiaoUFProduto`, `ComercRegiaoUFProduto`.

O operador OData `$filter eq` nao funciona nos novos endpoints. O client usa
`contains(nomeProduto,'...')` para filtro server-side por produto, e filtra
ano/UF client-side apos download paginado. Filtros por programa e tipo de
seguro tambem sao client-side (API nao suporta `$filter` nesses campos).

Retry com backoff exponencial (6 tentativas, timeout read 120s).
A API retorna HTTP 500 de forma intermitente. Desde v0.8.0, o agrobr
utiliza Base dos Dados (BigQuery) como fallback automatico quando a API
OData falha. Instale com `pip install agrobr[bigquery]`.

## SGS — Series Temporais

Acesso a series temporais do Sistema Gerenciador de Series Temporais do BCB.

### Endpoint

`api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados?formato=json`

Sem autenticacao, sem paginacao.

### Exemplo

```python
from agrobr import bcb

# Por nome canonico
df = await bcb.sgs("selic")

# Por codigo numerico
df = await bcb.sgs(432)
```

### Series pre-mapeadas

| Nome | Codigo | Descricao |
|------|--------|-----------|
| selic | 432 | Taxa Selic |
| ipca | 433 | IPCA mensal |
| ipca_alimentacao | 1635 | IPCA alimentacao |
| pib_agropecuaria | 22083 | PIB agropecuaria |
| dolar_ptax_venda | 1 | Dolar PTAX venda |
| igpm | 189 | IGP-M |
| cdi | 4392 | CDI |

### Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| data | datetime | Data da observacao |
| valor | float | Valor da serie |
| codigo | int | Codigo SGS |
| nome_serie | str | Nome da serie |

---

## PTAX — Cotacao Dolar

Cotacao diaria do dolar PTAX (compra e venda).

### Endpoint

`olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`

### Exemplo

```python
from agrobr import bcb

# Periodo
df = await bcb.ptax(data_inicial="01/01/2026", data_final="31/01/2026")
```

### Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| data | date | Data da cotacao |
| cotacao_compra | float | Cotacao compra (R$/US$) |
| cotacao_venda | float | Cotacao venda (R$/US$) |
| data_hora | datetime | Data e hora da cotacao |

---

## Focus — Expectativas de Mercado

Expectativas de mercado coletadas pelo BCB (relatorio Focus).

### Endpoint

`olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/`

### Exemplo

```python
from agrobr import bcb

df = await bcb.focus("PIB Agropecuária")
```

### Indicadores

- PIB Agropecuária
- IPCA
- IGP-DI
- IGP-M
- Taxa de cambio
- Meta para taxa over-selic

### Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| indicador | str | Nome do indicador |
| data | date | Data da coleta |
| data_referencia | str | Periodo de referencia |
| media | float | Media das expectativas |
| mediana | float | Mediana |
| desvio_padrao | float | Desvio padrao |
| minimo | float | Valor minimo |
| maximo | float | Valor maximo |
| numero_respondentes | int | Numero de respondentes |
| base_calculo | int | Base de calculo (0 ou 1) |

---

## Fonte

- API SICOR: `https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata`
- API SGS: `https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados`
- API PTAX: `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/`
- API Focus: `https://olinda.bcb.gov.br/olinda/servico/Expectativas/versao/v1/odata/`
- Atualizacao: mensal (SICOR), diaria (SGS, PTAX, Focus)
- Historico: 2013+ (SICOR), variavel (SGS)
- Contract: v1.1 (11 novas colunas nullable desde v0.10.1)
