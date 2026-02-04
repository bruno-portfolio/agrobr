# IBGE - Instituto Brasileiro de Geografia e Estatistica

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | Governo Federal |
| **Website** | [ibge.gov.br](https://www.ibge.gov.br) |
| **API** | [SIDRA](https://sidra.ibge.gov.br) |
| **Acesso agrobr** | Via API SIDRA (JSON) |

## Origem dos Dados

### Fonte

- **API**: `https://sidra.ibge.gov.br/`
- **Formato**: JSON
- **Acesso**: Publico, sem autenticacao

## Pesquisas Disponiveis

### PAM - Producao Agricola Municipal

- **Tabela SIDRA**: 5457 (nova serie 2018+)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual

### LSPA - Levantamento Sistematico da Producao Agricola

- **Tabela SIDRA**: 6588
- **Cobertura**: Nacional/UF
- **Frequencia**: Mensal

## Variaveis

| Codigo | Nome | Unidade |
|--------|------|---------|
| 214 | Area plantada | hectares |
| 215 | Area colhida | hectares |
| 216 | Quantidade produzida | toneladas |
| 112 | Rendimento medio | kg/ha |

## Uso - PAM

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Dados de soja por UF
    df = await ibge.pam('soja', ano=2023)

    # Multiplos anos
    df = await ibge.pam('milho', ano=[2020, 2021, 2022, 2023])

    # Filtrar por UF
    df = await ibge.pam('soja', ano=2023, uf='MT')

    # Nivel municipal
    df = await ibge.pam('arroz', ano=2023, nivel='municipio', uf='RS')

    # Com metadados
    df, meta = await ibge.pam('soja', ano=2023, return_meta=True)

asyncio.run(main())
```

### Niveis Territoriais

| Nivel | Descricao |
|-------|-----------|
| `brasil` | Total nacional |
| `uf` | Por Unidade Federativa |
| `municipio` | Por municipio |

## Uso - LSPA

### Basico

```python
# Estimativas do ano
df = await ibge.lspa('soja', ano=2024)

# Mes especifico
df = await ibge.lspa('milho_1', ano=2024, mes=6)

# Filtrar por UF
df = await ibge.lspa('soja', ano=2024, uf='PR')

# Com metadados
df, meta = await ibge.lspa('soja', ano=2024, return_meta=True)
```

## Schema - PAM

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `produto` | str | Nome do produto |
| `area_plantada` | float | Hectares |
| `area_colhida` | float | Hectares |
| `producao` | float | Toneladas |
| `rendimento` | float | kg/ha |
| `valor_producao` | float | Mil reais |
| `fonte` | str | "ibge_pam" |

## Schema - LSPA

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `mes` | int | Mes de referencia |
| `variavel` | str | Nome da variavel |
| `valor` | float | Valor da variavel |
| `produto` | str | Nome do produto |
| `fonte` | str | "ibge_lspa" |

## Produtos PAM

```python
produtos = await ibge.produtos_pam()
# ['soja', 'milho', 'arroz', 'feijao', 'trigo', 'cafe', ...]
```

## Produtos LSPA

```python
produtos = await ibge.produtos_lspa()
# ['soja', 'milho_1', 'milho_2', 'arroz', 'feijao_1', 'feijao_2', ...]
```

Nota: No LSPA, `milho_1` e `milho_2` referem-se a primeira e segunda safras.

## UFs Disponiveis

```python
ufs = await ibge.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', ...]
```

## Cache

| Pesquisa | TTL | Stale maximo |
|----------|-----|--------------|
| PAM | 7 dias | 90 dias |
| LSPA | 24 horas | 30 dias |

## Atualizacao

| Pesquisa | Frequencia |
|----------|------------|
| PAM | Anual (agosto-setembro) |
| LSPA | Mensal |
