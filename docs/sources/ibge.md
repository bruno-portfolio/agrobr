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

### PPM - Pesquisa da Pecuaria Municipal

- **Tabelas SIDRA**: 3939 (rebanhos), 74 (producao de origem animal)
- **Cobertura**: Todos os municipios
- **Frequencia**: Anual
- **Serie**: 1974-presente (51 anos)

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

## Uso - PPM

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Rebanho bovino por UF
    df = await ibge.ppm('bovino', ano=2023)

    # Producao de leite
    df = await ibge.ppm('leite', ano=2023)

    # Multiplos anos
    df = await ibge.ppm('bovino', ano=[2020, 2021, 2022, 2023])

    # Filtrar por UF
    df = await ibge.ppm('bovino', ano=2023, uf='MT')

    # Nivel municipal
    df = await ibge.ppm('bovino', ano=2023, nivel='municipio', uf='MS')

    # Com metadados
    df, meta = await ibge.ppm('bovino', ano=2023, return_meta=True)

asyncio.run(main())
```

## Schema - PPM

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `especie` | str | Nome da especie/produto |
| `valor` | float | Valor (cabecas, mil litros, etc) |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_ppm" |

## Especies/Produtos PPM

### Rebanhos (tabela 3939)

| Codigo | Especie | Unidade |
|--------|---------|---------|
| `bovino` | Bovino | cabecas |
| `bubalino` | Bubalino | cabecas |
| `equino` | Equino | cabecas |
| `suino_total` | Suino (total) | cabecas |
| `suino_matrizes` | Suino matrizes | cabecas |
| `caprino` | Caprino | cabecas |
| `ovino` | Ovino | cabecas |
| `galinaceos_total` | Galinaceos (total) | cabecas |
| `galinhas_poedeiras` | Galinhas poedeiras | cabecas |
| `codornas` | Codornas | cabecas |

### Producao de origem animal (tabela 74)

| Codigo | Produto | Unidade |
|--------|---------|---------|
| `leite` | Leite | mil litros |
| `ovos_galinha` | Ovos de galinha | mil duzias |
| `ovos_codorna` | Ovos de codorna | mil duzias |
| `mel` | Mel de abelha | kg |
| `casulos` | Casulos de bicho-da-seda | kg |
| `la` | La | kg |

```python
especies = await ibge.especies_ppm()
# ['bovino', 'bubalino', 'caprino', 'casulos', 'codornas', ...]
```

## Cache

| Pesquisa | TTL | Stale maximo |
|----------|-----|--------------|
| PAM | 7 dias | 90 dias |
| LSPA | 24 horas | 30 dias |
| PPM | 7 dias | 90 dias |

## Atualizacao

| Pesquisa | Frequencia |
|----------|------------|
| PAM | Anual (agosto-setembro) |
| LSPA | Mensal |
| PPM | Anual (setembro) |
