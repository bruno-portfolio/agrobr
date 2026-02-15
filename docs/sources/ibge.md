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

### Abate - Pesquisa Trimestral do Abate de Animais

- **Tabelas SIDRA**: 1092 (bovinos), 1093 (suinos), 1094 (frangos)
- **Cobertura**: Brasil + UF (27 UFs)
- **Frequencia**: Trimestral
- **Serie**: 1997-presente
- **Especies**: bovino, suino, frango
- **Variaveis**: animais abatidos (cabecas), peso das carcacas (kg)

### Censo Agropecuario 2017

- **Tabelas SIDRA**: 6907 (efetivo rebanho), 6881 (uso terra), 6957 (lavoura temporaria), 6956 (lavoura permanente)
- **Cobertura**: Brasil + UF + municipio
- **Frequencia**: Decenial
- **Periodo**: 2017 (referencia out/2016 a set/2017)
- **Temas**: efetivo_rebanho, uso_terra, lavoura_temporaria, lavoura_permanente
- **Formato**: Long format (variavel/valor por linha)

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

## Uso - Abate Trimestral

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Abate bovino por UF
    df = await ibge.abate('bovino', trimestre='202303')

    # Abate de frango no Parana
    df = await ibge.abate('frango', trimestre='202303', uf='PR')

    # Abate de suinos — Brasil
    df = await ibge.abate('suino', trimestre='202304')

    # Com metadados
    df, meta = await ibge.abate('bovino', trimestre='202303', return_meta=True)

asyncio.run(main())
```

## Schema - Abate Trimestral

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `trimestre` | str | Trimestre no formato YYYYQQ |
| `localidade` | str | UF |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `especie` | str | bovino, suino ou frango |
| `animais_abatidos` | float | Quantidade abatida (cabecas) |
| `peso_carcacas` | float | Peso total das carcacas (kg) |
| `fonte` | str | "ibge_abate" |

## Especies Abate

| Codigo | Especie | Tabela SIDRA |
|--------|---------|--------------|
| `bovino` | Bovino | 1092 |
| `suino` | Suino | 1093 |
| `frango` | Frango | 1094 |

```python
especies = await ibge.especies_abate()
# ['bovino', 'suino', 'frango']
```

## Uso - Censo Agropecuario

### Basico

```python
import asyncio
from agrobr import ibge

async def main():
    # Efetivo de rebanho por UF
    df = await ibge.censo_agro('efetivo_rebanho')

    # Uso da terra em Mato Grosso
    df = await ibge.censo_agro('uso_terra', uf='MT')

    # Lavoura temporaria por municipio
    df = await ibge.censo_agro('lavoura_temporaria', nivel='municipio', uf='PR')

    # Lavoura permanente — Brasil
    df = await ibge.censo_agro('lavoura_permanente')

    # Com metadados
    df, meta = await ibge.censo_agro('efetivo_rebanho', return_meta=True)

asyncio.run(main())
```

## Schema - Censo Agropecuario

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `ano` | int | Ano de referencia (2017) |
| `localidade` | str | Nome da localidade |
| `localidade_cod` | int | Codigo IBGE da localidade |
| `tema` | str | Tema do censo |
| `categoria` | str | Categoria dentro do tema |
| `variavel` | str | Nome da variavel |
| `valor` | float | Valor da variavel |
| `unidade` | str | Unidade de medida |
| `fonte` | str | "ibge_censo_agro" |

## Temas Censo Agropecuario

| Codigo | Tema | Tabela SIDRA |
|--------|------|--------------|
| `efetivo_rebanho` | Efetivo de rebanho | 6907 |
| `uso_terra` | Uso da terra | 6881 |
| `lavoura_temporaria` | Lavoura temporaria | 6957 |
| `lavoura_permanente` | Lavoura permanente | 6956 |

```python
temas = await ibge.temas_censo_agro()
# ['efetivo_rebanho', 'uso_terra', 'lavoura_temporaria', 'lavoura_permanente']
```

## Cache

| Pesquisa | TTL | Stale maximo |
|----------|-----|--------------|
| PAM | 7 dias | 90 dias |
| LSPA | 24 horas | 30 dias |
| PPM | 7 dias | 90 dias |
| Abate | 7 dias | 90 dias |
| Censo Agro | 30 dias | 365 dias |

## Atualizacao

| Pesquisa | Frequencia |
|----------|------------|
| PAM | Anual (agosto-setembro) |
| LSPA | Mensal |
| PPM | Anual (setembro) |
| Abate | Trimestral (T+2 meses) |
| Censo Agro | Decenial (ultimo: 2017) |
