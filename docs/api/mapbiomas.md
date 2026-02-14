# MapBiomas (Cobertura e Uso da Terra)

Dados tabulares do Projeto MapBiomas — area (ha) por classe de cobertura e uso da terra, bioma e estado, com serie historica anual desde 1985.

## `mapbiomas.cobertura()`

Area por classe de cobertura e uso da terra x bioma x estado x ano.

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(bioma="Cerrado", ano=2020, estado="GO")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `bioma` | `str` | Nao | Bioma: "Amazonia", "Cerrado", "Caatinga", "Mata Atlantica", "Pampa", "Pantanal". Se None, todos |
| `estado` | `str` | Nao | Filtrar por UF (ex: "MT", "SP") ou nome do estado |
| `ano` | `int` | Nao | Ano (1985-2024). Se None, todos os anos |
| `classe_id` | `int` | Nao | Codigo de classe MapBiomas (ex: 15 para Pastagem) |
| `colecao` | `int` | Nao | Colecao MapBiomas (default: 10) |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `bioma` | str | Nome do bioma |
| `estado` | str | Codigo UF (ex: "MT") |
| `classe_id` | int | Codigo da classe MapBiomas |
| `classe` | str | Nome da classe (ex: "Pastagem", "Formacao Florestal") |
| `nivel_0` | str | Categoria: "Natural", "Antropico", "Natural/Antropico", "Indefinido" |
| `ano` | int | Ano de referencia |
| `area_ha` | float | Area em hectares |

### Classes MapBiomas (principais)

| Codigo | Classe | Nivel 0 |
|--------|--------|---------|
| 3 | Formacao Florestal | Natural |
| 4 | Formacao Savanica | Natural |
| 12 | Formacao Campestre | Natural |
| 15 | Pastagem | Antropico |
| 18 | Agricultura | Antropico |
| 39 | Soja | Antropico |
| 20 | Cana | Antropico |
| 40 | Arroz | Antropico |
| 9 | Silvicultura | Antropico |
| 21 | Mosaico de Usos | Antropico |
| 24 | Area Urbanizada | Antropico |
| 33 | Rio, Lago e Oceano | Natural |

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
| `estado` | `str` | Nao | Filtrar por UF ou nome do estado |
| `periodo` | `str` | Nao | Periodo (ex: "2019-2020", "1985-2024") |
| `classe_de_id` | `int` | Nao | Codigo da classe de origem |
| `classe_para_id` | `int` | Nao | Codigo da classe de destino |
| `colecao` | `int` | Nao | Colecao MapBiomas (default: 10) |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `bioma` | str | Nome do bioma |
| `estado` | str | Codigo UF |
| `classe_de_id` | int | Codigo da classe de origem |
| `classe_de` | str | Nome da classe de origem |
| `classe_para_id` | int | Codigo da classe de destino |
| `classe_para` | str | Nome da classe de destino |
| `periodo` | str | Periodo no formato "YYYY-YYYY" |
| `area_ha` | float | Area em hectares |

### Periodos Disponiveis

- **Anuais consecutivos:** 1985-1986, 1986-1987, ..., 2023-2024
- **Quinquenais:** 1985-1990, 1990-1995, ..., 2020-2024
- **Decenais:** 1985-2000, 2000-2024, 1990-2000, 2000-2010, 2010-2020
- **Total:** 1985-2024

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

### Evolucao da soja no Brasil

```python
import agrobr

df = await agrobr.mapbiomas.cobertura(classe_id=39)  # Soja
pivot = df.groupby("ano")["area_ha"].sum()
print(pivot)
```

## Fonte dos Dados

- **Projeto:** MapBiomas — Mapeamento Anual de Cobertura e Uso da Terra no Brasil
- **Colecao:** 10 (agosto 2025)
- **Serie historica:** 1985-2024
- **Resolucao:** 30m (Landsat)
- **Provedor:** Rede colaborativa multi-institucional
- **Dados:** [brasil.mapbiomas.org/estatisticas](https://brasil.mapbiomas.org/estatisticas/)
- **Licenca:** Dados publicos — livre para uso com citacao ao Projeto MapBiomas
