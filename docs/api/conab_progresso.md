# CONAB Progresso de Safra

Dados semanais de progresso de plantio e colheita das principais culturas anuais, publicados pela CONAB.

## `conab.progresso_safra()`

Percentuais de semeadura e colheita por cultura x estado x semana.

```python
import agrobr

df = await agrobr.conab.progresso_safra(cultura="Soja", estado="MT")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `cultura` | `str` | Nao | Cultura: "Soja", "Milho 1a", "Milho 2a", "Arroz", "Algodao", "Feijao 1a", "Trigo". Se None, todas |
| `estado` | `str` | Nao | Filtrar por UF (ex: "MT", "GO", "PR"). Se None, todos |
| `operacao` | `str` | Nao | "Semeadura" ou "Colheita". Se None, ambas |
| `semana_url` | `str` | Nao | URL de uma semana especifica. Se None, busca a mais recente |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `cultura` | str | Nome da cultura (ex: "Soja", "Milho 2a") |
| `safra` | str | Safra no formato "YYYY/YY" (ex: "2025/26") |
| `operacao` | str | "Semeadura" ou "Colheita" |
| `estado` | str | Codigo UF (ex: "MT", "GO") |
| `semana_atual` | str | Data de referencia da semana (YYYY-MM-DD) |
| `pct_ano_anterior` | float | % mesma semana do ano anterior (0.0-1.0) |
| `pct_semana_anterior` | float | % semana anterior (0.0-1.0) |
| `pct_semana_atual` | float | % semana atual (0.0-1.0) |
| `pct_media_5_anos` | float | % media dos ultimos 5 anos (0.0-1.0) |

### Culturas Disponiveis

| Cultura | Estados | Operacoes |
|---------|---------|-----------|
| Soja | 12 estados (~96% area) | Semeadura, Colheita |
| Milho 1a | 9 estados (~92% area) | Semeadura, Colheita |
| Milho 2a | 9 estados (~91% area) | Semeadura |
| Arroz | 6 estados (~88% area) | Semeadura, Colheita |
| Feijao 1a | 8 estados (~91% area) | Semeadura, Colheita |
| Algodao | 7 estados (~98% area) | Semeadura |
| Trigo | Variavel (safra inverno) | Semeadura, Colheita |

---

## `conab.semanas_disponiveis()`

Lista semanas disponiveis no portal CONAB Progresso de Safra.

```python
import agrobr

semanas = await agrobr.conab.semanas_disponiveis()
for s in semanas[:3]:
    print(s["descricao"], s["url"])
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `max_pages` | `int` | Nao | Maximo de paginas a buscar (default 4 = ~80 semanas) |

### Retorno

Lista de dicts com `descricao` e `url` para cada semana disponivel.

---

## Uso Sincrono

```python
from agrobr import sync

df = sync.conab.progresso_safra(cultura="Soja")
semanas = sync.conab.semanas_disponiveis()
```

## Exemplos

### Progresso da soja no Mato Grosso

```python
import agrobr

df = await agrobr.conab.progresso_safra(
    cultura="Soja",
    estado="MT",
    operacao="Colheita",
)
print(f"Colheita soja MT: {df.iloc[0]['pct_semana_atual']:.1%}")
```

### Buscar semana especifica

```python
import agrobr

semanas = await agrobr.conab.semanas_disponiveis(max_pages=1)
url_semana = semanas[0]["url"]

df = await agrobr.conab.progresso_safra(semana_url=url_semana)
```

### Comparar progresso entre estados

```python
import agrobr

df = await agrobr.conab.progresso_safra(
    cultura="Soja",
    operacao="Colheita",
)
pivot = df[["estado", "pct_semana_atual"]].sort_values(
    "pct_semana_atual", ascending=False
)
print(pivot.to_string(index=False))
```

## Fonte dos Dados

- **Provedor:** CONAB — Companhia Nacional de Abastecimento
- **Frequencia:** Semanal (publicado as sextas-feiras)
- **Dados:** % plantio e colheita por cultura x estado
- **Formato:** XLSX
- **Serie:** Safra atual + comparativo ano anterior + media 5 anos
- **Licenca:** Dados publicos governo federal (livre)
- **Portal:** [Progresso de Safra](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/progresso-de-safra)
