# CONAB Progresso de Safra

Dados semanais de progresso de plantio e colheita das principais culturas anuais, publicados pela CONAB.

## `conab.progresso_safra()`

Percentuais de semeadura e colheita por cultura x estado x semana.

```python
import agrobr

df = await agrobr.conab.progresso_safra(produto="Soja", uf="MT")
```

### Parametros

| Parametro | Tipo | Obrigatorio | Descricao |
|-----------|------|-------------|-----------|
| `produto` | `str` | Nao | Cultura publicada, sem diferenciar caixa e acento: "Soja", "Milho 1a", "Milho 2a", "Arroz", "Algodao", "Feijao 1a", "Trigo". Também aceita os nomes do dataset (`milho_1`, `milho_2`, `feijao_1`), `feijao` para a 1ª safra e `milho` para a 1ª e a 2ª. Outro valor, inclusive um pedaço do nome, levanta `InvalidParameterError` com as culturas publicadas, antes do pedido. Se None, todas |
| `uf` | `str` | Nao | Sigla da UF (ex: "MT", "GO", "PR"), ou "MEDIA_ESTADOS" para a média da CONAB dos estados monitorados, que não é a média simples das UFs ([contrato](../contracts/progresso_safra.md)). Nome por extenso ou sigla inexistente levanta `InvalidParameterError` antes do pedido. "BR" é recusado com `InvalidParameterError`, porque a CONAB não publica Brasil. Se None, todos |
| `operacao` | `str` | Nao | "Semeadura" ou "Colheita" (outro valor levanta `InvalidParameterError` antes do pedido). Se None, ambas |
| `semana_url` | `str` | Nao | URL de uma semana especifica, em `https://www.gov.br/conab/` (outra URL levanta `InvalidParameterError` antes do pedido). Se None, busca a mais recente |
| `as_polars` | `bool` | Nao | Se True, retorna `polars.DataFrame` |
| `return_meta` | `bool` | Nao | Se True, retorna `(DataFrame, MetaInfo)` |

### Colunas de Retorno

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `cultura` | str | Nome da cultura como publicado (ex: "Soja", "Milho 2ª") |
| `safra` | str | Safra como publicada: "YYYY/YY" (ex: "2025/26"), ou ano civil "YYYY" no trigo (ex: "2026") |
| `operacao` | str | "Semeadura" ou "Colheita" |
| `uf` | str | UF (ex: "MT", "GO"); "MEDIA_ESTADOS" na linha "N estados" da planilha (média da própria CONAB dos estados monitorados, não a média simples das UFs nem o Brasil); "BR" só se a planilha publicar "Brasil" |
| `semana_atual` | str | Data de referencia da semana (YYYY-MM-DD) |
| `pct_ano_anterior` | float | % mesma semana do ano anterior (0.0-1.0) |
| `pct_semana_anterior` | float | % semana anterior (0.0-1.0) |
| `pct_semana_atual` | float | % semana atual (0.0-1.0) |
| `pct_media_5_anos` | float | % media dos ultimos 5 anos (0.0-1.0) |
| `revisado` | bool | Algum percentual da linha veio com a marca de revisão `*` da CONAB; nulo sem percentual numérico |
| `n_estados` | int | Estados da média ("7 estados"), lido da planilha; nulo nas UFs |
| `cobertura_area_pct` | float | Fração da área cultivada coberta por esses estados, lida da nota "(Esses N estados correspondem a X% da área cultivada)" (0,98 = 98%), sem recálculo; nulo nas UFs |

A linha "N estados" é uma média calculada pela própria CONAB e não se reproduz com as áreas do levantamento: não é o total do
Brasil. O percentual de colheita dos blocos marcados com `*` é calculado sobre o semeado acumulado (nota da planilha), não sobre a
área total.

Os percentuais por UF são compilados pela CONAB a partir dos levantamentos estaduais, e `semana_atual` é a semana da publicação da
CONAB. No Paraná, o valor repete o levantamento do DERAL da segunda-feira anterior (no boletim de 18/09/2026, o DERAL de 14/09):
para a data do levantamento, use `deral.condicao_lavouras`.

### Culturas Disponiveis

| Cultura | Estados | Operacoes |
|---------|---------|-----------|
| Soja | 12 estados (96% da área) | Semeadura, Colheita |
| Milho 1ª | 9 estados (92% da área) | Semeadura, Colheita |
| Milho 2ª | 9 estados (91% da área) | Semeadura, Colheita |
| Arroz | 6 estados (88% da área) | Semeadura, Colheita |
| Feijão 1ª | 8 estados (91% da área) | Semeadura, Colheita |
| Algodão | 7 estados (98% da área) | Semeadura, Colheita |
| Trigo | 8 estados (99,9% da área) | Colheita |

A tabela vale para os boletins de 27/09/2025, 22/02/2026, 28/08/2026 e 18/09/2026, e o trigo só aparece com colheita neles. Os
estados e a cobertura de cada bloco saem da nota da planilha e podem mudar de safra para safra.

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
| `max_pages` | `int` | Nao | Maximo de paginas a buscar (default 4 = ~80 semanas). Precisa ser inteiro positivo: 0 ou negativo levanta `InvalidParameterError` |

### Retorno

Lista de dicts com `descricao` e `url` para cada semana disponivel.

---

## Uso Sincrono

```python
from agrobr import sync

df = sync.conab.progresso_safra(produto="Soja")
semanas = sync.conab.semanas_disponiveis()
```

## Exemplos

### Progresso da soja no Mato Grosso

```python
import agrobr

df = await agrobr.conab.progresso_safra(
    produto="Soja",
    uf="MT",
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
    produto="Soja",
    operacao="Colheita",
)
pivot = df[["uf", "pct_semana_atual"]].sort_values(
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
