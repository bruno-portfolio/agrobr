# API ABIOVE

O módulo ABIOVE fornece dados de exportacao do complexo soja — grao, farelo, oleo e milho — publicados pela Associacao Brasileira das Industrias de Oleos Vegetais.

!!! warning "Licença zona_cinza"
    Fonte privada sem licença de reutilização das estatísticas localizada. Atribuição não substitui eventual permissão necessária.

## Funções

### `exportacao`

Volumes e receita de exportacao do complexo soja.

```python
async def exportacao(
    ano: int,
    *,
    mes: int | None = None,
    produto: str | None = None,
    agregacao: str = "detalhado",
    edicao: str | None = None,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]
```

**Parâmetros:**

| Parâmetro | Tipo | Descrição |
|-----------|------|-----------|
| `ano` | `int` | Ano de referência, de 2010 ao corrente |
| `mes` | `int \| None` | Mês dos dados (1-12). None retorna todos os meses publicados |
| `produto` | `str \| None` | Filtrar: `"grao"`, `"farelo"`, `"oleo"`, `"milho"`. Na soma mensal, a linha sai com o produto filtrado. Para o total dos produtos, use `agregacao="mensal"` sem `produto` ou com `produto="total"` (sai com `produto="total"`); `produto="total"` com `agregacao="detalhado"` levanta `InvalidParameterError` antes da rede |
| `agregacao` | `str` | `"detalhado"` (por produto/mes) ou `"mensal"` (soma) |
| `edicao` | `str \| None` | Edição da planilha, `"AAAA-MM"` (ex.: `"2025-12"`), de `ano` ou `ano + 1`. None lê a edição mais recente que publica `ano` |
| `as_polars` | `bool` | Retorna polars DataFrame |
| `return_meta` | `bool` | Se True, retorna tupla (DataFrame, MetaInfo) |

**Retorno:**

DataFrame com colunas: `ano`, `mes`, `produto`, `volume_ton`, `receita_usd_mil`. Volume ou receita em branco na planilha saem nulos, não 0; no total mensal, volume e receita saem nulos quando falta o valor de algum produto; quando o mesmo grupo tem valores conhecidos e ausentes, `MetaInfo.validation_warnings` registra o aviso.

**Exemplo:**

```python
from agrobr import abiove

# Exportacao completa 2024
df = await abiove.exportacao(2024)

# Apenas farelo
df = await abiove.exportacao(2024, produto="farelo")

# Mes especifico
df = await abiove.exportacao(2024, mes=6)

# Número original de dez/2025, da edição de dezembro
df = await abiove.exportacao(2025, mes=12, edicao="2025-12")
```

**Edição:**

A ABIOVE publica uma planilha por edição mensal (`exp_AAAAMM.xlsx`), com o ano da edição e o anterior, e revê meses já publicados. Sem `edicao`, o agrobr lê a edição mais recente que traz `ano`: primeiro as do ano seguinte (que trazem `ano` como comparação), depois as do próprio ano, da mais nova para a mais antiga, sem passar do mês corrente. Em setembro/2026, `exportacao(2025)` lê `exp_202608.xlsx`, que revê 19 das 96 células de 2025 publicadas em `exp_202512.xlsx` (farelo, dez/2025: 1.990.304,323 t, e não 2.020.365,023 t).

- A edição lida fica em `MetaInfo.source_details["edicao"]` (`arquivo` e `mes`), e o SHA-256 da planilha em `raw_content_hash`.
- `mes` só filtra o mês dos dados: mês ainda não publicado devolve DataFrame vazio, e `ano` que não é inteiro de 2010 ao corrente ou `mes` fora de 1-12 levantam `InvalidParameterError` antes da rede, como `edicao` em outro formato ou de outro ano, `produto` fora da lista e `agregacao` diferente de `"detalhado"` e `"mensal"`.
- Falha na edição mais recente (timeout, HTTP 5xx) levanta `SourceUnavailableError`; o agrobr só passa para a edição anterior quando a mais recente não existe (HTTP 404).

## Versão Síncrona

```python
from agrobr.sync import abiove

df = abiove.exportacao(2024)
```

## Notas

- Fonte: [ABIOVE](https://abiove.org.br) — licença `zona_cinza`
- Dados em Excel com formato multi-secao
