# Reprodutibilidade

O agrobr oferece consultas ao cache local e metadados para apoiar análises reproduzíveis. A garantia temporal depende do dataset e da preservação dos dados consultados.

## Modo Determinístico

```python
from agrobr import datasets

async with datasets.deterministic(snapshot="2025-12-31"):
    df = await datasets.preco_diario("soja")
```

Também disponível como decorator:

```python
from agrobr.datasets.deterministic import deterministic_decorator

@deterministic_decorator("2025-12-31")
async def meu_pipeline():
    df = await datasets.preco_diario("soja")
    return df
```

## Semântica do Snapshot

| Aspecto | Definição |
|---------|-----------|
| **Formato** | `"YYYY-MM-DD"` — data máxima de corte |
| **Filtro** | Datasets com suporte a snapshot (ex.: `preco_diario`) filtram por `data <= snapshot` |
| **Rede** | Datasets com suporte a snapshot consultam apenas o cache local (offline); sem o produto no cache, `preco_diario` levanta `SourceUnavailableError` |
| **Escopo** | Isolado por contexto async (contextvars) — não afeta outras tasks |
| **MetaInfo** | Campo `snapshot` registra o contexto nos datasets que aceitam esse modo; isoladamente, não comprova uma versão histórica |
| **Aviso** | Dataset que consulta a fonte corrente dentro do contexto avisa em `validation_warnings` e com `UserWarning` que o dado não é o da data |

!!! note "Suporte por dataset"
    O filtro por data e o modo offline são aplicados só por `preco_diario`. Parte dos datasets roda dentro do contexto consultando a fonte corrente, e o aviso em `validation_warnings` diz que o dado não é o da data. Outros recusam o contexto antes da rede, com `InvalidParameterError`: `autorizacoes_defensivos`, `cadastro_rural`, `composicao_defensivos`, `cotacoes_cambio`, `cultivares_protegidas`, `cultivares_registradas`, `custo_producao`, `custo_sociobiodiversidade`, `defensivos_formulados`, `defensivos_tecnicos`, `desmatamento`, `empregadores_lista_suja`, `expectativas_mercado`, `exportacao`, `importacao`, `moedas_cambio`, `precos_diesel`, `series_economicas`, `unidades_conservacao`, `unidades_conservacao_federais`, `uso_do_solo` e `zoneamento_agricola`. Consulte o contrato de cada dataset.

Em `clima`, o contexto fornece somente o ano padrão do modo UF quando `ano` é omitido. Uma consulta com snapshot `2001-06-01` pode incluir dezembro de 2001. O modo estação mantém o intervalo explícito; nenhum dos modos congela revisões da fonte ou força execução offline. `meta.source_details.deterministic` declara esses limites. Para os ZIPs INMET, `source_details.resources` registra URL, SHA-256, instante da aquisição, cache e membros selecionados; preserve os arquivos ou resultados correspondentes para reproduzir a edição. O cache de processo expira e não substitui um acervo de pesquisa.

## Verificando o Modo

```python
from agrobr.datasets import is_deterministic, get_snapshot

async with datasets.deterministic("2025-12-31"):
    print(is_deterministic())  # True
    print(get_snapshot())      # "2025-12-31"

print(is_deterministic())  # False
print(get_snapshot())      # None
```

## Casos de Uso

### Papers Acadêmicos

```python
async with datasets.deterministic("2024-12-31"):
    df_precos = await datasets.preco_diario("soja")

df_safra = await datasets.estimativa_safra("soja", safra="2024/25")
df_safra.to_parquet("estimativa_safra_2024_25.parquet")
```

A `estimativa_safra` não fica congelada: a CONAB revisa a estimativa a cada levantamento, e o dataset consulta o corrente
(dentro do contexto, avisa isso em `validation_warnings`). Guarde o arquivo usado, ou um [snapshot](../guides/snapshots.md),
junto do paper.

### Backtests

```python
async def backtest(data_corte: str):
    async with datasets.deterministic(data_corte):
        df = await datasets.preco_diario("soja")
        return calcular_estrategia(df)

resultados = [await backtest(f"2024-{m:02d}-01") for m in range(1, 13)]
```

### Auditoria

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

audit_log = {
    "snapshot": meta.snapshot,
    "source": meta.source,
    "fetched_at": meta.fetched_at.isoformat(),
    "records": meta.records_count,
    "contract": meta.contract_version,
}
```

## Thread/Async Safety

O modo determinístico usa `contextvars`, garantindo isolamento:

- Cada task async tem seu próprio contexto
- Threads diferentes não interferem
- Contextos aninhados funcionam corretamente

```python
async def task_a():
    async with datasets.deterministic("2024-01-01"):
        assert get_snapshot() == "2024-01-01"

async def task_b():
    async with datasets.deterministic("2025-01-01"):
        assert get_snapshot() == "2025-01-01"

await asyncio.gather(task_a(), task_b())
```

## Pré-Requisitos

Para reprodutibilidade total, o cache local deve conter os dados históricos:

1. Execute as consultas normalmente primeiro (popula o cache)
2. Use modo determinístico para reproduzir

Sem o produto no cache, o `preco_diario` levanta `SourceUnavailableError`; um período sem dado, com o produto no
cache, volta vazio.

```python
df = await datasets.preco_diario("soja")

async with datasets.deterministic("2025-01-15"):
    df_reproduzido = await datasets.preco_diario("soja")
```
