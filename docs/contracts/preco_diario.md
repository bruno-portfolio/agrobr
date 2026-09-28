# preco_diario v1.1

Preço diário spot de commodities agrícolas brasileiras.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | CEPEA/ESALQ | Coleta direta, com Notícias Agrícolas como fallback |
| 2 | Cache local | DuckDB; devolve as mesmas colunas e tipos da coleta CEPEA |

## Produtos

`soja`, `milho`, `boi`, `bezerro`, `cafe`, `cafe_robusta`, `trigo`, `algodao`

## Schema

| Coluna | Tipo | Nullable | Unidade | Descrição |
|--------|------|----------|---------|-----------|
| `data` | date | ❌ | - | Data do indicador |
| `produto` | str | ❌ | - | Nome do produto |
| `praca` | str | ✅ | - | Praça de referência |
| `valor` | float64 | ❌ | Conforme `unidade` | Preço na unidade indicada na mesma linha |
| `unidade` | str | ❌ | - | Ex: "BRL/sc60kg", "BRL/ton", "cBRL/lb" (centavos de real por libra) |
| `fonte` | str | ❌ | - | Origem dos dados |
| `metodologia` | str | ✅ | - | Metodologia do indicador, quando disponível |
| `anomalies` | str | ✅ | - | Lista de anomalias serializada como texto JSON; nulo quando vazia |
| `valor_usd` | float64 | ✅ | USD | Preço em dólar publicado pelo CEPEA na mesma linha (opcional, desde 1.1); nulo no fallback Notícias Agrícolas e no histórico anterior à migração 10 |
| `peso_medio_kg` | float64 | ✅ | kg | Peso médio do bezerro (MS) da tabela auxiliar do CEPEA (opcional, desde 1.1); nulo para os demais produtos |

**Nota sobre precisão:** `valor` usa `float64` (não `Decimal`) para
compatibilidade com pandas/polars e performance em pipelines. Precisão
IEEE 754 é suficiente para preços agrícolas (máx ~R$ 999.999,99).
Para uso contábil que exija precisão exata, converter com
`df["valor"].apply(Decimal)` após o fetch.

As colunas opcionais 1.1 só são preenchidas pela coleta CEPEA: `valor_usd` acompanha a
coluna `Valor US$` da tabela do indicador e `peso_medio_kg` vem da tabela `Peso Médio`
da página do bezerro. Linhas do fallback Notícias Agrícolas e registros em cache
anteriores à migração 10 permanecem nulos até nova coleta.

## Garantias

- `data` é sempre dia útil
- `valor` é sempre positivo
- Ordenado por `data` decrescente

## Seleção de observações

A chave continua sendo `data` e `produto`. Para a mesma observação e praça,
CEPEA tem precedência sobre Notícias Agrícolas, inclusive quando ambos já
estão no cache. A nova aquisição da mesma fonte substitui sua revisão anterior.
As observações dos dois provedores permanecem no DuckDB.

Use `praca=` para selecionar uma praça. Sem esse filtro, o dataset prioriza a
praça de referência já cadastrada para o produto: Paranaguá/PR para soja,
Campinas/SP para milho, São Paulo/SP para boi, café e algodão, Mato Grosso do
Sul para bezerro, Espírito Santo para café robusta e Paraná para trigo.
Quando a referência não estiver presente, o desempate usa o slug da praça
em ordem alfabética; isso não transforma uma série regional em média nacional.
Para obter todas as praças, use `cepea.indicador()`.

`data_sources` contém somente as fontes das linhas selecionadas. A coluna
`valor` segue `float64` também no fallback direto ao DuckDB. Com sanity,
use `json.loads(df.loc[indice, "anomalies"])` para ler marcadores não nulos.

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.preco_diario("soja")
df, meta = await datasets.preco_diario("soja", return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.preco_diario("soja")
```

## Modo Determinístico

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
    # Filtra data <= 2025-12-31
    # Usa apenas cache local; sem o produto no cache, levanta SourceUnavailableError
```

## Schema JSON

Disponível em `agrobr/schemas/preco_diario.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("preco_diario")
print(contract.primary_key)  # ['data', 'produto']
print(contract.to_json())
```

## MetaInfo

Quando `return_meta=True`, retorna tupla `(DataFrame, MetaInfo)`:

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

print(meta.source)            # "datasets.preco_diario/cepea"
print(meta.dataset)           # "preco_diario"
print(meta.contract_version)  # "1.1"
print(meta.records_count)     # 365
print(meta.from_cache)        # False
print(meta.snapshot)          # None (ou "2025-12-31" se determinístico)
```
