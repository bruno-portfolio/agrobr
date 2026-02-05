# Contratos de Dados

O agrobr garante estabilidade de schema. Seu pipeline não vai quebrar.

## Garantias Globais

| Garantia | Descrição |
|----------|-----------|
| **Nomes estáveis** | Colunas nunca mudam de nome (só adicionam) |
| **Tipos só alargam** | int→float ok, float→int nunca |
| **Datas ISO-8601** | Sempre YYYY-MM-DD |
| **Unidades explícitas** | Coluna dedicada |
| **Breaking = Major** | Quebras só em versão major |

## Datasets

| Dataset | Descrição | Fontes |
|---------|-----------|--------|
| [preco_diario](./preco_diario.md) | Preços diários spot | CEPEA → cache |
| [producao_anual](./producao_anual.md) | Produção anual consolidada | IBGE PAM → CONAB |
| [estimativa_safra](./estimativa_safra.md) | Estimativas safra corrente | CONAB → IBGE LSPA |
| [balanco](./balanco.md) | Oferta/demanda | CONAB |

## Uso

```python
from agrobr import datasets

# Listar datasets
datasets.list_datasets()
# ['balanco', 'estimativa_safra', 'preco_diario', 'producao_anual']

# Listar produtos de um dataset
datasets.list_products("preco_diario")
# ['soja', 'milho', 'boi', 'cafe', 'trigo', 'algodao']

# Info de um dataset
datasets.info("preco_diario")
# {'name': 'preco_diario', 'sources': ['cepea', 'cache'], ...}
```

## Fallback Automático

Cada dataset tem múltiplas fontes com prioridade. Se a fonte primária
falhar, o agrobr automaticamente tenta a próxima:

```
preco_diario: CEPEA → cache local
producao_anual: IBGE PAM → CONAB
estimativa_safra: CONAB → IBGE LSPA
balanco: CONAB
```

## MetaInfo

Toda chamada com `return_meta=True` retorna metadados de proveniência:

```python
df, meta = await datasets.preco_diario("soja", return_meta=True)

print(meta.source)            # Fonte usada
print(meta.dataset)           # Nome do dataset
print(meta.contract_version)  # Versão do contrato
print(meta.records_count)     # Registros retornados
print(meta.from_cache)        # Se veio do cache
print(meta.snapshot)          # Data de corte (modo determinístico)
```
