# exportacao v1.0

Exportações agrícolas brasileiras por produto, UF e mês.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | ComexStat | Dados oficiais MDIC por NCM |
| 2 | ABIOVE | Fallback nacional com normalização de unidades; indisponível com filtro de UF |

## Produtos

`soja`, `milho`, `cafe`, `algodao`, `acucar`, `farelo_soja`, `oleo_soja`

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|--------|------|----------|---------|---------|
| `ano` | int | ❌ | - | Sim |
| `mes` | int | ❌ | - | Sim |
| `produto` | str | ❌ | - | Sim |
| `uf` | str | ✅ | - | Sim |
| `kg_liquido` | float | ✅ | kg | Sim |
| `valor_fob_usd` | float | ✅ | USD | Sim |

**Primary key:** `[ano, mes, produto, uf]`

**Constraints:** `ano >= 1997`, `mes` entre 1 e 12, `kg_liquido >= 0`, `valor_fob_usd >= 0`

## Semântica dos produtos

- `oleo_soja` cobre toda a posição NCM `1507`, incluindo óleo bruto,
  refinado e outras apresentações. O adaptador consolida os diferentes NCMs
  por `ano`, `mes` e `uf` antes de validar a chave primária.
- `oleo_soja_bruto` continua disponível na API autônoma do ComexStat como o
  código específico `15071000`, mas não integra o vocabulário deste dataset.
- O fallback ABIOVE usa a categoria genérica `oleo`, equivalente ao escopo de
  `oleo_soja`, e devolve o nome canônico do dataset.

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `ano` sempre >= 1997
- `mes` entre 1 e 12
- Valores numéricos sempre >= 0

## Comportamento do fallback

- Sem `ano`, é usado o último ano civil completo.
- A ABIOVE publica somente totais nacionais. Quando `uf` é informado, o
  fallback não substitui o recorte estadual por esses totais; se o ComexStat
  estiver indisponível, a chamada levanta `SourceUnavailableError`.
- O adaptador converte `soja`, `farelo_soja`, `oleo_soja` e `milho` para os
  nomes usados pela ABIOVE e restaura o nome canônico na saída. A coluna `uf`
  fica nula para esses totais nacionais.
- `cafe`, `algodao` e `acucar` não existem na ABIOVE. Para esses produtos, uma
  falha do ComexStat encerra a cascata sem tentar baixar a planilha do fallback.

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.exportacao("soja", ano=2024)
df = await datasets.exportacao("soja", ano=2024, uf="MT")

# Com metadados
df, meta = await datasets.exportacao("soja", ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.exportacao("soja", ano=2024)
```

## Schema JSON

Disponível em `agrobr/schemas/exportacao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("exportacao")
print(contract.to_json())
```
