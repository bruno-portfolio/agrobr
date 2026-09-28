# importacao v1.2

Importações agrícolas brasileiras por produto, UF e mês.

## Fontes

| Prioridade | Fonte | Descrição |
|------------|-------|-----------|
| 1 | ComexStat | Dados oficiais MDIC por NCM |

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
| `valor_frete_usd` | float | ✅ | USD | Não |
| `valor_seguro_usd` | float | ✅ | USD | Não |
| `volume_ton` | float | ✅ | t (`kg_liquido` / 1000) | Não |

Quando publicadas, `valor_frete_usd` e `valor_seguro_usd` preservam separadamente
os valores de frete e seguro em dólares (USD). São colunas opcionais no contrato.

**Primary key:** `[ano, mes, produto, uf]`

O dataset devolve só essas colunas, nessa ordem.

**Constraints:** `ano >= 1997`, `mes` entre 1 e 12, `kg_liquido >= 0`, `valor_fob_usd >= 0`

## Semântica dos produtos

Cada produto soma todos os códigos NCM que o compõem, com os códigos vigentes em cada ano
(tabela "entra / não entra" na [API ComexStat](../api/comexstat.md)):

- `soja`: grão, mesmo triturado, exceto semeadura (`12019000`; `12010090` até 2013).
- `milho`: toda a posição `1005`.
- `cafe`: não torrado e torrado, com ou sem cafeína (`09011`, `09012`); ficam fora cascas e
  sucedâneos (`09019000`) e o solúvel (`2101`).
- `algodao`: `5201` (não cardado nem penteado) e `5203` (cardado ou penteado); ficam fora
  desperdícios (`5202`), fios e tecidos.
- `acucar`: toda a posição `1701` (bruto de cana e de beterraba e refinado).
- `farelo_soja`: toda a posição `2304` (farinhas e pellets e bagaços e outros resíduos).
- `oleo_soja`: toda a posição `1507` (bruto, refinado e outros).

O adaptador consolida os códigos de cada produto por `ano`, `mes` e `uf` antes de validar a
chave primária; a coluna `ncm` não sai no dataset (o detalhe por NCM está em
`agrobr.comexstat`).

## Garantias

- Nomes de coluna nunca mudam (só adicionam)
- `ano` sempre >= 1997
- `mes` entre 1 e 12
- Valores numéricos sempre >= 0

## Exemplo

```python
from agrobr import datasets

# Async
df = await datasets.importacao("soja", ano=2024)
df = await datasets.importacao("soja", ano=2024, uf="SP")

# Com metadados
df, meta = await datasets.importacao("soja", ano=2024, return_meta=True)

# Sync
from agrobr.sync import datasets
df = datasets.importacao("soja", ano=2024)
```

## Schema JSON

Disponível em `agrobr/schemas/importacao.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("importacao")
print(contract.to_json())
```
