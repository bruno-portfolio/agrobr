# exportacao v1.1

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
| `volume_ton` | float | ✅ | t (`kg_liquido` / 1000) | Não |

**Primary key:** `[ano, mes, produto, uf]`

O dataset devolve só essas colunas, nessa ordem, pelas 2 fontes. O `receita_usd_mil` da ABIOVE, que é o `valor_fob_usd` em mil, fica na [fonte](../api/abiove.md).

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
`agrobr.comexstat`). `oleo_soja_bruto` continua disponível na API autônoma do ComexStat
como o código específico `15071000`, mas não integra o vocabulário deste dataset. O
fallback ABIOVE usa as categorias genéricas `farelo` e `oleo`, com o mesmo escopo de
`farelo_soja` e `oleo_soja` (farelo 2025: ABIOVE 23,27 mi t na edição de ago/2026, 23,30 mi t na de dez/2025, × posição `2304` 23,27 mi t),
e devolve o nome canônico do dataset.

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
- A licença muda no fallback: o ComexStat é `livre`, e a ABIOVE é `zona_cinza`. O `meta.license` diz a
  do dado entregue, e o `datasets.info("exportacao")["licenses"]` lista as 2.

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

As flags de saída são somente por nome. `kg_liquido` e valores monetários usam `float64`; texto usa o padrão do pandas instalado no cheio e no vazio.
