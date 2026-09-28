# bcb.credito_rural_total v1.0

Crédito rural por UF e finalidade, sem produto, da entidade `RegiaoUF` do SICOR: as quatro finalidades, inclusive a industrialização, que o SICOR não publica por produto. Contrato de fonte da função `bcb.credito_rural_total`; não é dataset.

## Fonte

| Fonte | Entidade | Descrição |
|---|---|---|
| BCB/SICOR (OData v2, Olinda) | `RegiaoUF` | Quantidade e valor por mês, UF, programa, subprograma, fonte de recursos e atividade, com as quatro finalidades em colunas |

Sem fallback: a tabela da Base dos Dados não foi conferida contra o oráculo deste contrato.

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|---|---|---|---|---|
| `safra` | str | Não | — | Sim |
| `uf` | str | Não | — | Sim |
| `finalidade` | str | Não | — | Sim |
| `agregacao` | str | Não | — | Sim |
| `programa` | str | Sim | — | Sim |
| `cd_programa` | str | Sim | — | Sim |
| `qtd_contratos` | int | Não | contratos | Sim |
| `valor` | float | Não | BRL | Sim |
| `fonte` | str | Não | — | Sim |

**Chave primária:** `[safra, uf, finalidade, programa]`

**Restrições:** `qtd_contratos >= 0`, `valor >= 0`

- `safra` no formato `AAAA/AAAA`, de julho a junho.
- `finalidade`: `custeio`, `investimento`, `comercializacao` ou `industrializacao`.
- Na agregação `uf`, `programa` e `cd_programa` são nulos. Na agregação `programa`, identificam o programa, com o nome vigente da tabela oficial.
- `valor` é a soma em BRL, ao centavo; `qtd_contratos` é a soma dos contratos.

## Regras da fonte

- **Zero de preenchimento:** na linha larga, a fonte traz 0 nas finalidades sem operação. O par quantidade = 0 e valor = 0 não vira linha, e a finalidade sem operação fica ausente. Exemplo: na safra 2022/23, industrialização no AM, no AP e em RR e comercialização no AP.
- **Sem linha Brasil:** o SICOR não publica total do país. O total do Brasil é a soma das UFs, e a função não inventa a linha.
- **Safra parcial:** a safra corrente é parcial. `MetaInfo.source_details["meses"]` registra o primeiro e o último mês com dado e a quantidade de meses.
- **Consulta por safra × soma das mensais:** o SICOR pode devolver, na consulta da safra, números diferentes da soma das consultas mês a mês. Em 26/09/2026, na safra 2026/27 (julho e agosto), 51 pares UF × finalidade divergiram; no custeio do AC, 274 contratos e R$ 56.788.261,98 na consulta por safra, que a função usa, contra 272 e R$ 56.541.830,82 nas mensais. A causa não foi identificada, e o agrobr reproduz o corpo recebido.
- **Coerência entre entidades**, conferida em 2022 e 2023: o total por UF e finalidade é igual à soma dos municípios da `CusteioInvestimentoComercialIndustrialSemFiltros` e, em custeio, investimento e comercialização, à soma por produto das entidades `*RegiaoUFProduto`, que o `credito_rural` lê.

## Histórico de versões

| Versão | Mudança |
|---|---|
| v1.0 | Contrato inicial |

## Exemplo

```python
from agrobr import bcb

df = await bcb.credito_rural_total(safra="2022/23")
industrializacao = await bcb.credito_rural_total(safra="2022/23", finalidade="industrializacao")
por_programa = await bcb.credito_rural_total(safra="2022/23", uf="MT", agregacao="programa")
```

## Schema JSON

Disponível em `agrobr/schemas/bcb_credito_rural_total.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("bcb_credito_rural_total")
print(contract.to_json())
```
