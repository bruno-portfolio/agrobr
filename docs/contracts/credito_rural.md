# credito_rural v2.0

Crédito rural por cultura e UF, via camada semântica. O contrato representa os dois níveis que o endpoint SICOR realmente fornece: agregação por UF (padrão) ou por programa.

## Fontes

| Prioridade | Fonte | Descrição |
|---|---|---|
| 1 | BCB/SICOR (OData) | API oficial do Banco Central |
| 2 | BigQuery (Base dos Dados) | Fallback para falhas de rede ou HTTP 5xx |

## Produtos

`soja`, `milho`, `cafe`, `algodao`, `trigo`, `arroz`, `feijao`, `cana`, `sorgo`

## Schema

| Coluna | Tipo | Nullable | Unidade | Estável |
|---|---|---|---|---|
| `safra` | str | Não | — | Sim |
| `produto` | str | Não | — | Sim |
| `uf` | str | Sim | — | Sim |
| `finalidade` | str | Não | — | Sim |
| `agregacao` | str | Não | — | Sim |
| `programa` | str | Sim | — | Sim |
| `cd_programa` | str | Sim | — | Sim |
| `qtd_contratos` | int | Sim | contratos | Sim |
| `valor` | float | Sim | BRL | Sim |
| `area_financiada` | float | Sim | ha | Sim |
| `fonte` | str | Não | — | Sim |

**Chave primária:** `[safra, produto, uf, finalidade, programa]`

**Restrições:** `qtd_contratos >= 0`, `valor >= 0`, `area_financiada >= 0`

Na agregação `uf`, `programa` e `cd_programa` são nulos. Na agregação `programa`, identificam a dimensão agrupada.

## Histórico de versões

| Versão | Mudança |
|---|---|
| v1.0 | Schema inicial |
| v1.1 | Dimensões opcionais adicionadas |
| v2.0 | Schema alinhado aos modos reais do SICOR; remove `volume` e dimensões que não pertencem à saída agregada; padrão passa de `municipio` para `uf` |

## Exemplo

```python
from agrobr import datasets

df = await datasets.credito_rural("soja", safra="2024/25")
df_programa = await datasets.credito_rural(
    "soja",
    safra="2024/25",
    agregacao="programa",
)
```

O OData usado pelo agrobr não possui dimensão municipal. `agregacao="municipio"` levanta `InvalidParameterError`; dados municipais podem ser consultados diretamente com o extra `agrobr[bigquery]`.

## Schema JSON

Disponível em `agrobr/schemas/credito_rural.json`.

```python
from agrobr.contracts import get_contract

contract = get_contract("credito_rural")
print(contract.to_json())
```

## Requisitos do fallback

```bash
pip install agrobr[bigquery]
```
