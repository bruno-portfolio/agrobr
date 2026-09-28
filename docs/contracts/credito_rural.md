# credito_rural v2.0

Crédito rural por cultura e UF, via camada semântica. O contrato representa os dois níveis que o endpoint SICOR realmente fornece: agregação por UF (padrão) ou por programa. O registro a registro (`agregacao="registro"`) tem contrato próprio, com 23 colunas: [bcb.credito_rural_registro](./bcb_credito_rural_registro.md).

## Fontes

| Prioridade | Fonte | Descrição |
|---|---|---|
| 1 | BCB/SICOR (OData) | API oficial do Banco Central |
| 2 | BigQuery (Base dos Dados) | Fallback para falhas de rede ou HTTP 5xx, só na agregação `uf` sem `programa` nem `tipo_seguro`: a tabela agrega por município e não traz programa nem seguro |

## Produtos

`soja`, `milho`, `cafe`, `algodao`, `trigo`, `arroz`, `feijao`, `cana`, `mandioca`, `sorgo`

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

A safra em curso (julho a junho, pela data de hoje) sai marcada: aviso em `validation_warnings` e em `UserWarning`, e `source_details["safra_em_curso"]` e `source_details["meses_cobertos"]`. O total dela muda até o fim da safra.

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
df_registro = await datasets.credito_rural("soja", safra="2024/25", agregacao="registro")
```

As entidades por produto que o agrobr lê (`*RegiaoUFProduto`) não têm município. `agregacao="municipio"` levanta `InvalidParameterError`; o SICOR publica município por produto (`CusteioMunicipioProduto` e `InvestMunicipioProduto`), que o agrobr não lê, e o extra `agrobr[bigquery]` traz dados municipais. A industrialização não sai por produto: está no [bcb.credito_rural_total](./bcb_credito_rural_total.md).

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
