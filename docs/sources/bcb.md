# BCB/SICOR — Crédito Rural

Dados de crédito rural do Sistema de Operações do Crédito Rural (SICOR),
disponibilizados via API OData do Banco Central.

## API

```python
from agrobr import bcb

# Crédito de custeio para soja, safra 2024/25
df = await bcb.credito_rural(produto="soja", safra="2024/25", finalidade="custeio")

# Filtrar por UF
df = await bcb.credito_rural(produto="soja", safra="2024/25", uf="MT")

# Agregação por UF (soma municípios)
df = await bcb.credito_rural(produto="soja", safra="2024/25", agregacao="uf")
```

## Colunas — `credito_rural`

| Coluna | Tipo | Descrição |
|---|---|---|
| `safra` | str | Safra no formato "2024/2025" |
| `ano_emissao` | int | Ano de emissão do contrato |
| `mes_emissao` | int | Mês de emissão |
| `uf` | str | UF do município |
| `municipio` | str | Nome do município |
| `produto` | str | Produto financiado |
| `finalidade` | str | Finalidade (custeio, investimento, comercializacao) |
| `valor` | float | Valor financiado (R$) |
| `area_financiada` | float | Área financiada (ha) |
| `qtd_contratos` | int | Quantidade de contratos |

## Finalidades

- `custeio` — financiamento da produção
- `investimento` — aquisição de máquinas, infraestrutura
- `comercializacao` — financiamento da comercialização

## Produtos

Soja, milho, café, algodão, arroz, trigo, feijão, cana-de-açúcar, mandioca,
sorgo, aveia, cevada, entre outros. Use o nome canônico do agrobr.

## MetaInfo

```python
df, meta = await bcb.credito_rural(produto="soja", safra="2024/25", return_meta=True)
print(meta.source)  # "bcb"
```

## Fonte

- API: `https://olinda.bcb.gov.br/olinda/servico/SICOR/versao/v2/odata`
- Atualização: mensal
- Histórico: 2013+
