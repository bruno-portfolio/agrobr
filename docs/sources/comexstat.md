# ComexStat — Exportações

Dados de comércio exterior do MDIC/SECEX. Exportações e importações
por produto (NCM), UF e país de destino.

## API

```python
from agrobr import comexstat

# Exportações mensais de soja em 2024
df = await comexstat.exportacao("soja", ano=2024, agregacao="mensal")

# Exportações detalhadas (por país/UF/via)
df = await comexstat.exportacao("soja", ano=2024, agregacao="detalhado")

# Filtrar por UF
df = await comexstat.exportacao("soja", ano=2024, uf="MT")
```

## Colunas — `exportacao` (mensal)

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano |
| `mes` | int | Mês (1-12) |
| `ncm` | str | Código NCM (8 dígitos) |
| `uf` | str | UF de origem |
| `kg_liquido` | float | Peso líquido (kg) |
| `valor_fob_usd` | float | Valor FOB (USD) |
| `volume_ton` | float | Volume em toneladas |

## Produtos

17 produtos mapeados por NCM:

| Produto | NCM |
|---|---|
| soja | 12019000 |
| milho | 10059010 |
| cafe | 09011110 |
| algodao | 52010000 |
| trigo | 10019900 |
| arroz | 10063021 |
| acucar | 17011400 |
| ... | ... |

## MetaInfo

```python
df, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)
print(meta.source)  # "comexstat"
```

## Fonte

- Bulk CSV: `https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm`
- Atualização: semanal/mensal
- Histórico: 1997+
