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

19 produtos mapeados por prefixo NCM:

| Produto | NCM | Tipo |
|---|---|---|
| soja | 12019000 | exato |
| soja_semeadura | 12011000 | exato |
| oleo_soja_bruto | 15071000 | exato |
| farelo_soja | 23040010 | exato |
| milho | 10059010 | exato |
| cafe | 09011110 | exato |
| cafe_conilon | 09011190 | exato |
| algodao | 520100 | prefixo (5201.00.20 + 5201.00.90) |
| algodao_cardado | 520300 | prefixo (5203.00.00) |
| trigo | 10019900 | exato |
| arroz | 10063021 | exato |
| acucar | 17011400 | exato |
| etanol | 22071000 | exato |
| carne_bovina | 02023000 | exato |
| carne_frango | 02071400 | exato |
| carne_suina | 02032900 | exato |

> **Nota:** O filtro NCM usa `str.startswith(prefix)`. Prefixos de 8 digitos
> equivalem a match exato; prefixos menores (6 digitos) capturam todas as
> subposicoes. Isso e necessario porque alguns produtos (ex: algodao) nao
> possuem NCM generico no CSV — o Brasil usa subposicoes detalhadas.

## MetaInfo

```python
df, meta = await comexstat.exportacao("soja", ano=2024, return_meta=True)
print(meta.source)  # "comexstat"
```

## Notas tecnicas

- O site `balanca.economia.gov.br` possui certificado SSL incompleto.
  O client usa `verify=False` no httpx para contornar o problema.
- Cada CSV anual tem ~100MB. O download e feito uma vez e filtrado em memoria.

## Fonte

- Bulk CSV: `https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm`
- Atualizacao: semanal/mensal
- Historico: 1997+
