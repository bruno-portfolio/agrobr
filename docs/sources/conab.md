# CONAB - Companhia Nacional de Abastecimento

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | Ministerio da Agricultura |
| **Website** | [conab.gov.br](https://www.conab.gov.br) |
| **Acesso agrobr** | Direto (XLSX publicos) |

## Origem dos Dados

### Fonte

- **URL**: `https://www.conab.gov.br/info-agro/safras/graos`
- **Formato**: XLSX (planilhas Excel)
- **Acesso**: Publico, sem restricoes

## Levantamentos

A CONAB publica levantamentos mensais de safra:

| Mes | Levantamento |
|-----|--------------|
| Outubro | 1o Levantamento |
| Novembro | 2o Levantamento |
| Dezembro | 3o Levantamento |
| Janeiro | 4o Levantamento |
| Fevereiro | 5o Levantamento |
| Marco | 6o Levantamento |
| Abril | 7o Levantamento |
| Maio | 8o Levantamento |
| Junho | 9o Levantamento |
| Julho | 10o Levantamento |
| Agosto | 11o Levantamento |
| Setembro | 12o Levantamento |

## Dados Disponiveis

### Safras

- Area plantada (mil hectares)
- Area colhida (mil hectares)
- Produtividade (kg/ha)
- Producao (mil toneladas)

### Balanco de Oferta e Demanda

- Estoque inicial
- Producao
- Importacao
- Consumo
- Exportacao
- Estoque final

## Uso

### Safras por Produto

```python
import asyncio
from agrobr import conab

async def main():
    # Dados de safra da soja
    df = await conab.safras('soja')

    # Safra especifica
    df = await conab.safras('milho', safra='2025/26')

    # Filtrar por UF
    df = await conab.safras('soja', uf='MT')

    # Com metadados
    df, meta = await conab.safras('soja', return_meta=True)

asyncio.run(main())
```

### Balanco de Oferta/Demanda

```python
# Balanco de todos os produtos
df = await conab.balanco()

# Balanco de produto especifico
df = await conab.balanco(produto='soja')
```

### Totais Brasil

```python
# Totais nacionais por produto
df = await conab.brasil_total()
```

## Schema - Safras

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| `fonte` | str | "conab" |
| `produto` | str | Nome do produto |
| `safra` | str | Safra (ex: "2024/25") |
| `uf` | str | Sigla da UF |
| `area_plantada` | Decimal | Mil hectares |
| `area_colhida` | Decimal | Mil hectares |
| `produtividade` | Decimal | kg/ha |
| `producao` | Decimal | Mil toneladas |
| `levantamento` | int | Numero do levantamento (1-12) |
| `data_publicacao` | date | Data de publicacao |

## Produtos Disponiveis

```python
produtos = await conab.produtos()
# ['soja', 'milho', 'arroz', 'feijao', 'algodao', 'trigo', ...]
```

## UFs Disponiveis

```python
ufs = await conab.ufs()
# ['AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', ...]
```

## Levantamentos Disponiveis

```python
levs = await conab.levantamentos()
for lev in levs[:5]:
    print(f"{lev['safra']} - {lev['levantamento']}o levantamento")
```

## Cache

| Aspecto | Valor |
|---------|-------|
| **TTL** | 24 horas |
| **Stale maximo** | 30 dias |
| **Politica** | TTL fixo |

## Atualizacao

| Aspecto | Valor |
|---------|-------|
| **Frequencia** | Mensal |
| **Publicacao** | Geralmente entre dias 10-15 |
