# UN Comtrade — Comercio Internacional

United Nations Comtrade Database. Dados de comercio internacional bilateral reportados por ~200 paises.

## Configuracao

API key opcional. Funciona em guest mode (sem key):

```python
df = await comtrade.comercio("soja", partner="CN")
```

Para mais capacidade (100k records/call vs 500):

```bash
export AGROBR_COMTRADE_API_KEY="sua-key-aqui"
```

Registre gratuitamente em [comtradeplus.un.org](https://comtradeplus.un.org).

## API

```python
from agrobr import comtrade

# Comercio bilateral
df = await comtrade.comercio("soja", reporter="BR", partner="CN", periodo=2024)

# Trade mirror (exportacoes BR vs importacoes CN)
df = await comtrade.trade_mirror("soja", reporter="BR", partner="CN", periodo=2024)

# Mensal com paginacao automatica
df = await comtrade.trade_mirror("soja", partner="CN", freq="M", periodo="2022-2024")

# Complexo soja (HS 1201, 1507, 2304)
df = await comtrade.comercio("complexo_soja", partner="CN")
```

## Colunas — `comercio`

| Coluna | Tipo | Descricao |
|---|---|---|
| `periodo` | str | Periodo: "2024" (anual) ou "202401" (mensal) |
| `ano` | int | Ano extraido do periodo |
| `mes` | int | Mes (mensal) ou null (anual) |
| `reporter_iso` | str | ISO3 do pais reporter (ex: "BRA") |
| `partner_iso` | str | ISO3 do pais parceiro (ex: "CHN") |
| `fluxo_code` | str | "X" (exportacao) ou "M" (importacao) |
| `hs_code` | str | Codigo HS (4 digitos) |
| `produto_desc` | str | Descricao do produto |
| `peso_liquido_kg` | float | Peso liquido em kg |
| `volume_ton` | float | Volume em toneladas (peso_liquido_kg / 1000) |
| `valor_fob_usd` | float | Valor FOB em USD |
| `valor_cif_usd` | float | Valor CIF em USD |
| `valor_primario_usd` | float | FOB para exports, CIF para imports |

## Colunas — `trade_mirror`

Colunas de ambos os lados + discrepancias calculadas:

| Coluna | Descricao |
|---|---|
| `peso_liquido_kg_reporter` / `_partner` | Peso declarado por cada lado |
| `valor_fob_usd_reporter` / `_partner` | Valor FOB de cada lado |
| `valor_cif_usd_partner` | CIF do lado importador |
| `diff_peso_kg` | reporter - partner |
| `diff_valor_fob_usd` | FOB reporter - FOB partner |
| `ratio_valor` | FOB reporter / CIF partner (esperado ~0.85-0.95) |
| `ratio_peso` | Peso reporter / peso partner (esperado ~1.0) |

## Produtos Mapeados

| Nome agrobr | HS Code(s) |
|---|---|
| `soja` | 1201 |
| `complexo_soja` | 1201, 1507, 2304 |
| `milho` | 1005 |
| `cafe` | 0901 |
| `acucar` | 1701 |
| `carne_bovina` | 0201, 0202 |
| `carne_frango` | 0207 |
| `celulose` | 4703 |

Use `comtrade.produtos()` para lista completa.

## MetaInfo

```python
df, meta = await comtrade.comercio("soja", partner="CN", return_meta=True)
print(meta.source)  # "comtrade"
print(meta.source_method)  # "httpx"
```

## Fonte

- API: `https://comtradeapi.un.org/data/v1/get/C/{freq}/HS`
- Formato: JSON (REST API)
- Atualizacao: mensal (paises reportam com 2-6 meses de atraso)
- Historico: mensal desde 2000, anual desde 1988
- Cobertura: ~200 paises, classificacao HS 2-6 digitos
