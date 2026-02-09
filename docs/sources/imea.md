# IMEA — Cotações e Indicadores MT

Instituto Mato-Grossense de Economia Agropecuária.
Cotações diárias, indicadores de preço e dados de safra para Mato Grosso.

## API

```python
from agrobr import imea

# Cotações de soja em MT
df = await imea.cotacoes("soja")

# Filtrar por safra
df = await imea.cotacoes("soja", safra="24/25")

# Filtrar por unidade
df = await imea.cotacoes("soja", unidade="R$/sc")

# Outras cadeias
df = await imea.cotacoes("milho")
df = await imea.cotacoes("algodao")
df = await imea.cotacoes("bovinocultura")
```

## Colunas — `cotacoes`

| Coluna | Tipo | Descrição |
|---|---|---|
| `cadeia` | str | Cadeia produtiva |
| `localidade` | str | Macrorregião/localidade MT |
| `valor` | float | Valor da cotação |
| `variacao` | float | Variação (%) |
| `safra` | str | Safra (ex: "24/25") |
| `unidade` | str | Unidade (R$/sc, R$/t, %) |
| `unidade_descricao` | str | Descrição da unidade |
| `data_publicacao` | str | Data de publicação |

## Cadeias Produtivas

| Nome agrobr | Cadeia IMEA |
|---|---|
| `soja` | Soja |
| `milho` | Milho |
| `algodao` | Algodão |
| `bovinocultura` / `boi` | Bovinocultura |
| `arroz` | Arroz |
| `feijao` | Feijão |

## MetaInfo

```python
df, meta = await imea.cotacoes("soja", return_meta=True)
print(meta.source)  # "imea"
print(meta.source_method)  # "httpx"
```

## Fonte

- API: `https://api1.imea.com.br/api/v2/mobile/cadeias`
- Formato: JSON (REST API)
- Atualização: diária
- Cobertura: Mato Grosso
- Autenticação: nenhuma (API pública)
