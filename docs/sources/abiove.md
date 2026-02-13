# ABIOVE — Exportação Complexo Soja

> **Licença:** Sem termos de uso públicos localizados. Autorização formal
> solicitada em fev/2026 — aguardando resposta.
> Classificação: `zona_cinza`

!!! note "Autorização pendente"
    Autorização formal para redistribuição de dados foi solicitada à ABIOVE
    em fevereiro/2026. Aguardando resposta. Verifique diretamente com a
    ABIOVE antes de uso comercial.

Associação Brasileira das Indústrias de Óleos Vegetais. Dados de exportação
mensal de grão de soja, farelo, óleo e milho.

## API

```python
from agrobr import abiove

# Exportação do complexo soja
df = await abiove.exportacao(ano=2024)

# Filtrar por produto
df = await abiove.exportacao(ano=2024, produto="grao")

# Filtrar por mês
df = await abiove.exportacao(ano=2024, mes=6)

# Agregação mensal (soma todos os produtos)
df = await abiove.exportacao(ano=2024, agregacao="mensal")
```

## Colunas — `exportacao`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência |
| `mes` | int | Mês (1-12) |
| `produto` | str | Produto (grao, farelo, oleo, milho) |
| `volume_ton` | float | Volume exportado (toneladas) |
| `receita_usd_mil` | float | Receita FOB (mil USD) |

## Produtos

- `grao` — Grão de soja
- `farelo` — Farelo de soja
- `oleo` — Óleo de soja
- `milho` — Milho

## MetaInfo

```python
df, meta = await abiove.exportacao(ano=2024, return_meta=True)
print(meta.source)  # "abiove"
print(meta.source_method)  # "httpx+openpyxl"
```

## Nota de Risco

ABIOVE publica dados em planilhas Excel. O layout pode variar entre anos.
O parser do agrobr detecta automaticamente a posição do header.

## Fonte

- URL: `https://abiove.org.br/estatisticas/`
- Formato: Excel (.xlsx)
- Atualização: mensal
- Histórico: 2010+
- Licença: `zona_cinza` — autorização solicitada (fev/2026)
