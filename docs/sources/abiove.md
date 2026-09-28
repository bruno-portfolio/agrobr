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
| `produto` | str | Produto (grao, farelo, oleo, milho); total na agregação mensal |
| `volume_ton` | float | Volume exportado (toneladas) |
| `receita_usd_mil` | float | Receita FOB (mil USD) |

São lidas as tabelas mensais de cada produto, com o ano selecionado no cabeçalho.
Peso publicado em mil toneladas é convertido para toneladas; preço médio por
tonelada não é receita FOB. Quadros comparativos entre Brasil e complexo soja
não entram como produtos. O fallback `datasets.exportacao` converte volume para
kg e receita para USD, conforme o contrato do dataset.

Cada edição mensal (`exp_AAAAMM.xlsx`) traz o ano da edição e o anterior, e a ABIOVE revê meses já
publicados. O agrobr entrega o número mais recente: lê a edição mais nova que publica o ano pedido e
registra qual em `MetaInfo.source_details["edicao"]`. `edicao="AAAA-MM"` lê uma edição específica (a
original de um mês, por exemplo). Veja a [API](../api/abiove.md).

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
print(meta.source_details["edicao"])  # {"arquivo": "exp_202608.xlsx", "mes": "2026-08"}
```

## Nota de Risco

ABIOVE publica dados em planilhas Excel. O layout pode variar entre anos.
O parser do agrobr lê o layout publicado (seções por produto em linhas, com os
meses na coluna de rótulos) e recusa com `ParseError` o que não reconhece, sem
adivinhar produto nem coluna.

## Fonte

- URL: `https://abiove.org.br/estatisticas/`
- Formato: Excel (.xlsx)
- Atualização: mensal
- Histórico: 2010+
- Licença: `zona_cinza` — autorização solicitada (fev/2026)
