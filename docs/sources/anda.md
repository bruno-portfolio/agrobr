# ANDA — Fertilizantes

> **Licença:** Sem termos de uso públicos localizados. Autorização formal
> solicitada em fev/2026 — aguardando resposta.
> Classificação: `zona_cinza`

!!! note "Autorização pendente"
    Autorização formal para redistribuição de dados foi solicitada à ANDA
    em fevereiro/2026. Aguardando resposta. Verifique diretamente com a
    ANDA antes de uso comercial.

Associação Nacional para Difusão de Adubos. Dados de entregas de
fertilizantes por UF e mês.

## Instalação

ANDA requer `pdfplumber` como dependência opcional:

```bash
pip install agrobr[pdf]
```

## API

```python
from agrobr import anda

# Entregas de fertilizantes por UF/mês
df = await anda.entregas(ano=2024)

# Filtrar por UF
df = await anda.entregas(ano=2024, uf="MT")

# Agregação mensal (soma todas as UFs)
df = await anda.entregas(ano=2024, agregacao="mensal")
```

## Colunas — `entregas`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano |
| `mes` | int | Mês (1-12) |
| `uf` | str | UF |
| `produto_fertilizante` | str | Tipo de fertilizante |
| `volume_ton` | float | Volume entregue (toneladas) |

## Nota de Risco

ANDA publica dados em PDF. O layout pode mudar sem aviso entre anos.
O parser do agrobr detecta automaticamente a orientacao das tabelas
(UFs nas linhas vs colunas), e tambem suporta o layout "Principais
Indicadores" (dados nacionais agregados com meses/valores em celulas
concatenadas com `\n`). Mudancas drasticas de formato podem exigir
atualizacao do parser.

No agrobr-insights, dados ANDA sao tratados com peso dinamico: quando
parecem distorcidos, o peso no SCI e reduzido automaticamente.

## MetaInfo

```python
df, meta = await anda.entregas(ano=2024, return_meta=True)
print(meta.source)  # "anda"
print(meta.source_method)  # "httpx+pdfplumber"
```

## Fonte

- URL: `https://anda.org.br/recursos/`
- Formato: PDF/Excel
- Atualizacao: mensal
- Histórico: 2010+
- Licença: `zona_cinza` — autorização solicitada (fev/2026)
