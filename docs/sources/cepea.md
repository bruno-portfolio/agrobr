# CEPEA - Centro de Estudos Avancados em Economia Aplicada

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Instituicao** | ESALQ/USP |
| **Website** | [cepea.esalq.usp.br](https://www.cepea.esalq.usp.br) |
| **Acesso agrobr** | Via Noticias Agricolas (mirror autorizado) |

## Origem dos Dados

### Fonte Primaria

- **URL oficial**: `https://www.cepea.esalq.usp.br/br/indicador/soja.aspx`
- **Status**: Bloqueado por Cloudflare (acesso programatico negado)

### Fonte Alternativa (atual)

- **URL**: `https://www.noticiasagricolas.com.br/cotacoes/{produto}/`
- **Tipo**: Mirror autorizado dos indicadores CEPEA
- **Status**: Funcional

## Produtos Disponiveis

| Produto | Praca Principal | Unidade | Frequencia |
|---------|-----------------|---------|------------|
| Soja | Paranagua/PR | BRL/sc 60kg | Diaria |
| Milho | Campinas/SP | BRL/sc 60kg | Diaria |
| Boi Gordo | Sao Paulo/SP | BRL/@ | Diaria |
| Cafe Arabica | Sao Paulo/SP | BRL/sc 60kg | Diaria |
| Trigo | Parana | BRL/ton | Diaria |
| Algodao | Sao Paulo/SP | BRL/@ | Diaria |

## Metodologia CEPEA

O CEPEA calcula indicadores baseado em:

- Pesquisa diaria com agentes de mercado
- Media ponderada por volume negociado
- Ajuste para qualidade padrao

Fonte: [Metodologia CEPEA](https://www.cepea.esalq.usp.br/br/metodologia.aspx)

## Atualizacao e Defasagem

| Aspecto | Valor |
|---------|-------|
| **Horario de atualizacao** | ~17:00 - 18:00 (dias uteis) |
| **Defasagem tipica** | D+0 (mesmo dia) |
| **Dias sem publicacao** | Fins de semana, feriados nacionais |
| **Cache agrobr** | Expira as 18:00 (Smart TTL) |

## Uso

### Basico

```python
import asyncio
from agrobr import cepea

async def main():
    # Ultimo ano de dados
    df = await cepea.indicador('soja')

    # Periodo especifico
    df = await cepea.indicador('milho', inicio='2024-01-01', fim='2024-12-31')

    # Ultimo valor disponivel
    ultimo = await cepea.ultimo('boi')
    print(f"Boi gordo: R$ {ultimo.valor}")

asyncio.run(main())
```

### Com Metadados

```python
df, meta = await cepea.indicador('soja', return_meta=True)

print(meta.source)      # "noticias_agricolas"
print(meta.source_url)  # "https://www.noticiasagricolas.com.br/..."
print(meta.fetched_at)  # datetime(2026, 2, 4, 8, 24, 36)
print(meta.from_cache)  # True/False
```

## Schema dos Dados

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `data` | date | Nao | Data do indicador |
| `produto` | str | Nao | Nome do produto |
| `praca` | str | Sim | Praca de referencia |
| `valor` | Decimal | Nao | Preco em BRL |
| `unidade` | str | Nao | Unidade (BRL/sc60kg, etc) |
| `fonte` | str | Nao | Fonte dos dados |
| `metodologia` | str | Sim | Descricao da metodologia |

## Cache

O CEPEA usa Smart TTL - o cache expira automaticamente as 18:00:

```
08:00 - Busca soja -> Cache valido ate 18:00
10:00 - Busca soja -> Usa cache
17:59 - Busca soja -> Usa cache
18:01 - Busca soja -> Cache expirou -> Busca fonte -> Valido ate 18:00 amanha
```

## Funcoes Auxiliares

```python
# Lista produtos disponiveis
produtos = await cepea.produtos()
# ['soja', 'milho', 'boi', 'cafe', 'trigo', 'algodao']

# Lista pracas para um produto
pracas = await cepea.pracas('soja')
# ['paranagua', 'parana', 'rio_grande_do_sul']
```
