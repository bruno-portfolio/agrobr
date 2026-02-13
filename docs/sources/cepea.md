# CEPEA - Centro de Estudos Avançados em Economia Aplicada

> **Licença:** Dados CEPEA/ESALQ licenciados sob
> [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/deed.pt-br).
> Uso comercial requer autorização do CEPEA (cepea@usp.br).
> Ref: [Licença de uso de dados](https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx)

## Visão Geral

| Campo | Valor |
|-------|-------|
| **Instituição** | ESALQ/USP |
| **Website** | [cepea.esalq.usp.br](https://www.cepea.esalq.usp.br) |
| **Licença** | CC BY-NC 4.0 |
| **Acesso agrobr** | Via Notícias Agrícolas (mirror autorizado) |

## Origem dos Dados

### Fonte Primaria

- **URL oficial**: `https://www.cepea.esalq.usp.br/br/indicador/soja.aspx`
- **Status**: Bloqueado por Cloudflare (acesso programatico negado)

### Fonte Alternativa (atual)

- **URL**: `https://www.noticiasagricolas.com.br/cotacoes/{produto}/`
- **Tipo**: Mirror autorizado dos indicadores CEPEA
- **Status**: Funcional

## Produtos Disponiveis (20 indicadores)

| Produto | Praca Principal | Unidade | Frequencia |
|---------|-----------------|---------|------------|
| Soja | Paranagua/PR | BRL/sc 60kg | Diaria |
| Soja Parana | Parana | BRL/sc 60kg | Diaria |
| Milho | Campinas/SP | BRL/sc 60kg | Diaria |
| Boi Gordo | Sao Paulo/SP | BRL/@ | Diaria |
| Cafe Arabica | Sao Paulo/SP | BRL/sc 60kg | Diaria |
| Trigo | Parana + RS | BRL/ton | Diaria |
| Algodao | Sao Paulo/SP | cBRL/lb | Diaria |
| Arroz em casca | ESALQ/BBM | BRL/sc 50kg | Diaria |
| Acucar cristal | Sao Paulo/SP | BRL/sc 50kg | Diaria |
| Acucar refinado | Sao Paulo/SP | BRL/sc 50kg | Diaria |
| Etanol hidratado | Sao Paulo/SP | BRL/L | Semanal |
| Etanol anidro | Sao Paulo/SP | BRL/L | Semanal |
| Frango congelado | Sao Paulo/SP | BRL/kg | Diaria |
| Frango resfriado | Sao Paulo/SP | BRL/kg | Diaria |
| Suino vivo | Sao Paulo/SP | BRL/kg | Diaria |
| Leite | Ao produtor | BRL/L | Mensal |
| Laranja industria | Sao Paulo/SP | BRL/cx 40,8kg | Diaria |
| Laranja in natura | Sao Paulo/SP | BRL/cx 40,8kg | Diaria |

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
# ['soja', 'soja_parana', 'milho', 'boi', 'cafe', 'algodao', 'trigo',
#  'arroz', 'acucar', 'acucar_refinado', 'etanol_hidratado', 'etanol_anidro',
#  'frango_congelado', 'frango_resfriado', 'suino', 'leite',
#  'laranja_industria', 'laranja_in_natura']

# Lista pracas para um produto
pracas = await cepea.pracas('soja')
# ['paranagua', 'parana', 'rio_grande_do_sul']
```
