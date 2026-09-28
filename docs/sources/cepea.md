# CEPEA - Centro de Estudos Avançados em Economia Aplicada

> **Licença:** Dados CEPEA/ESALQ licenciados sob
> [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/deed.pt-br).
> Uso comercial requer autorização do CEPEA (cepea@usp.br).
> Ref: [Licença de uso de dados](https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx)

## Visão Geral

| Campo | Valor |
|-------|-------|
| **Instituição** | ESALQ/USP |
| **Website** | [cepea.org.br](https://www.cepea.org.br) |
| **Licença** | CC BY-NC 4.0 |
| **Acesso agrobr** | CEPEA direto, com fallback Notícias Agrícolas quando disponível |

## Origem dos Dados

### Fonte Primaria

- **URL oficial**: `https://www.cepea.org.br/br/indicador/soja.aspx`
- **Acesso**: Tentativa direta por HTTP; bloqueios e indisponibilidade podem acionar o fallback

### Fonte Alternativa

- **URL**: `https://www.noticiasagricolas.com.br/cotacoes/{produto}/`
- **Tipo**: Mirror autorizado dos indicadores CEPEA
- **Uso**: Quando habilitado e disponível para o produto consultado

## Produtos Disponíveis (22 produtos)

O bezerro traz também `valor_usd` (coluna Valor US$) e `peso_medio_kg` (tabela Peso Médio da página); os demais produtos preenchem `valor_usd` quando o CEPEA publica o dólar.

| Produto | Praca Principal | Unidade | Frequencia |
|---------|-----------------|---------|------------|
| Soja | Paranagua/PR | BRL/sc 60kg | Diaria |
| Soja Parana | Parana | BRL/sc 60kg | Diaria |
| Milho | Campinas/SP | BRL/sc 60kg | Diaria |
| Bezerro | Mato Grosso do Sul | BRL/cabeca | Diária |
| Boi Gordo | Sao Paulo/SP | BRL/@ | Diaria |
| Cafe Arabica | Sao Paulo/SP | BRL/sc 60kg | Diaria |
| Cafe Robusta | Espirito Santo | BRL/sc 60kg | Diaria |
| Trigo | Parana + RS | BRL/ton | Diaria |
| Algodao | Sao Paulo/SP | cBRL/lb | Diaria |
| Arroz em casca | ESALQ/BBM | BRL/sc 50kg | Diaria |
| Acucar cristal | Sao Paulo/SP | BRL/sc 50kg | Diaria |
| Açúcar refinado | São Paulo/SP | BRL/kg | Diária |
| Etanol hidratado | Sao Paulo/SP | BRL/L | Semanal |
| Etanol anidro | Sao Paulo/SP | BRL/L | Semanal |
| Frango congelado | Sao Paulo/SP | BRL/kg | Diaria |
| Frango resfriado | Sao Paulo/SP | BRL/kg | Diaria |
| Suíno vivo | MG, PR, RS, SC e SP (condição da praça preservada) | BRL/kg | Diária |
| Leite | UF e BRASIL, ao produtor | BRL/L | Mensal |
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
| **Cache agrobr** | Vale até a próxima virada das 18:00 BRT em dia útil, contada da última coleta do produto (Smart TTL) |

## Uso

### Basico

```python
import asyncio
from agrobr import cepea

async def main():
    # Janela recente disponível na fonte e no cache
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

print(meta.source)
print(meta.source_url)
print(meta.fetched_at)
print(meta.from_cache)  # True/False
```

## Schema dos Dados

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `data` | date | Nao | Data do indicador |
| `produto` | str | Nao | Nome do produto |
| `praca` | str | Sim | Praca de referencia |
| `valor` | float | Nao | Preço na unidade da linha; algodão em centavos de real por libra |
| `unidade` | str | Nao | Unidade (BRL/sc60kg, etc) |
| `fonte` | str | Nao | Fonte dos dados |
| `metodologia` | str | Sim | Descricao da metodologia |

## Cache

O CEPEA usa Smart TTL - o cache expira automaticamente as 18:00:

```
08:00 - Busca soja -> Cache valido ate 18:00
10:00 - Busca soja -> Usa cache
17:59 - Busca soja -> Usa cache
18:01 - Busca soja -> Cache expirou -> Busca fonte -> Válido até 18:00 do próximo dia útil
```

Coleta depois das 18:00, no sábado ou no domingo vale até as 18:00 do próximo dia útil (segunda a sexta; feriados não entram no cálculo). Um período fechado (`fim` anterior aos últimos 25 dias corridos) não volta à fonte: sai do cache com `cache_expires_at` nulo.

## Funcoes Auxiliares

```python
# Lista produtos disponiveis
produtos = await cepea.produtos()
# ['soja', 'soja_parana', 'milho', 'bezerro', 'boi', 'boi_gordo',
#  'cafe', 'cafe_arabica', 'cafe_robusta', 'algodao',
#  'trigo', 'arroz', 'acucar', 'acucar_refinado', 'etanol_hidratado',
#  'etanol_anidro', 'frango_congelado', 'frango_resfriado', 'suino', 'leite',
#  'laranja_industria', 'laranja_in_natura']

# Lista pracas para um produto
pracas = await cepea.pracas('soja')
# ['paranagua'] - corresponde a "Paranaguá/PR" na coluna praca
```

## Cobertura e seleção das séries

A página do indicador publica uma janela recente, normalmente cerca de 15 pregões. O período anterior a ela vem da série histórica do CEPEA, a planilha que o CEPEA publica por indicador (desde 1996 a 2010, conforme o produto): na primeira consulta que precisa dela, o agrobr baixa a série inteira do produto e grava no DuckDB só os dias que o cache ainda não tem, e as consultas seguintes saem do cache. A série é baixada de novo quando o período pedido passa do último dia que ela publicou (o cache registra essa data no download), e nunca antes da virada das 18h do CEPEA seguinte ao download anterior. Laranja não tem série: antes da janela, o resultado traz só o que o cache acumulou, com aviso em `validation_warnings` e `UserWarning`. Com `fim` dentro dos últimos 25 dias corridos (as cerca de 15 datas publicadas), a página é consultada quando a última coleta do produto venceu ou, sem coleta registrada, quando falta no cache algum dia útil desse trecho do período pedido; dentro da validade, a resposta sai do cache, mesmo com um feriado ou com o dia de hoje ainda sem publicação. Com `fim` anterior a essa janela, a página não é consultada. `force_refresh=True` sempre consulta a fonte, e `offline=True` nunca consulta.

Na série, o dia com valor 0 (sem cotação) fica fora, como na página. O leite sai com 2 casas na série e com 4 na página: quando os 2 existem, vale a página, e `source_details["pagina_desde"]` traz o primeiro mês que veio dela. As linhas da série gravam `parser_version` 101, e o `MetaInfo` da consulta que baixou a série traz cada planilha em `source_details["resources"]` (URL, SHA-256, bytes e horário; a do peso do bezerro com `papel` `serie_peso`). Série indisponível avisa em `validation_warnings` e `UserWarning`; se o período ficar sem dado, `indicador` levanta `SourceUnavailableError` (ou `ParseError`, se a planilha mudou de layout).

Sem rede e sem nada no cache para o período, `indicador` e `ultimo` levantam `SourceUnavailableError`, com `attempted_sources` (as fontes tentadas e o cache); com cache, a resposta sai dele com `StaleDataWarning`. Com `offline=True` e o cache vazio, `indicador` devolve a tabela vazia, com `from_cache=False` e `cache_expires_at` nulo, e `ultimo` levanta `SourceUnavailableError` ("offline sem dado no cache").

Páginas com vários indicadores são selecionadas pelo título do produto, nunca pela posição da tabela. O suíno preserva as cinco praças da coluna Estado. A migração do cache remove as antigas linhas de suíno rotuladas incorretamente; dados corrigidos são coletados nas chamadas seguintes.

Para leite, `data` representa o primeiro dia do mês de referência, `praca` preserva a UF (ou BRASIL) e a unidade é `BRL/L`. A tabela de leite spot não integra essa série. `ultimo("leite")` usa uma janela mensal e pode retornar um mês anterior à publicação.

Leite não usa o fallback Notícias Agrícolas na API CEPEA. A página NA informa
fechamento e referência separadamente, mas seu parser autônomo expõe o
fechamento. A migração 9 preserva essas linhas legadas em quarentena, sem
presumir uma defasagem fixa para convertê-las em mês de referência.

O açúcar refinado usa a [página própria do indicador](https://cepea.org.br/br/indicador/acucar-refinado-amorfo-sp.aspx), em `BRL/kg`, e não a tabela do cristal. HTTP 200 sem tabela reconhecida também aciona o fallback Notícias Agrícolas, quando habilitado e disponível para o produto, com o aviso de licença habitual. A tabela sem a coluna de valor em reais reconhecida pelo cabeçalho ("Valor R$", "R$/litro", "Preço médio" no leite, "A Prazo" na laranja) também: o parser levanta `ParseError` e não usa outro número da linha, e uma coluna em US$ nunca vira preço em BRL.

## Validação de preços

`validate_sanity=True` confere unidade e faixa para os 22 identificadores,
incluindo os aliases `cafe_arabica` e `boi_gordo`. Unidade incompatível é marcada
antes de comparar valores. Leite mensal e etanol semanal não recebem limites
de variação diária. Consulte as [faixas e seus limites de interpretação](../advanced/resilience.md#validacao-estatistica).

A coluna `anomalies` do DataFrame contém a lista serializada como texto JSON,
ou `None` quando vazia, conforme o contrato de `preco_diario`.

## Histórico legado no cache

A migração preserva automaticamente os originais afetados em
`indicadores_quarentena`, no mesmo DuckDB, fora das consultas normais. Isso
inclui suíno, séries CEPEA com seleção antiga e refinado legado do Notícias
Agrícolas rotulado como saca em vez de kg. A atualização é transacional; falhas
levantam `CacheMigrationError`. Registros já excluídos por versões anteriores
não são recriados. Veja [preservação e recuperação](../guides/migracao-2.md#18-preservacao-automatica-do-cache-existente).
A migração 10 acrescenta `valor_usd` e `peso_medio_kg` a `indicadores` antes das demais
migrações, em passo idempotente fora da transação principal; registros anteriores ficam
nulos nessas colunas até nova coleta.
A migração 11 acrescenta `anomalies` a `indicadores` e à quarentena, no mesmo passo, e
grava a marca de média semanal no cache. As linhas gravadas antes dela recebem
`["media_semanal"]` quando são do etanol hidratado ou anidro do Notícias Agrícolas, cujas
duas páginas só publicam médias semanais, e lista vazia nas demais. Sem ela, a leitura do
cache (`offline` ou dentro do prazo) devolvia a média semanal como cotação do dia.

Os parsers corrigidos identificam novas observações com versões CEPEA 2 e Notícias
Agrícolas 3. `MetaInfo.data_sources` preserva a origem das linhas, inclusive quando
`selected_source="cache"`. O aviso de fallback não bloqueia por si só a leitura
offline de outra fonte; aplicações com restrições por provedor devem avaliar esses
metadados ou a coluna `fonte`.

Para a mesma data, produto e praça, CEPEA tem precedência sobre Notícias
Agrícolas nas consultas normais e offline; ambas as observações continuam
armazenadas. A migração 9 também corrige os rótulos legados de trigo
(`BRL/sc60kg` → `BRL/ton`) e algodão (`BRL/@` → `cBRL/lb`) do parser CEPEA
anterior ao 2, sem modificar valores e preservando os originais em quarentena.
