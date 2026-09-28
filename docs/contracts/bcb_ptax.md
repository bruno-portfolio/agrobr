# BCB PTAX — cotações 2.0 e moedas 1.0

Contratos das APIs de fonte `bcb.ptax()` e `bcb.ptax_moedas()`, registrados como `bcb_ptax` e `bcb_ptax_moedas`. Constantes `BCB_PTAX_V2` e `BCB_PTAX_MOEDAS_V1` em `agrobr.contracts.bcb_ptax`. Schemas exportados em `agrobr/schemas/bcb_ptax.json` e `agrobr/schemas/bcb_ptax_moedas.json`. São reutilizados pelos datasets `cotacoes_cambio` e `moedas_cambio`.

## Cotações

| Coluna | Tipo pandas | Nulo | Significado |
|--------|-------------|------|-------------|
| `cotacao_compra` | float64 | Sim | Valor publicado de compra, sem conversão |
| `cotacao_venda` | float64 | Sim | Valor publicado de venda, sem conversão |
| `data_hora` | datetime64[ns], sem fuso | Não | Relógio publicado, preservando até nove dígitos fracionários |
| `data` | datetime64[ns], sem fuso | Não | Dia civil de data_hora, sem horário |
| `moeda` | texto | Não | Símbolo ASCII maiúsculo, validado no catálogo adquirido |
| `paridade_compra` | float64 | Sim | Paridade publicada de compra |
| `paridade_venda` | float64 | Sim | Paridade publicada de venda |
| `tipo_boletim` | texto | Sim | Rótulo original publicado, sem normalização de saída |

A ordem das oito colunas conserva as quatro antigas como prefixo. Vazio mantém todas e seus tipos. A chave é `moeda, data_hora, tipo_boletim` dentro da aquisição/rota escolhida. Nulo é dimensão ausente. Não usar dia ou segundo truncado como chave: intermediários e fechamento podem ter frações diferentes no mesmo segundo. Dados são adquiridos por timestamp crescente; a data civil precisa coincidir com sua normalização.

As rotas por dia e período publicam rótulos de fechamento diferentes para o mesmo evento: `Fechamento PTAX` e `Fechamento`. O seletor reconhece ambos, mas o contrato mantém o texto bruto; leve essa variação em conta ao juntar resultados de rotas distintas. Rótulo desconhecido, vazio/whitespace ou null é permitido em todos com aviso; vazio é distinto de null; filtro específico sem classificação segura gera erro.

Quatro medidas exigem números JSON finitos ou null explícito; campo obrigatório ausente, texto numérico, bool, infinito, JSON ambíguo e underflow geram erro. Não há imputação, inversão de paridades ou cálculo de fechamento. Valores finitos não positivos ou compra maior que venda permanecem publicados com diagnóstico. Horários inválidos, com fuso ou fora do domínio ns são rejeitados, sem truncar precisão. O contrato não certifica a qualidade financeira dos valores.

A unidade de cotação é moeda doméstica da referência por unidade da moeda selecionada, sem afirmar BRL em todo histórico. Paridade depende do tipo A/B do catálogo; o contexto e unidades estão nos metadados. A coleta usa UTC; o relógio publicado não recebe um fuso inferido.

## Catálogo de moedas

| Coluna | Tipo pandas | Nulo | Significado |
|--------|-------------|------|-------------|
| `moeda` | texto | Não | Símbolo ASCII maiúsculo de três letras |
| `nome` | texto | Não | Nome publicado, sem substituir a grafia |
| `tipo_moeda` | texto | Não | Tipo publicado; A/B conhecido, novidade preservada com diagnóstico |

Chave moeda, única e ordenada. Todos os textos são não vazios. O vazio mantém três colunas. Os registros descrevem o catálogo OData corrente e não comprovam vigência histórica ou o universo da tabela geral de moedas do portal. A ausência de um símbolo é diferente da indisponibilidade do catálogo.

## Aquisição e proveniência

Cada aquisição de cotações lê o catálogo com paginação independente, e valida todos os boletins antes de selecionar. Duplicatas no corpo/entre páginas, alteração de seleção e falha de página geram erro sem retorno parcial. Sem count, página curta avança pelo recebido até vazio. `coverage` das cotações distingue recebido, retornado e excluído pelo filtro; `catalog.coverage` descreve o catálogo. Filtro intencional não é truncamento. Complete exige total reconciliado; sem total, unknown. Não há snapshot de revisão.

MetaInfo usa schema/contrato 2.0 para cotações, 1.0 para catálogo e parser 2 em ambos. Fontes são `bcb_ptax` e `bcb_ptax_moedas`. Recursos achatados identificam o papel catalog/quotes, índice por papel base0, URL/parâmetros, contagens, coleta UTC, hashes/bytes e layout. Hash/tamanho superiores representam manifesto canônico UTF-8 de query e recursos; soma dos corpos é separada. Catálogo selecionado, unidades, tipos, nulos e avisos acompanham o resultado.

```python
from agrobr import bcb, contracts

df, meta = await bcb.ptax(moeda="EUR", boletim="todos", data="04/09/2026", return_meta=True)
contracts.validate_dataset(df, "bcb_ptax")
moedas = await bcb.ptax_moedas()
contracts.validate_dataset(moedas, "bcb_ptax_moedas")
```

Veja [API](../api/bcb.md#ptax), [fonte](../sources/bcb.md#ptax-cotacoes-moedas-e-boletins) e [migração](../guides/migracao-2.md).
