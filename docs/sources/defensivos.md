# Agrofit/MAPA — Defensivos agrícolas

O [Portal de Dados Abertos do MAPA](https://dados.agricultura.gov.br/dataset/sistema-de-agrotoxicos-fitossanitarios-agrofit) publica dois CSVs de cadastro federal: produtos formulados e técnicos. O portal informa atualização diária, UTF-8 e licença Creative Commons Attribution, sem indicar versão no link de licença. A classificação no agrobr permanece `livre`.

## O que a fonte publica e o agrobr entrega

As quantidades abaixo são da exportação de 18/09/2026 e podem mudar.

| Informação | Exportação capturada | Entrega do agrobr |
|---|---|---|
| Produtos formulados | 4.403 registros, 15 colunas de origem | `formulados()`, uma linha por registro, schema 1.1 |
| Relações de uso | 279.707 linhas no CSV de formulados | `autorizacoes()`, preservando multiplicidade, schema 1.1 |
| Produtos técnicos | 2.992 registros, 8 colunas de origem | `tecnicos()`, schema 1.1 |
| Composição | Ingrediente, grupo e concentração embutidos em texto nas duas famílias | `composicao()`: 5.678 componentes formulados e 2.993 técnicos, schema 1.0 |
| Situação | `SITUACAO=TRUE` em todas as linhas de formulados; campo ausente nos técnicos | Texto original em formulados e autorizações, com filtro textual |
| Empresas / países / tipos | Campo composto presente nos dois CSVs | Não estruturado; coluna ignorada identificada nos metadados |
| Campos previstos na API, ausentes no layout atual | Sem modalidade de emprego nos formulados nem nome científico separado nos técnicos | `modalidade_de_emprego` e `nome_cientifico` permanecem nulos, respectivamente |

A exportação formulada tinha 392.110.268 bytes e a técnica 712.935 bytes. O cadastro capturado inclui misturas de até seis componentes, grupos com parênteses internos, unidades biológicas e registros com o mesmo ingrediente em posições diferentes. A composição conserva essas posições e o texto publicado.

O parser interpreta números e notação científica explícita quando possível. Expressões ambíguas mantêm texto e valor nulo com diagnóstico. Não há conversão entre unidades nem correção implícita de unidades possivelmente inconsistentes na origem.

## Uso

```python
from agrobr import defensivos

produtos = await defensivos.formulados(situacao="TRUE")
usos = await defensivos.autorizacoes(cultura="soja")
componentes, meta = await defensivos.composicao(
    tipo="tecnicos", nr_registro="00301", return_meta=True,
)
```

Veja filtros, colunas e contratos na [API Defensivos](../api/defensivos.md). As mesmas tabelas estão disponíveis nos [quatro datasets Agrofit](../api/defensivos_datasets.md), com fonte única e proveniência da coleta preservada.

## Integridade e proveniência

O client mantém timeout, retry e identificação HTTP do projeto. A leitura usa a cadeia de encoding compartilhada. Os campos antigos mantêm a limpeza textual anterior; `composicao_texto`, `componente_texto`, `concentracao_texto` e `situacao` preservam o conteúdo correspondente da exportação.

Pydantic valida os registros externos. Cabeçalhos duplicados, linhas com quantidade incorreta de campos, registros inválidos e atributos de produto conflitantes geram `ParseError`. Contratos verificam colunas, tipos e chaves. Os metadados registram assinatura SHA-256 do cabeçalho, contagens, colunas ignoradas e diagnósticos de interpretação.

O cache de 24h usa `formulados.v3.zip` e `tecnicos.v3.zip`. Cada arquivo reúne tabelas relacionadas, manifesto versionado, hashes, tipos e proveniência da mesma coleta. A substituição é atômica; arquivos expirados, corrompidos ou incompatíveis não são reutilizados. Arquivos legados permanecem no disco e exigem nova coleta para a API atual. `agrobr.defensivos.cache.invalidate()` apaga os dois snapshots e os arquivos legados. `use_cache=False` ignora leitura e gravação. Onde fica e como limpar: [O que o agrobr grava no disco](../advanced/disco.md).

## Limites

A captura só demonstrou o token `TRUE`; seu significado não foi equiparado a vigência. O CSV técnico não publica situação. O conjunto descreve cadastro e relações de uso, sem constituir recomendação agronômica.

O hash identifica o conteúdo coletado; o portal corrente não oferece, por essa interface, uma data de corte histórica reproduzível. Os datasets de formulados, técnicos, autorizações e composição recusam o contexto `deterministic` antes de cache/rede. Histórico de cancelamentos, bulas e interpretação do campo de empresas não fazem parte das tabelas.

## Exportação de 18/09/2026

Duas expressões ambíguas da exportação de 18/09/2026, `1.9 10*10 UFC/g` e `200 1x10E10 UFC/g`, mantêm o texto e saem com valor e unidade nulos e diagnóstico. A interpretação numérica não é garantida para toda a população de componentes.

O cache preserva valores não nulos, tipos, posição dos componentes e proveniência UTC; `None` e `pd.NA` só se equivalem em campos anuláveis. Colunas textuais de composição e situação preservam o literal; outros campos mantêm a limpeza já documentada. Autorizações não são deduplicadas.

Um sufixo publicado nos campos de concentração pode conter expressão ambígua e não certifica, por si, uma unidade ou interpretação numérica. Catálogo, CSV e cache têm a mesma origem: nenhum deles confirma a população histórica.
