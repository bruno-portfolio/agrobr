# zoneamento_agricola v2.1

Zoneamento Agrícola de Risco Climático, com as ocorrências publicadas por município, cultura, solo e ciclo (ZARC/MAPA).

## Fonte

| Prioridade | Fonte | Método |
|---|---|---|
| 1 | ZARC / MAPA | CSV (`zarc.zoneamento`) |

Consulte a [documentação da fonte](../sources/zarc.md).

## Schema

As **59 colunas** abaixo seguem a ordem do contrato. A linha de decêndios representa 36 colunas individuais.

| Coluna | Tipo | Nullable | Descrição |
|---|---|---|---|
| `cultura` | STRING | Não | Nome canônico da cultura |
| `safra` | STRING | Não | Safra anual AAAA/AAAA ou modalidade perene, olericola ou sem_safra |
| `geocodigo` | STRING | Não | Código IBGE municipal com 7 dígitos ASCII |
| `uf` | STRING | Não | Sigla oficial da UF em maiúsculas |
| `municipio` | STRING | Não | Nome publicado do município; vazio preservado |
| `solo_codigo` | INTEGER | Não | Código de solo publicado |
| `ciclo_codigo` | INTEGER | Não | Código de ciclo publicado |
| `clima` | STRING | Não | Restrição climática publicada; vazio preservado |
| `manejo` | STRING | Não | Manejo publicado; vazio preservado |
| `portaria` | STRING | Não | Identificador da portaria publicada |
| `dec1`..`dec36` | INTEGER | Sim | Risco publicado: 0/20/30/40/50; vazio é nulo |
| `cultura_original` | STRING | Não | Nome da cultura conforme publicado na tábua |
| `safra_inicio` | STRING | Não | Texto original de SafraIni; vazio preservado |
| `safra_fim` | STRING | Não | Texto original de SafraFin, inclusive modalidades não anuais |
| `cultura_codigo` | STRING | Não | Código textual da cultura; zeros à esquerda preservados |
| `clima_codigo` | STRING | Não | Código textual de clima; zeros à esquerda e vazio preservados |
| `manejo_codigo` | STRING | Não | Código textual de manejo; zeros à esquerda e vazio preservados |
| `produtividade_texto` | STRING | Não | Texto publicado; decimal com vírgula preservado; unidade não inferida |
| `nm_codigo` | STRING | Não | Código textual NM; zeros à esquerda e vazio preservados |
| `municipio_sicor_codigo` | STRING | Não | Código textual de município SICOR; zeros à esquerda e vazio preservados |
| `mesorregiao_codigo` | STRING | Não | Código textual de mesorregião; zeros à esquerda e vazio preservados |
| `microrregiao_codigo` | STRING | Não | Código textual de microrregião; zeros à esquerda e vazio preservados |
| `registro_origem` | INTEGER | Não | Posição CSV antes de filtros; somente identificável junto ao hash do corpo |
| `cod_municipio` | INTEGER | Sim | O `geocodigo` em inteiro, a chave comum dos datasets municipais |

Todos os campos INTEGER usam o dtype pandas `Int64`; apenas os riscos admitem nulos. Campos textuais vazios permanecem strings vazias, sem conversão para nulo.

## Semântica e limites

Não há chave primária afirmada; `registro_origem` é a posição no CSV, válida só junto ao hash do corpo. A posição é positiva, única e crescente no corpo processado antes dos filtros; o SHA-256 desse corpo está em `meta.raw_content_hash`. Ela não identifica a mesma observação entre revisões.

Os 36 decêndios cobrem o ano, com três por mês. Os valores publicados de risco são **0, 20, 30, 40 e 50**, em `Int64`; célula vazia é nula e não é zero. O valor 50 ocorre na tábua perene. Os valores 0 e 50 são preservados sem interpretação agronômica presumida.

A chave `cultura` de 11 rótulos de cultura muda na safra 2024/2025 (tabela na [página da fonte](../sources/zarc.md)). Para séries entre safras, junte por `cultura_codigo` e `manejo`.

`safra_inicio` e `safra_fim` mantêm o texto original. `safra` representa o par anual ou a modalidade `perene`, `olericola` ou `sem_safra`; o seletor `safra="perene"` consulta a tábua que reúne as três modalidades. `produtividade_texto` preserva o texto publicado, inclusive decimal com vírgula e célula vazia, sem inferir unidade.

Garantias do contrato, reproduzidas literalmente:

- Cada ocorrência publicada é preservada, inclusive duplicatas literais e riscos distintos
- Não há chave semântica única afirmada para as edições processadas
- Registro de origem é posicional e restrito ao hash do CSV, sem estabilidade entre revisões
- Riscos usam Int64; célula vazia é nula e não se confunde com o valor zero
- Valores 0 e 50 publicados são preservados sem interpretação agronômica presumida
- Códigos textuais mantêm zeros à esquerda e ausências publicadas como strings vazias
- Produtividade permanece texto publicado, inclusive decimal com vírgula e unidade não inferida
- Safras anuais e modalidades não anuais mantêm os dois campos originais
- Validação até EOF comprova processamento do corpo, sem afirmar total externo ou snapshot

## Exemplo

`as_polars=True` requer `pip install agrobr[polars]`.

```python
from agrobr import datasets

df, meta = await datasets.zoneamento_agricola(
    cultura="soja", municipio=5107925, safra="2025/2026", return_meta=True
)
print(df.head())
print(meta.raw_content_hash)
```

## Schema JSON

`agrobr/schemas/zoneamento_agricola.json`, também disponível via `get_contract("zoneamento_agricola")`.

## Reconciliação e culturas legadas

O catálogo de filtros inclui `Arroz Sequeiro`/`arroz_sequeiro` e `Trigo Sequeiro`/`trigo_sequeiro`, publicados na tábua de 2016/2017. Esses aliases conservam os valores já retornados pelo parser e não são convertidos para `arroz`/`trigo`. Nomes desconhecidos continuam sendo recusados antes da rede; a presença de cada cultura depende da tábua consultada.

As 59 colunas do contrato 2.1 preservam os 55 campos publicados, além de cultura normalizada, safra derivada, posição no CSV e `cod_municipio` (o `geocodigo` em inteiro). Registros repetidos são mantidos. A posição `registro_origem` é válida somente junto a `meta.raw_content_hash`: os três corpos capturados em 18/09/2026 tinham SHA diferente dos de 07/09, mas a comparação integral como multiconjunto confirmou os mesmos registros em outra ordem. Uma alteração de SHA não demonstra mudança dos valores.

Aquisição UTC e hash do corpo permanecem em `meta.fetched_at`, `meta.raw_content_hash` e `meta.source_details["resource"]`, inclusive no cache. A leitura até EOF comprova que o corpo recebido foi processado, sem certificar total externo de municípios ou snapshot transacional. O catálogo CKAN consultado pela API declara frequência semanal, enquanto o dicionário PDF declara diária. O dataset usa `update_frequency="weekly"`, tomando o catálogo ativo de descoberta como referência operacional; a declaração conflitante do PDF permanece registrada. Nenhuma das declarações comprova a cadência efetiva de revisão de cada safra. Produtividade e códigos NM são preservados literalmente, sem inferir unidade ausente no dicionário.
