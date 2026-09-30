# cadastro_rural v2.1

Registros de imoveis rurais do Cadastro Ambiental Rural (CAR) por UF.

## Fontes

| Prioridade | Fonte | Descricao |
|------------|-------|-----------|
| 1 | SICAR/GeoServer WFS | Servico Florestal Brasileiro / MMA |

## Schema

| Coluna | Tipo | Nullable | Descricao |
|--------|------|----------|-----------|
| `cod_imovel` | str | ❌ | Codigo unico do imovel no CAR |
| `status` | str | ❌ | Status do registro: AT, PE, SU, CA |
| `data_criacao` | datetime64[ns, UTC] | ✅ | Instante UTC de criação do registro |
| `data_atualizacao` | datetime64[ns, UTC] | ✅ | Instante UTC da última atualização, onde disponível |
| `area_ha` | float64 | ❌ | Area do imovel em hectares (>= 0) |
| `condicao` | str | ✅ | Condicao do imovel |
| `uf` | str | ❌ | Sigla da UF |
| `municipio` | str | ❌ | Nome do municipio |
| `cod_municipio_ibge` | int | ❌ | Codigo IBGE do municipio |
| `cod_municipio` | int | ✅ | Código IBGE do município (7 dígitos), a chave comum dos datasets municipais; nulo fora da linha de município; igual ao `cod_municipio_ibge` |
| `modulos_fiscais` | float64 | ❌ | Quantidade de modulos fiscais (>= 0) |
| `tipo` | str | ❌ | Tipo do imovel: IRU, AST, PCT |

## Primary Key

`[cod_imovel]`

## Filtros

| Parametro | Tipo | Descricao |
|-----------|------|-----------|
| `uf` | str | Sigla da UF (obrigatorio) |
| `municipio` | int \| str | Código IBGE de 7 dígitos ou nome inteiro do município da UF, sem diferenciar caixa e acento; ambíguo, inexistente ou pedaço de nome gera `InvalidParameterError` com os candidatos |
| `status` | str | AT (Ativo), PE (Pendente), SU (Suspenso), CA (Cancelado) |
| `tipo` | str | IRU, AST ou PCT, conforme a classificação SICAR |
| `area_min` | float | Area minima em hectares |
| `area_max` | float | Area maxima em hectares |
| `criado_apos` | str | `YYYY-MM-DD`; criação maior ou igual à data (`>=`) |
| `atualizado_apos` | str | Data ou datetime ISO com fração opcional e `Z`/offset; sem fuso, interpreta UTC. Atualização estritamente posterior (`>`) |
| `as_polars` | bool | Retorna Polars após validar o contrato; exige o extra `[polars]` |
| `return_meta` | bool | Retorna também `MetaInfo`, incluindo o URL com os filtros efetivos |

`return_meta`, `atualizado_apos` e `as_polars` são argumentos somente nomeados; os sete primeiros (`uf` a `criado_apos`) podem ser posicionais. UF aceita letras minúsculas e espaços externos. O município é conferido no cadastro de municípios do IBGE antes da rede.

Datas precisam existir no calendário. Áreas devem ser finitas, não negativas e ter mínimo menor ou igual ao máximo. Parâmetros desconhecidos geram erro, em vez de serem descartados. Em **PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP e TO** o campo de atualização não existe na camada WFS: `atualizado_apos` gera `InvalidParameterError` antes da rede. Nas outras 15 camadas, a coluna é solicitada e preserva os valores fornecidos pelo serviço; pode conter nulos.

## Semântica temporal

O WFS retorna o cadastro corrente. Os filtros de criação e atualização selecionam registros desse cadastro e não recuperam versões anteriores, exclusões ou o estado completo em uma data passada.

`cadastro_rural` rejeita um contexto `datasets.deterministic(...)` ativo com `InvalidParameterError` antes da rede, inclusive quando há datas explícitas. A implementação anterior transformava o snapshot em `criado_apos`, selecionando registros criados depois do corte. Essa transformação foi removida. Nas consultas normais, `meta.snapshot` é nulo.

O contrato passa a **2.0** por tornar UTC explícito nas duas datas; as onze colunas e a chave não mudam. Inclusive colunas totalmente nulas e resultados vazios usam dtype UTC. O transporte tabular usa GeoJSON projetado apenas nos atributos, sem geometria nem dependência de GeoPandas. Consulte o [guia de migração](../guides/migracao-2.md).

As capturas oficiais mostraram horários diferentes entre CSV e GeoJSON no mesmo registro, com deslocamentos de duas e três horas. O CQL comparou os limites pelo instante UTC do GeoJSON. Por isso a API tabular deixou o CSV: os timestamps UTC retornados agora podem alimentar `atualizado_apos` diretamente via `.isoformat()`. Datas naive de capturas CSV antigas não recebem um fuso presumido nem deslocamento fixo.

O filtro suporta precisão de milissegundos. Zeros além da terceira casa são removidos sem alterar o instante (`.212000` vira `.212`); frações submilissegundo são rejeitadas antes da rede, inclusive quando excedem a precisão de microssegundos do Python. O GeoServer compara `.212000` de maneira diferente de `.212`; por isso o agrobr envia a fração em três casas, sem arredondar valores mais precisos. Isso não afirma uma precisão máxima do armazenamento interno da fonte.

## Ocorrências do mesmo imóvel e proveniência

Na consulta tabular com uma única rota, o dataset identifica a fonte como `sicar` em `attempted_sources` e `selected_source`; a API `sicar.imoveis()` usa `sicar_wfs`. Ambos preservam os detalhes da aquisição em `source_details["sicar"]`.

A fonte pode publicar mais de uma ocorrência do mesmo `cod_imovel`, com ids de feature distintos.
O dataset e `sicar.imoveis()` entregam uma linha por código entre as ocorrências que satisfazem os
filtros da consulta. Depois de validar a varredura completa, escolhem a base de comparação uma vez
por grupo: `data_atualizacao` se todas as ocorrências tiverem esse campo preenchido; senão
`data_criacao` se todas tiverem criação; senão o id da feature. Fica a maior data na base escolhida.
Empate nessa data, ou ausência de uma base temporal completa, usa o maior sufixo numérico do id
(o id textual desempata sufixos iguais). O critério por id é uma seleção determinística e não
comprova qual ocorrência foi atualizada mais recentemente.

Em PE, PI, PR, RJ, RN, RO, RR, RS, SC, SE, SP e TO, a camada não publica `data_atualizacao`;
nessas UFs a seleção usa criação quando disponível em todo o grupo. Datas inválidas em qualquer
ocorrência continuam causando erro antes da seleção, inclusive nas que seriam descartadas.

Com `return_meta=True`, `MetaInfo.validation_warnings` informa quantos códigos tinham múltiplas
ocorrências e a regra aplicada. `MetaInfo.source_details["sicar"]` contém:

- `anunciados`: última contagem WFS observada; `features_unicas`: quantidade de ids únicos recebidos;
- `codigos_colapsados`: quantidade de códigos com mais de uma ocorrência;
- `versoes_descartadas_total`: número de ocorrências removidas, antes do limite da lista;
- `versoes_descartadas`: até 1.000 itens com `cod_imovel`, `feature_id`, `feature_id_mantida`,
  `data_atualizacao`, `data_criacao` e `criterio` (`data_atualizacao`, `data_criacao` ou `feature_id`);
- `versoes_descartadas_truncadas`: indica que existem mais descartes que itens na lista;
- `criterios`: contagem de descartes por critério, incluindo os itens além do limite.

O critério de cada descarte compara aquela ocorrência com a vencedora final: uma data diferente
registra a base temporal; um empate registra `feature_id`. Assim,
`len(df) == features_unicas - versoes_descartadas_total`. Sem ocorrências repetidas, os contadores
de colapso e descarte são zero, a lista é vazia e não há aviso de seleção.

A identidade da paginação é o id da feature, sem acrescentar coluna ao DataFrame. Repetir o mesmo
id ou terminar com quantidade de ids diferente da última contagem anunciada gera `ParseError`
com orientação para repetir a consulta. Mudanças de contagem geram avisos; o maior total observado
define as páginas solicitadas. A fonte declara `PagingIsTransactionSafe=FALSE`: a conferência de
contagem não garante uma fotografia consistente entre páginas. O contrato 2.0 e a chave
`[cod_imovel]` permanecem os mesmos.

## Garantias

- `cod_imovel` sempre nao-vazio
- `status` sempre AT, PE, SU ou CA
- `tipo` sempre IRU, AST ou PCT
- `area_ha` sempre >= 0
- `uf` sempre codigo valido de estado brasileiro
- Resultados vazios conservam as colunas do contrato

## Exemplo

```python
from agrobr import datasets

df, meta = await datasets.cadastro_rural(
    "DF", municipio=5300108,
    atualizado_apos="2026-09-01", return_meta=True,
)

recentes = await datasets.cadastro_rural(
    "MT", municipio="Cuiabá", criado_apos="2026-09-01"
)

print(meta.source_url)
```

## Schema JSON

Disponivel em `agrobr/schemas/cadastro_rural.json`.

```python
from agrobr.contracts import get_contract
contract = get_contract("cadastro_rural")
print(contract.to_json())
```
