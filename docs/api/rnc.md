# API RNC/SNPC

O módulo `rnc` consulta duas exportações distintas do CultivarWeb/MAPA: cultivares registradas no RNC e registros de proteção do SNPC. A população recebida é validada antes dos filtros. Registro e proteção não são unidos por nome.

## Funções

### `registradas`

```python
async def registradas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    grupo: str | None = None,
    situacao: str | None = None,
    mantenedor: str | None = None,
    nr_registro: str | None = None,
    nr_formulario: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

| Filtros | Regra |
|---|---|
| `cultivar`, `especie`, `grupo`, `situacao`, `mantenedor` | Substring literal, sem regex e sem distinguir maiúsculas/minúsculas nem acento (`"feijao"` acha `"Feijão"`); `especie` consulta `nome_comum` |
| `nr_registro`, `nr_formulario` | Igualdade textual exata, preservando zeros iniciais |

O resultado tem **dez colunas**, nesta ordem: `cultivar`, `nome_comum`, `nome_cientifico`, `grupo`, `situacao`, `nr_formulario`, `nr_registro`, `data_registro`, `data_validade`, `mantenedor`.

A chave é `nr_registro`, com escopo da exportação identificada pelo hash. Formulários podem se repetir e estar vazios. Cultivar e mantenedor vazios publicados são preservados; não são preenchidos a partir de outros campos.

```python
from agrobr import rnc

soja = await rnc.registradas(especie="soja")
registro = await rnc.registradas(nr_registro="42039", use_cache=False)
```

### `protegidas`

```python
async def protegidas(
    *,
    cultivar: str | None = None,
    especie: str | None = None,
    situacao: str | None = None,
    titular: str | None = None,
    nr_processo: str | None = None,
    nr_certificado: str | None = None,
    use_cache: bool = True,
    as_polars: bool = False,
    return_meta: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, MetaInfo]: ...
```

| Filtros | Regra |
|---|---|
| `cultivar`, `especie`, `situacao`, `titular` | Substring literal, sem regex e sem distinguir maiúsculas/minúsculas nem acento (`"feijao"` acha `"Feijão"`); `especie` consulta `nome_comum` |
| `nr_processo`, `nr_certificado` | Igualdade textual exata |

O resultado tem **doze colunas**: `cultivar`, `nome_cientifico`, `nome_comum`, `nr_processo`, `situacao`, `nr_certificado`, `inicio_protecao`, `termino_protecao`, `titular`, `representante_legal`, `melhoristas`, `termino_protecao_texto`.

A chave é `nr_processo`, com escopo da exportação identificada pelo hash. O certificado pode se repetir entre processos; um filtro por certificado pode retornar várias linhas. Situações canceladas, expiradas, de renúncia ou nulidade permanecem conforme a fonte. Campos compostos de pessoas não são divididos nem deduplicados.

```python
df, meta = await rnc.protegidas(
    nr_processo="21806.000132/2019",
    return_meta=True,
)
print(df[["termino_protecao", "termino_protecao_texto"]])
```

## Datas e ausências

As quatro colunas de datas usam `datetime64[ns]`, como datas civis à meia-noite, sem fuso. Célula vazia permanece `NaT`; texto não vazio deve ser uma data `DD/MM/YYYY` válida. Data impossível ou texto desconhecido provoca `ParseError`, em vez de virar ausência silenciosamente.

O término da proteção também admite o literal oficial `até a emissão do certificado definitivo`. Nesse caso, `termino_protecao` é `NaT` e `termino_protecao_texto` conserva a condição. A coluna textual contém a célula publicada após remover espaços externos, inclusive quando ela contém uma data ou está vazia. Assim, uma condição não se confunde com ausência. Não se calcula um prazo para essa expressão nem se deduz a situação administrativa a partir dela.

As outras colunas usam texto, no dtype padrão do pandas instalado (`str` no pandas 3, `object` no 2). Espaços externos são removidos; textos vazios, pontuação e conteúdo composto permanecem. Em `nome_cientifico` e `nome_comum`, os brancos internos repetidos (inclusive o espaço não separável) também viram 1 espaço: a SNPC publica a soja como `Glycine max (L.)  Merr.`, com 2 espaços, e o RNC como `Glycine max (L.) Merr.`, e assim o nome científico casa entre as 2 famílias. Nas outras colunas, o espaço interno fica como publicado. Identificadores não são convertidos em números.

## Validação e seleção

Todos os filtros são opcionais e combinados por interseção. Seus espaços externos são removidos antes da comparação. Texto vazio, tipo diferente de string, parâmetro desconhecido ou flag não booleana causa `InvalidParameterError` antes de I/O. Não há conversão automática de um ID numérico para string.

O parser 2 exige o layout completo de cada família, confere a largura dos registros, valida as células e rejeita chaves duplicadas antes do recorte. O contrato 1.0 é aplicado à população e ao resultado, inclusive com `return_meta=False`. Um filtro sem correspondência retorna DataFrame vazio com todas as colunas e dtypes preservados; isso difere de um CSV de origem inválido ou sem registros.

`as_polars=True` converte o resultado após a validação e requer o extra `[polars]`. `return_meta=True` retorna `(DataFrame, MetaInfo)`, também no modo Polars.

## Cache e proveniência

`use_cache=True` reutiliza, por família, um pacote local com CSV bruto e manifesto de aquisição. O TTL é de **24 horas desde o recebimento do CSV**, sem renovação pelo acesso. Hash, tamanho, família, versões e expiração são conferidos; o CSV cacheado passa novamente pelo parser e contrato. Pacotes incompatíveis, expirados ou ilegíveis são ignorados. A gravação é atômica. O cache antigo de tabela normalizada não fornece a proveniência exigida por essa rota.

`use_cache=False` não lê nem escreve o cache. Não é seleção de uma revisão histórica.

Com `return_meta=True`, os metadados incluem:

- `selected_source`/`attempted_sources`: rota `rnc_registradas` ou `rnc_protegidas`;
- `raw_content_hash`, `raw_content_size` e `fetched_at` UTC da aquisição original, também em cache;
- `from_cache`, chave/expiração quando aplicáveis e tempos de fetch/parsing; o tempo do filtro fica em `source_details.filter_duration_ms`;
- `source_details.acquisition`: identidade da pesquisa e do CSV, URLs, horários, hashes e headers públicos;
- `source_details.parser`: fingerprint de layout, população validada, ausências, identificadores repetidos e estatísticas de datas;
- `source_details.selection`: filtros normalizados e número de linhas do recorte.

`source_details.coverage.status` é `count_matched` quando a quantidade de linhas do CSV coincide com o total declarado pela pesquisa que o precedeu; divergência aborta a aquisição. Sem total verificável, o estado é `unknown`. A igualdade de contagem não prova uma transação única nem uma revisão imutável: `transactional_snapshot` permanece `False`.

## Versão síncrona e datasets

```python
from agrobr.sync import rnc

df = rnc.registradas(especie="soja")
```

Os wrappers semânticos `datasets.cultivares_registradas` e `datasets.cultivares_protegidas` reutilizam essas rotas e contratos, com os mesmos filtros. São datasets de fonte única e recusam contexto `deterministic` antes de I/O: o cache corrente não congela uma edição histórica.

## Fonte e limites

O acesso segue formulários públicos de pesquisa/exportação, com sessão e renovação de CSRF antes dos POSTs; não exige credencial pessoal. Tokens e cookies não integram os metadados públicos. Não há fallback para outra instituição. Os campos descrevem a publicação recebida; não se infere vigência jurídica nem cobertura de revisões anteriores. Consulte a [fonte](../sources/rnc.md) e a [classificação de licença](../licenses.md).

## Exportação de 18/09/2026

Os CSVs da exportação de 18/09/2026 têm 38.325 registros RNC e 5.424 registros SNPC. A contagem HTML da pesquisa que precede cada CSV coincide com ele; essa concordância não garante snapshot transacional nem cobertura histórica.

No RNC, os 22.946 formulários, 4.256 nomes de cultivar e 4.271 mantenedores vazios publicados permanecem texto vazio; ambas as datas estão preenchidas nessa exportação. Há 924 grupos de formulários repetidos, com 15.142 ocorrências. No SNPC, o término tem 5.267 datas, 155 condições e dois vazios; as 155 condições e os dois vazios produzem data nula, preservando o texto. Há 102 certificados repetidos entre processos distintos, com 204 ocorrências. Nenhuma dessas repetições de identificadores secundários é deduplicada.

A aquisição original em `fetched_at` conserva UTC com fuso explícito, inclusive no cache e no dataset. Na fonte e no dataset, `fetch_timestamp` coincide com a aquisição. As datas civis da tabela são independentes desses instantes.
