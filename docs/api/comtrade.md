# API UN Comtrade

Comércio bilateral de mercadorias por HS e espelho entre exportações e importações inversas. Fonte, espelho e dataset usam contrato **2.0**, parser **2**.

## Consulta bilateral

```python
from agrobr import comtrade

df, meta = await comtrade.comercio(
    "1201,1005,0901,1701,2304",
    reporter="BR",
    partner="all",
    fluxo="X",
    periodo=2023,
    freq="A",
    require_complete=True,
    return_meta=True,
)
print(meta.source_details["coverage"])
```

| Argumento | Padrão | Semântica |
|---|---|---|
| `produto: str` | obrigatório | Alias de `produtos()`, HS ASCII de 2/4/6 dígitos ou lista textual desses códigos |
| `reporter: str` | `"BR"` | Alias conhecido ou código numérico textual positivo |
| `partner: str \| None` | `None` | `None`, `world`, `mundo`, `"0"`: agregado World explícito; `all`/`todos`: todos os parceiros publicados |
| `fluxo: str` | `"X"` | Exportação X ou importação M |
| `periodo: str \| int \| None` | ano anterior em UTC | Ano/mês, lista homogênea ou intervalo inclusivo |
| `freq: str` | `"A"` | Anual A ou mensal M |
| `api_key: str \| None` | `None` | Chave textual não vazia; None consulta `AGROBR_COMTRADE_API_KEY` |
| `require_complete: bool` | `False` | True rejeita cobertura partial/unknown; False preserva registros e emite aviso |
| `as_polars: bool` | `False` | Converte a saída validada para Polars, com o extra instalado |
| `return_meta: bool` | `False` | Retorna `(DataFrame, MetaInfo)` |

Seleções repetidas são normalizadas; registros retornados não são deduplicados. Não some World com os demais parceiros, nem HS agregado com seus descendentes. Argumentos desconhecidos, booleanos usados como período, HS ímpar, fluxo inválido e intervalos invertidos falham antes de rede.

## Aliases de produto

Cada alias soma os códigos HS da tabela. O mesmo alias tem o mesmo significado na [ComexStat](comexstat.md): onde a HS de
um período não separa o produto, entra o menor código que o contém. Descrições pelas referências oficiais H0 a H6
da Comtrade.

| Alias | HS | Não entra |
|---|---|---|
| `soja` | `120190` (desde a HS 2012) e `120100` (até 2011) | semente `120110`; até 2011 a HS não separa a semente (0,01 % do valor em 2025) |
| `complexo_soja` | `soja` + `farelo_soja` + `oleo_soja` | — |
| `farelo_soja` | `2304` | — |
| `oleo_soja` | `1507` | — |
| `milho` | `1005` | — |
| `arroz` | `1006` | — |
| `trigo` | `1001` (trigo e mistura de trigo com centeio) | — |
| `cafe` | `090111`, `090112`, `090121`, `090122` | cascas, películas e sucedâneos (`090190`; `090130`/`090140` na HS 1992) |
| `acucar` | `1701` | — |
| `etanol` | `2207` | — |
| `algodao` | `5201` e `5203` (em pluma, cardado ou penteado) | desperdícios `5202`; fios e tecidos |
| `carne_bovina` | `0201` e `0202` | — |
| `carne_frango` | `020711` a `020714` (galinha, *Gallus domesticus*) | peru, pato, ganso e galinha-d'angola (US$ 212 mi em 2025); períodos antes de 1996 levantam `InvalidParameterError`, e 1996 também quando o reporter é o Brasil, que reportou na HS 1992 |
| `carne_suina` | `0203` | — |
| `celulose` | `4703` (pasta química à soda ou ao sulfato) | pasta para dissolução `4702` (US$ 1,08 bi em 2025, 10,5 % das pastas), mecânica `4701`, ao bissulfito `4704`, semiquímica `4705` e de outras fibras `4706` |
| `tabaco` | `2401` (não manufaturado e desperdícios) | charutos e cigarros `2402`, manufaturados `2403` e produtos para inalação `2404` |
| `suco_laranja` | `200911`, `200912`, `200919` | sucos de outras frutas e hortaliças (US$ 357 mi em 2025) |

Antes de 1996 (HS 1992), a Comtrade não separa a carne de galinha fresca das outras aves, e o código dos cortes de galinha
congelados (`020741`) passou a ser pato na HS 2012. Para esses anos, peça os códigos explicitamente (`"020721,020741"`).

A guarda segue a classificação que o país reportou no ano, não o calendário. O Brasil reportou 1996 na HS 1992 (H0), onde nenhum dos quatro códigos existe: `comercio("carne_frango", periodo=1996)` levanta `InvalidParameterError` com a dica acima, em vez de voltar vazio com cobertura completa. A classificação do Brasil vem da disponibilidade oficial da Comtrade (H0 em 1996, H1 de 1997 a 2001, H2 de 2002 a 2006, H3 de 2007 a 2011, H4 de 2012 a 2016, H5 de 2017 a 2021 e H6 de 2022 a 2024). Para outros países, a classificação de cada ano não é conhecida antes da consulta, e a guarda vale só para antes de 1996. Os outros aliases têm ao menos um código em toda classificação que o Brasil reportou: `soja` e `complexo_soja` até 2011 só com `120100` e `suco_laranja` até 2001 sem `200912` são cobertura parcial.

## Períodos

| Frequência | Exemplos aceitos |
|---|---|
| A | `2023`, `"2022,2023"`, `"2021-2023"` |
| M | `202301`, `"202301,202303"`, `"202211-202302"` |
| M, ano completo | `2023`, `"2022-2023"` expandem todos os meses |

A validação de calendário não garante disponibilidade na fonte. O ano anterior é um padrão de seleção, sem promessa de publicação completa. O preview consulta um período por requisição; não reconstrói totais anuais somando meses.

## Cobertura e aquisição

O agrobr consulta `countOnly=true` com os mesmos filtros do bloco inicial e compara essa contagem à união dos dados. A contagem da resposta normal é apenas o número devolvido. O [preview oficial](https://uncomtrade.org/docs/what-is-data-preview/) limita a resposta a 500 registros. Quando faltam linhas, o client divide períodos e HS explicitamente solicitados em partições disjuntas, conserva a evidência do pai e utiliza somente as folhas.

Uma folha mínima ainda limitada pode devolver `partial`. Não há paginação por offset nem enumeração automática de todos os parceiros. Falhas HTTP, envelope inválido, dimensões incompatíveis ou conflito entre pai e filhos interrompem a aquisição. A cobertura é operacional: contagem e dados de uma coleta, sem snapshot atômico de revisões.

Sem chave configurada, usa preview público. O transporte autenticado solicita até 100.000 registros e planeja até 12 períodos por bloco; o limite efetivo com chave não é garantido. Recusa 401/403 reinicia todo o plano no preview, conservando as tentativas descartadas. Cotas da conta não são garantidas pelo agrobr.

## Colunas e metadados

As 22 colunas anteriores permanecem, com `classificacao` e `classificacao_original` e, no contrato 2.1, as 3 marcas de estimativa da ONU (`peso_liquido_estimado`, `peso_bruto_estimado` e `quantidade_estimada`), total **27**. Códigos, ano/mês e nível usam `Int64`; medidas usam `float64`; flags usam `boolean` anulável. O código de revisão publicado, como H6, é preservado; HS na URL é um alias. ISO e nomes podem ser nulos. Veja o [contrato completo](../contracts/comercio_internacional.md).

`MetaInfo` informa schema/contrato 2.1 (2.0 no espelho), canal `comtrade_guest` ou `comtrade_authenticated`, aquisição UTC e avisos. `source_details` contém query, recursos, cobertura, fallback e diagnóstico de parsing/layout. Cada recurso tem URL, SHA256 e tamanho. `raw_content_hash` identifica o manifesto canônico JSON UTF-8 de `query` e `resources`; `raw_content_size` mede esse manifesto, e `resource_bytes` soma os corpos. A chave de acesso não integra os metadados.

Os recursos descrevem a resposta final de cada requisição lógica e as partições descartadas. Tentativas intermediárias de retry HTTP não têm histórico de corpos nos metadados da API.

## Espelho comercial

```python
df, meta = await comtrade.trade_mirror(
    "soja", reporter="BR", partner="CN", periodo=2023,
    require_complete=True, return_meta=True,
)
```

Aceita os argumentos bilaterais, exceto `fluxo`; partner tem padrão `"CN"` e deve ser um país positivo distinto do reporter. Faz junção externa 1:1 por período/HS entre exportação e importação inversa. As 18 colunas anteriores recebem classificação e flag original por perna, além de `reporter_code` e `partner_code`, total **24**.

`ratio_valor` é FOB do reporter / CIF do parceiro; `ratio_peso` é peso do reporter / peso do parceiro. Não existe faixa de normalidade garantida. Ausência ou denominador zero produz nulo. Revisões HS diferentes na mesma célula geram erro: esta função não harmoniza classificações.

Quando uma perna volta vazia (a China ← Brasil na soja de abr/2025, no acesso convidado), as colunas do parceiro, os `diff_*` e os `ratio_*` saem nulos, não zero, sem aviso e com a cobertura `complete`. A contagem de cada perna fica em `meta.source_details["coverage"]["legs"]` (`received_count` e `expected_count`), e os nulos, em `meta.source_details["parsing"]["null_counts"]`.

Os metadados preservam ambas as aquisições em `source_details["legs"]`, e as células com peso líquido estimado pela ONU, por perna, em `source_details["peso_estimado"]`: o `ratio_peso` usa o peso publicado, estimado ou não. Cobertura conjunta completa exige as duas pernas completas; canais diferentes aparecem como `comtrade_mixed`.

## Dataset, sync e catálogos

`datasets.comercio_internacional(...)` aceita os mesmos seletores bilaterais, HS textual múltiplo, completude e Polars, preservando o contrato e a proveniência. Em contexto determinístico, snapshot preenche somente o ano omitido; não congela revisões da fonte.

```python
from agrobr.sync import comtrade

df = comtrade.comercio("soja", partner="world", periodo=2023, require_complete=True)
```

`paises()` lista os aliases ISO do mapa local, sem declarar catálogo mundial dinâmico. `produtos()` devolve uma cópia dos aliases agrícolas e seus HS. A categoria interna de licença é `zona_cinza`, com aviso na primeira chamada; veja os [termos verificados](../licenses.md#un-comtrade).
