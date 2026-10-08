# IBAMA — Embargos Ambientais

## Visao Geral

| Item | Detalhe |
|------|---------|
| Provedor | IBAMA (Instituto Brasileiro do Meio Ambiente e dos Recursos Naturais Renovaveis) |
| Dados | Termos de embargo por infracoes ambientais (sistema de fiscalizacao do IBAMA) |
| Acesso | CSV do conjunto "Fiscalização - termo de embargo" no portal de dados abertos do IBAMA |
| Formato | CSV UTF-8 com BOM, `;`, campos entre aspas, ~208 MB sem compressão (a fonte não publica versão compactada), geometrias WKT |
| Autenticacao | Nenhuma |
| Licenca | "Outra (Aberta)" no catálogo; dados abertos federais, uso livre com crédito da fonte ([detalhes](../licenses.md#ibama)) |
| Registros | 116.332 termos (edição de 23/09/2026), atualização diária |

> Desde a 2.0.0 o agrobr lê o recurso vigente do conjunto. O ZIP antigo
> (`dadosabertos.ibama.gov.br/dados/SIFISC/termo_embargo/termo_embargo/termo_embargo_csv.zip`) parou em 03/05/2026
> e não está mais no catálogo. O link do catálogo para o CSV tem erro de caminho (`dados/TERMOS/TERMO_EMBARGO/...`,
> 404); o arquivo publicado está no endereço abaixo.

## Acesso

| Parametro | Valor |
|-----------|-------|
| URL | `stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados/TERMOS_DE_EMBARGO/TERMO_EMBARGO/termo_de_embargo.csv` |
| Catálogo | [dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo](https://dadosabertos.ibama.gov.br/dataset/fiscalizacao-termo-de-embargo) |
| Atualizacao | Diária; a edição lida (`ULTIMA_ATUALIZACAO_RELATORIO`, horário de Brasília) vai em `meta.source_details["ultima_atualizacao_relatorio"]` |
| Filtros | `uf` e `bbox` aplicados client-side apos o download |

## Exemplo de Uso

```python
import asyncio
from agrobr import ibama

async def main():
    # Todos os embargos do Brasil
    df = await ibama.embargos()

    # Filtrar por UF
    df = await ibama.embargos(uf="MT")

    # Com geometria WKT (requer geopandas — extra [geo])
    gdf = await ibama.embargos_geo(uf="RR")
    gdf = await ibama.embargos_geo(bbox=(-56, -16, -54, -14))

    # Com metadados (edição da fonte em meta.source_details)
    df, meta = await ibama.embargos(return_meta=True)

    # Polars
    df = await ibama.embargos(as_polars=True)

    # Ignorar o cache e baixar de novo
    df = await ibama.embargos(use_cache=False)

asyncio.run(main())
```

## Colunas

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| seq_tad | str | Identificador do termo no sistema de fiscalizacao (vazio nos 2.862 termos do AIe) |
| numero_tad | str | Numero do Termo de Embargo (numérico até 07/10/2019; alfanumérico no AIe) |
| data_embargo | datetime | Data e hora da lavratura, horário de Brasília sem fuso |
| num_processo | str | Numero do processo administrativo |
| descricao | str | Descricao do embargo, texto livre da fonte |
| codigo_municipio | str | Codigo IBGE do municipio (7 dígitos; 2 termos trazem `431173 `, 6 dígitos e espaço, como na fonte) |
| municipio | str | Municipio |
| uf | str | UF (sigla) |
| latitude / longitude | float | Ponto de referência do termo, como na fonte: 94.655 termos com ponto; 4.416 com os dois zerados (`0` = não informado) e 17.261 com coordenada vazia; 387 pontos fora do retângulo do Brasil |
| area_embargada_ha | float | Area embargada em hectares (4 casas decimais com vírgula na fonte; área com ponto levanta `ParseError`) |
| nome_imovel | str | Nome do imovel |
| status | str | Situação do termo: Lavrado, Cancelado, Substituído por outro, Excluído (vazio nos termos do AIe) |
| cancelado | bool | `SIT_CANCELADO = S` |
| data_desembargo | datetime | Data do registro do desembargo; NaT = sem desembargo registrado (a fonte marca `SIT_DESEMBARGO = S` exatamente nesses termos) |

`embargos_geo` adiciona `geometry` (Polygon/MultiPolygon) lida do WKT do proprio CSV — somente registros com
poligono (58.984 na edição de 23/09/2026). O CSV não declara SRID; a base de origem no GIS do IBAMA
(`adm_embargos_ibama_a`) está em SIRGAS 2000 (EPSG:4674), e o agrobr rotula EPSG:4326 sem reprojetar (a
transformação SIRGAS 2000 → WGS 84 da EPSG é nula).

## Particularidades

- **Dado pessoal**: o CSV da fonte traz nome e CPF/CNPJ do embargado; as tabelas do agrobr não expõem esses
  campos (política do projeto). `descricao` é texto livre da fonte. O cache de 1 hora, porém, guarda o arquivo
  inteiro, com essas colunas, e a primeira gravação avisa com `UserWarning`. Quem não quer o dado pessoal em disco usa
  `use_cache=False`, que não grava nada, ou apaga a pasta `ibama/` do cache depois da consulta. Quem precisar dos
  campos para compliance pode baixar o CSV bruto da fonte.
- **Datas sujas na fonte**: há datas fora de qualquer faixa plausível (anos 1667, 2063, 2080, 2090 e 2925). Pela regra
  de datas do agrobr ([Normalização](../guides/normalizacao.md#datas-das-fontes)), igual no pandas 2 e no 3, vira `NaT`
  a data com ano fora de 1900–2099 (1667 e 2925) e a de dia posterior à edição do próprio arquivo
  (`ULTIMA_ATUALIZACAO_RELATORIO`; na de 23/09/2026, 2063, 2080 e 2090): um termo não pode ser datado depois do arquivo que o
  publica. A consulta avisa com `UserWarning` e em `meta.validation_warnings`, com a coluna e a quantidade. As duas colunas
  de data saem em `datetime64[ns]`.
- **bbox**: `embargos(bbox=...)` filtra pelo ponto de referência (lat/lon) do termo; `embargos_geo(bbox=...)`
  filtra pela interseção do polígono com a caixa. Os dois podem divergir: no bbox do exemplo, 9 termos têm o ponto
  dentro e o polígono fora, e 4 têm o polígono dentro e o ponto fora ou ausente. O ponto com latitude e longitude
  zeradas (não informado) fica fora do filtro de `embargos(bbox=...)`, mesmo numa caixa que contém (0, 0); sem `bbox`,
  sai como na fonte.
- **Geometrias**: 1 WKT ilegível (anel aberto) é descartado com aviso no log; 129 polígonos com topologia inválida
  saem como publicados.
- **Cache de 1 hora**: o CSV (~208 MB) fica em `ibama/termo_embargo.csv` na pasta de cache, com um
  manifesto (SHA-256 e hora da coleta), e é reusado por 1 hora a partir da coleta. Chamadas seguidas, com
  filtros diferentes, baixam o arquivo uma vez só, inclusive quando rodam juntas. O `MetaInfo` traz
  `from_cache=True` e, em `fetched_at`, a hora da coleta, e não a da chamada. `use_cache=False` baixa de
  novo sem ler nem gravar o cache. Arquivo corrompido ou vencido é baixado outra vez. O arquivo vencido não é
  apagado: fica no disco até a próxima coleta sobrescrevê-lo.
- **Geo sem filtro**: `embargos_geo()` sem `uf`/`bbox` parseia WKT do Brasil
  inteiro (~4 s); um warning e emitido. Com `bbox`, o WKT de toda a seleção é lido.

## Limitacoes

- Geometria presente em parte dos registros (embargos sem poligono ficam fora do geo)
- O arquivo inteiro (~208 MB) é baixado, e os filtros são locais; o cache de 1 hora evita repetir o download
- Licença: ver [Licenças](../licenses.md#ibama)

## Coleta bruta

`agrobr.bruto.coletar("ibama", "termos_embargo", ...)` guarda o CSV de termos de embargo inteiro (~209 MB) como o
IBAMA publica, sem cache nem leitura das colunas; `embargos()` e `embargos_geo()` seguem com o cache de 1 hora. O IBAMA
não publica edição: `selecao.edicao` fica `null`, e a data do arquivo está em `cabecalhos` (`last-modified`). UF e bbox
são recusadas.

**Dado pessoal:** o arquivo traz nome e CPF/CNPJ das pessoas físicas e jurídicas embargadas, colunas que as funções de
tabela não leem. Quem guarda o arquivo bruto guarda dado pessoal e deve tratá-lo conforme a LGPD. Veja a
[API da coleta bruta](../api/bruto.md) e o [contrato do manifesto](../contracts/bruto.md).
