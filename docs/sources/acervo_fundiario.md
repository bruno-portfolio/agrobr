# Acervo Fundiário — SIGEF, SNCI e Assentamentos (INCRA)

!!! info "Licença livre"
    Os dados públicos SIGEF, SNCI e assentamentos do Acervo Fundiário/INCRA são classificados como livre pela LAI, pelo Decreto 8.777/2016 e pela política institucional do INCRA, após busca sem restrição comercial específica localizada. A alegação anterior de veto comercial não tinha cláusula comprovada. A indicação histórica de CC BY não foi recapturada e não sustenta versão numérica. Citar INCRA, família, UF/abrangência, arquivo, edição e transformações, preservando direitos de terceiros expressos. O PDA 2021–2023 prova política e origem, não atualidade de todo serviço em 2026.

!!! note "Nao acessivel de fora do Brasil nos ambientes testados"
    O host `certificacao.incra.gov.br` respondeu normalmente do Brasil, mas nao respondeu
    (connection timeout) a partir dos runners do GitHub Actions nem de nenhum dos 8 nos
    internacionais testados em 31/08/2026 (Austria, Chipre, Finlandia, Ira, Servia, Ucrania).
    Se voce roda o agrobr fora do Brasil e recebe nesta fonte `SourceUnavailableError`
    com `ConnectTimeout` na mensagem, a causa provavel e essa restricao de rede — nao um bug da biblioteca.
    Por isso os testes live desta fonte usam o marker `integration_br` e ficam fora do CI.

!!! info "Dependência geoespacial"
    Instale `pip install agrobr[geo]` antes de qualquer consulta. O extra inclui
    `geopandas` e `pyogrio`; a disponibilidade é conferida antes do download dos
    ZIPs, que podem ter centenas de megabytes.

## Visão Geral

| Item | Detalhe |
|------|---------|
| Provedor | INCRA (Instituto Nacional de Colonização e Reforma Agrária) |
| Dados | Parcelas certificadas (SIGEF/SNCI) + assentamentos |
| Acesso | Download de shapefile ZIP estático |
| Endpoint | `https://certificacao.incra.gov.br/csv_shp/zip/` |
| CRS | EPSG:4674 (SIRGAS 2000) |
| Encoding | DBF latin1 (cp1252) |
| Atualização | Contínua (varia por UF, exposta via `Last-Modified`) |
| Autenticação | Nenhuma |
| Licença | Dados públicos federais — `livre`; citar INCRA, camada e extração |

## Cobertura por dataset

| Dataset | UFs disponíveis | Tamanho típico | Granularidade |
|---|---|---|---|
| **SIGEF** | 27/27, em 2 arquivos (público e privado) | público 0,4-21 MB, privado 1-749 MB por UF | Por UF |
| **SNCI** | 24/27 em 01/10/2026 (AC, DF e RR sem arquivo) | 0,07-23 MB por UF | Por UF |
| **Assentamentos** | Brasil único | 50 MB | Brasil completo, filtro UF client-side |

O INCRA publica o SIGEF de cada UF em 2 arquivos, `Sigef Público_{UF}.zip` e `Sigef Privado_{UF}.zip`, que particionam a UF: em 01/10/2026, nas 27 UFs, a soma dos registros dos dois era igual à do `Sigef Brasil_{UF}.zip` (1.848.075 = 159.681 + 1.688.394), e no DF e no AP nenhuma parcela aparecia nos dois. O SNCI sai por UF, e a lista muda com o tempo: em 01/10/2026, AC, DF e RR estavam sem arquivo (o de RR existia em 22/09/2026). UF sem arquivo no servidor levanta `SourceUnavailableError` (HTTP 404).

## Funções públicas

```python
import asyncio
from agrobr import acervo_fundiario

async def main():
    # SIGEF — parcelas certificadas pós-2013, dos arquivos público e privado
    df = await acervo_fundiario.sigef("GO")
    df = await acervo_fundiario.sigef("GO", natureza="publico")  # baixa só o arquivo público
    df, meta = await acervo_fundiario.sigef("MG", return_meta=True)
    df_pl = await acervo_fundiario.sigef("SP", as_polars=True)
    gdf = await acervo_fundiario.sigef_geo("GO", bbox=(-50, -16, -49, -15))

    # SNCI — certificações do sistema anterior ao SIGEF (sem corte por data; há registros até 2016)
    df = await acervo_fundiario.snci("GO")
    gdf = await acervo_fundiario.snci_geo("MT")

    # Assentamentos — Brasil único, uf opcional
    df = await acervo_fundiario.assentamentos()             # todas as UFs
    df = await acervo_fundiario.assentamentos(uf="GO")      # filtro client-side
    gdf = await acervo_fundiario.assentamentos_geo(uf="MG")

asyncio.run(main())
```

## Cache filesystem

Arquivos baixados ficam em `~/.agrobr/cache/acervo_fundiario/{tema}/{UF}.zip` com `{UF}.json` ao lado contendo `last_modified`, `etag`, `sha256`, `size_bytes`, `fetched_at`, `source_url`. Os temas são `sigef_publico`, `sigef_privado`, `snci`, `snci_publico`, `snci_privado` e `assentamentos` (este em `brasil.zip`). A pasta `sigef/` das versões anteriores guardava o `Sigef Brasil_{UF}.zip`, que não é mais lido: pode ser apagada. Onde fica e como limpar: [O que o agrobr grava no disco](../advanced/disco.md).

Com `return_meta=True`, o `MetaInfo` diz de onde veio o arquivo. Quando o HEAD confirma o cache: `from_cache=True`, `fetched_at` = coleta original do ZIP (o `fetched_at` do `{UF}.json`) e `source_details` com `revalidado_em`, `etag` e `last_modified`. Num download novo: `from_cache=False`, `fetched_at` = o download e `source_details` só com `etag` e `last_modified`. No SIGEF, que pode ler 2 arquivos, esses campos ficam por arquivo: ver [`natureza` no SIGEF](#natureza-no-sigef).

A revalidação faz HEAD e exige ao menos um validador presente (`ETag` ou
`Last-Modified`) coincidente. Divergências nesses headers ou no tamanho
invalidam o cache; ausência de ambos exige novo download. Locks são locais
ao event loop, e cada escrita limpa somente seu próprio temporário.

**Tamanho potencial do cache:**

- SIGEF completo (27 UFs, público + privado) ≈ 3,1 GB (0,2 GB público + 2,9 GB privado; maior: MG, 749 MB no privado)
- SNCI completo (24 UFs publicadas em 01/10/2026) ≈ 104 MB
- Assentamentos Brasil = 50 MB

Por demanda. Caso casual de 1-3 UFs costuma ficar abaixo de 1 GB.

**Opt-out:**

```python
df = await acervo_fundiario.sigef("GO", use_cache=False)
```

```bash
export AGROBR_ACERVO_FUNDIARIO_CACHE_DISABLED=1
```

Com o cache desligado, o ZIP vai para uma pasta temporária do sistema e é apagado ao fim da consulta, com
sucesso, erro ou cancelamento; nada é gravado em `~/.agrobr/cache/acervo_fundiario/`. Os booleanos da variável
aceitam `1`/`true`/`yes`.

## Schemas

### SIGEF

| Coluna | Tipo | Descrição |
|---|---|---|
| codigo_parcela | str | UUID da parcela |
| rt | str | Responsável técnico |
| art | str | Anotação de responsabilidade técnica |
| situacao | str | Situação informada |
| codigo_imovel | str | Código do imóvel rural |
| data_submissao | datetime | Data de submissão |
| data_aprovacao | datetime | Data de aprovação |
| status | str | Status da certificação |
| nome_area | str | Nome da área/fazenda |
| registro_matricula | str | Matrícula do registro |
| registro_data | datetime | Data do registro (nullable) |
| cod_municipio | int | Código IBGE do município |
| uf | str | Sigla UF (mapeada de `uf_id` IBGE) |
| natureza | str | `publico` ou `privado`: o arquivo do INCRA de onde veio a parcela (`Sigef Público_{UF}.zip` ou `Sigef Privado_{UF}.zip`) |
| geometry | Polygon Z | Geometria com altitude nos vértices (apenas em `_geo`) |

### SNCI

| Coluna | Tipo | Descrição |
|---|---|---|
| num_processo | str | Número do processo |
| sr | str | Superintendência regional |
| num_certificacao | str | Número da certificação |
| data_certificacao | datetime | Data da certificação |
| area_peca_tecnica | float | Área em hectares (peça técnica) |
| cod_profissional | str | Código do profissional credenciado |
| cod_imovel_rural | str | Código do imóvel rural |
| nome_imovel | str | Nome do imóvel |
| uf | str | Sigla UF (de `uf_municip`) |
| natureza | str | Só com `natureza`: `publico` ou `privado`, o arquivo de onde veio a linha |
| geometry | Polygon | Apenas em `_geo` |

### Assentamentos

| Coluna | Tipo | Descrição |
|---|---|---|
| codigo_sipra | str | Código SIPRA do projeto |
| nome_projeto | str | Nome do projeto |
| municipio | str | Município |
| uf | str | Sigla UF |
| area_ha | float | Área declarada em hectares |
| capacidade | int | Capacidade de famílias |
| num_familias | int | Número de famílias assentadas |
| fase | int | Fase do projeto |
| data_criacao | datetime | Data de criação |
| forma_obtencao | str | Forma de obtenção |
| data_obtencao | datetime | Data de obtenção |
| area_calc_ha | float | Área calculada em hectares |
| sr | str | Superintendência regional (nullable) |
| descricao_fase | str | Descrição da fase (nullable) |
| geometry | Polygon | Apenas em `_geo`; nula quando a fonte publica o registro sem geometria (1 de 8.216 em 22/09/2026) |

## Filtros

### `bbox`

Aplicado pelo `pyogrio` durante a leitura do shapefile (filtro espacial pré-leitura, 4-9x menos RAM/tempo que filtrar `gdf.cx[]` após carregar tudo).

```python
gdf = await acervo_fundiario.sigef_geo("MG", bbox=(-44, -18, -43, -17))
```

### `natureza` no SIGEF

O shapefile do SIGEF não tem campo que separe parcela pública de privada: a separação é o arquivo que o INCRA publica. A coluna `natureza` diz de qual dos dois a linha veio.

- `natureza=None` (padrão) lê os 2 arquivos, com o mesmo volume do antigo `Sigef Brasil_{UF}.zip`, e devolve as linhas do público e depois as do privado, cada parte na ordem do arquivo.
- `natureza="publico"` ou `"privado"` baixa só aquele arquivo. Caixa e acento são ignorados (`"Público"` vale); outro valor levanta `InvalidParameterError` antes da rede.
- `MetaInfo`: `source_url` é a URL do 1º arquivo lido (o público, quando lê os 2); `attempted_sources` lista os arquivos lidos (`acervo_fundiario_sigef_publico`, `acervo_fundiario_sigef_privado`); `source_details["arquivos"]` traz, por natureza, `url`, `from_cache`, `fetched_at`, `sha256`, `size_bytes`, `etag`, `last_modified` e, no acerto de cache, `revalidado_em`. `from_cache` só é `True` quando todos os arquivos vieram do cache; `fetched_at` é a coleta mais antiga; `raw_content_hash` só vem preenchido quando é 1 arquivo; `raw_content_size` soma os arquivos; `schema_version` é `1.1`.
- O INCRA regrava os 2 arquivos em horários diferentes. Se um `codigo_parcela` aparecer nos dois, as 2 linhas ficam e o `MetaInfo.validation_warnings` avisa.

```python
df = await acervo_fundiario.sigef("DF")                      # público + privado
publico = await acervo_fundiario.sigef("DF", natureza="publico")
```

### `natureza` no SNCI

Além do `SNCI Brasil_{UF}.zip`, o INCRA publica o SNCI de cada UF em `Imóvel certificado SNCI Público_{UF}.zip` e `Imóvel certificado SNCI Privado_{UF}.zip`, com os mesmos campos.

- `natureza=None` (padrão) lê o arquivo Brasil da UF, como antes: mesmas colunas, cache em `snci/`, `schema_version` `1.0`.
- `natureza="publico"` ou `"privado"` baixa só aquele arquivo, acrescenta a coluna `natureza` (também no resultado vazio) e marca `schema_version` `1.1`; o cache fica em `snci_publico/` ou `snci_privado/`. Caixa e acento são ignorados; outro valor levanta `InvalidParameterError` antes da rede.
- O agrobr não une os dois arquivos nem promete que a união é o arquivo Brasil: em AL, em 03/10/2026, os tamanhos não somavam (11.900 + 58.927 bytes contra 69.506 do Brasil, publicado em outra data). Quem precisar da partição tem de conferir pelos registros, na mesma janela.

### `uf` em assentamentos

O dataset de assentamentos é Brasil único — o filtro `uf` é client-side, normalizando a coluna `uf` (`.str.upper().str.strip()`) e comparando.

Se a fonte trouxer UF fora das 27 siglas, o parser não descarta a linha: o log `acervo_fundiario_dirty_uf_data` reporta as contagens. Filtro `uf="MG"` retorna apenas linhas com `MG`; as demais continuam no DataFrame quando `uf=None`. Na captura de 22/09/2026, as 8.216 linhas tinham UF válida.

## Limitações

- **TLS verificado** — downloads e health check validam certificado e hostname.
  Falhas de certificado interrompem a conexão, sem desabilitar a verificação.
- **Público × privado vem do arquivo, não de um campo** — o shapefile não tem campo de tipo; a coluna `natureza` diz de qual dos 2 arquivos do INCRA a parcela veio (ver [`natureza` no SIGEF](#natureza-no-sigef))
- **Tamanho de cache pode acumular GB** — ver seção "Cache filesystem"

## Coleta bruta

`agrobr.bruto.coletar("acervo_fundiario", recurso, uf=..., ...)` guarda o ZIP da UF como o INCRA publica, com um único GET, sem HEAD, cache nem leitura do shapefile: `sigef_publico`, `sigef_privado`, `snci_publico`, `snci_privado` e `snci_brasil`. O manifesto registra a natureza (`publico`, `privado` ou nula no `snci_brasil`). 404 vira `ausente_na_fonte`, e a próxima retomada tenta de novo; resposta 200 que não é ZIP é erro. Os arquivos por natureza não são unidos nem comparados ao arquivo Brasil. O recurso `assentamentos` guarda do mesmo modo o `Assentamento Brasil.zip` nacional (~50 MB), sem UF: UF e bbox são recusadas, o `nome` padrão é `brasil` e `selecao.edicao` fica nula (a data do arquivo está no `last-modified`). Veja a [API da coleta bruta](../api/bruto.md) e o [contrato do manifesto](../contracts/bruto.md).
