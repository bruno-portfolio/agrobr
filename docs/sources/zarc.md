# ZARC (Zoneamento Agricola de Risco Climatico)

## Sobre

O **ZARC** e o sistema oficial do MAPA (Ministerio da Agricultura) e Embrapa
que define janelas de plantio recomendadas por municipio, cultura, tipo de solo
e ciclo do cultivar. Publicado como Portaria no Diario Oficial da Uniao,
o ZARC e requisito para acesso ao credito rural subsidiado (Proagro, PSR).

Dados publicados como CSV no portal [dados.agricultura.gov.br](https://dados.agricultura.gov.br)
(CKAN), licenca CC-BY, periodicidade semanal no catálogo ativo (o PDF declara diária; veja o conflito abaixo).

## Dados disponiveis

- **Tabua de Risco:** janelas de plantio (36 decendios) por municipio/cultura/solo/ciclo
- **Culturas:** 107 culturas no catálogo de aliases, uma por rótulo publicado nas 12 tábuas oficiais, incluindo nomes legados; disponibilidade variável por safra; frutas e café usam `safra="perene"`
- **Safras:** 2016/2017 a atual + perene (cafe, cana, banana, etc.)
- **Solos:** 3 tipos classicos (arenoso/medio/argiloso) + 6 niveis AD (agua disponivel)
- **Cobertura:** municípios presentes em cada publicação; total nacional completo não afirmado

## Campos retornados

| Campo | Tipo | Descricao |
|-------|------|-----------|
| cultura | string | Nome canonico da cultura (ex: "soja", "milho_1", "trigo") |
| safra | string | "2025/2026" ou "perene" |
| geocodigo | string | Codigo IBGE do municipio (7 digitos) |
| uf | string | Sigla da UF |
| municipio | string | Nome do municipio |
| solo_codigo | int | Tipo de solo (1-3 classico, 11-16 AD) |
| ciclo_codigo | int | Ciclo do cultivar (13, 19, 20, 21, 22, 24, 25, 26) |
| clima | string | Restricao climatica (ex: "Sem restricao") |
| manejo | string | Manejo especifico (ex: "Sem restricao", "Irrigado") |
| portaria | string | Numero da portaria MAPA |
| dec1-dec36 | Int64, nullable | Risco publicado por decendio (0/20/30/40/50); vazio é nulo |

## Decendios

Cada mes e dividido em 3 decendios:

- dec1-dec3: janeiro
- dec4-dec6: fevereiro
- ...
- dec34-dec36: dezembro

Valores publicados: 0, 20, 30, 40 e 50. Célula vazia é nula, distinta de zero. O valor 50 ocorre na tábua perene; 0 e 50 são preservados sem interpretação agronômica presumida.

## Notas

- **CSV grande:** arquivos de aproximadamente 224 MB por safra anual e 535 MB na tábua perene. A primeira consulta de cada revisão baixa e parseia a tábua inteira (cerca de 3 minutos, quase todo na validação de cada registro). As seguintes consultam os dados validados no cache local DuckDB, inclusive em outro processo Python: TTL de 24 horas desde a aquisição e até três revisões. O arquivo ZARC é separado do cache CEPEA. O catálogo usa cache em memória por uma hora. `use_cache=False` ignora leitura e gravação de ambos; falhas no cache local geram log de aviso e seguem por download e parse. Metadados preservam SHA, aquisição original e culturas observadas na tábua inteira. O download é conferido contra o tamanho que o servidor publica (o `Content-Range` do portal do MAPA, ou o `Content-Length`): corpo menor levanta `SourceUnavailableError` e não vai para o cache. Sem o tamanho publicado, o resultado avisa em `validation_warnings` ("tamanho do arquivo não conferido") e não é gravado; entrada do cache sem tamanho conferido ou sem registros é baixada de novo. Onde fica e como limpar: [O que o agrobr grava no disco](../advanced/disco.md).
- **CKAN discovery:** URLs mudam a cada publicacao; o client faz discovery via API CKAN
- **User-Agent:** portal requer headers browser-like (retorna 403 com bot UA)
- **Encoding:** UTF-8 com BOM, separador `;`
- **Produtividade:** publicada como texto em `produtividade_texto` (quase sempre vazia; decimal com vírgula preservado; unidade não inferida)

## Licenca

Dados publicos do governo federal brasileiro (CC-BY). Uso livre com citacao da fonte.

## Links

- [Portal CKAN](https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico)
- [ZARC - MAPA](https://www.gov.br/agricultura/pt-br/assuntos/riscos-seguro/programa-nacional-de-zoneamento-agricola-de-risco-climatico)

## Culturas por safra

Nomes da tábua e aliases são aceitos: `Milho 2ª Safra` equivale a `milho_2`, `Algodão Herbáceo` a `algodao` e `Sorgo Granífero 2ª Safra` a `sorgo_2`. `culturas()` apresenta o catálogo geral, não a garantia de presença em toda safra. Cultura ausente informa os aliases disponíveis na tábua consultada; use `safras_disponiveis()` para escolher outra safra.

Culturas fora do catálogo são rejeitadas antes de acessar a rede, com sugestões quando houver nomes semelhantes. Culturas válidas ausentes na safra só são rejeitadas após a leitura da tábua, com indicação da tábua adequada quando conhecida.

## Renomeações da safra 2024/2025

A partir da tábua 2024/2025, o ZARC troca o nome de 11 rótulos de cultura. O conteúdo é o mesmo: nas tábuas oficiais de 2023/2024 e 2024/2025, cada par tem os mesmos registros de município, solo, ciclo e decêndios. O agrobr não converte uma chave na outra. Pedir a chave antiga numa safra nova, ou o contrário, levanta `InvalidParameterError` com a chave equivalente daquela tábua.

| Até 2023/2024 | `cultura_codigo` | Desde 2024/2025 | Filtro na tábua nova | `cultura_codigo` |
|---|---|---|---|---|
| `milho` (Milho) | 12015080000011 | `milho_1` (Milho 1ª Safra) | — | 12015080000011 |
| `feijao_1` (Feijão 1ª Safra) | 12013560000011 | `feijao` (Feijão) | — | 12013560000011 |
| `arroz_sequeiro` (Arroz Sequeiro) | 12010900000011 | `arroz` (Arroz) | `manejo == "Sequeiro"` | 12010900000011 |
| `arroz_irrigado` (Arroz Irrigado) | 12010900000051 | `arroz` (Arroz) | `manejo == "Irrigado"` | 12010900000011 |
| `aveia_sequeiro` (Aveia Sequeiro) | 12011000000031 | `aveia` (Aveia) | `manejo == "Sequeiro"` | 12011000000031 |
| `aveia_irrigada` (Aveia Irrigada) | 12011000000051 | `aveia` (Aveia) | `manejo == "Irrigado"` | 12011000000031 |
| `cevada_graos_sequeiro` (Cevada Grãos Sequeiro) | 12012320000031 | `cevada_graos` (Cevada Grãos) | `manejo == "Sequeiro"` | 12012320000031 |
| `cevada_graos_irrigada` (Cevada Grãos Irrigada) | 12012320000051 | `cevada_graos` (Cevada Grãos) | `manejo == "Irrigado"` | 12012320000031 |
| `trigo_sequeiro` (Trigo Sequeiro) | 12017100000031 | `trigo` (Trigo) | `manejo == "Sequeiro"` | 12017100000031 |
| `trigo_irrigado` (Trigo Irrigado) | 12017100000051 | `trigo` (Trigo) | `manejo == "Irrigado"` | 12017100000031 |
| `mamona_semiarido_sequeiro` (Mamona Semi-árido Sequeiro) | 12014720000011 | `mamona` (Mamona) | `cultura_codigo == "12014720000011"` | 12014720000011 |

Para juntar safras, use o `cultura_codigo` onde ele se mantém (milho, feijão, mamona do semiárido e as versões de sequeiro) e o `manejo` nas versões irrigadas, cujo código muda. Em 2024/2025, `feijao` é o feijão da 1ª safra, não o total, e `mamona` passa a incluir a mamona do semiárido: são 51.784 registros em 2023/2024 e 62.441 em 2024/2025, dos quais 10.657 do semiárido.

## Culturas legadas e identidade dos registros

O catálogo de filtros inclui `Arroz Sequeiro`/`arroz_sequeiro` e `Trigo Sequeiro`/`trigo_sequeiro`, publicados na tábua de 2016/2017. Esses aliases conservam os valores já retornados pelo parser e não são convertidos para `arroz`/`trigo`. Nomes desconhecidos continuam sendo recusados antes da rede; a presença de cada cultura depende da tábua consultada.

As 59 colunas do contrato 2.1 preservam os 55 campos publicados, além de cultura normalizada, safra derivada, posição no CSV e `cod_municipio` (o `geocodigo` em inteiro). Registros repetidos são mantidos. A posição `registro_origem` é válida somente junto a `meta.raw_content_hash`: os três corpos de 18/09/2026 tinham SHA diferente dos de 07/09, com os mesmos registros em outra ordem. Uma alteração de SHA não demonstra mudança dos valores.

Aquisição UTC e hash do corpo permanecem em `meta.fetched_at`, `meta.raw_content_hash` e `meta.source_details["resource"]`, inclusive no cache. A leitura até EOF comprova que o corpo recebido foi processado, sem certificar total externo de municípios ou snapshot transacional. O catálogo CKAN consultado pela API declara frequência semanal, enquanto o dicionário PDF declara diária. O dataset usa `update_frequency="weekly"`, tomando o catálogo ativo de descoberta como referência operacional; a declaração conflitante do PDF permanece registrada. Nenhuma das declarações comprova a cadência efetiva de revisão de cada safra. Produtividade e códigos NM são preservados literalmente, sem inferir unidade ausente no dicionário.
