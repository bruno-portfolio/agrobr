# O que o agrobr grava no disco

Quase tudo fica na pasta de cache: `~/.agrobr/cache` por padrão, ou a pasta da variável
`AGROBR_CACHE_CACHE_DIR`. A exceção são os snapshots, em `~/.agrobr/snapshots` (ou no
`snapshot_path` do `agrobr.config.set_mode`).

Apagar qualquer arquivo da tabela abaixo é seguro: na próxima consulta, o agrobr baixa de novo o que
precisar. A CLI da 2.0 não tem comando de limpeza.
Apague com o gerenciador de arquivos ou com `rm`.

## Na pasta de cache

| Fonte | Caminho | O que é | Validade e tamanho | Como limpar |
|---|---|---|---|---|
| CEPEA | `agrobr.duckdb` (o nome vem de `AGROBR_CACHE_DB_NAME`) | banco DuckDB com os indicadores já coletados, a cobertura da série histórica, a quarentena das migrações (`indicadores_quarentena`) e o histórico do health check com estado (`agrobr.health.run_checks_with_state`) | vale até a virada das 18h do CEPEA; sem teto: cresce com cada produto e período consultado | apague o arquivo. A quarentena das migrações vai junto: veja a [preservação do cache](../guides/migracao-2.md#18-preservacao-automatica-do-cache-existente) antes |
| CEPEA, banco danificado | `agrobr.duckdb.corrompido-<AAAAMMDDHHMM>` (e `agrobr.duckdb.wal.corrompido-<AAAAMMDDHHMM>`, se havia WAL) | o banco que o DuckDB não conseguiu ler, movido para o lado (hora em UTC); a consulta segue, e um banco novo é criado | não expira | apague quando não precisar mais dele |
| ZARC | `zarc_tabuas.duckdb` | as tábuas de risco já validadas, por revisão | 24 horas desde a aquisição; guarda até 3 revisões, e a mais antiga sai | apague o arquivo |
| ANEC | `anec/<ano>/week_<NN>/shipment.pdf` e `meta.json` | o PDF da semana e os metadados (SHA-256 e a data de publicação da ANEC) | baixado de novo quando a ANEC publica versão mais nova da semana; sem teto: 1 PDF (cerca de 0,9 MB) por semana consultada | apague a pasta `anec/` ou a da semana. `AGROBR_ANEC_CACHE_DISABLED=1` desliga o cache |
| Acervo Fundiário | `acervo_fundiario/<tema>/<UF>.zip` (ou `brasil.zip`) e o `.json` ao lado | o ZIP do tema e os metadados (`etag`, `last_modified`, SHA-256) | revalidado por HEAD a cada consulta; sem teto: 1 ZIP por tema e UF | apague a pasta do tema |
| RNC | `rnc/registradas.acquisition.v1.zip` e `rnc/protegidas.acquisition.v1.zip` | o CSV bruto e o manifesto | 24 horas desde a aquisição; 1 arquivo por família, sobrescrito | apague a pasta `rnc/`. `use_cache=False` não lê nem grava |
| Agrofit (defensivos) | `defensivos/formulados.v3.zip` e `defensivos/tecnicos.v3.zip` | as tabelas e o manifesto | 24 horas; 1 arquivo por tipo, sobrescrito | apague a pasta `defensivos/` |

Se o processo morrer no meio de uma gravação, pode sobrar um `.<nome>.<letras>.tmp` na pasta do
arquivo. É seguro apagar.

## Snapshots

`~/.agrobr/snapshots/<nome>/` guarda os snapshots que você cria (`agrobr snapshot create`). Eles não
expiram. Para apagar: `agrobr snapshot delete <nome>`. Veja o [guia de snapshots](../guides/snapshots.md).

## O que não vai para o disco

- O ZIP histórico do INMET fica em memória, com teto de 256 MB: 1 hora para o ano corrente e 24 horas
  para ano fechado.
- A ComexStat, o PSR e a ANTT baixam para arquivo temporário do sistema, apagado ao fim da consulta.
- Os catálogos (ZARC, custos da CONAB) ficam em memória por 1 hora.
- O agrobr não grava log em arquivo nem credencial: o `AGROBR_INMET_TOKEN` e as chaves de API ficam só
  no ambiente.

## Banco do CEPEA danificado

Uma queda de energia, um disco cheio ou um antivírus no meio da gravação podem deixar o `agrobr.duckdb`
ilegível. Quando o DuckDB acusa o arquivo (leitura incompleta, checksum ou arquivo inválido), o agrobr
move o banco para o lado, avisa uma vez com os 2 caminhos (`UserWarning`) e segue sem cache; a
consulta seguinte cria um banco novo. Arquivo em uso por outro processo e disco cheio não movem nada:
a operação segue sem cache e tenta de novo na próxima. Se o arquivo não puder ser movido, o aviso é o
de cache indisponível, e a saída é fechar os outros processos do agrobr e apagar o arquivo à mão.
