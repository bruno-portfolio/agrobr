# Linha de comando (CLI)

O comando `agrobr` vem com o pacote. Fora do PATH, use `python -m agrobr`.

```bash
agrobr --version
agrobr cepea indicador soja --inicio 2024-01-01 --formato csv > soja.csv
agrobr health --formato json
```

## Convenções

- **Opções globais**, antes do comando: `--version` (`-v`) mostra a versão e sai; `--verbose` manda os logs `INFO` para a
  saída de erro.
- **Formato:** `--formato` (`-o`) é a única opção de formato. Nos comandos de dados, aceita `table` (padrão), `csv` e `json`;
  no `health`, no `doctor` e no `snapshot list`, aceita `text` (padrão) e `json`.
- **Saída:** os dados vão para a saída padrão, em UTF-8, também redirecionada no Windows. O aviso de progresso
  (`Consultando ...`) e as mensagens de erro vão para a saída de erro. O `json` sai como lista de registros, com as datas em
  ISO 8601; o `csv` sai sem índice. Sem dados, só a `table` imprime `Nenhum dado encontrado`: o `csv` sai só com o
  cabeçalho, e o `json`, com a lista vazia (`[]`).
- **Código de saída:**
  - `0`: deu certo;
  - `1`: a consulta falhou, ou a função recusou um valor que a CLI repassou sem conferir, com a mensagem `Erro: ...` na
    saída de erro (por exemplo, `ibge pam soja --nivel invalid`, `ibge lspa soja --mes 13` e `--ultimo` junto de
    `--inicio`); ou o `health`/`doctor` achou fonte com falha;
  - `2`: a CLI recusou o uso antes de chamar a função: comando ou opção inexistente, valor fora da lista (`--formato`,
    `--pesquisa`, `--source`), `--levantamento` menor que 1, `--ano` ou `--mes` que não é número.

## Dados

| Comando | Equivale a | Opções |
|---|---|---|
| `agrobr cepea indicador <produto>` | `cepea.indicador` | `--inicio`/`-i` e `--fim`/`-f` (`AAAA-MM-DD`), `--praca`, `--ultimo`/`-u`, `--formato` |
| `agrobr conab safras <produto>` | `conab.safras` | `--safra`/`-s` (`2025/26`), `--uf`/`-u`, `--levantamento` (≥ 1), `--formato` |
| `agrobr conab balanco [produto]` | `conab.balanco` | `--safra`/`-s`, `--levantamento` (≥ 1), `--formato` |
| `agrobr ibge pam <produto>` | `ibge.pam` | `--ano`/`-a` (`2023` ou `2020,2021,2022`), `--uf`/`-u`, `--nivel`/`-n` (`brasil`, `uf` ou `municipio`; padrão `uf`), `--formato` |
| `agrobr ibge lspa <produto>` | `ibge.lspa` | `--ano`/`-a`, `--mes`/`-m` (1–12), `--uf`/`-u`, `--formato` |
| `agrobr ibge censo-historico <tema>` | `ibge.censo_agro_historico` | `--ano`/`-a` (um ano ou lista), `--uf`/`-u`, `--nivel`/`-n` (`brasil`, `regiao` ou `uf`; padrão `uf`), `--formato` |
| `agrobr ibge censo-municipal-1985 <tema>` | `ibge.censo_agro_municipal_1985` | `--uf`/`-u`, `--nivel`/`-n` (`uf`, `mesorregiao`, `microrregiao` ou `municipio`), `--formato` |

`--ultimo` devolve o último indicador publicado, como `cepea.ultimo`, e não combina com `--inicio`/`--fim`. Com ele, a linha
tem as mesmas colunas e os mesmos tipos da série.

## Catálogos

| Comando | Lista | Opções |
|---|---|---|
| `agrobr conab produtos` | os produtos aceitos pelos comandos da CONAB (`conab.produtos`) | — |
| `agrobr conab levantamentos` | os 10 primeiros levantamentos de `conab.levantamentos` e quantos faltam | — |
| `agrobr ibge produtos` | os produtos da PAM (`ibge.produtos_pam`) ou do LSPA (`ibge.produtos_lspa`) | `--pesquisa`/`-p` (`pam` ou `lspa`; padrão `pam`) |
| `agrobr ibge temas-historico` | os temas do `censo-historico` (`ibge.temas_censo_agro_historico`) | — |
| `agrobr ibge temas-municipal-1985` | os temas do `censo-municipal-1985` (`ibge.temas_censo_agro_municipal_1985`) | — |

## Diagnóstico

| Comando | O que faz | Opções |
|---|---|---|
| `agrobr health` | testa a conexão e a resposta de cada fonte; sai com `1` se alguma falhar | `--source`/`-s` (uma fonte, pelo nome do módulo: `cepea`, `mapa_psr`...), `--deep`/`-d`, `--formato` (`text` ou `json`) |
| `agrobr doctor` | estado das fontes, cache local e próxima atualização; sai com `1` se alguma fonte der erro | `--verbose`/`-v`, `--formato` (`text` ou `json`) |
| `agrobr config show` | pasta do cache, nome do banco, timeout de leitura e total de tentativas em vigor ([variáveis de ambiente](ambiente.md)) | — |

`--deep` só muda o CEPEA: compara o fingerprint da página com a baseline do pacote e faz o parse. No `doctor`, `-v` é o
`--verbose`; antes do comando, `-v` é o `--version`.

## Snapshots

| Comando | O que faz | Opções |
|---|---|---|
| `agrobr snapshot list` | lista os snapshots salvos | `--formato` (`text` ou `json`) |
| `agrobr snapshot create [nome]` | cria um snapshot (nome padrão: a data de hoje, `AAAA-MM-DD`); precisa do `pyarrow` | `--sources`/`-s` (`cepea`, `conab` e `ibge`, separados por vírgula; padrão: as 3) |
| `agrobr snapshot delete <nome>` | remove um snapshot; pede confirmação | `--force`/`-f` (sem confirmação) |

O `json` do `snapshot list` é uma lista com `name`, `created_at` (ISO 8601), `size_mb`, `sources` e `files`.

A CLI não ativa snapshot para execuções futuras. Para ler um snapshot, use o Python: `load_from_snapshot` ou o modo
determinístico (`datasets.deterministic("AAAA-MM-DD")`), no [guia de snapshots](../guides/snapshots.md).

## Mudanças da 2.0

| 1.x | 2.0 |
|---|---|
| `agrobr health --output json` | `agrobr health --formato json` |
| `agrobr doctor --json` | `agrobr doctor --formato json` |
| `agrobr snapshot list --json` | `agrobr snapshot list --formato json` |
| `agrobr snapshot use <nome>` | removido: não ativava nada. Use `load_from_snapshot(..., snapshot_name=<nome>)` |

As opções antigas saem com código `2`. O resto da migração está no [guia da 2.0](../guides/migracao-2.md).
