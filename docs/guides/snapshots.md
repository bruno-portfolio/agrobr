# Snapshots e modo determinístico

Snapshots exportam conjuntos de dados para arquivos Parquet. O modo determinístico é um recurso separado e, atualmente, atua somente no dataset `preco_diario`. Não há garantia global de execução sem rede para todos os datasets.

## Criando um snapshot

### Programaticamente

```python
from agrobr.snapshots import create_snapshot

info = await create_snapshot()

info = await create_snapshot(
    "2025-Q4",
    sources=["cepea", "conab", "ibge"],
)
print(info.name, info.path, info.file_count)
```

`create_snapshot()` cobre somente estas fontes e arquivos:

| Fonte | Arquivos | Cobertura |
|---|---|---|
| CEPEA | `cepea/<produto>.parquet` | Um arquivo por produto disponível no cache DuckDB; a coleta usa `indicador(..., offline=True)` |
| CONAB | `conab/safras.parquet`, `conab/balanco.parquet` | Safras somente de soja e balanço de oferta e demanda |
| IBGE | `ibge/pam.parquet`, `ibge/lspa.parquet` | PAM e LSPA somente de soja |

A criação pode acessar a rede nas coletas da CONAB e do IBGE. Na CEPEA, apenas dados já presentes no cache DuckDB são exportados.

### CLI

```bash
agrobr snapshot create
agrobr snapshot create 2025-Q4 --sources cepea,conab,ibge
```

Sem nome explícito, a data atual é usada. Os snapshots ficam em `~/.agrobr/snapshots/<nome>/`, salvo quando outro diretório é configurado.

## Listando snapshots

```python
from agrobr.snapshots import list_snapshots

for snapshot in list_snapshots():
    size_mb = snapshot.size_bytes / 1024 / 1024
    print(f"{snapshot.name} — {snapshot.file_count} arquivos, {size_mb:.1f} MB")
    print(f"  Fontes: {', '.join(snapshot.sources)}")
    print(f"  Criado em: {snapshot.created_at}")
```

```bash
agrobr snapshot list
agrobr snapshot list --json
```

## Modo determinístico em `preco_diario`

O context manager força `preco_diario` a consultar o cache DuckDB do CEPEA com `offline=True` e limita `fim` à data do snapshot. Se o usuário informar um `fim` anterior, essa data prevalece.

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
    historico = await datasets.preco_diario("milho", fim="2025-06-30")
```

Os demais datasets não consultam esse modo e podem acessar a rede normalmente. O context manager usa `contextvars`, portanto o estado é seguro entre threads e tarefas assíncronas.

### Decorator

```python
from agrobr import datasets
from agrobr.datasets.deterministic import deterministic_decorator

@deterministic_decorator("2025-12-31")
async def meu_pipeline():
    return await datasets.preco_diario("soja")
```

### Configuração de snapshots

```python
from agrobr.config import set_mode
from agrobr.snapshots import load_from_snapshot

set_mode("deterministic", snapshot="2025-12-31")
df = load_from_snapshot("cepea", "soja")

set_mode("normal")
```

O argumento `snapshot` define o nome padrão usado por `load_from_snapshot()`. `snapshot_path` define o diretório base usado tanto na criação quanto na leitura. `set_mode()` não ativa o context manager de `datasets`, e o campo `network_enabled` da configuração não bloqueia requests HTTP.

O comando `agrobr snapshot use <nome>` apenas valida a existência do snapshot e mostra como configurar o processo Python; ele não altera execuções futuras.

## Carregando dados de um snapshot

```python
from agrobr.snapshots import load_from_snapshot

df = load_from_snapshot("cepea", "soja", snapshot_name="2025-Q4")
```

O segundo argumento é o nome real do arquivo sem a extensão `.parquet`.

## Estrutura no disco

```text
~/.agrobr/snapshots/
  2025-Q4/
    manifest.json
    cepea/
      soja.parquet
      milho.parquet
      <produto>.parquet
    conab/
      safras.parquet
      balanco.parquet
    ibge/
      pam.parquet
      lspa.parquet
```

## Removendo snapshots

```python
from agrobr.snapshots import delete_snapshot

delete_snapshot("2025-Q4")
```

```bash
agrobr snapshot delete 2025-Q4
agrobr snapshot delete 2025-Q4 --force
```

## Boas práticas

- Confirme quais fontes e produtos foram gravados no `manifest.json`.
- Não trate o modo determinístico como bloqueio global de rede.
- Use nomes descritivos, como `2025-Q4` ou `paper-submission-v2`.
- Em CI, crie o snapshot uma vez e reutilize os mesmos arquivos.
