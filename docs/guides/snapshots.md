# Snapshots e modo determinístico

Snapshots exportam conjuntos de dados para arquivos Parquet. O modo determinístico é um recurso separado: `preco_diario` aplica corte de data e execução offline, enquanto outros datasets podem usar o contexto apenas para selecionar um ano ou registrar proveniência. Não há garantia global de execução sem rede para todos os datasets.

Em `clima`, o ano do contexto é o padrão do modo UF quando `ano` é omitido. Os dados não são truncados na data do snapshot e as revisões INMET/NASA não são congeladas; o modo estação mantém `inicio`/`fim`. `meta.source_details.deterministic` explicita essas limitações. A exportação por `create_snapshot()` continua limitada às fontes listadas abaixo e não inclui ZIPs climáticos.

## Criando um snapshot

Instale uma engine Parquet: `pip install "pyarrow>=14.0.1"` (ou `fastparquet`). O extra `agrobr[polars]` já inclui o `pyarrow` com esse piso. Sem engine, a criação levanta `ImportError` antes de criar diretórios.

Somente `cepea`, `conab` e `ibge` são aceitas em `sources`; outras fontes levantam `ValueError`. Se nenhuma coleta produzir arquivos, o diretório recém-criado é removido e `SnapshotError` informa os erros por fonte. A CLI encerra com código 1. Snapshots parciais são preservados, com os erros em `manifest.json`, no campo `metadata.errors`.

### Programaticamente

A coleta ocorre em um diretório temporário dentro da raiz de snapshots. O nome final só aparece após a gravação do manifesto; cancelamentos e falhas de publicação limpam o temporário, permitindo repetir o nome. Snapshots existentes não são sobrescritos. Uma interrupção abrupta do processo ou do sistema pode deixar um temporário oculto: ele não é listado como snapshot concluído e pode ser inspecionado antes de uma remoção manual.

Snapshots novos registram SHA-256 por arquivo. `load_from_snapshot()` confere esse hash antes de ler o Parquet e levanta `SnapshotError` em caso de divergência. Snapshots legados sem hash continuam legíveis, sem essa garantia de integridade; hashes detectam alteração dos arquivos, mas não autenticam a origem dos dados.

**Carregue só snapshot de origem confiável.** O `pyarrow` anterior ao 14.0.1 executa código ao ler um Parquet malicioso (CVE-2023-47248), e é por isso que o piso é 14.0.1: com um `pyarrow` mais antigo instalado (por outro pacote, já que o core do agrobr não depende dele), `load_from_snapshot()` levanta `ImportError` antes de ler e manda atualizar. O SHA-256 confere os arquivos contra o `manifest.json` do próprio snapshot, que vem junto com eles: quem recebe um snapshot de outra pessoa confere o SHA-256 do `manifest.json` contra o que o autor publicou por outro canal.

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
agrobr snapshot list --formato json
```

## Modo determinístico em `preco_diario`

O context manager força `preco_diario` a consultar o cache DuckDB do CEPEA com `offline=True` e limita `fim` à data do snapshot. Se o usuário informar um `fim` anterior, essa data prevalece. Sem o produto no cache, levanta `SourceUnavailableError`.

```python
from agrobr import datasets

async with datasets.deterministic("2025-12-31"):
    df = await datasets.preco_diario("soja")
    historico = await datasets.preco_diario("milho", fim="2025-06-30")
```

O contexto não garante modo offline para os demais datasets; cada contrato define seu comportamento. Quem consulta a fonte corrente avisa em `validation_warnings` e com `UserWarning` que o dado não é o da data. `cadastro_rural` rejeita `deterministic` antes da rede, pois os filtros SICAR consultam registros correntes e não recuperam versões históricas do cadastro. O context manager usa `contextvars`, portanto o estado é seguro entre threads e tarefas assíncronas.

Os [quatro datasets Agrofit](../api/defensivos_datasets.md) também recusam esse contexto, antes de cache ou HTTP. Os CSVs são exportações correntes, e o bundle de cache identifica a coleta original; nenhum deles reconstitui arbitrariamente o cadastro de outra data. A ausência de contexto permite uso normal do cache, sem atribuir um snapshot histórico.

O dataset [`empregadores_lista_suja`](../api/empregadores_lista_suja.md) também recusa `deterministic` antes de I/O. Ele consulta a publicação corrente do MTE sem cache persistente; ID local do registro, data de atualização e hash não recuperam arbitrariamente uma edição histórica. Os metadados retornam `snapshot=None`.

O dataset [`uso_do_solo`](../contracts/uso_do_solo.md) recusa `deterministic` antes de I/O em todos os modos. `colecao=10` ou `11` seleciona uma edição MapBiomas, mas não reconstitui o arquivo disponível em uma data arbitrária. Cada chamada adquire a publicação correspondente e retorna `snapshot=None`; seus hashes identificam os recursos recebidos.

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

A CLI não tem comando para ativar um snapshot: o `snapshot use` da 1.x saiu na 2.0, porque não alterava execuções futuras. Para ler um snapshot, use `load_from_snapshot()`; o modo determinístico não lê os arquivos do snapshot.

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
- Use nomes descritivos, como `2025-Q4` ou `paper-submission-v2`. Os nomes reservados do Windows (`CON`, `NUL`, `COM1`…)
  e os terminados em ponto são recusados em todo sistema, para o snapshot abrir no Windows.
- Em CI, crie o snapshot uma vez e reutilize os mesmos arquivos.
