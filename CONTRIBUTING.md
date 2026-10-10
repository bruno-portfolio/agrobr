# Contribuindo para o agrobr

Obrigado pelo interesse em contribuir! O agrobr é um projeto open source e toda contribuição é bem-vinda — bug reports, melhorias de documentação, novos datasets, novas fontes de dados ou correções de código.

Ao participar, você concorda com nosso [Código de Conduta](CODE_OF_CONDUCT.md).

## Como contribuir

### Reportando bugs

1. Verifique se já não foi reportado nas [Issues](https://github.com/bruno-portfolio/agrobr/issues)
2. Abra uma nova issue com:
   - Título descritivo
   - Passos para reproduzir
   - Comportamento esperado vs atual
   - Versão do Python, do agrobr e do SO
   - Traceback completo (se aplicável)

### Sugerindo melhorias

Abra uma issue descrevendo o caso de uso e a proposta antes de começar a implementar. Isso evita trabalho duplicado e garante alinhamento com a direção do projeto.

### Pull Requests

1. Fork o repositório e crie uma branch: `git checkout -b feature/descricao-curta`
2. Faça commits com mensagens claras no formato `tipo(escopo): mensagem`
3. Escreva testes para toda funcionalidade nova
4. Certifique-se que `ruff check`, `ruff format --check` e `pytest` passam
5. Abra um Pull Request com descrição do que muda e por quê

Para mudanças grandes, abra uma issue primeiro para discussão.

## Setup de desenvolvimento

```bash
git clone https://github.com/bruno-portfolio/agrobr.git
cd agrobr

python -m venv .venv
source .venv/bin/activate   # Linux/Mac
# .venv\Scripts\activate    # Windows

pip install -e ".[dev]"

pre-commit install
```

Extras opcionais conforme necessidade:

```bash
pip install -e ".[polars]"    # Suporte a Polars
pip install -e ".[pdf]"       # pdfplumber (ANDA)
pip install -e ".[browser]"   # Playwright (fontes com JS)
pip install -e ".[geo]"       # GeoPandas (PRODES, DETER, SICAR)
pip install -e ".[bigquery]"  # Base dos Dados (fallback BCB)
```

## Rodando testes

```bash
pytest                        # Unitários sem rede (exceto slow/benchmark/integration)
pytest -x -q                  # Fail-fast
pytest --cov=agrobr           # Com cobertura
pytest -m integration         # Apenas integração (requer rede)
pytest -m slow                # Testes lentos
pytest tests/test_cepea/      # Apenas um módulo
```

Os testes unitários usam `pytest-socket` para bloquear sockets de rede; sockets Unix
continuam permitidos para o event loop assíncrono. No Windows, o par local de
`socketpair` permite o canal interno do `asyncio`, mantendo bloqueados os demais
sockets TCP/UDP e a resolução DNS. Testes marcados como `integration`
habilitam a rede explicitamente. Complete os mocks de transporte dos testes unitários
quando uma consulta auxiliar for adicionada.

Para verificar a leitura decimal e os controles da calculadora, use Node.js 24:

```bash
node --test calculadora/tests/*.test.mjs
```

O coverage gate é **85%**. PRs que reduzam cobertura significativamente precisam de justificativa.

## Linting e formatação

O projeto usa **ruff** para linting e formatação, e **mypy** em modo strict:

```bash
ruff check agrobr/ tests/          # Linting
ruff format agrobr/ tests/         # Formatação
mypy agrobr/                       # Type checking (strict)
```

Pre-commit hooks rodam `ruff` e `mypy` automaticamente em cada commit. Se preferir rodar manualmente:

```bash
pre-commit run --all-files
```

Os hooks `trailing-whitespace`, `end-of-file-fixer` e `check-added-large-files` ignoram `tests/golden_data/`: o golden
guarda os bytes publicados pela fonte, e o hook não pode reescrevê-los nem barrar o tamanho deles.

## Padrões de código

### Geral

- **Type hints obrigatórios** em todo código de produção
- **Sem comentários no código** — o código deve ser autoexplicativo
- **Docstrings Google style** apenas quando a assinatura + nome da função não são autoexplicativos
- **Line length**: 100 caracteres (ruff)
- **Python**: 3.11+ (`from __future__ import annotations` quando necessário)

### Imports

```python
# Correto
from agrobr import cepea
from agrobr.exceptions import ParseError

# Evitar
from agrobr.cepea.api import indicador
```

- `from modulo import submodulo` (não classe direta)
- Sempre no topo do arquivo
- Lazy imports dentro de fetchers de datasets (evita circular imports)

### Exceções

Use exceções específicas. O `except Exception` genérico fica só como última barreira, como na cascata de fallback do
`BaseDataset`, que registra o erro e passa para a próxima fonte:

- `httpx.HTTPError` / `httpx.TimeoutException` → erros de rede
- `ParseError` → layout da fonte mudou
- `ContractViolationError` → contrato quebrado

### Testes

- Um arquivo por módulo: `tests/test_<fonte>/test_<arquivo>.py`
- Nomes: `test_<funcionalidade>_<cenario>`
- Mocke chamadas HTTP com helpers de `tests/helpers.py`
- Golden data em `tests/golden_data/<fonte>/<caso>/`
- Múltiplas asserções por teste são OK
- `-> None` não necessário em métodos de teste

Remover testes exige prova de que o que fica ainda pega o defeito:

- sem oráculo independente (o dado publicado pela fonte, conferido fora do código), não há corte;
- o teste que fica tem de pegar bug de rótulo, recorte, unidade ou valor: um mutante de `scripts/mutacao_testes.py`
  que produz esse bug precisa falhar numa asserção, não numa exceção qualquer;
- todo corte passa pelo gate de cobertura (`--cov-branch`, antes × depois): nenhuma linha ou ramo do `agrobr`
  deixa de ser coberto sem destino declarado;
- o executor de mutantes roda numa cópia congelada da árvore (`--root`), nunca na árvore de trabalho.

## Estrutura do projeto

```
agrobr/
├── agrobr/
│   ├── __init__.py            # Namespace público
│   ├── constants.py           # Fonte (enum), URLs, Settings
│   ├── exceptions.py          # Hierarquia de exceções
│   ├── models.py              # Indicador, Safra, MetaInfo
│   ├── config.py              # set_mode(), get_config(), AgrobrConfig
│   ├── sync.py                # Wrapper sync sobre APIs async
│   ├── cli.py                 # CLI Typer
│   │
│   ├── cepea/                 # ── Fontes autônomas ──
│   ├── conab/                 #    Cada fonte tem:
│   ├── ibge/                  #    client.py → parser.py → models.py → api.py
│   ├── usda/                  #
│   ├── b3/                    #    41 fontes no total
│   ├── ...                    #
│   │
│   ├── alt/                   # Fontes alternativas (anp_diesel, antt, mapa_psr, sicar)
│   │
│   ├── datasets/              # ── Camada semântica ──
│   │   ├── base.py            #    BaseDataset (fallback, contrato, meta)
│   │   ├── registry.py        #    Auto-descoberta de datasets
│   │   ├── deterministic.py   #    Modo determinístico (contextvars)
│   │   └── *.py               #    55 datasets
│   │
│   ├── contracts/             # Schema contracts + validação
│   ├── schemas/               # JSON schemas gerados
│   ├── http/                  # Retry, rate limiter, user agents
│   ├── cache/                 # DuckDB cache (CEPEA indicadores)
│   ├── normalize/             # Encoding, numérico, datas, regiões
│   ├── utils/                 # Helpers compartilhados
│   └── validators/            # Validação sanity/estrutural
│
├── tests/                     # Suíte de testes
│   ├── conftest.py            # Fixtures globais
│   ├── helpers.py             # Factories compartilhadas
│   ├── test_golden.py         # Golden data (non-regression)
│   ├── golden_data/           # Fixtures por fonte
│   └── test_<modulo>/         # Testes por módulo
│
├── docs/                      # MkDocs + mkdocs-material
├── examples/                  # Notebooks e scripts de exemplo
└── pyproject.toml             # Build, deps, ruff, mypy, pytest
```

## Adicionando nova fonte de dados

### 1. Módulo da fonte (`agrobr/<fonte>/`)

```
agrobr/<fonte>/
    __init__.py         # __all__ = a API pública da fonte (docs/api/index.md)
    client.py           # httpx.AsyncClient com retry/timeout
    parser.py           # Parsing do response → DataFrame
    models.py           # Modelos Pydantic v2 (se necessário)
    api.py              # Funções públicas async com @overload
```

Padrão da API pública em `api.py`:

```python
@overload
async def minha_funcao(produto: str, *, return_meta: Literal[False] = ...) -> pd.DataFrame: ...
@overload
async def minha_funcao(produto: str, *, return_meta: Literal[True]) -> tuple[pd.DataFrame, MetaInfo]: ...

async def minha_funcao(produto, *, as_polars=False, return_meta=False):
    t0 = time.monotonic()
    raw = await client.fetch(produto)
    fetch_ms = int((time.monotonic() - t0) * 1000)

    t1 = time.monotonic()
    df = parser.parse(raw)
    parse_ms = int((time.monotonic() - t1) * 1000)

    meta = build_source_meta("fonte", url, "httpx", fetch_ms, parse_ms, df, parser_version=1)
    return finalize_result(df, meta, as_polars=as_polars, return_meta=return_meta)
```

### 2. Contrato (`agrobr/contracts/`)

Defina colunas, PK e garantias:

```python
MEU_CONTRATO = Contract(
    name="meu_dataset",
    version="1.0",
    columns=[Column("produto", ColumnType.STRING), ...],
    primary_key=["produto", "data"],
    guarantees=["Column names never change"],
)
register_contract("meu_dataset", MEU_CONTRATO)
```

### 3. Dataset (`agrobr/datasets/<nome>.py`)

1. Fetcher com lazy import da fonte
2. `DatasetInfo` com sources, produtos, metadata
3. Subclasse de `BaseDataset` com `fetch()` e `_normalize()`
4. `register()` no module level
5. Função pública async que delega ao singleton

### 4. Wiring

- Import em `agrobr/datasets/__init__.py`
- Adicionar ao `__all__`
- Gerar os JSON schemas com `contracts.generate_json_schemas("agrobr/schemas")`

### 5. Testes

- Testes unitários em `tests/test_<fonte>/`
- Testes de dataset em `tests/test_datasets/test_<nome>.py`
- Golden data em `tests/golden_data/<fonte>/<caso>/` (metadata.json + response + expected.json)

### 6. Licença

Verifique e documente em `docs/licenses.md` antes do merge:

| Classificação | Ação |
|---------------|------|
| `livre` | Uso irrestrito |
| `nc` | `warnings.warn()` na primeira chamada |
| `zona_cinza` | `warnings.warn()` na primeira chamada |
| `restrito` | Não entra em fallback automático, salvo exceção declarada em `docs/licenses.md` |

### 7. Documentação

- API reference em `docs/api/<fonte>.md`
- Contract doc em `docs/contracts/<dataset>.md`
- Source doc em `docs/sources/<fonte>.md`
- Atualizar `mkdocs.yml`, `README.md`, `docs/index.md` e `CHANGELOG.md`
- Acrescentar o módulo à tabela de fontes da [API pública](docs/api/index.md): o que está no `__all__` dele passa a
  ter garantia de SemVer

## Gates de publicação

`publish.yml` chama o workflow local `tests.yml` do mesmo commit antes do build.
Esse gate reúne Python 3.11–3.13, dependências mínimas, Windows, Ruff, mypy,
calculadora, documentação estrita e instalação somente core. O pacote é
construído uma vez; o smoke de instalação e os destinos de publicação baixam
esse mesmo artefato por ID, com verificação de digest.

O workflow semanal de integração produz log, JUnit e `live_matrix.json`.
Indisponibilidade inesperada falha. Exceções conhecidas ficam registradas por
dataset: USDA sem chave e HTTP 522 no arquivo ANTAQ de 2024. Uma execução da
matriz sem nenhum dataset validado também falha, mesmo que todas as consultas
selecionadas tenham uma exceção conhecida. Revise a exceção quando a fonte
voltar a responder; violações de contrato e erros de parser não são exceções.

Uma execução local reproduz o registro de cobertura com:

```bash
pytest tests/integration/test_datasets_live.py -m integration --live-matrix-report=live_matrix.json --junitxml=live_matrix.xml
```

## Reconciliação semanal

O workflow `reconciliacao.yml` roda toda segunda-feira (09:00 UTC) os `scripts/reconciliar_*.py`
contra as fontes oficiais, um por vez, e dá a cada fonte um de cinco estados:

- `ok`;
- `mismatch`: a saída do agrobr ou a estrutura da fonte divergiu do registrado;
- `indisponível`: a fonte está fora do ar ou bloqueou o acesso;
- `não verificado`: falta a credencial, ou o script está fora da execução semanal (sem modo ao
  vivo, por exemplo). Nunca conta como sucesso;
- `erro do script`.

O resumo (`resumo.json` e `resumo.md`) fica como artefato da execução. O job falha só com
`mismatch` ou `erro do script`. Para rodar localmente uma fonte, ou todas:

```bash
python -m scripts.reconciliacao_semanal imea
python -m scripts.reconciliacao_semanal
```

O resumo e o JSON completo de cada script, com os problemas encontrados, ficam em
`reports/reconciliacao_semanal/`.

Cada fonte com `mismatch` abre a issue "Reconciliação semanal: mismatch em <fonte>". Enquanto
ela estiver aberta, as execuções seguintes comentam nela. A issue traz só o estado, a contagem,
os identificadores dos casos divergentes e o link do artefato. O detalhe, que pode ter valores
da fonte, sai da execução local.

Um script novo em `scripts/reconciliar_<fonte>.py` entra sozinho, rodado com `--output`.
Argumentos extras, credencial e scripts fora da execução semanal ficam declarados no topo de
`scripts/reconciliacao_semanal.py`.

## Commits

- Formato: `tipo(escopo): mensagem`
- Tipos: `feat`, `fix`, `docs`, `refactor`, `test`, `ci`, `chore`
- Mensagens em inglês ou português, modo imperativo
- Referencie issues quando aplicável: `Fix #123`

## Dúvidas?

- Abra uma issue com a label `question`
- Ou abra uma [Discussion](https://github.com/bruno-portfolio/agrobr/discussions)
