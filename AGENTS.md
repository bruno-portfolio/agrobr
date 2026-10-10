# AGENTS.md

Guia curto para agentes de código (e pessoas) que trabalham neste repositório. O guia completo está no
[CONTRIBUTING.md](CONTRIBUTING.md). Aqui fica o que é preciso para não quebrar a CI nem o contrato dos dados.

## O projeto

- O `agrobr` é uma biblioteca Python (3.11+) de dados agrícolas brasileiros. Cada fonte oficial (CONAB, IBGE, CEPEA, BCB,
  INMET, …) tem um módulo próprio, e `agrobr.datasets` expõe a camada semântica, com contratos versionados.
- Async primeiro (`httpx.AsyncClient`); `agrobr.sync` oferece a mesma API em versão síncrona.
- A documentação fica em `docs/` (MkDocs Material, PT e EN) e é publicada em https://www.agrobr.dev/docs/.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev,pdf,geo]"     # o mesmo conjunto da CI
pre-commit install
```

Extras opcionais: `polars`, `pdf`, `geo`, `browser`, `bigquery` e `docs` (ver `pyproject.toml`).

## O que a CI roda

```bash
ruff check agrobr/ tests/ scripts/ examples/
ruff format --check agrobr/ tests/ scripts/ examples/
mypy agrobr/
pytest tests/ -n auto --disable-socket --allow-unix-socket -m "not integration and not benchmark" --cov=agrobr
python -m mkdocs build --strict            # requer o extra docs
node --test calculadora/tests/*.test.mjs   # Node.js 24
```

- O mypy roda em modo strict, e a cobertura mínima é 85%.
- O `pytest` sem argumentos já bloqueia a rede e pula `slow`, `benchmark` e `integration`. Teste que precisa de rede leva o
  marcador `integration`.
- A CI roda também:
  - Python 3.11, 3.12 e 3.13;
  - as dependências mínimas (`scripts/constraints-minimum.txt`);
  - Windows;
  - a instalação só do core, fora do checkout;
  - `pip check` e `pip-audit` (auditoria de vulnerabilidades das dependências).

## Arquitetura

- `agrobr/<fonte>/`: `client.py` (HTTP) → `parser.py` → `models.py` (Pydantic v2) → `api.py` (funções públicas async).
  O parsing fica na fonte, nunca em `datasets/`.
- `agrobr/datasets/`: orquestra as fontes, normaliza e valida o contrato final. O `BaseDataset` fica em `base.py`, e o registro
  automático em `registry.py`. As fontes são importadas dentro dos fetchers (import lazy), o que evita import circular.
- `agrobr/contracts/`: o schema de cada dataset, com versão semântica própria. Quebra de compatibilidade = major; coluna nova
  opcional = minor; correção de parsing = patch. Ver `docs/contracts/semver.md`.
- `agrobr/http/`, `agrobr/utils/` e `agrobr/normalize/`: helpers compartilhados (retry, user agent, encoding, datas,
  downloads). Procure ali antes de criar um novo.
- `agrobr/constants.py`: URLs, limites, a tabela de licenças (`LICENCAS`) e o enum `Fonte`.
- As funções públicas que devolvem tabela seguem o padrão `utils.result.finalize_result` com `utils.result.build_source_meta`
  (exemplo no CONTRIBUTING), e `return_meta=True` devolve `(df, meta)`. Os catálogos (`produtos()`, `list_datasets()`,
  `info()`) e as APIs que montam o próprio `MetaInfo` (`cepea.indicador`, `BaseDataset`) ficam fora desse padrão.

## Convenções para código novo

São as do CONTRIBUTING ("Padrões de código"). Parte do código antigo não as segue, e isso não é motivo para refatorar.

- Type hints em todo código de produção (o `mypy --strict` é gate da CI).
- Sem comentários. Docstring (Google style) só quando o nome e a assinatura não bastam.
- Imports no topo do arquivo, na forma `from pacote import modulo`. As exceções são o import lazy nos fetchers de
  `datasets/` e o import local que evita import circular.
- Linhas de até 100 caracteres (ruff).
- Exceções específicas por categoria:
  - `httpx.HTTPError` e `httpx.TimeoutException` para rede;
  - `ParseError` quando o layout da fonte muda;
  - `ContractViolationError` quando o contrato quebra;
  - `InvalidParameterError` para argumento inválido, antes de qualquer requisição.

  O `except Exception` genérico fica só como última barreira, como na cascata de fallback do `BaseDataset`, que registra o
  erro e passa para a próxima fonte.
- Valores monetários em `float64`.

## Regras que os testes conferem

- Status HTTP de erro passa por `agrobr.http.responses.raise_for_status`. O `tests/test_http/test_status_de_erro.py`
  recusa `.raise_for_status()` direto fora da lista declarada.
- Membro de ZIP e planilha XLSX passam por `agrobr.utils.io` (`read_zip_member`, `open_zip_member`, `read_excel_safe` e
  `open_excel_safe`), que aplica o teto de descompressão. Quem confere é o `tests/test_utils/test_teto_de_expansao.py`, que
  aceita só as exceções declaradas nele (o histórico do INMET, com teto próprio, e os pacotes locais gravados pelo agrobr).
- A licença de cada fonte em `constants.LICENCAS` tem de bater com `docs/licenses.md` (`tests/test_licencas.py`).
- Os scripts de `examples/` chamam a API pública com argumentos válidos (`tests/test_examples.py`).

## Testes

- Um diretório por fonte (`tests/test_<fonte>/`); os datasets ficam em `tests/test_datasets/`.
- Nome `test_<funcionalidade>_<cenario>`. Várias asserções no mesmo teste são ok.
- Os mocks de HTTP usam os helpers de `tests/helpers.py`.
- **Golden data** (`tests/golden_data/<fonte>/<caso>/`) guarda os bytes publicados pela fonte:
  - nunca reserializar, reformatar nem trocar o fim de linha;
  - os arquivos listados no `.gitattributes` são `-text`, e os hooks `trailing-whitespace`, `end-of-file-fixer` e
    `check-added-large-files` ignoram `tests/golden_data/`.
- Teste que valida dado (parser, fonte ou dataset) compara a saída com um valor publicado pela fonte, e não com a saída do
  próprio parser. Ele tem de pegar erro de dado: rótulo, recorte, unidade ou valor de outra linha. Testes de HTTP, cache e
  exceções seguem o padrão comum.
- A remoção de teste segue a seção "Testes" do CONTRIBUTING.
- Teste que depende de extra opcional (`polars`, `geopandas`, `shapely`) usa `pytest.importorskip`: dentro do teste quando
  só ele depende do extra, no topo quando o módulo inteiro depende. A coleta da suíte não pode quebrar sem o extra.

## Licenças das fontes

- Fonte nova só entra com a licença registrada em `docs/licenses.md` (PT e EN) e em `constants.LICENCAS`.
- As classes são `livre`, `nc`, `zona_cinza` e `restrito`:
  - `nc`, `zona_cinza` e `restrito` avisam na primeira chamada (`utils.warnings.warn_once`);
  - `restrito` não entra em fallback automático, salvo a exceção declarada em `docs/licenses.md`.

## Documentação

- Cada página tem as duas línguas, `pagina.md` (PT) e `pagina.en.md` (EN), e as duas mudam juntas.
- Fonte nova ganha `docs/sources/<fonte>`, `docs/api/<fonte>`, `docs/contracts/<dataset>` e a entrada na navegação do
  `mkdocs.yml`.
- Mudança de comportamento de função pública:
  - entra no `CHANGELOG.md`, na seção da próxima versão, no topo, com os grupos Added, Improved, Changed, Fixed e Security, cada um
    uma vez só;
  - se quebra compatibilidade, entra também no guia de migração em `docs/guides/`.

## Commits e pull requests

- Mensagem no formato `tipo(escopo): descrição`, com os tipos listados no CONTRIBUTING.
- Um assunto por commit e por PR: não misture feature com refatoração.
- Os hooks do pre-commit (ruff, ruff-format e mypy em `agrobr/`) têm de passar.

## Não faça

- Commitar credenciais. Chaves e tokens vêm de variáveis de ambiente (`AGROBR_USDA_API_KEY`, `AGROBR_COMTRADE_API_KEY`,
  `AGROBR_INMET_TOKEN`, `AGROBR_MAPBIOMAS_ALERTA_TOKEN`, `AGROBR_CONAB_CEASA_USER` e `AGROBR_CONAB_CEASA_PASS`).
- Desligar um hook ou o bloqueio de rede dos testes.
- Editar um golden para o teste passar.
- Pôr dependência nova no core sem justificativa (tamanho, manutenção e licença). O core não depende de extra, e extra não
  depende de `dev`.
