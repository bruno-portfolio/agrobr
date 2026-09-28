# Política de Dependências

## Classificação

### Core (obrigatórias)

Instaladas com `pip install agrobr`:

| Dependência | Uso | Pin |
|---|---|---|
| `httpx` | HTTP client async | `>=0.28.1` |
| `httpcore` | Mínimo de segurança do transporte HTTP | `>=1.0.9` |
| `beautifulsoup4` | HTML parsing | `>=4.12.0` |
| `soupsieve` | Seletores CSS do BeautifulSoup | `>=2.9.0` |
| `lxml` | Parser HTML/XML | `>=6.1.0` |
| `pandas` | DataFrames | `>=2.2.2` |
| `pydantic` | Validação de modelos | `>=2.5.0` |
| `pydantic-settings` | Configurações | `>=2.1.0` |
| `duckdb` | Cache local | `>=1.5.2` |
| `structlog` | Logging estruturado | `>=23.2.0` |
| `chardet` | Detecção de encoding | `>=5.2.0` |
| `typer` | CLI | `>=0.26.0` |
| `openpyxl` | Leitura de Excel (.xlsx) | `>=3.1.0` |
| `python-calamine` | Fallback Excel (Rust, ignora estilos) | `>=0.3.0` |
| `xlrd` | Leitura de Excel legado (.xls) | `>=2.0.1` |
| `requests` | HTTP síncrono (basedosdados, utilitários) | `>=2.33.0` |

### Opcionais

Instaladas via extras:

```bash
pip install agrobr[pdf]       # pdfplumber para PDFs
pip install agrobr[browser]   # Playwright para sites com JS
pip install agrobr[polars]    # Suporte a Polars DataFrames
pip install agrobr[geo]       # GeoDataFrames (SICAR, desmatamento, etc)
pip install agrobr[bigquery]  # Fallback BigQuery (BCB/SICOR)
pip install agrobr[all]       # Todas as integrações opcionais de runtime
```

| Extra | Dependência | Uso |
|---|---|---|
| `[pdf]` | `pdfplumber>=0.11.10` | Parsing de PDFs |
| `[browser]` | `playwright>=1.55.1` | Sites que requerem JS |
| `[polars]` | `polars>=0.19.0`, `pyarrow>=14.0.1` | DataFrames Polars |
| `[bigquery]` | `basedosdados>=2.0.0` | Fallback BigQuery (BCB/SICOR) |
| `[geo]` | `geopandas>=1.1.4`, `pyogrio>=0.8.0` | GeoDataFrames (SICAR, desmatamento, etc) |

### Dev

```bash
pip install agrobr[dev]
```

Inclui: pytest (+asyncio, cov, recording, timeout), ruff, mypy, pre-commit, pandas-stubs, types-requests, xlwt.

## Regras de Pin

- **Lower bound only** (`>=X.Y.Z`): permite atualizações compatíveis
- **Sem upper bound**: evita "dependency hell" com conflitos de pins
- **Exceção**: se uma versão específica tem bug conhecido, usamos `!=`

## Adicionando dependências

Critérios para aceitar uma nova dependência core:

1. **Necessária**: não há forma razoável de implementar sem ela
2. **Estável**: >= 1.0.0 ou com histórico de estabilidade comprovado
3. **Mantida**: último release < 6 meses
4. **Licença compatível**: MIT, BSD, Apache 2.0
5. **Sem dependências transitivas pesadas**: evitar C extensions complexas

Se uma dependência é útil mas não essencial, vai como **optional extra**.

## Python

- Mínimo suportado: **Python 3.11**
- Target principal: **Python 3.12**
- Testado em CI: 3.11, 3.12, 3.13

O perfil mínimo da CI fixa as dependências core e os extras PDF, geo e Polars no Python 3.11 com `scripts/constraints-minimum.txt`, incluindo NumPy 2. Dependências atuais e instalações somente core do wheel são verificadas no Python 3.11–3.13. O SIDRA usa diretamente o cliente HTTP assíncrono existente; `sidrapy` deixa de ser necessário.

`httpcore` já é instalado transitivamente pelo HTTPX. Seu limite explícito não adiciona um componente novo de runtime: a versão 1.0.9 exige h11 0.16 ou superior e impede resolver a série antiga vulnerável. Os limites de lxml e requests também excluem avisos de segurança publicados, sem afirmar que essas vulnerabilidades foram exploradas pelo agrobr.

`soupsieve` também já vem com o BeautifulSoup. O limite explícito existe por 2 motivos. O BeautifulSoup 4.12.0 aceita `soupsieve>1.2`, e do 1.2.1 ao 1.6 ele não importa em Python 3.10 ou superior: o BeautifulSoup então desliga os seletores CSS (`select`/`select_one`), que a CONAB usa. E o piso anterior, 1.6.1, aceitava versões com 4 falhas de segurança de 2026 no parser de seletor (ReDoS e memória); no agrobr o seletor é sempre literal, sem caminho para elas, e o piso 2.9.0 é higiene. Com o 2.9, as famílias que usam seletor CSS (CONAB, IBGE, RNC e ZARC, 1.558 itens) dão o mesmo resultado no BeautifulSoup 4.12.0 e no 4.15.0.

No extra geo, GeoPandas 1.1.4 inclui as correções e o endurecimento de `to_postgis` descritos no [changelog da dependência](https://geopandas.org/en/stable/docs/changelog.html#version-1-1-4-june-26-2026). O agrobr não executa essa operação, mas não permite versões anteriores do extra. O CI atualiza pip, setuptools e wheel separadamente: ferramentas de instalação não são dependências de runtime da library.

Wheel e sdist excluem configurações locais de ferramentas e arquivos `.env` explicitamente, mesmo em cópias sem `.gitignore`. O smoke do pacote instalado verifica que esses arquivos não foram distribuídos.
