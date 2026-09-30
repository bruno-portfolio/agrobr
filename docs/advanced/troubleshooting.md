# Troubleshooting

Guia para resolver problemas comuns.

## O que capturar

Toda exceção do agrobr herda de `AgrobrError`. Nas fontes e nos datasets, o status HTTP de erro não sai como exceção do
httpx: sai como `SourceUnavailableError`.

| Exceção | Significa |
|---------|-----------|
| `SourceUnavailableError` | A fonte não entregou o dado: timeout, falha de conexão ou status HTTP de erro. A mensagem traz a fonte, a URL e o status ("HTTP 403: a fonte recusou o pedido (bloqueio de WAF ou permissão)", "HTTP 404: o recurso não existe na URL"), e `__cause__` guarda a exceção original. No dataset, todas as fontes falharam: `attempted_sources` lista as tentadas, na ordem, `errors` traz o motivo de cada uma, e `__cause__` é o erro da última |
| `ParseError` | O dado chegou, mas o layout da fonte mudou e o parser não o lê |
| `InvalidParameterError` | Parâmetro recusado antes da rede; também é `ValueError` |
| `AgrobrError` | A base: captura qualquer erro do agrobr |

```python
from agrobr import datasets
from agrobr.exceptions import AgrobrError, ParseError, SourceUnavailableError

try:
    df = await datasets.producao_anual("soja", ano=2023)
except SourceUnavailableError as erro:
    print(erro.attempted_sources, erro.errors)
except ParseError:
    ...
except AgrobrError:
    ...
```

## Erros de Conexão

### `SourceUnavailableError`

**Causa:** Fonte de dados não acessível após todas as tentativas.

**Soluções:**

1. Verifique sua conexão com a internet
2. Tente modo offline:
   ```python
   df = await cepea.indicador('soja', offline=True)
   ```
3. Aguarde e tente novamente (fonte pode estar temporariamente fora)
4. Verifique se há um proxy configurado que pode estar bloqueando

### `SourceFallbackWarning`

**Causa:** A fonte primária falhou, mas o dataset conseguiu devolver dados de uma
fonte alternativa. A mensagem informa a fonte original, o motivo resumido da
falha e o fallback usado. A execução continua normalmente.

### `TimeoutError`

**Causa:** Requisição demorou muito.

**Soluções:**

1. Aumente o timeout:
   ```bash
   export AGROBR_HTTP_TIMEOUT_READ=60
   ```
2. Verifique sua conexão
3. Tente em horário de menor tráfego

### `403 Forbidden` (CEPEA)

**Causa:** Cloudflare bloqueando requisições diretas ao CEPEA.

**Solução:** O agrobr automaticamente usa Notícias Agrícolas como fallback (httpx puro, sem Playwright). Se ainda falhar:

1. Tente forçar atualização:
   ```python
   df = await cepea.indicador('soja', force_refresh=True)
   ```
2. Use modo offline com dados do cache:
   ```python
   df = await cepea.indicador('soja', offline=True)
   ```

## Erros de Parsing

### `ParseError`

**Causa:** Layout da fonte mudou e parser não consegue extrair dados.

**Soluções:**

1. Atualize o agrobr:
   ```bash
   pip install --upgrade agrobr
   ```
2. Verifique issues no GitHub para problemas conhecidos
3. Use dados do cache enquanto o problema é corrigido:
   ```python
   df = await cepea.indicador('soja', offline=True)
   ```

### Dados Vazios ou Incompletos

**Causa:** Fonte retornou dados parciais.

**Verificações:**

1. O produto está correto?
   ```python
   produtos = await cepea.produtos()
   print(produtos)
   ```
2. O período solicitado tem dados?
3. Tente um período menor

## Erros de Validação

### `InvalidParameterError`

**Causa:** Um parâmetro fornecido pelo usuário é inválido. Essa exceção também é
um `ValueError`, para preservar compatibilidade com código que já captura
`ValueError`, e interrompe a cascata sem mascarar o problema como fonte
indisponível.

### `ValidationError`

**Causa:** Dados não passaram validação Pydantic ou estatística.

**Soluções:**

1. Verifique se está usando produto válido
2. Desabilite validação estatística se necessário:
   ```python
   df = await cepea.indicador('soja', validate_sanity=False)
   ```

### Anomalias estatísticas

**Causa:** Valores fora do range histórico esperado.

Com `validate_sanity=True`, anomalias são marcadas na coluna `anomalies` do DataFrame (não bloqueiam o retorno) e registradas no log. Não há exceção nem warning específico para anomalias.

**Isso é normal quando:**
- Preços tiveram variação atípica (eventos de mercado)
- Dados de nova safra com volumes diferentes

**Verificar:**
- Compare com outras fontes
- Verifique notícias do setor

## Erros de Cache

### DuckDB Lock / Segfault em Multi-Thread

**Causa:** O DuckDB `DuckDBPyConnection` não é thread-safe. Se o agrobr
é usado em um processo multi-thread (ex: MCP server, FastAPI com threads),
chamadas concorrentes ao cache podem causar segfault ou deadlock.

**Solução:** A partir da v0.10.1, o `DuckDBStore` usa
`threading.Lock` interno em todos os métodos. Se estiver em versão anterior,
atualize:

```bash
pip install --upgrade agrobr
```

### Cache corrompido

**Causa:** o `agrobr.duckdb` ficou ilegível (queda de energia, disco cheio ou antivírus no meio da gravação).

**Solução:** o agrobr move o banco danificado para `agrobr.duckdb.corrompido-<AAAAMMDDHHMM>`, avisa com os 2 caminhos e cria um banco novo na consulta seguinte. Se o aviso for o de cache indisponível (o arquivo estava em uso e não pôde ser movido), feche os outros processos do agrobr e apague o arquivo (recriado no próximo uso):

```bash
rm ~/.agrobr/cache/agrobr.duckdb
```

Tudo o que o agrobr grava, e como limpar: [O que o agrobr grava no disco](disco.md).

### Cache não Atualizando

**Causa:** Cache fresh está sendo retornado.

**Solução:**
```python
df = await cepea.indicador('soja', force_refresh=True)
```

## Problemas com Polars

### `ImportError: polars not found`

**Causa:** Polars não instalado.

**Solução:**
```bash
pip install agrobr[polars]
```

### Conversão Falha

**Causa:** Tipos incompatíveis na conversão pandas → polars.

**Solução:** Use pandas (default) e converta manualmente se necessário.

## Problemas com CLI

### Comando não encontrado: `agrobr`

**Causa:** CLI não instalada no PATH.

**Soluções:**

1. Verifique instalação:
   ```bash
   pip show agrobr
   ```
2. Reinstale:
   ```bash
   pip install --force-reinstall agrobr
   ```
3. Use via Python:
   ```bash
   python -m agrobr.cli cepea indicador soja
   ```

### Encoding no Windows

**Causa:** Terminal não suporta UTF-8.

**Solução:**
```bash
# PowerShell
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

# Ou exporte para arquivo
agrobr cepea indicador soja --formato csv > soja.csv
```

## Debug

### Habilitar Logs Detalhados

Como biblioteca, o agrobr não escreve log na saída padrão. Os logs passam pelo `logging` da biblioteca padrão, em JSON: sem
configuração, só avisos e erros saem, na saída de erro.

```bash
# Via CLI (os logs vão para a saída de erro)
agrobr --verbose cepea indicador soja
```

```python
import logging

logging.basicConfig(level=logging.DEBUG)             # todos os logs, na saída de erro
logging.getLogger("agrobr").setLevel(logging.INFO)   # ou só o nível do agrobr
```

O agrobr não configura o structlog: a configuração do structlog da sua aplicação vale só para os logs dela, e os do agrobr
seguem no `logging`.

### Ver Configuração Atual

```bash
agrobr config show
```

### Verificar Health das Fontes

```bash
agrobr health           # todas as fontes
agrobr health --deep    # checagem profunda (faz parsing real)
```

### Inspecionar Cache

```bash
agrobr doctor
```

## Reportando Bugs

Se o problema persistir:

1. Verifique se já existe issue: https://github.com/bruno-portfolio/agrobr/issues
2. Colete informações:
   ```bash
   python --version
   pip show agrobr
   agrobr doctor
   ```
3. Abra issue com:
   - Versão do Python e agrobr
   - Sistema operacional
   - Código que causa o erro
   - Mensagem de erro completa
   - Logs de debug (se possível)

## FAQ

### O agrobr funciona em Jupyter?

Sim! Use a versão async diretamente:
```python
df = await cepea.indicador('soja')
```

Ou a versão sync:
```python
from agrobr.sync import cepea
df = cepea.indicador('soja')
```

### Posso usar com proxies?

Atualmente não há suporte nativo. Considere configurar proxy a nível de sistema.

### Os dados são gratuitos?

Sim, todas as fontes são públicas e gratuitas. O agrobr apenas facilita o acesso.

### Com que frequência os dados são atualizados?

| Fonte | Frequência |
|-------|------------|
| CEPEA | Diária (~18h) |
| CONAB | Mensal |
| IBGE PAM | Anual |
| IBGE LSPA | Mensal |
