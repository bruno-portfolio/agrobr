# Troubleshooting

Guia para resolver problemas comuns.

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

### `ValidationError`

**Causa:** Dados não passaram validação Pydantic ou estatística.

**Soluções:**

1. Verifique se está usando produto válido
2. Desabilite validação estatística se necessário:
   ```python
   df = await cepea.indicador('soja', validate_sanity=False)
   ```

### `AnomalyDetectedWarning`

**Causa:** Valores fora do range histórico esperado.

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

**Solução:** A partir do Unreleased (pós-0.10.0), o `DuckDBStore` usa
`threading.Lock` interno em todos os métodos. Se estiver em versão anterior,
atualize:

```bash
pip install --upgrade agrobr
```

### `CacheError`

**Causa:** Problema com DuckDB ou arquivo de cache corrompido.

**Soluções:**

1. Limpe o cache:
   ```bash
   agrobr cache clear
   ```
2. Ou via código:
   ```python
   from agrobr.cache.duckdb_store import get_store
   store = get_store()
   store.cache_clear()
   ```
3. Se persistir, delete o arquivo:
   ```bash
   rm ~/.agrobr/cache/agrobr.duckdb
   ```

### Cache não Atualizando

**Causa:** Cache fresh está sendo retornado.

**Solução:**
```python
df = await cepea.indicador('soja', force_refresh=True)
```

### Cache Strict Mode

Se você quer garantir que o cache sempre corresponda à versão atual do agrobr:

```bash
export AGROBR_CACHE_STRICT=1
```

Com strict mode ativo, qualquer entrada de cache gravada por uma versão diferente do agrobr é ignorada (cache miss). Útil em ambientes de produção onde consistência é mais importante que performance.

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
   python -m agrobr.cli cepea soja
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

```bash
# Via CLI
agrobr --log-level DEBUG cepea soja

# Via código
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Ver Configuração Atual

```bash
agrobr config show
```

### Verificar Health das Fontes

```bash
agrobr health --all --verbose
```

### Inspecionar Cache

```bash
agrobr cache status
```

## Reportando Bugs

Se o problema persistir:

1. Verifique se já existe issue: https://github.com/bruno-portfolio/agrobr/issues
2. Colete informações:
   ```bash
   python --version
   pip show agrobr
   agrobr health --all
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
