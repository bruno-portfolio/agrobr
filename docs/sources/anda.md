# ANDA — Fertilizantes

> **Licença:** Sem termos de uso públicos localizados. Autorização formal
> solicitada em fev/2026 — aguardando resposta.
> Classificação: `zona_cinza`

!!! note "Autorização pendente"
    Autorização formal para redistribuição de dados foi solicitada à ANDA
    em fevereiro/2026. Aguardando resposta. Verifique diretamente com a
    ANDA antes de uso comercial.

Associação Nacional para Difusão de Adubos. Entregas mensais de
fertilizantes ao mercado brasileiro (total nacional, `uf="BR"`).

## Instalação

ANDA requer `pdfplumber` como dependência opcional:

```bash
pip install agrobr[pdf]
```

## API

```python
from agrobr import anda

# Entregas mensais de fertilizantes
df = await anda.entregas(ano=2024)

# Agregação mensal (sem a coluna uf)
df = await anda.entregas(ano=2024, agregacao="mensal")
```

## Colunas — `entregas`

| Coluna | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano |
| `mes` | int | Mês (1-12) |
| `uf` | str | Sempre `BR` (total nacional) |
| `produto_fertilizante` | str | Sempre `total`; a fonte não publica entregas separadas por formulação |
| `volume_ton` | float | Volume entregue (toneladas) |

## Nota de Risco

ANDA publica dados em PDF. O layout pode mudar sem aviso entre anos.
Os boletins de entregas disponíveis trazem apenas o total agregado de
fertilizantes. Por isso, `produto="total"` é o único valor aceito;
formulações como `ureia`, `map` ou `kcl` levantam `InvalidParameterError` antes do
download.

O parser do agrobr lê o layout "Principais Indicadores" (dados nacionais
agregados, com meses e valores às vezes em células concatenadas com `\n`).
Nenhum PDF publicado tem tabela por UF, e o parser não tenta lê-la.
Mudanças drásticas de formato podem exigir atualização do parser.

Não há fallback de ano. Se nenhum link de PDF corresponder ao ano solicitado,
o client levanta `InvalidParameterError` e informa os anos disponíveis no site.
No link selecionado, o ano real é extraído do texto do link ou do nome do arquivo
e repassado ao parser.

No agrobr-insights, dados ANDA sao tratados com peso dinamico: quando
parecem distorcidos, o peso no SCI e reduzido automaticamente.

## MetaInfo

```python
df, meta = await anda.entregas(ano=2024, return_meta=True)
print(meta.source)  # "anda"
print(meta.source_method)  # "httpx+pdfplumber"
print(meta.source_url)  # PDF usado, ex.: .../Principais_Indicadores_2026.pdf
print(meta.source_details["pdf"])
# {"url", "rotulo_catalogo" (ex.: "Dados 2026"), "edicao_impressa" (ex.: "Janeiro a Junho"; "Total do Ano"
#  em ano fechado), "sha256", "bytes", "pagina_de_recursos"}
```

`source_url` é o PDF concreto escolhido no catálogo; a edição impressa é o rótulo da linha acumulada da
seção de entregas (diz até que mês o PDF vai). `raw_content_hash` é o SHA-256 do PDF.

## Fonte

- URL: `https://anda.org.br/recursos/`
- Formato: PDF/Excel
- Atualizacao: mensal
- Catálogo público: 2016–2026
- Licença: `zona_cinza` — autorização solicitada (fev/2026)


## Cobertura e validação da publicação

O catálogo público contém 11 PDFs, de 2016 a 2026, todos com entregas nacionais mensais (`uf="BR"`). O boletim 2026 publica janeiro a junho; meses posteriores vazios não são zero. Como nenhum deles publica recorte estadual, a 2.0.0 tirou o parâmetro `uf` da fonte e do dataset `fertilizante` (guia de migração 2.0, seção 50).

O parser 3 exige a seção `Fertilizantes Entregues ao Mercado (em toneladas de produto)` e procura o ano somente nela. Se o ano ou essa identificação estiver ausente, a fonte levanta `ParseError`, e o dataset também (`"Todas as fontes falharam por layout"`), com o motivo da fonte em `errors`. Produção, importação, exportação e relações de troca do mesmo PDF não podem substituir entregas. Valores publicados e o contrato 2.0 permanecem iguais.

`ano` deve ser inteiro de 2000 até o ano corrente. `ano` e `mes` usam `Int64` anulável; `volume_ton` usa `float64`. Parâmetros inválidos falham antes do download do boletim.
