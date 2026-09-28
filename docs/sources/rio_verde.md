# Fundacao Rio Verde — Ensaios de Cultivares de Soja

> **Licenca:** Sem termos publicos.
> Classificacao: `zona_cinza`

Resultados de ensaios de cultivares de soja conduzidos pela Fundacao Rio Verde
em Lucas do Rio Verde, MT.

## Visao Geral

| Campo | Valor |
|-------|-------|
| **Operador** | Fundacao Rio Verde (Lucas do Rio Verde, MT) |
| **Website** | [fundacaorioverde.com.br](https://fundacaorioverde.com.br) |
| **Licenca** | `zona_cinza` — Sem termos publicos |
| **Formato** | PDF text-based |
| **Atualizacao** | Anual (por safra) |
| **Cobertura** | Safras 2023/24 (76 linhas), 2024/25 (94) e 2025/26 (107); até 4 épocas de semeio |

## Dados Disponiveis

### Ensaio de Soja

Resultados de produtividade por cultivar e epoca de semeio.

**Colunas:** `safra`, `empresa`, `cultivar`, `grupo_maturacao`, `ciclo_dias`,
`produtividade_1_epoca_sc_ha`, `produtividade_2_epoca_sc_ha`, `produtividade_3_epoca_sc_ha`,
`produtividade_4_epoca_sc_ha`, `produtividade_media_sc_ha`

## API

```python
import asyncio
from agrobr import rio_verde

async def main():
    # Ensaio da safra 2025/2026
    df = await rio_verde.ensaio_soja("2025/2026")

    # Safra especifica
    df = await rio_verde.ensaio_soja("2024/2025")

    # Filtros parciais, sem diferenciar caixa
    df = await rio_verde.ensaio_soja("2025/2026", cultivar="neo")
    df = await rio_verde.ensaio_soja("2025/2026", empresa="agroeste")

    # Listar safras disponiveis
    safras = await rio_verde.safras_disponiveis()

    # Com metadados
    df, meta = await rio_verde.ensaio_soja("2025/2026", return_meta=True)

    # Polars
    df = await rio_verde.ensaio_soja("2025/2026", as_polars=True)

asyncio.run(main())
```

## Notas Tecnicas

- Requer `pip install agrobr[pdf]` (pdfplumber)
- PDF text-based (nao requer OCR)
- Parser extrai tabelas de produtividade por epoca de semeio
- Produtividade em sacas/hectare (sc/ha)
- Safras disponiveis dependem dos PDFs publicados pela fundacao: 2023/2024, 2024/2025 e 2025/2026. A fundação também publica a
  safra 2022/23 num layout que o agrobr não lê (3 épocas de semeio e sem produtividade média); `ensaio_soja("2022/2023")`
  levanta `InvalidParameterError` dizendo isso, antes da rede. O agrobr não calcula média que a fonte não publica.
- A lista de safras é fixa em cada versão do agrobr: a safra nova que a fundação publicar pede uma versão nova, e até lá `ensaio_soja` a recusa com `InvalidParameterError`
- A 2025/26 publica também o G.M. estimado; `grupo_maturacao` é o G.M. declarado, como texto ("6.7")
- Algumas produtividades vêm sem decimal no PDF ("87"); saem como 87.0
- Argumento desconhecido levanta `TypeError` antes de qualquer requisição
- Termos de uso: nenhuma página de termos no site (busca de 23/09/2026)

## Fonte

- URL: `https://fundacaorioverde.com.br`
- Formato: PDF
- Atualizacao: anual (por safra)
- Licenca: `zona_cinza` — Sem termos publicos (verificar com a fundacao)

O parser extrai células da tabela-resumo, preservando empresa e cultivar compostas. As safras 2024/25 e 2025/26 têm layouts diferentes, respectivamente sem e com G.M. estimado; `grupo_maturacao` preserva o G.M. declarado. Épocas sem medição ficam nulas. O número de linhas representa observações, não cultivares únicas: uma cultivar pode aparecer mais de uma vez no relatório.
