# Contrato: comercio_internacional

**Versão 3.0**, com colunas e parâmetros do dataset em português. A fonte `comtrade.comercio()` e o registro `comercio_bilateral` mantêm a versão 2.1 e os nomes da fonte. Implementação: `agrobr.contracts.comtrade.COMERCIO_INTERNACIONAL_V3`.

## Schema

Todas as **27 colunas** são estáveis, inclusive em vazio.

| Coluna | Tipo pandas | Anulável | Unidade | Limites |
|---|---|---|---|---|
| `periodo` | `str` | Não | — | — |
| `ano` | `Int64` | Não | — | >= 1, <= 9999 |
| `mes` | `Int64` | Sim | — | >= 1, <= 12 |
| `codigo_declarante` | `Int64` | Não | — | >= 1 |
| `iso_declarante` | `str` | Sim | — | — |
| `declarante` | `str` | Sim | — | — |
| `codigo_parceiro` | `Int64` | Não | — | >= 0 |
| `iso_parceiro` | `str` | Sim | — | — |
| `parceiro` | `str` | Sim | — | — |
| `codigo_fluxo` | `str` | Não | — | — |
| `fluxo` | `str` | Sim | — | — |
| `codigo_hs` | `str` | Não | — | — |
| `descricao_produto` | `str` | Sim | — | — |
| `nivel_hs` | `Int64` | Não | — | >= 2, <= 6 |
| `peso_liquido_kg` | `float64` | Sim | kg | >= 0 |
| `peso_bruto_kg` | `float64` | Sim | kg | >= 0 |
| `volume_ton` | `float64` | Sim | ton | >= 0 |
| `valor_fob_usd` | `float64` | Sim | USD | >= 0 |
| `valor_cif_usd` | `float64` | Sim | USD | >= 0 |
| `valor_primario_usd` | `float64` | Sim | USD | >= 0 |
| `quantidade` | `float64` | Sim | — | >= 0 |
| `unidade_qtd` | `str` | Sim | — | — |
| `classificacao` | `str` | Não | — | — |
| `classificacao_original` | `boolean` | Sim | — | — |
| `peso_liquido_estimado` | `boolean` | Sim | — | — |
| `peso_bruto_estimado` | `boolean` | Sim | — | — |
| `quantidade_estimada` | `boolean` | Sim | — | — |

**Chave:** `periodo, codigo_declarante, codigo_parceiro, codigo_hs, codigo_fluxo, classificacao`.

`periodo` contém YYYY anual ou YYYYMM mensal, coerente com ano/mês. `nivel_hs` coincide com a largura de HS, que aceita 2/4/6 dígitos ASCII e preserva zeros. `codigo_fluxo` aceita X/M; `classificacao` preserva a revisão Hn publicada. ISO e nomes são descrições opcionais, sem substituição por códigos fabricados. Medidas são finitas; ausências permanecem nulas.

As 3 marcas de estimativa (2.1) vêm da ONU (`isNetWgtEstimated`, `isGrossWgtEstimated` e `isQtyEstimated`). `True` diz que a medida publicada é uma estimativa da ONU, e não o valor declarado pelo país: no frango do Brasil em 2024, o peso líquido do HS 020714 é estimado. Com peso líquido estimado no resultado, `meta.validation_warnings` traz o HS e o período, e `peso_liquido_kg` e `volume_ton` seguem o valor estimado. O `trade_mirror` lista essas células por perna em `source_details["peso_estimado"]`.

## Seleção e proveniência

`parceiro=None/world/mundo/"0"` seleciona o agregado mundial explícito. `parceiro="all"/"todos"` preserva todos os parceiros publicados. Não some linhas agregadas com seus componentes. `produto` aceita os aliases agrícolas e códigos textuais, inclusive listas separadas por vírgula.

`exigir_completo=True` exige cobertura comprovada por contagem independente e união disjunta. O padrão False permite parcial com aviso; falha de HTTP, layout ou identidade interrompe a coleta. Completo significa cobertura do recorte consultado, sem garantir publicação definitiva do país.

O dataset preserva canal real, query, recursos, hashes, aquisição UTC, cobertura e avisos. O hash superior e seu tamanho identificam um manifesto de recursos, não um corpo único. Snapshot determinístico somente preenche o ano omitido e não congela revisões. Polars é convertido depois da validação.

```python
from agrobr import datasets

df, meta = await datasets.comercio_internacional(
    "1201,1005,0901,1701,2304", parceiro="all", periodo=2023,
    exigir_completo=True, return_meta=True,
)
```

## Espelho relacionado

`TRADE_MIRROR_V2` registra `trade_mirror` com 24 colunas e chave `periodo, hs_code, reporter_code, partner_code`. Mantém as 18 colunas anteriores e adiciona `classificacao_reporter`, `classificacao_partner`, `classificacao_original_reporter`, `classificacao_original_partner` e os dois códigos numéricos.

A junção é externa 1:1 entre exportação e importação inversa. Revisões HS incompatíveis falham; uma perna ausente permanece nula. Ratios com denominador zero ou ausente são nulos. Veja a [API](../api/comtrade.md).

## Relação com ComexStat

| Aspecto | comercio_internacional | exportacao / importacao |
|---|---|---|
| Fonte | UN Comtrade | ComexStat/MDIC |
| Recorte | Bilateral, conforme disponibilidade do reporter | Brasil |
| Classificação | HS | NCM |
| Dimensão geográfica | Países por códigos numéricos | País de destino/origem e UF brasileira |

A categoria interna de licença Comtrade é `zona_cinza`; veja [Licenças](../licenses.md#un-comtrade) e a [migração](../guides/migracao-2.md).

`declarante`, `parceiro`, `frequencia` e `exigir_completo` são os parâmetros do dataset. A fonte mantém `reporter`, `partner`, `freq` e `require_complete`. Os anos pedidos devem estar entre 1962 e o ano corrente; seleções anuais, mensais, listas e intervalos são conferidos antes da rede. Texto usa o padrão do pandas instalado, tanto no resultado cheio como no vazio.
