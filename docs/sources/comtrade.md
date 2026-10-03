# UN Comtrade — comércio internacional

A integração entrega comércio bilateral de mercadorias (tipo C), exportação/importação, períodos anuais ou mensais e classificações HS. O espelho cruza declarações de exportação com importações inversas. Veja a [API e seus seletores](../api/comtrade.md) e o [contrato 3.0 do dataset](../contracts/comercio_internacional.md) (a fonte valida o `comercio_bilateral` 2.1).

## Rotas e opções

| Camada | Entrega |
|---|---|
| Preview público | Consulta sem chave, um período por chamada e contagem independente |
| Aquisição autenticada | Transporte com chave opcional e replanejamento integral para preview em 401/403, com aviso que aponta `AGROBR_COMTRADE_API_KEY` e `api_key=` |
| Bilateral | World explícito ou todos os parceiros publicados; HS individual, alias agrícola ou lista textual |
| Espelho | Junção externa 1:1, identidade numérica e revisão HS de cada declaração |
| Dataset semântico | Mesmos seletores, contrato completo inclusive vazio, metadados, sync e Polars |

Rotas: `https://comtradeapi.un.org/public/v1/preview/C/{freq}/HS` e `https://comtradeapi.un.org/data/v1/get/C/{freq}/HS`. O client usa httpx assíncrono, timeout, retry, limites de ritmo e validação Pydantic. Credenciais não entram nos recursos/metadados.

## Cobertura e limites

Exemplo: o preview BR/X/2023 com cinco HS e todos os parceiros devolve 500 linhas, e a contagem independente informa 516; a união das consultas disjuntas por HS traz os 516 registros, incluindo os 16 ausentes. Para soja, World explícito devolve uma linha, e partner omitido, 63 parceiros. São exemplos, não contagens globais da base.

A consulta pode permanecer parcial quando um único período/HS excede o acesso disponível. `require_complete=True` rejeita essa saída; o padrão emite aviso e preserva cobertura nos metadados. Revisões e falhas de blocos não viram sucesso vazio. Os limites com chave não são garantidos.

A disponibilidade depende de país, período e classificação. A integração não entrega serviços, tarifas, bulk, catálogo dinâmico de países, detalhe por transporte/aduana, nem harmonização ou histórico congelado de revisões.

O lado Brasil vem da declaração do Brasil ao Comtrade e pode divergir da [ComexStat](comexstat.md), que é revisada: no milho para a China em 2024, o Comtrade tem 2.285.068 t e a ComexStat, 2.227.000 t (+2,6%), provavelmente por revisão da SECEX depois do envio. Na soja e no farelo de 2024 e 2025 e no milho de 2025, a diferença não passou de 0,06%. A diferença também pode vir da estimativa da ONU: no frango de 2024 (HS 020711 a 020714), o peso do Comtrade passa o da ComexStat em 0,88%, com o FOB praticamente igual (+0,007%), porque o peso líquido do 020714 é estimado pela ONU. A coluna `peso_liquido_estimado` e o aviso no `MetaInfo` mostram quando isso acontece.

A categoria interna de licença é `restrito`: consulte [Licenças](../licenses.md#un-comtrade). Referências técnicas: [preview](https://uncomtrade.org/docs/what-is-data-preview/) e [SDK oficial, com countOnly](https://github.com/uncomtrade/comtradeapicall). A classificação preserva as dispensas expressas de redistribuição da política da ONU.

Os anos pedidos devem estar entre 1962 e o ano corrente. Período inválido levanta `InvalidParameterError` antes da rede. A fonte mantém seletores e colunas técnicos; o [contrato do dataset](../contracts/comercio_internacional.md) descreve os nomes em português. Texto usa o padrão do pandas instalado, inclusive no vazio.
