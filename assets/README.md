# Assets da landing

`index.html` e `en/index.html` usam os mesmos arquivos. O código do explorer está em `landing/explorer.js`; `landing/boot.js` só importa o módulo quando o mapa está a 250 px da viewport. As traduções ficam no módulo compartilhado.

| Arquivos | Origem e uso |
|---|---|
| `hero/campo-*.webp`, `hero/og.png` | [Fotografia de Helena Lopes no Pexels](https://www.pexels.com/photo/brown-field-under-white-sky-3045202/), pasto em Curvelo (MG), sob a [licença Pexels](https://www.pexels.com/license/); recorte central, sem edição. Variantes 1280/1920/2560 para o hero e imagem social 1200×630. Crédito nas duas páginas. |
| `landing/fonts/*.woff2` | DM Sans, JetBrains Mono e Playfair Display, sob SIL Open Font License. Os três textos da licença estão na mesma pasta. Apenas o subconjunto latino é distribuído. |
| `landing/fonts.css` | Declarações das quatro faces locais. O mesmo conteúdo é embutido em `style#landingFonts` nas duas páginas para evitar uma folha bloqueante; ao mudar as fontes, sincronizar os três locais. |
| `vendor/three/` | Three.js **0.180.0**, módulos e addons utilizados pelo explorer. [Licença MIT](vendor/three/LICENSE). Sem resolução de versão ou CDN em runtime. |
| `terrain/blue-marble.jpg` | Composição histórica [NASA Blue Marble](https://science.nasa.gov/earth/earth-observatory/blue-marble-next-generation/) via GIBS. Base visual, sem representar a safra selecionada. Origem e limites geográficos em `terrain/attribution.json`. |
| `explorer/conab.json` | Histórico e levantamento corrente coletados pelas APIs públicas do agrobr. Mantém consultas, metadados e hashes de origem; café corrente ausente continua nulo. O total Brasil soma as UFs disponíveis, sem representar uma conferência nacional independente. |
| `mapbiomas/atlas.json` | Coleção 11 do [MapBiomas](https://brasil.mapbiomas.org/iniciativas-e-produtos/cobertura-e-uso-da-terra/cobertura-30m/cobertura/), **CC BY 4.0**, e malha IBGE 2024. Inclui fontes, hashes, anos, cores, grupos e URLs dos arquivos. |
| `mapbiomas/records.json.gz`, `areas.bin.gz`, `index.png` | Registros municipais, áreas Float64 little-endian e índice raster. São baixados somente ao ativar Uso da terra. Os arquivos gzip têm compressão própria; o leitor também aceita respostas que o servidor já descomprimiu. |

As proporções municipais usam a soma das classes publicadas pelo MapBiomas como denominador: **área mapeada**, que pode divergir da área territorial IBGE. Há 5.571 entradas geográficas e 5.570 municípios com estatísticas; cada entrada reserva 48 valores de área (oito anos, seis grupos). A ausência de estatística não é convertida em zero.

Os packs de cobertura local em `mapbiomas/municipal/` são ignorados pelo Git. Sem `data-assets-base` em `#terrainViewer`, a única vista municipal é Proporção. Uma base externa pode ser configurada futuramente nas duas páginas; os packs não fazem parte da publicação v1.

## Atualização e publicação

O ticker é atualizado pelo workflow `landing_data.yml`, com mudanças apenas nas âncoras `agrobr:ticker` e `agrobr:proof`. O número de pregões do gráfico corresponde à amostra disponível, limitada a 30. O exemplo de código não contém a antiga âncora `pregoes`.

```console
python -m scripts.update_landing_data --check
python -m scripts.update_conab_explorer_data --input assets/explorer/conab.json --output assets/explorer/conab.json --cache-dir reports/explorer-monthly --check
```

Remover `--check` grava as saídas por substituição atômica. O ticker aceita `--root` para testar uma cópia das duas páginas. O gerador Conab aceita `--year` e `--resume`; o padrão é o ano UTC corrente, e capturas retomadas precisam corresponder à consulta e ao hash registrado. Não usar `--resume` para buscar uma revisão nova da mesma safra.

`explorer_data.yml` agenda a coleta Conab no dia 15, às 12h UTC; os dois bots compartilham o mesmo grupo de concorrência. O atlas e a malha têm atualização anual e preservam a edição explicitamente. `docs.yml` copia esta pasta para `site/assets`; o checkout não inclui os packs ignorados, protótipos `index2.html` ou `tmp_design_review/`.

Para conferir localmente, servir a raiz do repositório por HTTP e abrir `/` e `/en/`. Importmaps e `DecompressionStream` exigem navegadores modernos; falhas do 3D preservam a consulta no painel, e falhas dos arquivos exibem uma ação de tentar novamente. O fallback `noscript` informa as fontes e mantém o ticker e a prova visíveis.
