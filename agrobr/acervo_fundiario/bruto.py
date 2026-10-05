from __future__ import annotations

from typing import Literal

from agrobr.bruto import arquivo, models, protocols
from agrobr.exceptions import ParseError

from . import client

ASSINATURAS_ZIP = (b"PK\x03\x04", b"PK\x05\x06")

TEMAS: dict[str, tuple[str, Literal["publico", "privado"] | None]] = {
    "sigef_publico": ("sigef_publico", "publico"),
    "sigef_privado": ("sigef_privado", "privado"),
    "snci_publico": ("snci_publico", "publico"),
    "snci_privado": ("snci_privado", "privado"),
    "snci_brasil": ("snci", None),
}


class AdaptadorAcervo:
    """Um GET do ZIP da UF, sem HEAD, cache nem leitor: o arquivo vai como o INCRA publica."""

    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto:
        tema, natureza = TEMAS[pedido.recurso]
        return models.PlanoBruto(
            fonte=pedido.fonte,
            recurso=pedido.recurso,
            nome=pedido.nome,
            url_solicitada=client._build_url(tema, pedido.uf),
            parametros={},
            selecao=models.Selecao(
                uf=pedido.uf, bbox=None, bbox_crs=None, camada=None, edicao=None, natureza=natureza
            ),
            modo="arquivo",
            formato="zip",
            opcoes=models.Opcoes(
                tamanho_pagina=pedido.tamanho_pagina,
                compactar=pedido.compactar,
                limites=pedido.limites,
            ),
            campo_id=None,
            crs_esperado=None,
        )

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: protocols.ContextoBruto
    ) -> models.ConclusaoBruta:
        pedido = models.PedidoHTTP(
            url=plano.url_solicitada, parametros={}, papel="arquivo", numero=1, formato="zip"
        )
        async with client.sessao() as http:
            resposta = await contexto.obter(http, pedido)
        if resposta.http_status == 404:
            return models.ConclusaoBruta(status="ausente_na_fonte")
        if resposta.temporario is not None:
            with resposta.temporario.open("rb") as arquivo:
                inicio = arquivo.read(4)
            if inicio not in ASSINATURAS_ZIP:
                raise ParseError(
                    "acervo_fundiario",
                    1,
                    f"{resposta.url}: resposta 200 não começa com a assinatura de ZIP ({inicio!r})",
                )
        contexto.registrar_arquivo(resposta)
        return models.ConclusaoBruta(status="ok")


adaptador = AdaptadorAcervo()
assentamentos = arquivo.AdaptadorArquivo(
    client._build_url("assentamentos", None), None, arquivo.conferir_zip
)
