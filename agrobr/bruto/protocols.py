from __future__ import annotations

from typing import Literal, Protocol

import httpx

from agrobr.bruto import models


class ContextoBruto(Protocol):
    async def obter(
        self, http: httpx.AsyncClient, pedido: models.PedidoHTTP
    ) -> models.RespostaCapturada: ...

    def registrar_pagina(
        self, resposta: models.RespostaCapturada, leitura: models.LeituraPagina
    ) -> models.PaginaBruta: ...

    def registrar_controle(
        self, resposta: models.RespostaCapturada, leitura: models.LeituraControle
    ) -> models.ControleBruto: ...

    def registrar_arquivo(self, resposta: models.RespostaCapturada) -> None: ...

    def conferir_ids(
        self,
        ids: tuple[models.IdFeicao, ...],
        *,
        papel: Literal["esperados", "recebidos"],
    ) -> None: ...

    def conferir_prazo(self) -> None: ...


class AdaptadorBruto(Protocol):
    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto: ...

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: ContextoBruto
    ) -> models.ConclusaoBruta: ...
