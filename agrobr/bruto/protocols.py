from __future__ import annotations

from typing import Literal, Protocol

import httpx

from agrobr import exceptions
from agrobr.bruto import models


class ContextoBruto(Protocol):
    async def obter(
        self, http: httpx.AsyncClient, pedido: models.PedidoHTTP
    ) -> models.RespostaCapturada: ...

    async def consultar(self, http: httpx.AsyncClient, url: str) -> bytes: ...

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

    def divergencia(self, mensagem: str) -> exceptions.ParseError: ...


class AdaptadorBruto(Protocol):
    def planejar(self, pedido: models.PedidoBruto) -> models.PlanoBruto: ...

    async def adquirir(
        self, plano: models.PlanoBruto, *, contexto: ContextoBruto
    ) -> models.ConclusaoBruta: ...
