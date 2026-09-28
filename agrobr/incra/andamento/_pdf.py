from __future__ import annotations

import math
from functools import cache
from typing import Any, Protocol, cast


def intersection(left: Any, right: Any) -> Any:
    if left is None:
        return right
    if right is None:
        return left
    return (
        max(left[0], right[0]),
        max(left[1], right[1]),
        min(left[2], right[2]),
        min(left[3], right[3]),
    )


def _black(color: Any) -> bool:
    return bool(color == 0 or color in ((0, 0, 0), (0, 0, 0, 1)))


class GlyphData(Protocol):
    glyphs: list[dict[str, Any]]
    black_segments: list[dict[str, Any]]


@cache
def _backend_classes() -> tuple[type[Any], type[Any], type[Any]]:
    from pdfminer.converter import PDFPageAggregator
    from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
    from pdfminer.utils import apply_matrix_pt

    class Glyphs(PDFPageAggregator):
        def __init__(self, manager: PDFResourceManager) -> None:
            super().__init__(manager)
            self.glyphs: list[dict[str, Any]] = []
            self.clip: Any = None
            self.text_object = 0
            self.black_segments: list[dict[str, Any]] = []
            self.paint_index = 0

        def render_char(self, *args: Any, **kwargs: Any) -> float:
            advance = super().render_char(*args, **kwargs)
            char = cast(Any, self.cur_item._objs[-1])
            if not all(math.isfinite(value) for value in char.bbox):
                raise ValueError("Coordenadas não finitas no texto PDF")
            self.glyphs.append(
                {
                    "text": char.get_text(),
                    "bbox": char.bbox,
                    "clip": self.clip,
                    "text_object": self.text_object,
                    "upright": char.upright,
                }
            )
            return advance

        def paint_path(
            self, gstate: Any, stroke: bool, fill: bool, evenodd: bool, path: Any
        ) -> None:
            self.paint_index += 1
            if (stroke and _black(gstate.scolor)) or (fill and _black(gstate.ncolor)):
                previous = None
                initial = None
                for item in path:
                    if item[0] in ("m", "l"):
                        point = apply_matrix_pt(self.ctm, (item[1], item[2]))
                        if item[0] == "m":
                            initial = point
                        elif previous is not None:
                            self.black_segments.append(
                                {"paint_index": self.paint_index, "segment": [*previous, *point]}
                            )
                        previous = point
                    elif item[0] == "h" and previous is not None and initial is not None:
                        self.black_segments.append(
                            {"paint_index": self.paint_index, "segment": [*previous, *initial]}
                        )
                        previous = initial
                    else:
                        previous = None
            super().paint_path(gstate, stroke, fill, evenodd, path)

    class Interpreter(PDFPageInterpreter):
        def init_state(self, ctm: Any) -> None:
            super().init_state(ctm)
            self.clip: Any = None
            self.clip_stack: list[Any] = []
            self.pending_clip: Any = None

        def do_q(self) -> None:
            super().do_q()
            self.clip_stack.append(self.clip)

        def do_Q(self) -> None:
            super().do_Q()
            if not self.clip_stack:
                raise ValueError("Estado gráfico PDF desequilibrado")
            self.clip = self.clip_stack.pop()

        def do_W(self) -> None:
            if len(self.curpath) != 5 or [item[0] for item in self.curpath] != [
                "m",
                "l",
                "l",
                "l",
                "h",
            ]:
                raise ValueError("Clipping PDF não retangular não suportado")
            points = [
                apply_matrix_pt(self.ctm, (item[1], item[2]))
                for item in cast(Any, self.curpath[:4])
            ]
            xs, ys = sorted({point[0] for point in points}), sorted({point[1] for point in points})
            if len(xs) != 2 or len(ys) != 2:
                raise ValueError("Clipping PDF transformado não suportado")
            self.pending_clip = xs[0], ys[0], xs[1], ys[1]

        def do_W_a(self) -> None:
            self.do_W()

        def do_n(self) -> None:
            if self.pending_clip is not None:
                self.clip = intersection(self.clip, self.pending_clip)
                self.pending_clip = None
            super().do_n()

        def do_BT(self) -> None:
            super().do_BT()
            cast(Glyphs, self.device).text_object += 1

        def do_TJ(self, seq: Any) -> None:
            if self.pending_clip is not None:
                raise ValueError("Clipping PDF pendente antes do texto")
            cast(Glyphs, self.device).clip = self.clip
            super().do_TJ(seq)

    return PDFResourceManager, Glyphs, Interpreter


def collect(page: Any) -> GlyphData:
    manager_class, glyphs_class, interpreter_class = _backend_classes()
    manager = manager_class()
    device: GlyphData = glyphs_class(manager)
    interpreter = interpreter_class(manager, device)
    interpreter.process_page(page)
    if interpreter.pending_clip is not None or interpreter.clip_stack:
        raise ValueError("Estado gráfico PDF não encerrado")
    return device
