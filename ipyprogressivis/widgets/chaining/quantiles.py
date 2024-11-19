# type: ignore
from .utils import (
    make_button,
    stage_register,
    VBoxTyped,
    TypedBase,
    amend_last_record,
    is_recording,
    runner,
    needs_dtypes,
)
import ipywidgets as ipw
from progressivis import (
    Quantiles
)
from progressivis.core.api import Sink
from typing import Any as AnyType

WidgetType = AnyType


class QuantilesW(VBoxTyped):
    class Typed(TypedBase):
        selection: ipw.SelectMultiple
        freeze_ck: ipw.Checkbox
        start_btn: ipw.Button

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = self.dtypes
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}
        num_cols = [
            (col, c)
            for (col, (c, t)) in self.col_typed_names.items()
            if (t.startswith("float") or t.startswith("int"))
        ]
        self.child.selection = ipw.SelectMultiple(
            options=num_cols,
            value=[],
            rows=10,
            description="Columns:",
            disabled=False,
        )
        self.child.selection.observe(self._selection_cb, "value")
        is_rec = is_recording()
        self.child.freeze_ck = ipw.Checkbox(description="Freeze",
                                            value=is_rec,
                                            disabled=(not is_rec))
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_quantiles(content)
        self.output_slot = "result"

    def init_quantiles(self, content: list[str]) -> None:
        s = self.input_module.scheduler()
        with s:
            quantiles = Quantiles(scheduler=s)
            quantiles.input.table = self.input_module.output.result[tuple(content)]
            sink = Sink(scheduler=s)
            sink.input.inp = quantiles.output.result
            sink2 = Sink(scheduler=s)
            sink2.input.inp = quantiles.output.table
            self.output_module = quantiles
            self.output_slot = "result"
            self.output_dtypes = self.dtypes
            return quantiles

    def _selection_cb(self, change: AnyType) -> None:
        self.child.start_btn.disabled = not change["new"]

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        content = self.child.selection.value
        amend_last_record({"frozen": content})
        self.output_module = self.init_quantiles(content)
        btn.disabled = True
        self.child.selection.disabled = True
        self.dag_running()
        self.make_chaining_box()
        self.manage_replay()


stage_register["Quantiles"] = QuantilesW
