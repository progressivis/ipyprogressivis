# type: ignore
from .utils import (
    starter_callback,
    chaining_widget,
    VBox,
    runner,
    needs_dtypes,
    modules_producer,
)
import ipywidgets as ipw
from progressivis import Quantiles
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    select_multiple,
    restore,
)

from progressivis.core.api import Sink
from typing import Any as AnyType

WidgetType = AnyType


@chaining_widget(label="Quantiles")
class QuantilesW(VBox):
    def get_num_cols(self):
        return [
            (col, c)
            for (col, (c, t)) in self.col_typed_names.items()
            if (t.startswith("float") or t.startswith("int"))
        ]

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = self.dtypes
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}
        self._proxy = anybox(
            self,
            select_multiple("Columns:", options=self.get_num_cols(), rows=10)
            .uid("selection")
            .observe(self._selection_cb),
            button("Start", disabled=True)
            .uid("start_btn")
            .on_click(self._start_btn_cb),
        )

    @runner
    def run(self) -> AnyType:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        self.children = self._proxy.widget.children
        self.output_module = self.init_modules(self._proxy.that.selection.widget.value)
        self.output_slot = "result"

    def init_ui(self) -> None:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        options = self.get_num_cols()
        values = self._proxy.that.selection.widget.value
        new_val = []
        for v in values:
            if v in self.dtypes:
                new_val.append(v)
        self._proxy.that.selection.attrs(value=[])
        self._proxy.that.selection.attrs(options=options)
        self._proxy.that.selection.attrs(value=new_val)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children

    @modules_producer
    def init_modules(self, content: list[str]) -> None:
        s = self.input_module.scheduler
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

    def _selection_cb(self, proxy: Proxy, change: AnyType) -> None:
        proxy.that.start_btn.attrs(disabled=not change["new"])

    @starter_callback
    def _start_btn_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        self.record = self._proxy.dump()
        self.output_module = self.init_modules(proxy.that.selection.widget.value)
        proxy.that.selection.attrs(disabled=True)
