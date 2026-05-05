# type: ignore
from .utils import (
    starter_callback,
    is_leaf,
    no_progress_bar,
    chaining_widget,
    VBox,
    runner,
    needs_dtypes,
    modules_producer,
    Coro,
)
from ..utils import sanitize
import ipywidgets as ipw
from progressivis.stats.api import Corr
from progressivis.core.api import Sink, Module, asynchronize
from .._corr_schema import corr_spec_no_data
from ..vega import VegaWidget
from .desc_stats import corr_as_vega_dataset
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    radiobuttons,
    box,
    select_multiple,
    restore,
)
from typing import Any as AnyType

WidgetType = AnyType

class AfterRun(Coro):
    async def action(self, m: Module, run_number: int) -> None:
        assert isinstance(m, Corr)
        cols = m.columns
        dataset = sanitize(corr_as_vega_dataset(m, cols))
        def _func():
            assert self.leaf is not None
            assert hasattr(self.leaf, "_proxy")
            vega_box = self.leaf._proxy.that.vega_box.widget
            if not vega_box.children:
                return
            vega_box.children[0].update("data", remove="true", insert=dataset)
        await asynchronize(_func)

@is_leaf
@no_progress_bar
@chaining_widget(label="Corr")
class CorrelationW(VBox):
    """
    class Typed(TypedBase):
        selection: ipw.SelectMultiple
        mode: ipw.RadioButtons
        start_btn: ipw.Button
        vega: VegaWidget | None
    """
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
        self._proxy = anybox(
            self,
            select_multiple("Columns:",
                            options=num_cols,
                            value=[],
                            rows=10

            )
            .uid("selection")
            .observe(self._selection_cb),
            radiobuttons("Mode:",
                         options=[
                             ("Pearson", "Pearson"),
                             ("Covariance (only)", "CovarianceOnly")],
                         value="Pearson",
                         style={"description_width": "initial"},
                         ).uid("mode"),
            button("Start",
                   disabled=True
                   )
            .uid("start_btn")
            .on_click(self._start_btn_cb),
            box().uid("vega_box")
        )


    @runner
    def run(self) -> AnyType:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        self.children = self._proxy.widget.children  # type: ignore
        content = dict(
            selection=self._proxy.that.selection.widget.value,
            mode=self._proxy.that.mode.widget.value
            )
        self.init_modules(content)

    @modules_producer
    def init_modules(self, content: dict[str, AnyType]) -> None:
        s = self.input_module.scheduler
        mode = content["mode"]
        selection = content["selection"]
        with s:
            corr = Corr(mode=mode, scheduler=s)
            corr.input.table = self.input_module.output.result[tuple(selection)]
            sink = Sink(scheduler=s)
            sink.input.inp = corr.output.result
            self.output_module = corr
            self.output_slot = "result"
            self.output_dtypes = None
        self.after_run = after_run = AfterRun()
        corr.on_after_run(after_run)
        vegabox = self._proxy.that.vega_box.widget
        assert hasattr(vegabox, "children")
        if not vegabox.children:
            vegabox.children = [VegaWidget(spec=corr_spec_no_data)]

    def _selection_cb(self, proxy: Proxy, change: AnyType) -> None:
        self._proxy.that.start_btn.attrs(disabled = len(change["new"]) < 2)

    @starter_callback
    def _start_btn_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        content = dict(
            selection=self._proxy.that.selection.widget.value,
            mode=self._proxy.that.mode.widget.value
            )
        self.record = self._proxy.dump()
        self.init_modules(content)

    def init_ui(self) -> None:
        content = self.record
        self._proxy = restore(content, globals(), obj=self)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children

