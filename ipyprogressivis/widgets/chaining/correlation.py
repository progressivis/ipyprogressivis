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
    modules_producer,
    Coro,
)
import ipywidgets as ipw
from progressivis.stats.api import Corr
from progressivis.core.api import Sink, Module
from .._corr_schema import corr_spec_no_data
from ..vega import VegaWidget
from .desc_stats import corr_as_vega_dataset

from typing import Any as AnyType

WidgetType = AnyType

class AfterRun(Coro):
    async def action(self, m: Module, run_number: int) -> None:
        assert isinstance(m, Corr)
        cols = m.columns
        dataset = corr_as_vega_dataset(m, cols)
        self.leaf.child.vega.update("data", remove="true", insert=dataset)  # type: ignore



class CorrelationW(VBoxTyped):
    class Typed(TypedBase):
        selection: ipw.SelectMultiple
        mode: ipw.RadioButtons
        start_btn: ipw.Button
        vega: VegaWidget | None

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
        self.child.mode = ipw.RadioButtons(
                options=[
                         ("Pearson", "Pearson"),
                         ("Covariance (only)", "CovarianceOnly")],
                value="Pearson",
                description="Mode:",
                disabled=False,
                style={"description_width": "initial"},
                )
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_corr(content)
        self.output_slot = "result"

    @modules_producer
    def init_corr(self, content: dict[str, AnyType]) -> None:
        s = self.input_module.scheduler()
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
        self.child.vega = VegaWidget(spec=corr_spec_no_data)
        after_run = AfterRun()
        corr.on_after_run(after_run)
        self.dag_running()
        self.make_leaf_bar(after_run)

    def _selection_cb(self, change: AnyType) -> None:
        self.child.start_btn.disabled = len(change["new"]) < 2

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        content = dict(
            selection=self.child.selection.value,
            mode=self.child.mode.value
            )
        if is_recording():
            amend_last_record({"frozen": content})
        self.output_module = self.init_corr(content)
        btn.disabled = True
        self.child.selection.disabled = True
        #self.dag_running()
        #self.make_chaining_box()
        self.manage_replay()


stage_register["Corr"] = CorrelationW
