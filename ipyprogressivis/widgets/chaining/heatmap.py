from .utils import make_button, stage_register, VBoxTyped, TypedBase
from ..utils import historized_widget
from .._hist2d_schema import hist2d_spec_no_data
import ipywidgets as ipw
from ..vega import VegaWidget
import numpy as np
from progressivis.core import Scheduler, Sink
from progressivis.stats import Histogram2D
from typing import Any as AnyType, Dict, cast, Type, TypeAlias

WidgetType = AnyType
_l = ipw.Label

N = 4  # 1X + 3Y


HVegaWidget: TypeAlias = cast(Type[AnyType],
                              historized_widget(VegaWidget, "update"))  # noqa: F821


class HeatmapW(VBoxTyped):
    class Typed(TypedBase):
        x_col: ipw.Dropdown
        y_col: ipw.Dropdown
        input_: ipw.Dropdown
        min_: ipw.Dropdown
        max_: ipw.Dropdown
        btn_apply: ipw.Button
        vega: HVegaWidget
    _histogram2d: Histogram2D | None

    def initialize(self) -> None:
        self.output_dtypes = None
        self.c_.x_col = ipw.Dropdown(
            options=list(self.dtypes.keys()) + [""],
            value="",
            description="X:",
            disabled=False,
        )
        self.c_.x_col.observe(self._dropdown_cb, "value")
        self.c_.y_col = ipw.Dropdown(
            options=list(self.dtypes.keys()) + [""],
            value="",
            description="Y:",
            disabled=False,
        )
        self.c_.y_col.observe(self._dropdown_cb, "value")
        self.c_.input_ = ipw.Dropdown(
            options=self.input_module.members + [""],  # type: ignore
            value="",
            description="Input:",
            disabled=False,
        )
        self.c_.input_.observe(self._dropdown_cb, "value")
        self.c_.min_ = ipw.Dropdown(
            options=self.input_module.members + [""],  # type: ignore
            value="",
            description="Min:",
            disabled=False,
        )
        self.c_.min_.observe(self._dropdown_cb, "value")
        self.c_.max_ = ipw.Dropdown(
            options=self.input_module.members + [""],  # type: ignore
            value="",
            description="Max:",
            disabled=False,
        )
        self.c_.max_.observe(self._dropdown_cb, "value")
        self.c_.btn_apply = self._btn_ok = make_button(
            "Apply", disabled=True, cb=self._btn_apply_cb
        )

    def _dropdown_cb(self,  change: Dict[str, AnyType]) -> None:
        for widget in self.children:
            if not isinstance(widget, ipw.Dropdown):
                continue
            if not widget.value:
                self.c_.btn_apply.disabled = True
                return
        self.c_.btn_apply.disabled = False

    def _update_vw(self, s: Scheduler, run_number: int) -> None:
        """
        """
        if self._histogram2d is None:
            return
        if not self._histogram2d.result:
            return
        last = self._histogram2d.result.last()
        assert last
        res = last.to_dict()
        arr = res["array"]
        maxa = arr.max()
        if maxa != 0:
            hist = np.cbrt(arr/maxa)
        else:
            hist = maxa
        self.c_.vega.update("data", insert=hist, remove="true")

    def _btn_apply_cb(self, btn: AnyType) -> None:
        """
        """
        facade = self.input_module
        scheduler = facade.scheduler()
        with scheduler:
            histogram2d = Histogram2D(
                x_column=self.c_.x_col.value,
                y_column=self.c_.y_col.value, scheduler=scheduler
            )
            histogram2d.input.table = facade.output[self.c_.input_.value]
            histogram2d.input.min = facade.output[self.c_.min_.value]
            histogram2d.input.max = facade.output[self.c_.max_.value]
            histogram2d.params.xbins = 64
            histogram2d.params.ybins = 64
            sink = Sink(scheduler=scheduler)
            sink.input.inp = histogram2d.output.result
            self._histogram2d = histogram2d
        self.c_.vega = HVegaWidget(spec=hist2d_spec_no_data)
        self.input_module.scheduler().on_tick(self._update_vw)
        self.dag_running()
        self.c_.btn_apply.disabled = True
        for widget in self.children:
            if not isinstance(widget, ipw.Dropdown):
                continue
            widget.disabled = True


stage_register["Heatmap"] = HeatmapW
