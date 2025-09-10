from .utils import (
    make_button,
    stage_register,
    disable_all,
    VBoxTyped,
    TypedBase,
    needs_dtypes,
    replay_next,
    is_recording,
    amend_last_record,
    runner,
    GuestWidget,
    dongle_widget,
    Coro,
    modules_producer
)
import ipywidgets as ipw
from progressivis.core.api import Module
from progressivis.vis.heatmap import Heatmap
from progressivis.stats.api import Histogram2D, Min, Max
from progressivis import Quantiles
from typing import Any as AnyType

WidgetType = AnyType
_l = ipw.Label

MAX_DIM = 512

class AfterRun(Coro):
    async def action(self, m: Module, run_number: int) -> None:
        assert isinstance(m, Heatmap)
        image = m.get_image_bin()
        assert self.leaf is not None
        if image is not None:
            self.leaf.child.image.value = image


def make_float(
    description: str = "", disabled: bool = False, value: float = 0.0
) -> ipw.BoundedFloatText:
    return ipw.BoundedFloatText(
        value=value,
        min=0.0,
        max=1.0,
        step=0.001,
        description=description,
        disabled=disabled,
        layout={"width": "initial"},
    )


class HeatmapW(VBoxTyped):
    class Typed(TypedBase):
        choice_dim: ipw.Dropdown
        choice_x: ipw.Dropdown
        choice_y: ipw.Dropdown
        min_q: ipw.BoundedFloatText
        max_q: ipw.BoundedFloatText
        choice_trans: ipw.Dropdown
        gaussian_blur: ipw.IntSlider
        start_btn: ipw.Button
        image: ipw.Image

    def __init__(self) -> None:
        super().__init__()
        self.column_x: str = ""
        self.column_y: str = ""
        self.has_quantiles: bool = False
        self._heatmap: Heatmap | None = None
        self._last_display: int = 0

    def obs_columns(self, change: dict[str, AnyType]) -> None:
        if self.child.choice_x.value and self.child.choice_y.value:
            self.child.start_btn.disabled = False
            self.column_x = self.child.choice_x.value.split(":")[0]
            self.column_y = self.child.choice_y.value.split(":")[0]
        else:
            self.child.start_btn.disabled = True

    def obs_trans(self, change: dict[str, AnyType]) -> None:
        if self._heatmap is not None:
            self._heatmap.params.transform = int(change["new"])

    def obs_gaussian_blur(self, change: dict[str, AnyType]) -> None:
        if self._heatmap is not None:
            self._heatmap.params.gaussian_blur = int(change["new"])

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = self.dtypes
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}
        num_cols = [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if (t.startswith("float") or t.startswith("int"))
        ]
        self.child.choice_dim = ipw.Dropdown(
            options=[("512*512", "512"), ("256*256", "256"), ("128*128", "128")],
            value="512",
            description="Definition",
            disabled=False,
            # layout={"width": "initial"},
        )
        self.child.choice_trans = ipw.Dropdown(
            options=[("NONE", "1"), ("SQRT", "2"), ("CBRT", "3"), ("LOG", "4")],
            value="4",
            description="Transform",
            disabled=False,
            # layout={"width": "initial"},
        )
        self.child.choice_trans.observe(self.obs_trans, "value")
        self.child.gaussian_blur = ipw.IntSlider(
            value=0,
            min=0,
            max=5,
            step=1,
            description="Gaussian blur:",
            style={'description_width': 'initial'},
            disabled=False,
            continuous_update=False,
            orientation="horizontal",
            readout=True,
            readout_format="d",
        )
        self.child.gaussian_blur.observe(self.obs_gaussian_blur, "value")
        self.child.choice_x = ipw.Dropdown(
            options=num_cols + [""],
            value="",
            description="X",
            disabled=False,
            # layout={"width": "initial"},
        )
        self.child.choice_x.observe(self.obs_columns, "value")
        self.child.choice_y = ipw.Dropdown(
            options=num_cols + [""],
            value="",
            description="Y",
            disabled=False,
            # layout={"width": "initial"},
        )
        self.child.choice_y.observe(self.obs_columns, "value")
        self.has_quantiles = isinstance(self.input_module, Quantiles)
        self.child.min_q = (
            make_float("MinQuant.:", value=0.03) if self.has_quantiles else dongle_widget()
        )
        self.child.max_q = (
            make_float("MaxQuant.:", value=0.97) if self.has_quantiles else dongle_widget()
        )
        self.child.image = dongle_widget()
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )
        replay_next()

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        assert self.column_x and self.column_y
        xy = dict(X=self.column_x, Y=self.column_y,
                  dim=self.child.choice_dim.value,
                  min_q=self.child.min_q.value,
                  max_q=self.child.max_q.value,
                  trans=self.child.choice_trans.value,
                  blur=self.child.gaussian_blur.value
                  )
        if is_recording():
            amend_last_record({"frozen": xy})
        self.init_heatmap(xy)
        self.make_leaf_bar(self.after_run)
        btn.disabled = True
        self.child.choice_x.disabled = True
        self.child.choice_y.disabled = True
        self.child.choice_dim.disabled = True

    @modules_producer
    def init_heatmap(self, ctx: dict[str, AnyType]) -> Heatmap:
        col_x = ctx["X"]
        col_y = ctx["Y"]
        print("XY", ctx)
        #DIM = int(self.child.choice_dim.value)
        DIM = ctx["dim"]
        self.child.image = ipw.Image(value=b"\x00")
        s = self.input_module.scheduler()
        query = quantiles = self.input_module
        with s:
            histogram2d = Histogram2D(col_x, col_y, xbins=DIM, ybins=DIM, scheduler=s)
            # Connect the module to the csv results and the min,max bounds to rescale
            if self.has_quantiles:
                histogram2d.input.table = quantiles.output.table
                histogram2d.input.min = quantiles.output.result[ctx["min_q"]]
                histogram2d.input.max = quantiles.output.result[ctx["max_q"]]
            else:
                histogram2d.input.table = query.output.result
                min_ = Min(scheduler=s)
                min_.input.table = query.output.result[col_x, col_y]
                max_ = Max(scheduler=s)
                max_.input.table = query.output.result[col_x, col_y]
                histogram2d.input.min = min_.output.result
                histogram2d.input.max = max_.output.result
            # histogram2d.input.min = query.output.min
            # histogram2d.input.max = query.output.max
            # Create a module to create an heatmap image from the histogram2d
            heatmap = Heatmap(scheduler=s)
            # Connect it to the histogram2d
            heatmap.input.array = histogram2d.output.result
            self.histogram = histogram2d
            self._heatmap = heatmap
            self._heatmap.params.transform = int(ctx["trans"])
            self._heatmap.params.gaussian_blur = ctx["blur"]
            self.after_run = AfterRun()
            heatmap.on_after_run(self.after_run)  # Install the callback
            self.dag_running()
            return heatmap

    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(self)
        return self

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_heatmap(content)
        self.output_slot = "result"

    def get_underlying_modules(self) -> list[object]:
        return []

stage_register["Heatmap"] = HeatmapW
