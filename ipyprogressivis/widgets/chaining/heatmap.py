# type: ignore
import time
from .utils import (
    make_button,
    stage_register,
    VBoxTyped,
    TypedBase,
    needs_dtypes,
    replay_next,
    is_recording,
    amend_last_record,
    runner,
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


def make_float(
    description: str = "", disabled: bool = False, value=0.0
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
        min_q: ipw.BoundedFloatText | ipw.Label
        max_q: ipw.BoundedFloatText | ipw.Label
        choice_trans: ipw.Dropdown
        gaussian_blur: ipw.IntSlider
        start_btn: ipw.Button
        image: ipw.Image | ipw.Label
        display_period: ipw.IntSlider | ipw.Label

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
            self.column_x: str = self.child.choice_x.value.split(":")[0]
            self.column_y: str = self.child.choice_y.value.split(":")[0]
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
            make_float("Min:", value=0.03) if self.has_quantiles else ipw.Label()
        )
        self.child.max_q = (
            make_float("Max:", value=0.97) if self.has_quantiles else ipw.Label()
        )
        self.child.image = ipw.Label()
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )
        replay_next()

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        assert self.column_x and self.column_y
        xy = dict(X=self.column_x, Y=self.column_y)
        if is_recording():
            amend_last_record({"frozen": xy})
        self.init_heatmap(xy)
        btn.disabled = True
        self.child.choice_x.disabled = True
        self.child.choice_y.disabled = True
        self.child.choice_dim.disabled = True

    async def _after_run(self, m: Module, run_number: int) -> None:
        now = int(time.time())
        if now - self._last_display < self.child.display_period.value:
            return
        assert isinstance(m, Heatmap)
        image = m.get_image_bin()  # get the image from the heatmap
        if image is not None:
            self.child.image.value = (
                image  # Replace the displayed image with the new one
            )
        self._last_display = int(time.time())

    def init_heatmap(self, ctx) -> None:
        col_x = ctx["X"]
        col_y = ctx["Y"]
        print("XY", ctx)
        DIM = int(self.child.choice_dim.value)
        self.child.image = ipw.Image(value=b"\x00", width=MAX_DIM, height=MAX_DIM)
        self.child.display_period = ipw.IntSlider(
            value=1,
            min=1,
            max=10,
            step=1,
            description="Display periodicity:",
            style={'description_width': 'initial'},
            disabled=False,
            continuous_update=False,
            orientation="horizontal",
            readout=True,
            readout_format="d",
        )
        s = self.input_module.scheduler()
        query = quantiles = self.input_module
        with s:
            histogram2d = Histogram2D(col_x, col_y, xbins=DIM, ybins=DIM, scheduler=s)
            # Connect the module to the csv results and the min,max bounds to rescale
            if self.has_quantiles:
                histogram2d.input.table = quantiles.output.table
                histogram2d.input.min = quantiles.output.result[self.child.min_q.value]
                histogram2d.input.max = quantiles.output.result[self.child.max_q.value]
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
            self._heatmap = heatmap
            self._heatmap.params.transform = int(self.child.choice_trans.value)
            self._heatmap.params.gaussian_blur = self.child.gaussian_blur.value
            heatmap.on_after_run(self._after_run)  # Install the callback
            return heatmap

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_heatmap(content)
        self.output_slot = "result"


stage_register["Heatmap"] = HeatmapW
