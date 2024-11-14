# type: ignore
from .utils import (
    make_button,
    stage_register,
    VBoxTyped,
    TypedBase,
    needs_dtypes,
    replay_next,
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

DIM = 512


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
        choice_x: ipw.Dropdown
        choice_y: ipw.Dropdown
        # freeze_ck: ipw.Checkbox
        min_q: ipw.BoundedFloatText | ipw.Label
        max_q: ipw.BoundedFloatText | ipw.Label
        start_btn: ipw.Button
        image: ipw.Image | ipw.Label

    def __init__(self) -> None:
        super().__init__()
        self.column_x: str = ""
        self.column_y: str = ""
        self.has_quantiles: bool = False

    def obs_columns(self, change: dict[str, AnyType]) -> None:
        if self.child.choice_x.value and self.child.choice_y.value:
            self.child.start_btn.disabled = False
            self.column_x: str = self.child.choice_x.value.split(":")[0]
            self.column_y: str = self.child.choice_y.value.split(":")[0]
        else:
            self.child.start_btn.disabled = True

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
        # if self.child.freeze_ck.value:
        #    amend_last_record({"frozen": xy})
        self.init_heatmap(xy)
        btn.disabled = True

    def init_heatmap(self, ctx) -> None:
        col_x = ctx["X"]
        col_y = ctx["Y"]
        print("XY", ctx)
        self.child.image = ipw.Image(value=b"\x00", width=DIM, height=DIM)
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

            async def _after_run(m: Module, run_number: int) -> None:
                assert isinstance(m, Heatmap)
                image = m.get_image_bin()  # get the image from the heatmap
                if image is not None:
                    self.child.image.value = (
                        image  # Replace the displayed image with the new one
                    )

            heatmap.on_after_run(_after_run)  # Install the callback
            return heatmap

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_heatmap(content)
        self.output_slot = "result"


stage_register["Heatmap"] = HeatmapW
