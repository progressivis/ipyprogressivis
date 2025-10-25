from .utils import (
    make_button,
    stage_register,
    disable_all,
    VBoxTyped,
    TypedBase,
    needs_dtypes,
    replay_next,
    runner,
    GuestWidget,
    Coro,
    modules_producer,
    is_recording,
    amend_last_record
)
import copy
import pandas as pd
import ipywidgets as ipw
from collections import defaultdict
from progressivis.core.api import Module, asynchronize
from progressivis.vis import MCScatterPlot
from progressivis.core.api import JSONEncoderNp as JS
from ..scatterplot import Scatterplot
from ..df_grid import DataFrameGrid

from typing import Any as AnyType


WidgetType = AnyType
_l = ipw.Label

MAX_DIM = 512

class AfterRun(Coro):
    widget: Scatterplot | None = None
    async def action(self, m: Module, run_number: int) -> None:
        wg = self.widget
        assert wg is not None
        val = m.to_json()
        data_ = {
            k: v
            for (k, v) in val.items()
            if k not in ("hist_tensor", "sample_tensor")
        }
        ht = val.get("hist_tensor", None)
        def _func() -> None:
            if ht is not None:
                wg.hists = copy.copy(ht)  # TODO: avoid copy when possible
            st = val.get("sample_tensor", None)
            if st is not None:
                wg.samples = copy.copy(st)
            wg.data = JS.dumps(data_)  # type: ignore
        await asynchronize(_func)

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


class MCDensityMapW(VBoxTyped):
    class Typed(TypedBase):
        columns: DataFrameGrid
        start_btn: ipw.Button
        image: Scatterplot


    def __init__(self) -> None:
        super().__init__()
        self._last_display: int = 0


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
        df = pd.DataFrame(
            index=num_cols,
            columns=["Axis", "Class"],
            dtype=object,
        )
        df.loc[:, "Axis"] = lambda: ipw.Dropdown(  # type: ignore
            options=[("", ""), ("x", "x_column"), ("y", "y_column")],
            value="",
            description="",
            disabled=False)
        df.loc[:, "Class"] = lambda: ipw.Dropdown(  # type: ignore
            options=["", "A", "B", "C", "D"],
            value="",
            description="",
            disabled=False)
        self.child.columns = DataFrameGrid(df)
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=False
        )
        replay_next()

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        df = self.child.columns.df
        class_dict: dict[str, dict[str, str]] = defaultdict(dict)
        for row in df.itertuples():
            if not row.Class.value:
                continue
            class_dict[row.Class.value][row.Axis.value] = row.Index.split(":")[0]
            class_dict[row.Class.value]["name"] = row.Class.value
        # TODO: check validity
        if is_recording():
            ctx = list(class_dict.values())
            amend_last_record({"frozen": ctx})
        self.init_map(ctx=ctx)

    @modules_producer
    def init_map(self, ctx: list[dict[str, AnyType]]) -> MCScatterPlot:
        assert isinstance(self.input_module, Module)
        s = self.input_module.scheduler
        self.child.image = Scatterplot()
        with s:
            heatmap = MCScatterPlot(scheduler=s, classes=ctx, approximate=True)
            heatmap.create_dependent_modules(self.input_module, self.input_slot)
            after_run = AfterRun()
            after_run.widget = self.child.image
            self.child.image.link_module(heatmap, refresh=False)
            heatmap.on_after_run(after_run)  # Install the callback
            self.dag_running()
            self.make_leaf_bar(after_run)
            return heatmap

    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(self)
        return self

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_map(content)
        self.output_slot = "result"

    def get_underlying_modules(self) -> list[object]:
        return []

stage_register["MCDensityMap"] = MCDensityMapW
