# type: ignore
from .utils import (
    make_button,
    stage_register,
    VBoxTyped,
    TypedBase,
    amend_nth_record,
    get_last_record_index,
    is_recording,
    runner,
    needs_dtypes,
    modules_producer
)
import ipywidgets as ipw
import pandas as pd
from progressivis import (
    BinningIndexND,
    RangeQuery2D,
    Variable,
)
import progressivis.core.aio as aio
from progressivis.core.api import Sink, Module
from ..df_grid import DataFrameGrid
from typing import Any as AnyType

WidgetType = AnyType


class RangeQuery2DW(VBoxTyped):
    class Typed(TypedBase):
        grid: DataFrameGrid
        buttons: ipw.HBox

    def __init__(self) -> None:
        super().__init__()
        self.column_x: str = ""
        self.column_y: str = ""
        self.index: BinningIndexND | None = None
        self._saved_settings: dict[str, float] = {}
        self._record_index: int | None = None
        self._freeze_btn = make_button(
            "Freeze", cb=self._freeze_btn_cb, disabled=True
        )
        self._unfreeze_btn = make_button(
            "Unfreeze", cb=self._unfreeze_btn_cb, disabled=True
        )
        self._start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )
        self._unfilter_btn = make_button(
            "Unfilter", cb=self._unfilter_btn_cb, disabled=True
        )

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
            index=["X", "Y"], columns=["Column", "Min", "Filter", "Max"], dtype=object
        )
        df.loc[:, "Column"] = lambda: ipw.Dropdown(
            options=num_cols + [""],
            value="",
            description="",
            disabled=False,
            layout={"width": "initial"},
        )
        df.loc[:, "Min"] = lambda: ipw.FloatText(
            value=0.0,
            description="",
            disabled=False,
            layout={"width": "initial"},
        )
        df.loc[:, "Max"] = lambda: ipw.FloatText(
            value=0.0,
            description="",
            disabled=False,
            layout={"width": "initial"},
        )
        df.loc[:, "Filter"] = lambda: ipw.FloatRangeSlider(
            value=None,
            min=0.0,
            max=0.0,
            step=0.0,
            description="",
            disabled=False,
            continuous_update=False,
            orientation="horizontal",
            readout=True,
            readout_format=".1f",
            layout={"width": "initial"},
        )
        self.child.grid = DataFrameGrid(
            df,
            first="20px",
            repeat="100px",
            sizes={"Column": "200px", "Filter": "200px"},
        )
        self._freeze_btn.disabled = not is_recording()
        self.child.grid.observe_col("Column", self.obs_columns)
        self.child.buttons = ipw.HBox([self._freeze_btn, self._unfreeze_btn,
                                       self._start_btn, self._unfilter_btn])
        self.reset_buttons()

    def reset_buttons(self) -> None:
        self._freeze_btn.disabled = True
        self._unfreeze_btn.disabled = True
        self._saved_settings = {}

    def obs_columns(self, change: dict[str, AnyType]) -> None:
        df = self.child.grid.df
        if df.loc["X", "Column"].value and df.loc["Y", "Column"].value:
            self._start_btn.disabled = False
            self._freeze_btn.disabled = not is_recording()
            self.column_x: str = df.loc["X", "Column"].value.split(":")[0]
            self.column_y: str = df.loc["Y", "Column"].value.split(":")[0]
        else:
            self._start_btn.disabled = True

    def grid_update(self, m: Module, run_number: int) -> None:
        df = self.child.grid.df
        min_x = min_y = max_x = max_y = None
        col_x = self.column_x
        col_y = self.column_y
        min_x = self.index.min_out[col_x]
        min_y = self.index.min_out[col_y]
        max_x = self.index.max_out[col_x]
        max_y = self.index.max_out[col_y]
        df.loc["X", "Min"].value = min_x
        df.loc["Y", "Min"].value = min_y
        df.loc["X", "Max"].value = max_x
        df.loc["Y", "Max"].value = max_y
        if None in (min_x, min_y, max_x, max_y):
            return
        slider_x = df.loc["X", "Filter"]
        if min_x > slider_x.max:
            slider_x.max = max_x
            slider_x.min = min_x
        else:
            slider_x.min = min_x
            slider_x.max = max_x
        slider_y = df.loc["Y", "Filter"]
        if min_y > slider_y.max:
            slider_y.max = max_y
            slider_y.min = min_y
        else:
            slider_y.min = min_y
            slider_y.max = max_y
        if self._unfilter_btn.disabled:
            slider_x.value = [min_x, max_x]
            slider_x.step = (max_x - min_x) / 10
            slider_y.value = [min_y, max_y]
            slider_y.step = (max_y - min_y) / 10
            self._unfilter_btn.disabled = False

        def observer(_):
            async def _coro():
                x_min, x_max = slider_x.value
                y_min, y_max = slider_y.value
                await self.var_min.from_input({col_x: x_min, col_y: y_min})
                await self.var_max.from_input({col_x: x_max, col_y: y_max})

            aio.create_task(_coro())

        slider_x.observe(observer, "value")
        slider_y.observe(observer, "value")
        # slidex
        if not self.var_min.result:
            observer(slider_x)

    @modules_producer
    def init_min_max(self, ctx) -> None:
        col_x = ctx["X"]
        col_y = ctx["Y"]
        s = self.input_module.scheduler
        with s:
            index = BinningIndexND(scheduler=s)
            # Creates one index per numeric column
            index.input.table = self.input_module.output.result[col_x, col_y]
            # Create a querying module
            query = RangeQuery2D(column_x=col_x, column_y=col_y, scheduler=s)
            # Variable modules allow to dynamically modify their values; here, the query ranges
            init_val_min = ({col_x: ctx.get("x_min"), col_y: ctx.get("y_min")}
                            if "x_min" in ctx else None)
            var_min = Variable(init_val_min, name="var_min", scheduler=s)
            init_val_max = ({col_x: ctx.get("x_max"), col_y: ctx.get("y_max")}
                            if "x_max" in ctx else None)
            var_max = Variable(init_val_max, name="var_max", scheduler=s)
            self.var_min = var_min
            self.var_max = var_max
            query.input.lower = var_min.output.result
            query.input.upper = var_max.output.result
            query.input.index = index.output.result
            query.input.min = index.output.min_out
            query.input.max = index.output.max_out
            self.index = index
            sink = Sink(scheduler=s)
            sink.input.inp = query.output.result
            self.output_module = query
            self.output_slot = "result"
            self.output_dtypes = self.dtypes
            if self.column_x:
                self.index.on_after_run(self.grid_update)
            return query

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_min_max(content)
        self.output_slot = "result"

    def _save_settings(self) -> None:
        df = self.child.grid.df
        assert self.column_x and self.column_y
        slider_x = df.loc["X", "Filter"]
        slider_y = df.loc["Y", "Filter"]
        x_min, x_max = slider_x.value
        y_min, y_max = slider_y.value
        self._saved_settings = dict(
            x_min=x_min, x_max=x_max,
            y_min=y_min, y_max=y_max
        )

    def _freeze_btn_cb(self, btn: ipw.Button) -> None:
        self.child.buttons.children[1].disabled = False
        assert self.column_x and self.column_y
        self._save_settings()
        content = dict(X=self.column_x, Y=self.column_y)
        if self._saved_settings:
            content = dict(**content, **self._saved_settings)
            i = self._record_index
            assert i is not None
            amend_nth_record(i, {"frozen": content})

    def _unfreeze_btn_cb(self, btn: ipw.Button) -> None:
        self._saved_settings = {}
        self.child.buttons.children[1].disabled = True

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        assert self.column_x and self.column_y
        content = dict(X=self.column_x, Y=self.column_y)
        if self._saved_settings:
            content = dict(**content, **self._saved_settings)
            i = get_last_record_index()
            assert i is not None
            amend_nth_record(i, {"frozen": content})
        self.init_min_max(content)
        btn.disabled = True
        self.dag_running()
        self.make_chaining_box()
        self.manage_replay()

    def _unfilter_btn_cb(self, btn: ipw.Button) -> None:
        df = self.child.grid.df
        slider_x = df.loc["X", "Filter"]
        slider_y = df.loc["Y", "Filter"]
        min_x = slider_x.min
        max_x = slider_x.max
        slider_x.value = min_x, max_x
        slider_x.step = (max_x - min_x) / 10
        min_y = slider_y.min
        max_y = slider_y.max
        slider_y.value = min_y, max_y
        slider_y.step = (max_y - min_y) / 10


stage_register["RangeQuery2D"] = RangeQuery2DW
