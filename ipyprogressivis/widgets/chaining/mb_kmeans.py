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
import numpy as np
import ipywidgets as ipw
from progressivis.core.api import Module
from progressivis.vis import MCScatterPlot
from progressivis.cluster import MBKMeans
from progressivis.core.api import JSONEncoderNp as JS
from ..scatterplot import Scatterplot

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
        if ht is not None:
            wg.hists = copy.copy(ht)  # TODO: avoid copy when possible
        st = val.get("sample_tensor", None)
        if st is not None:
            wg.samples = copy.copy(st)
        wg.data = JS.dumps(data_)  # type: ignore


class MBKMeansW(VBoxTyped):
    class Typed(TypedBase):
        choice_x: ipw.Dropdown
        choice_y: ipw.Dropdown
        batch_size: ipw.IntText
        n_clusters: ipw.IntText
        awake_btn: ipw.Button
        start_btn: ipw.Button
        image: Scatterplot

    def __init__(self) -> None:
        super().__init__()
        self._last_display: int = 0
        self.column_x: str = ""
        self.column_y: str = ""

    def obs_columns(self, change: dict[str, AnyType]) -> None:
        if self.child.choice_x.value and self.child.choice_y.value:
            self.child.start_btn.disabled = False
            self.column_x = self.child.choice_x.value.split(":")[0]
            self.column_y = self.child.choice_y.value.split(":")[0]
        else:
            self.child.start_btn.disabled = True

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = self.dtypes
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t)
                                for (n, t) in self.col_types.items()}
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
        self.child.batch_size = ipw.IntText(
            value=100,
            description='Batch size:',
            disabled=False
        )
        self.child.n_clusters = ipw.IntText(
            value=15,
            description='N clusters:',
            disabled=False
        )

        self.child.awake_btn = make_button(
            "Awake", cb=self._awake_btn_cb, disabled=True
        )
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=False
        )
        replay_next()

    def _awake_btn_cb(self, btn: ipw.Button) -> None:
        self._awake()

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        ctx = dict(
            col_x=self.column_x,
            col_y=self.column_y,
            batch_size=self.child.batch_size.value,
            n_clusters=self.child.n_clusters.value,
        )
        if is_recording():
            amend_last_record({"frozen": ctx})
        self.init_map(ctx=ctx)

    @modules_producer
    def init_map(self, ctx: dict[str, AnyType]) -> MCScatterPlot:
        assert isinstance(self.input_module, Module)
        s = self.input_module.scheduler
        self.child.image = Scatterplot()
        _0 = ctx["col_x"]
        _1 = ctx["col_y"]
        with s:
            mbkmeans = MBKMeans(n_clusters=ctx["n_clusters"],
                                batch_size=ctx["batch_size"], is_input=False,
                                scheduler=s)
            sp = MCScatterPlot(scheduler=s,
                               classes=[('Scatterplot', _0, _1, mbkmeans)],  # type: ignore
                               approximate=True)
            sp.create_dependent_modules(self.input_module, self.input_slot)
            sp['Scatterplot'].min_value.result.update({_0: -np.inf, _1: -np.inf})  # type: ignore
            sp['Scatterplot'].max_value.result.update({_0: np.inf, _1: np.inf})  # type: ignore
            mbkmeans.create_dependent_modules(sp['Scatterplot'].range_query_2d)  # type: ignore
            sp.move_point = mbkmeans.dep.moved_center  # type: ignore
            after_run = AfterRun()
            after_run.widget = self.child.image
            self._awake = self.child.image.link_module(sp, refresh=False)
            self.child.awake_btn.disabled = False
            sp.on_after_run(after_run)  # Install the callback
            self.dag_running()
            self.make_leaf_bar(after_run)
            return sp

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


stage_register["MBKMeans"] = MBKMeansW
