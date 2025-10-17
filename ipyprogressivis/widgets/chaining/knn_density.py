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
    Coro,
    modules_producer
)
import numpy as np
import ipywidgets as ipw
from ..knn_kernel import KNNDensity
from progressivis.core.api import Module, asynchronize
from progressivis.stats.kernel_density import KernelDensity
from typing import Any as AnyType

WidgetType = AnyType



class AfterRun(Coro):
    widget: KNNDensity | None = None
    async def action(self, m: Module, run_number: int) -> None:
        assert isinstance(m, KernelDensity)
        assert self.widget is not None
        def _func() -> None:
            self.widget.data = m.to_json()  # type: ignore
        await asynchronize(_func)

class KNNDensityW(VBoxTyped):
    class Typed(TypedBase):
        choice_bins: ipw.Dropdown
        choice_kernel: ipw.Dropdown
        choice_x: ipw.Dropdown
        choice_y: ipw.Dropdown
        start_btn: ipw.Button
        image: KNNDensity

    def __init__(self) -> None:
        super().__init__()
        self.column_x: str = ""
        self.column_y: str = ""

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
        self.child.choice_bins = ipw.Dropdown(
            options=[(str(i*10), i*10) for i in range(1, 10)],
            value=30,
            description="Bins",
            disabled=False,
            # layout={"width": "initial"},
        )
        self.child.choice_kernel = ipw.Dropdown(
            options=["gaussian"],
            value="gaussian",
            description="Kernel",
            disabled=False,
            # layout={"width": "initial"},
        )
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

        self.child.image = KNNDensity()
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )
        replay_next()

    def obs_columns(self, change: dict[str, AnyType]) -> None:
        if self.child.choice_x.value and self.child.choice_y.value:
            self.child.start_btn.disabled = False
            self.column_x = self.child.choice_x.value.split(":")[0]
            self.column_y = self.child.choice_y.value.split(":")[0]
        else:
            self.child.start_btn.disabled = True

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        assert self.column_x and self.column_y
        params = dict(X=self.column_x, Y=self.column_y,
                      n_bins=self.child.choice_bins.value,
                      kernel=self.child.choice_kernel.value
                      )
        if is_recording():
            amend_last_record({"frozen": params})
        self.init_knn(params)
        btn.disabled = True

    @modules_producer
    def init_knn(self, ctx: dict[str, AnyType]) -> KernelDensity:
        col_x = ctx["X"]
        col_y = ctx["Y"]
        n_bins = ctx["n_bins"]
        norm = n_bins / 10
        offset = norm / 2
        samples = np.indices((n_bins + 1, n_bins + 1)).reshape(2, -1).T / n_bins * norm - offset
        s = self.input_module.scheduler
        with s:
            knn = KernelDensity(scheduler=s, samples=samples, bins=n_bins)
            knn.input.table = self.input_module.output[self.input_slot][dict(x=col_x, y=col_y)]
            after_run = AfterRun()
            after_run.widget = self.child.image
            knn.on_after_run(after_run)  # Install the callback
            self.dag_running()
            self.make_leaf_bar(after_run)
        return knn

    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(self)
        return self

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_knn(content)
        self.output_slot = "result"

    def get_underlying_modules(self) -> list[object]:
        return []

stage_register["KNN Density"] = KNNDensityW
