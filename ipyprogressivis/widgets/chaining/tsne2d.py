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
    GuestWidget, IpyHBoxTyped,
    Coro,
    modules_producer
)
import ipywidgets as ipw
from ..contour_density import ContourDensity  # type: ignore
from progressivis.core.api import Module, Sink
from progressivis.stats.tsne import TSNE
from typing import Any as AnyType

WidgetType = AnyType

class InfoBar(IpyHBoxTyped):
    class Typed(TypedBase):
        rows: ipw.IntText
        iteration: ipw.IntText
        quality: ipw.FloatText

class AfterRun(Coro):
    widget: ContourDensity | None = None
    async def action(self, m: Module, run_number: int) -> None:
        #print(f"{m.name} {self.widget}", flush=True)
        try:
            assert isinstance(m, TSNE)
            assert self.widget is not None
            if m.result is None:
                return
            #self.widget.child.image.data = m.result.to_array().tolist()  # type: ignore
            self.widget.child.image.update(m.result)
            info = self.widget.child.info
            info.child.rows.value = len(self.widget.input_module.result)
            info.child.iteration.value = self.widget._init_max_iter - m._max_iter
            info.child.quality.value = m.tsne.get_error()  # type: ignore
        except Exception as exc:
            print("ERRR", type(exc), exc, exc.args)
            raise
class TSNE2DW(VBoxTyped):
    class Typed(TypedBase):
        col_choice: ipw.Dropdown
        max_iter: ipw.IntText
        qual_lim: ipw.FloatText
        start_btn: ipw.Button
        image: ContourDensity
        info: InfoBar

    def __init__(self) -> None:
        super().__init__()
        self.array_column: str = ""
        self._init_max_iter = 0

    @needs_dtypes
    def initialize(self) -> None:
        print("BEGIN ini")
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}
        arr_cols = [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if "*" in t
        ]
        self.child.col_choice = ipw.Dropdown(
            options=arr_cols + [""],
            value="",
            description="Array column",
            disabled=False,
            style={"description_width": "initial"},
            # layout={"width": "initial"},
        )
        self.child.col_choice.observe(self.obs_columns, "value")
        self.child.max_iter = ipw.IntText(
            value=1000, description="Max iterations:", disabled=False,
            style={"description_width": "initial"},
        )
        self.child.qual_lim = ipw.FloatText(
            value=2., description="Quality to reach:", disabled=False,
            style={"description_width": "initial"},
        )

        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )
        print("END ini")
        replay_next()

    def obs_columns(self, change: dict[str, AnyType]) -> None:
        if self.child.col_choice.value:
            self.child.start_btn.disabled = False
            self.array_column = self.child.col_choice.value.split(":")[0]
        else:
            self.child.start_btn.disabled = True

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        assert self.array_column
        params = dict(array=self.array_column,
                      max_iter=self.child.max_iter.value,
                      qual_lim=self.child.qual_lim.value)
        if is_recording():
            amend_last_record({"frozen": params})
        self.init_module(params)
        btn.disabled = True
        self.manage_replay()
    @modules_producer
    def init_module(self, ctx: dict[str, AnyType]) -> Module:
        array = ctx["array"]
        self._init_max_iter = ctx["max_iter"]
        qual_lim = ctx["qual_lim"]
        self.child.image = ContourDensity()
        self.child.info = InfoBar()
        self.child.info.child.rows = ipw.IntText(description="Rows:")
        self.child.info.child.iteration = ipw.IntText(description="Iteration:")
        self.child.info.child.quality = ipw.FloatText(description="Error:")

        s = self.input_module.scheduler()
        with s:
            tsne = TSNE(array_col=array,
                        output_cols=["x", "y"],
                        max_iter=self._init_max_iter,
                        qual_lim=qual_lim,
                        scheduler=s)
            tsne.input.table = self.input_module.output[self.input_slot]
            sink = Sink(scheduler=s)
            sink.input.inp = tsne.output.result
            after_run = AfterRun()
            after_run.widget = self
            tsne.on_after_run(after_run)  # Install the callback
            self.dag_running()
            self.make_leaf_bar(after_run)
        return tsne

    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(self)
        return self

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.output_module = self.init_module(content)
        self.output_slot = "result"

    def get_underlying_modules(self) -> list[object]:
        return []

stage_register["T-SNE 2D"] = TSNE2DW
