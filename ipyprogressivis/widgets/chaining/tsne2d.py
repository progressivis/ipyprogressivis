from .utils import (
    chaining_widget,
    VBox,
    Coro,
    is_leaf,
    no_progress_bar
)
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    box,
    hbox,
    int_text,
    float_text,
    dropdown,
)
import ipywidgets as ipw
from ..contour_density import ContourDensity  # type: ignore
from progressivis.core.api import Module, Sink, asynchronize
from progressivis.stats.tsne import TSNE
from typing import Any as AnyType, cast

WidgetType = AnyType

class AfterRun(Coro):
    async def action(self, m: Module, run_number: int) -> None:
        try:
            assert isinstance(m, TSNE)
            if m.result is None:
                return
            def _func() -> None:
                assert self.leaf is not None
                assert self.leaf._proxy is not None
                proxy = self.leaf._proxy
                image_box = cast(ipw.Box, self.leaf._proxy.that.image.widget)
                if not image_box.children:
                    return
                contour_density = cast(ContourDensity, image_box.children[0])
                contour_density.update(m.result)
                proxy.that.rows.attrs(
                    value=len(self.leaf.input_module.result)  # type: ignore
                )
                proxy.that.iteration.attrs(
                    value=self.leaf._init_max_iter - m._max_iter  # type: ignore
                )
                proxy.that.quality.attrs(value=m.tsne.get_error())  # type: ignore
            await asynchronize(_func)
        except Exception as exc:
            print("ERRR", type(exc), exc, exc.args)
            raise

@is_leaf
@no_progress_bar
@chaining_widget(label="T-SNE 2D")
class TSNE2DW(VBox):
    def __init__(self) -> None:
        super().__init__()
        self._init_max_iter = 0

    def initialize(self) -> None:
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}
        arr_cols = [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if "*" in t
        ]
        self._proxy = anybox(
            self,
            dropdown(
                "Array column",
                options=arr_cols + [""],
                style={"description_width": "initial"}
            ).uid("col_choice").observe(self.obs_columns),
            int_text(
                "Max iterations:",
                value=1000,
                style={"description_width": "initial"}
            ).uid("max_iter"),
            float_text(
                "Quality to reach:",
                value=2.,
                style={"description_width": "initial"}
            ).uid("qual_lim"),
            button("Start")
            .uid("start_btn")
            .on_click(self.starter_callback),
            box().uid("image"),
            hbox(
                int_text("Rows:").uid("rows"),
                int_text("Iteration:").uid("iteration"),
                float_text("Error:").uid("quality"),

            )
        )

    def obs_columns(self, proxy: Proxy, change: dict[str, AnyType]) -> None:
        assert self._proxy is not None
        self._proxy.that.start_btn.attrs(
            disabled=not self._proxy.that.col_choice.widget.value
        )

    def starter_callback(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self._proxy is not None
        array_column = self._proxy.that.col_choice.widget.value.split(":")[0]
        assert array_column
        params = dict(array=array_column,
                      max_iter=self._proxy.that.max_iter.widget.value,
                      qual_lim=self._proxy.that.qual_lim.widget.value)
        self.record = self._proxy.dump()
        self.init_modules(params)

    def init_modules(self, ctx: dict[str, AnyType]) -> Module:
        assert self._proxy is not None
        array = ctx["array"]
        self._init_max_iter = ctx["max_iter"]
        qual_lim = ctx["qual_lim"]
        image_box =  self._proxy.that.image.widget
        assert hasattr(image_box, "children")
        if not image_box.children:
            image_box.children = [ContourDensity()]
        s = self.input_module.scheduler
        with s:
            tsne = TSNE(array_col=array,
                        output_cols=["x", "y"],
                        max_iter=self._init_max_iter,
                        qual_lim=qual_lim,
                        scheduler=s)
            tsne.input.table = self.input_module.output[self.input_slot]
            sink = Sink(scheduler=s)
            sink.input.inp = tsne.output.result
            self.after_run = AfterRun()
            tsne.on_after_run(self.after_run)  # Install the callback
        return tsne

    def run(self) -> AnyType:
        assert self._proxy is not None
        array_column = self._proxy.that.col_choice.widget.value.split(":")[0]
        assert array_column
        params = dict(array=array_column,
                      max_iter=self._proxy.that.max_iter.widget.value,
                      qual_lim=self._proxy.that.qual_lim.widget.value)
        self.output_module = self.init_modules(params)
        self.output_slot = "result"
