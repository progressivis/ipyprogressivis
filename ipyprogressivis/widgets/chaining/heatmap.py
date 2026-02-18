from .utils import (
    starter_callback,
    is_leaf,
    no_progress_bar,
    chaining_widget,
    disable_all,
    VBox,
    needs_dtypes,
    # replay_next,
    runner,
    GuestWidget,
    Coro,
    modules_producer,
)
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    dropdown,
    stack,
    html,
    bounded_float_text,
    int_slider,
    image,
    restore,
)

import ipywidgets as ipw
from progressivis.core.api import Module, asynchronize
from progressivis.vis.heatmap import Heatmap
from progressivis.stats.api import Histogram2D, Min, Max
from progressivis import Quantiles
from typing import Any as AnyType

WidgetType = AnyType
_l = ipw.Label

MAX_DIM = 512


class AfterRun(Coro):
    proxy: Proxy | None = None

    async def action(self, m: Module, run_number: int) -> None:
        if self.proxy is None:
            return
        assert isinstance(m, Heatmap)

        def _func() -> None:
            try:
                image = m.get_image_bin()
                if image is not None:
                    self.proxy.that.image.widget.value = image  # type: ignore
            except Exception:
                import traceback

                print(traceback.format_exc())

        await asynchronize(_func)


def make_float(
    description: str,
    uid: str,
    disabled: bool = False,
    value: float = 0.0,
    index: int = 0,
) -> Proxy:
    return stack(
        html(""),
        bounded_float_text(
            description, value=value, min=0.0, max=1.0, step=0.001, disabled=disabled
        ).uid(uid),
        selected_index=index,
    )


@is_leaf
@no_progress_bar
@chaining_widget(label="Heatmap")
class HeatmapW(VBox):
    def __init__(self) -> None:
        super().__init__()
        self.column_x: str = ""
        self.column_y: str = ""
        self.has_quantiles: bool = False
        self._heatmap: Heatmap | None = None
        self._last_display: int = 0

    def get_num_cols(self) -> list[tuple[str, str]]:
        return [
            (col, c)
            for (col, (c, t)) in self.col_typed_names.items()
            if (t.startswith("float") or t.startswith("int"))
        ] + [("", "")]

    def obs_columns(self, proxy: Proxy, change: dict[str, AnyType]) -> None:
        if proxy.that.choice_x.widget.value and proxy.that.choice_y.widget.value:
            proxy.that.start_btn.attrs(disabled=False)
            self.column_x = proxy.that.choice_x.widget.value.split(":")[0]
            self.column_y = proxy.that.choice_y.widget.value.split(":")[0]
        else:
            proxy.that.start_btn.attrs(disabled=True)

    def obs_trans(self, proxy: Proxy, change: dict[str, AnyType]) -> None:
        if self._heatmap is not None:
            self._heatmap.params.transform = int(change["new"])

    def obs_gaussian_blur(self, proxy: Proxy, change: dict[str, AnyType]) -> None:
        if self._heatmap is not None:
            self._heatmap.params.gaussian_blur = int(change["new"])

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = self.dtypes
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}

        self.has_quantiles = isinstance(self.input_module, Quantiles)
        self._proxy = anybox(
            self,
            dropdown(
                "Definition",
                options=[("512*512", "512"), ("256*256", "256"), ("128*128", "128")],
                value="512",
            ).uid("choice_dim"),
            dropdown(
                "X",
                options=self.get_num_cols(),
                value="",
            )
            .uid("choice_x")
            .observe(self.obs_columns),
            dropdown(
                "Y",
                options=self.get_num_cols(),
                value="",
            )
            .uid("choice_y")
            .observe(self.obs_columns),
            make_float("MinQuant.:", uid="min_q", value=0.03, index=self.has_quantiles),
            make_float("MaxQuant.:", uid="max_q", value=0.97, index=self.has_quantiles),
            dropdown(
                "Transform",
                options=[("NONE", "1"), ("SQRT", "2"), ("CBRT", "3"), ("LOG", "4")],
                value="4",
            )
            .uid("choice_trans")
            .observe(self.obs_trans),
            int_slider(
                "Gaussian blur:",
                value=0,
                min=0,
                max=5,
                step=1,
                style={"description_width": "initial"},
                continuous_update=False,
                orientation="horizontal",
                readout=True,
                readout_format="d",
            )
            .uid("gaussian_blur")
            .observe(self.obs_gaussian_blur),
            button("Start").uid("start_btn").on_click(self._start_btn_cb),
            image(width=512, height=512).uid("image"),
        )

        # replay_next()

    def fetch_parameters(self) -> dict[str, AnyType]:
        return dict(
            X=self._proxy.that.choice_x.widget.value.split(":")[0],
            Y=self._proxy.that.choice_y.widget.value.split(":")[0],
            dim=self._proxy.that.choice_dim.widget.value,
            min_q=self._proxy.that.min_q.widget.value,
            max_q=self._proxy.that.max_q.widget.value,
            trans=self._proxy.that.choice_trans.widget.value,
            blur=self._proxy.that.gaussian_blur.widget.value,
        )

    @starter_callback
    def _start_btn_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self.column_x and self.column_y
        content = self._proxy.dump()
        self.record = content  # saved for replay
        xy = self.fetch_parameters()
        self.output_module = self.init_modules(xy)

    @modules_producer
    def init_modules(self, ctx: dict[str, AnyType]) -> Heatmap:
        col_x = ctx["X"]
        col_y = ctx["Y"]
        print("XY", ctx)
        DIM = ctx["dim"]
        s = self.input_module.scheduler
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
            self.after_run = AfterRun(heatmap)
            self.after_run.proxy = self._proxy
            return heatmap

    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(
            self,
            exceptions=(
                self._proxy.that.image.widget,
                self._proxy.that.choice_trans.widget,
                self._proxy.that.gaussian_blur.widget,
            ),
        )
        return self

    @runner
    def run(self) -> AnyType:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
        content = self.fetch_parameters()
        self.output_module = self.init_modules(content)
        self.output_slot = "result"

    def init_ui(self) -> None:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        options = self.get_num_cols()
        self._proxy.that.choice_x.attrs(options=options)
        self._proxy.that.choice_y.attrs(options=options)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
