from .utils import (
    is_leaf,
    no_progress_bar,
    starter_callback,
    chaining_widget,
    disable_all,
    VBox,
    needs_dtypes,
    runner,
    GuestWidget,
    Coro,
    modules_producer,
)
import ipywidgets as ipw
from progressivis.core.api import Module, asynchronize, Sink
from progressivis.vis import MCScatterPlot
from progressivis.cluster import MBKMeans, MBKMeansFilter
from progressivis.core.api import JSONEncoderNp as JS
from ipytablewidgets import NumpyAdapter  # type: ignore
from ..scatterplot import Scatterplot
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    dropdown,
    int_text,
    restore,
)
from typing import Any as AnyType

WidgetType = AnyType
_l = ipw.Label

MAX_DIM = 512

class MyScatterplot(Scatterplot):
    def __init__(self,*args: AnyType, **kw: AnyType) -> None:
        super().__init__(*args, enable_centroids=True, **kw)

def scatterplot_wg(descr: str | None = None, **kw: AnyType) -> Proxy:
    kw2 = dict() if descr is None else dict(description=descr)
    proxy = Proxy(MyScatterplot())
    proxy.attrs(**kw, **kw2)
    return proxy

class AfterRun(Coro):
    proxy: Proxy | None = None

    async def action(self, m: Module, run_number: int) -> None:
        if self.proxy is None:
            return
        wg = self.proxy.that.scatterplot_.widget
        assert wg is not None
        val = m.to_json()
        data_ = {
            k: v
            for (k, v) in val.items()
            if k not in ("hist_tensor", "sample_tensor")
        }
        ht = val.get("hist_tensor")
        def _func() -> None:
            arrays = dict()
            if ht is not None:
                for i, arr in enumerate(ht):
                    arrays[f"hist_{i}"] = arr
                wg.histograms = NumpyAdapter(arrays, touch_mode=False)  # type: ignore
            st = val.get("sample_tensor")
            if st is not None:
                vectors = {f"v{i}": vec for (i, vec) in enumerate(st)}
                wg.samples = NumpyAdapter(vectors, touch_mode=False)  # type: ignore
            wg.data = JS.dumps(data_)  # type: ignore
        await asynchronize(_func)

@is_leaf
@no_progress_bar
@chaining_widget(label="MBKMeans")
class MBKMeansW(VBox):
    def __init__(self) -> None:
        super().__init__()
        self._last_display: int = 0
        self.column_x: str = ""
        self.column_y: str = ""

    def obs_columns(self, proxy: Proxy, change: dict[str, AnyType]) -> None:
        if proxy.that.choice_x.widget.value and proxy.that.choice_y.widget.value:
            proxy.that.start_btn.attrs(disabled=False)
            self.column_x = proxy.that.choice_x.widget.value.split(":")[0]
            self.column_y = proxy.that.choice_y.widget.value.split(":")[0]
        else:
            proxy.that.start_btn.attrs(disabled=True)

    def get_num_cols(self) -> list[str]:
        return [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if (t.startswith("float") or t.startswith("int"))
        ] + [""]

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = self.dtypes
        self.col_types = {k: str(t) for (k, t) in self.dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t)
                                for (n, t) in self.col_types.items()}
        self._proxy = anybox(
            self,
            dropdown(
                "X",
                options=self.get_num_cols(),
                value=""
                ).uid("choice_x")
            .observe(self.obs_columns),
            dropdown(
                "Y",
                options=self.get_num_cols(),
                value="",
            )
            .uid("choice_y")
            .observe(self.obs_columns),
            int_text(
                "Batch size:",
                value=100,
            ).uid("batch_size"),
            int_text(
                "N clusters:",
                value=5,
            ).uid("n_clusters"),
            button("Start").uid("start_btn").on_click(self._start_btn_cb),
            scatterplot_wg().uid("scatterplot_"),  # TODO: customize
        )

    """@starter_callback
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
    """
    @starter_callback
    def _start_btn_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self.column_x and self.column_y
        content = self._proxy.dump()
        self.record = content  # saved for replay
        xy = self.fetch_parameters()
        self.output_module = self.init_modules(xy)

    def fetch_parameters(self) -> dict[str, AnyType]:
        return dict(
            X=self._proxy.that.choice_x.widget.value.split(":")[0],
            Y=self._proxy.that.choice_y.widget.value.split(":")[0],
            batch_size=self._proxy.that.batch_size.widget.value,
            n_clusters=self._proxy.that.n_clusters.widget.value,
        )

    @modules_producer
    def init_modules(self, ctx: dict[str, AnyType]) -> MCScatterPlot:
        assert isinstance(self.input_module, Module)
        s = self.input_module.scheduler
        x_col = ctx["X"]
        y_col = ctx["Y"]
        n_clusters = ctx["n_clusters"]
        batch_size = ctx["batch_size"]
        with s:
            mbkmeans = MBKMeans(n_clusters=n_clusters, tol=0.01,
                                batch_size=batch_size, is_input=False,
                                scheduler=s)
            mbkmeans.create_dependent_modules(self.input_module, self.input_slot)
            classes = []
            for i in range(n_clusters):
                cname = f"k{i}"
                filt = MBKMeansFilter(i, scheduler=s)
                filt.create_dependent_modules(mbkmeans, self.input_module, self.input_slot)
                sink = Sink(scheduler=s)
                sink.input.inp = filt.output.result
                classes.append({'name': cname, 'x_column': x_col,
                                'y_column': y_col, 'sample': mbkmeans if i==0 else None,
                                'sample_slot': 'result',
                                'input_module': filt, 'input_slot': 'result'})
            """for i in range(n_clusters):
                cname = f"k{i}"
                filt = MBKMeansSelector(i, scheduler=s)
                filt.input.table = self.input_module.output.result
                filt.input.label_dict = mbkmeans.output.label_dict
                sink = Sink(scheduler=s)
                sink.input.inp = filt.output.result
                classes.append({'name': cname, 'x_column': x_col,
                                'y_column': y_col, 'sample': mbkmeans if i==0 else None,
                                'sample_slot': 'result',
                                'input_module': filt, 'input_slot': 'result'})"""

            sp = MCScatterPlot(scheduler=s,
                               classes=classes,
                               queryable=False)
            sp.create_dependent_modules()
            sp.move_point = mbkmeans.dep.moved_center  # type: ignore
            self.after_run = AfterRun(sp)
            self.after_run.proxy = self._proxy
            #after_run.widget = self.proxy.that.scatterplot.widget
            #self._awake = self.child.image.link_module(sp, refresh=False)
            #self.child.awake_btn.disabled = False
            #sp.on_after_run(after_run)  # Install the callback
            return sp

    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(
            self,
            exceptions=(
                self._proxy.that.scatterplot_.widget,
            ),
        )
        return self
    @runner
    def run(self) -> AnyType:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self, custom=dict(MyScatterplot=scatterplot_wg))
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
        content = self.fetch_parameters()
        self.output_module = self.init_modules(content)
        self.output_slot = "result"

    def init_ui(self) -> None:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self, custom=dict(MyScatterplot=scatterplot_wg))
        options = self.get_num_cols()
        self._proxy.that.choice_x.attrs(options=options)
        self._proxy.that.choice_y.attrs(options=options)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
