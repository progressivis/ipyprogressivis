from .utils import (
    starter_callback,
    is_leaf,
    no_progress_bar,
    chaining_widget,
    VBox,
    needs_dtypes,
    runner,
    Coro,
    modules_producer,
)

import ipywidgets as ipw
from collections import defaultdict
from itertools import chain, batched
from progressivis.core.api import Module, asynchronize
from progressivis.vis import MCScatterPlot
from progressivis.core.api import JSONEncoderNp as JS
from ipytablewidgets import NumpyAdapter  # type: ignore
from ..scatterplot import Scatterplot
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    label,
    dropdown,
    restore,
    gridbox,
    box,
)


from typing import Any as AnyType, cast


class AfterRun(Coro):
    widget: ipw.Box | None = None

    async def action(self, m: Module, run_number: int) -> None:
        assert self.widget is not None
        wg = self.widget.children[0]
        assert isinstance(wg, Scatterplot)
        val = m.to_json()
        data_ = {
            k: v for (k, v) in val.items() if k not in ("hist_tensor", "sample_tensor")
        }
        ht = val.get("hist_tensor")

        def _func() -> None:
            arrays = dict()
            if ht is not None:
                for i, arr in enumerate(ht):
                    arrays[f"hist_{i}"] = arr
                wg.histograms = NumpyAdapter(arrays, touch_mode=False)
            st = val.get("sample_tensor")
            if st is not None:
                vectors = {f"v{i}": vec for (i, vec) in enumerate(st)}
                wg.samples = NumpyAdapter(vectors, touch_mode=False)
            wg.data = JS.dumps(data_)  # type: ignore

        await asynchronize(_func)


@is_leaf
@no_progress_bar
@chaining_widget(label="MCDensityMap")
class MCDensityMapW(VBox):
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
        self._proxy = anybox(
            self,
            gridbox(
                label(""),
                label("Axis"),
                label("Class"),
                *list(
                    chain.from_iterable(
                        [
                            [
                                label(col),
                                dropdown(
                                    options=[
                                        ("", ""),
                                        ("x", "x_column"),
                                        ("y", "y_column"),
                                    ]
                                ),
                                dropdown(options=["", "A", "B", "C", "D"]),
                            ]
                            for col in num_cols
                        ]
                    )
                ),
            )
            .uid("columns")
            .layout(grid_template_columns="100px 100px 100px"),
            button("Start").uid("start_btn").on_click(self._start_btn_cb),
            box().uid("image"),
        )

    def get_params(self) -> list[dict[str, str]]:
        class_dict: dict[str, dict[str, str]] = defaultdict(dict)
        gbox = self._proxy.that.columns.widget
        assert hasattr(gbox, "children")
        rows = list(batched([wg.value for wg in gbox.children], 3))
        for col, axis, cls in rows[1:]:
            if not cls:
                continue
            class_dict[cls][axis] = col.split(":")[0]
            class_dict[cls]["name"] = cls
        return list(class_dict.values())

    @starter_callback
    def _start_btn_cb(self, p: Proxy, btn: ipw.Button) -> None:
        ctx = self.get_params()
        self.record = self._proxy.dump()
        self.init_modules(ctx=ctx)

    def init_ui(self) -> None:
        content = self.record
        self._proxy = restore(content, globals(), obj=self)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children

    @modules_producer
    def init_modules(self, ctx: list[dict[str, AnyType]]) -> MCScatterPlot:
        assert isinstance(self.input_module, Module)
        s = self.input_module.scheduler
        # self.child.image = Scatterplot()
        with s:
            heatmap = MCScatterPlot(scheduler=s, classes=ctx, approximate=True)
            heatmap.create_dependent_modules(self.input_module, self.input_slot)
            self.after_run = after_run = AfterRun()
            img_box = self._proxy.that.image.widget
            assert hasattr(img_box, "children")
            if not img_box.children:
                img_box.children = [Scatterplot()]
            after_run.widget = cast(ipw.Box, img_box)
            img_box.children[0].link_module(heatmap, refresh=False)
            heatmap.on_after_run(after_run)  # Install the callback
            return heatmap

    @runner
    def run(self) -> AnyType:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        self.children = self._proxy.widget.children  # type: ignore
        ctx = self.get_params()
        self.output_module = self.init_modules(ctx)
        self.output_slot = "result"
