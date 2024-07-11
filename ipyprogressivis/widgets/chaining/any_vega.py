from .utils import make_button, stage_register, VBoxTyped, TypedBase, ModuleOrFacade
from ..utils import historized_widget
import ipywidgets as ipw
from ..vega import VegaWidget
from ..df_grid import DataFrameGrid
import panel as pn
from jupyter_bokeh.widgets import BokehModel  # type: ignore
import pandas as pd
from progressivis.core import Module, Sink, notNone
from progressivis.table.table_facade import TableFacade
from typing import Any as AnyType, Dict, cast, Type, Sequence, Tuple
from typing_extensions import TypeAlias
import json

WidgetType = AnyType

js_dict = {
    "mark": "bar",
    "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json",
    "encoding": {
        "x": {
            "type": "ordinal",
            "field": "nbins",
            "title": "Values",
            "axis": {
                "labelExpr": "(datum.value%10>0 ? null : "
                "format(data('data')[datum.value].xvals, '.2f'))"
            },
        },
        "y": {"type": "quantitative", "field": "level", "title": "Histogram"},
    },
}
js_code = json.dumps(js_dict)

HVegaWidget: TypeAlias = cast(
    Type[AnyType], historized_widget(VegaWidget, "update")  # noqa: F821
)


class AnyVegaW(VBoxTyped):
    class Typed(TypedBase):
        mode: ipw.Dropdown | None
        editor: BokehModel | None
        btn_fetch_cols: ipw.Button
        grid: DataFrameGrid | None
        btn_apply: ipw.Button
        vega: HVegaWidget

    cols_mapping: dict[str, Tuple[ModuleOrFacade, str, str, str]] = {}

    def initialize(self) -> None:
        self.c_.mode = ipw.Dropdown(
            options=["tree", "view", "form", "text", "preview"],
            description="Edition mode",
            disabled=False,
        )
        self.c_.mode.observe(self._mode_cb, names="value")
        content = json.loads(js_code)
        self.json_editor = pn.widgets.JSONEditor(
            value=content, mode="form", width=600  # type: ignore
        )
        self.json_editor.param.trigger("value")
        self.c_.editor = pn.ipywidget(self.json_editor)
        self.output_dtypes = None
        self.child.btn_fetch_cols = self._btn_ok = make_button(
            "Fetch info", disabled=False, cb=self._btn_fetch_cols_cb
        )
        self.child.btn_apply = self._btn_ok = make_button(
            "Apply", disabled=False, cb=self._btn_apply_cb
        )

    def _observe_keys(self, change: Dict[str, AnyType]) -> None:
        obj = change["owner"]
        grid = self.c_.grid
        row, col = grid.get_coords(obj)
        assert col == "Mapping"
        m_key = obj.value
        k_widget = grid.df.loc[row, "Key"]
        facade = self.input_module
        assert isinstance(facade, TableFacade)
        mod_cls = facade.registry[m_key].module_cls
        name = facade.registry[m_key].output_name
        for outp in mod_cls.outputs:
            if outp.name == name:
                assert outp.datashape is not None
                k_widget.options = list(outp.datashape.keys())
                k_widget.disabled = False
                break
        else:
            print(f"{name} not found")
            return

    def _btn_fetch_cols_cb(self, btn: ipw.Button) -> None:
        edit_val = self.json_editor.value
        en_ = edit_val.get("encoding")
        if en_ is None:
            return
        cols = [v["field"] for (k, v) in en_.items() if "field" in v]
        facade = self.input_module
        assert isinstance(facade, TableFacade)
        members = facade.members
        df = pd.DataFrame(
            index=cols, columns=["Mapping", "Key", "Processing"], dtype=object
        )
        df.loc[:, "Mapping"] = lambda: ipw.Dropdown(  # type: ignore
            options=members + [""],
            value="",
            disabled=False,
            layout={"width": "initial"},
        )
        df.loc[:, "Key"] = lambda: ipw.Dropdown(  # type: ignore
            options=[""],
            value="",
            disabled=True,
        )
        df.loc[:, "Processing"] = lambda: ipw.Dropdown(  # type: ignore
            options=["", "enumerate"],
            value="",
            disabled=False,
        )
        self.c_.grid = DataFrameGrid(df, first="200px", index_title="Vega columns")
        self.c_.grid.observe_col("Mapping", self._observe_keys)

    def _mode_cb(self, change: Dict[str, AnyType]) -> None:
        self.json_editor.mode = change["new"]

    def _update_vw(self, s: Module, run_number: int) -> None:
        def _processing(fnc: str, arr: Sequence[AnyType]) -> Sequence[AnyType]:
            if fnc == "enumerate":
                return range(len(arr))
            assert not fnc
            return arr

        df_dict = {
            col: _processing(proc, res[sl])
            for (col, (m, attr, sl, proc)) in self.cols_mapping.items()
            if sl in (res := getattr(m, attr))
        }
        if not df_dict:
            print("nothing to do")
            return
        df = pd.DataFrame(df_dict)
        self.child.vega.update("data", remove="true", insert=df)

    def _btn_apply_cb(self, btn: AnyType) -> None:
        facade = self.input_module
        assert isinstance(facade, TableFacade)
        scheduler = facade.scheduler()
        out_m = None
        with scheduler:
            sink = Sink(scheduler=scheduler)
            for i, row in self.c_.grid.df.iterrows():
                key = row[0].value
                sink.input.inp = self.input_module.output[key]
                out_m = cast(Module, notNone(facade.get(key)).output_module)
                out_n = notNone(facade.get(key)).output_name
                slot = cast(str, row[1].value)
                proc = cast(str, row[2].value)
                self.cols_mapping[i] = out_m, out_n, slot, proc
        if out_m is not None:  # i.e. the last out_m in the previous 'for'
            out_m.on_after_run(self._update_vw)
        js_val = self.json_editor.value.copy()
        js_val["data"] = {"name": "data"}
        self.child.vega = HVegaWidget(spec=js_val)
        self.dag_running()


stage_register["Any Vega"] = AnyVegaW
