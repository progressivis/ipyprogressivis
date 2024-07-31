from .utils import (make_button, stage_register, VBoxTyped, TypedBase, ModuleOrFacade,
                    amend_last_record, replay_next, get_recording_state, disable_all)
from ..utils import historized_widget
import ipywidgets as ipw
from ..vega import VegaWidget
from ..df_grid import DataFrameGrid
import panel as pn
from jupyter_bokeh.widgets import BokehModel  # type: ignore
import pandas as pd
import numpy as np
from progressivis.core import Module, Sink, notNone
from progressivis.table import PTable
from progressivis.table.table_facade import TableFacade
from typing import Any as AnyType, Dict, cast, Type, Tuple, TypeAlias
import json
import time
import os

NdArray: TypeAlias = np.ndarray[AnyType, AnyType]

WidgetType = AnyType

HVegaWidget: TypeAlias = cast(
    Type[AnyType], historized_widget(VegaWidget, "update")  # noqa: F821
)


class AnyVegaW(VBoxTyped):
    class Typed(TypedBase):
        schemas: ipw.Dropdown | None
        mode: ipw.Dropdown | None
        editor: BokehModel | None
        save_schema: ipw.HBox | None
        grid: DataFrameGrid | None
        freeze_ck: ipw.Checkbox | None
        btn_apply: ipw.Button | None
        vega: HVegaWidget | None

    def __init__(self, *args: AnyType, **kw: AnyType) -> None:
        super().__init__(*args, **kw)
        self.cols_mapping: dict[str, Tuple[ModuleOrFacade, str, str, str]] = {}

    def initialize(self) -> None:
        self.c_.mode = ipw.Dropdown(
            options=["tree", "view", "form", "text", "preview"],
            description="Edition mode",
            disabled=False,
        )
        self.c_.mode.observe(self._mode_cb, names="value")
        self.json_editor = pn.widgets.JSONEditor(
            value={}, mode="form", width=600  # type: ignore
        )
        self.json_editor.param.trigger("value")
        self.c_.editor = pn.ipywidget(self.json_editor)
        self.c_.schemas = ipw.Dropdown(
                options=[""] + os.listdir(self.widget_dir),
                value="",
                rows=5,
                description="Schemas",
                disabled=False,
                layout=ipw.Layout(width="60%"),
            )
        self.c_.schemas.observe(self._schemas_cb, names="value")
        self.c_.save_schema = ipw.HBox(
            [
                make_button(
                    "Fetch info", disabled=False, cb=self._btn_fetch_cols_cb
                ),
                make_button(
                    "Save schema ...",
                    cb=self._save_schema_cb,
                    disabled=False,
                ),
                ipw.Text(
                    value=time.strftime("vega%Y%m%d_%H%M%S"),
                    placeholder="",
                    description="File:",
                    disabled=False,
                    layout=ipw.Layout(width="100%"),
                )]
        )
        self.output_dtypes = None
        is_rec = get_recording_state()
        self.c_.freeze_ck = ipw.Checkbox(description="Freeze",
                                         value=is_rec,
                                         disabled=(not is_rec))
        self.c_.btn_apply = self._btn_ok = make_button(
            "Apply", disabled=False, cb=self._btn_apply_cb
        )

    def _save_schema_cb(self, btn: AnyType) -> None:
        pv_dir = self.dot_progressivis
        assert pv_dir
        base_name = self.c_.save_schema.children[2].value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name, "w") as f:
            json.dump(self.json_editor.value, f, indent=4)
        self.c_.schemas.options = [""] + os.listdir(self.widget_dir)

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
        members = [m for m in facade.members if "/" in m]  # only configured members
        df = pd.DataFrame(
            index=cols, columns=["Mapping", "Key", "Processing"], dtype=object
        )
        df.loc[:, "Mapping"] = lambda: ipw.Dropdown(  # type: ignore
            options=members + [""],
            value="",
            disabled=False,
            layout={'width': 'max-content'}
        )
        df.loc[:, "Key"] = lambda: ipw.Dropdown(  # type: ignore
            options=[""],
            value="",
            disabled=True,

        )
        df.loc[:, "Processing"] = lambda: ipw.Dropdown(  # type: ignore
            options=["", "enumerate", "cbrt"],
            value="",
            disabled=False,
        )
        self.c_.grid = DataFrameGrid(df, index_title="Vega columns",
                                     grid_template_columns="100px 200px 100px 100px"
                                     )
        self.c_.grid.observe_col("Mapping", self._observe_keys)

    def _mode_cb(self, change: Dict[str, AnyType]) -> None:
        self.json_editor.mode = change["new"]

    def _schemas_cb(self, change: Dict[str, AnyType]) -> None:
        base_name = change['new']
        if not base_name:
            self.json_editor.value = {}
            return
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name) as f:
            self.json_editor.value = json.load(f)

    def _update_vw(self, _: Module, run_number: int) -> None:
        def _processing(fnc: str, arr: NdArray) -> NdArray:
            if fnc == "enumerate":
                return range(len(arr))  # type: ignore
            elif fnc == "cbrt":
                maxa = arr.max()
                if maxa != 0:
                    return cast(NdArray, np.cbrt(arr / maxa))
                else:
                    return arr
            assert not fnc
            return arr

        def _as_dict(res: dict[str, AnyType] | PTable) -> dict[str, AnyType]:
            if isinstance(res, dict):
                return res
            return notNone(notNone(res).last()).to_dict()

        df_dict = {
            col: _processing(proc, res[sl])
            for (col, (m, attr, sl, proc)) in self.cols_mapping.items()
            if sl in (res := _as_dict(getattr(m, attr)))
        }

        def _first_val(d: dict[str, NdArray]) -> NdArray | None:
            for _, val in d.items():
                return val
            return None

        if not df_dict:
            print("nothing to do")
            return
        first_val = notNone(_first_val(df_dict))
        data: NdArray | pd.DataFrame
        if len(df_dict) == 1 and len(first_val.shape) > 1:
            data = first_val
        else:
            data = pd.DataFrame(df_dict)
        self.child.vega.update("data", remove="true", insert=data)

    def _btn_apply_cb(self, btn: ipw.Button) -> None:
        df_dict = self.c_.grid.df.to_dict(orient="index")
        for i, row in df_dict.items():
            for k, wg in row.items():
                row[k] = wg.value
        js_val = self.json_editor.value.copy()
        if self.child.freeze_ck.value:
            amend_last_record({'frozen': dict(mapping_dict=df_dict, vega_schema=js_val)})
        self.init_modules(mapping_dict=df_dict, vega_schema=js_val)
        disable_all(self)

    def init_modules(self,
                     mapping_dict: dict[str, dict[str, str]],
                     vega_schema: AnyType) -> None:
        facade = self.input_module
        assert isinstance(facade, TableFacade)
        scheduler = facade.scheduler()
        out_m = None
        with scheduler:
            sink = Sink(scheduler=scheduler)
            for i, row in mapping_dict.items():
                key = row["Mapping"]
                if not key:
                    continue
                sink.input.inp = self.input_module.output[key]
                out_m = cast(Module, notNone(facade.get(key)).output_module)
                out_n = notNone(facade.get(key)).output_name
                slot = row["Key"]
                proc = row["Processing"]
                self.cols_mapping[i] = out_m, out_n, slot, proc
        if out_m is not None:  # i.e. the last out_m in the previous 'for'
            out_m.on_after_run(self._update_vw)
        vega_schema["data"] = {"name": "data"}
        self.child.vega = HVegaWidget(spec=vega_schema)
        self.dag_running()

    def run(self) -> AnyType:
        content = self.frozen_kw
        self.init_modules(**content)
        self.dag_running()
        replay_next()
        return self.child.vega


stage_register["Any Vega"] = AnyVegaW
