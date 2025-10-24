from .utils import (
    make_button,
    stage_register,
    VBoxTyped,
    TypedBase,
    ModuleOrFacade,
    amend_last_record,
    is_recording,
    disable_all,
    runner,
    needs_dtypes,
    GuestWidget,
    dot_progressivis,
    Coro,
    modules_producer
)
from ..utils import historized_widget
import ipywidgets as ipw
from ..vega import VegaWidget
from ..json_editor import JsonEditor
from ..df_grid import DataFrameGrid
import pandas as pd
import numpy as np
from progressivis.core.api import Module, Sink, notNone, asynchronize
from progressivis.table.table_facade import TableFacade
from progressivis.table.api import BasePTable
from typing import Any as AnyType, Dict, cast, Type, Tuple, TypeAlias
import json
import time
import os
from ipytablewidgets.source_adapter import SourceAdapter  # type: ignore


class ProgressivisAdapter(SourceAdapter):  # type: ignore
    """
    Actually this adapter requires a dict of ndarrays
    """

    def __init__(self, source: BasePTable, *args: AnyType, **kw: AnyType) -> None:
        assert source is None or isinstance(
            source, BasePTable
        )
        super().__init__(source, *args, **kw)

    @property
    def columns(self) -> AnyType:
        return self._columns or self._source.columns

    def to_array(self, col: str) -> AnyType:
        return self._source[col].values

    def equals(self, other: SourceAdapter | BasePTable) -> AnyType:
        if isinstance(other, SourceAdapter):
            other = other._source
        assert isinstance(other, BasePTable)
        return self._source.equals(other)

NdArray: TypeAlias = np.ndarray[AnyType, AnyType]

WidgetType = AnyType

MAX_SIZE = 10_000

HVegaWidget: TypeAlias = cast(
    Type[AnyType], historized_widget(VegaWidget, "update")  # noqa: F821
)

class AfterRun(Coro):
    columns: list[str] = []
    async def action(self, m: Module, run_number: int) -> None:
        tbl = m.result  # type: ignore
        if tbl is None:
            return
        len_tbl = len(tbl)
        max_size = MAX_SIZE
        start_sl = len_tbl - max_size if len_tbl > max_size else 0
        if start_sl:
            messg = ("<spread style='color: red;'>WARNING:</spread>"
                     f" The data to be displayed contains more than {max_size} rows."
                     f"Only the last {max_size} rows will be used.")
            self.bar.c_.message.value = messg
        # indices = np.random.randint(0, len_tbl, max_size)
        data = ProgressivisAdapter(m.result.loc[start_sl:, self.columns])  # type: ignore
        def _func() -> None:
            self.leaf.child.vega.update("data", remove="true", insert=data)  # type: ignore
        await asynchronize(_func)


class AnyVegaW(VBoxTyped):
    class Typed(TypedBase):
        schemas: ipw.Dropdown | None
        editor: JsonEditor
        save_schema: ipw.HBox | None
        grid: DataFrameGrid | None
        btn_apply: ipw.Button | None
        vega: VegaWidget | None

    def __init__(self, *args: AnyType, **kw: AnyType) -> None:
        super().__init__(*args, **kw)
        self.cols_mapping: dict[str, Tuple[ModuleOrFacade, str, str, str]] = {}
        self._updates_count: int = 0

    @needs_dtypes
    def initialize(self) -> None:
        self.c_.editor = JsonEditor()
        self.c_.editor.data = {}
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
                make_button("Fetch info", disabled=False, cb=self._btn_fetch_cols_cb),
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
                ),
            ]
        )
        self.output_dtypes = None  # type: ignore
        self.c_.btn_apply = self._btn_ok = make_button(
            "Apply", disabled=False, cb=self._btn_apply_cb
        )

    def _save_schema_cb(self, btn: AnyType) -> None:
        pv_dir = dot_progressivis()
        assert pv_dir
        base_name = self.c_.save_schema.children[2].value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name, "w") as f:
            json.dump(self.c_.editor.data, f, indent=4)
        self.c_.schemas.options = [""] + os.listdir(self.widget_dir)

    def _observe_keys(self, change: Dict[str, AnyType]) -> None:
        obj = change["owner"]
        grid = self.c_.grid
        row, col = grid.get_coords(obj)
        assert col == "Mapping"
        m_key = obj.value
        k_widget = grid.df.loc[row, "Key"]
        facade = self.input_module
        if isinstance(facade, TableFacade):
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
        else:
             k_widget.options = [m_key]
             k_widget.value = m_key

    def _btn_fetch_cols_cb(self, btn: ipw.Button) -> None:
        edit_val = self.c_.editor.data
        en_ = edit_val.get("encoding")
        if en_ is None:
            return
        cols = [v["field"] for (k, v) in en_.items() if "field" in v]
        facade = self.input_module
        if isinstance(facade, TableFacade):
            members = [m for m in facade.members if "/" in m]  # only configured members
        else:
            members = getattr(self.input_module, self.input_slot).columns
        df = pd.DataFrame(
            index=cols, columns=["Mapping", "Key", "Processing"], dtype=object
        )
        df.loc[:, "Mapping"] = lambda: ipw.Dropdown(  # type: ignore
            options=members + [""],
            value="",
            disabled=False,
            layout={"width": "max-content"},
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
        self.c_.grid = DataFrameGrid(
            df,
            index_title="Vega columns",
            grid_template_columns="100px 200px 100px 100px",
        )
        self.c_.grid.observe_col("Mapping", self._observe_keys)

    def _schemas_cb(self, change: Dict[str, AnyType]) -> None:
        base_name = change["new"]
        if not base_name:
            self.c_.editor.data = {}
            return
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name) as f:
            self.c_.editor.data = json.load(f)

    def _btn_apply_cb(self, btn: ipw.Button) -> None:
        df_dict = self.c_.grid.df.to_dict(orient="index")
        for i, row in df_dict.items():
            for k, wg in row.items():
                row[k] = wg.value
        js_val = self.c_.editor.data.copy()
        if is_recording():
            amend_last_record(
                {"frozen": dict(mapping_dict=df_dict, vega_schema=js_val)}
            )
        self.init_modules(mapping_dict=df_dict, vega_schema=js_val)
        disable_all(self)
        self.make_leaf_bar(self.after_run)
        self.manage_replay()

    @modules_producer
    def init_modules(
        self, mapping_dict: dict[str, dict[str, str]], vega_schema: AnyType
    ) -> None:
        facade = self.input_module
        scheduler = facade.scheduler
        out_m = None
        if isinstance(self.input_module, Module):
            out_m = self.input_module
        with scheduler:
            sink = Sink(scheduler=scheduler)
            for i, row in mapping_dict.items():
                key = row["Mapping"]
                if not key:
                    continue
                if isinstance(facade, TableFacade):
                    sink.input.inp = self.input_module.output[key]
                    out_m = cast(Module, notNone(facade.get(key)).output_module)
                    out_n = notNone(facade.get(key)).output_name
                else:
                    sink.input.inp = self.input_module.output[self.input_slot]
                    out_m = self.input_module  # type: ignore
                    out_n = self.input_slot
                slot = row["Key"]
                proc = row["Processing"]
                self.cols_mapping[i] = out_m, out_n, slot, proc  # type: ignore
        if out_m is not None:  # i.e. the last out_m in the previous 'for'
            after_run = AfterRun()
            after_run.columns = list({elt["Mapping"]: None for elt in mapping_dict.values()}.keys())
            out_m.on_after_run(after_run)
            self.after_run = after_run
        vega_schema["data"] = {"name": "data"}
        if isinstance(self.input_module, Module):
            for (col, (m, attr, sl, proc)) in self.cols_mapping.items():
                if col in vega_schema.get("encoding", {}):
                    vega_schema["encoding"][col]["field"] = sl
        self.vega_schema = vega_schema
        self.child.vega = VegaWidget(spec=vega_schema)
        self.dag_running()

    def provide_surrogate(self, title: str) -> GuestWidget:
        disable_all(self)
        return self

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        self.init_modules(**content)
        return self.child.vega


stage_register["Any Vega"] = AnyVegaW
