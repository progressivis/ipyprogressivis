from .utils import (
    is_leaf,
    no_progress_bar,
    chaining_widget,
    VBox,
    ModuleOrFacade,
    dot_progressivis,
    Coro,
    json_editor
)
from ..utils import historized_widget
import ipywidgets as ipw
from ..vega import VegaWidget
from itertools import chain, batched
import numpy as np
from progressivis.core.api import Module, Sink, notNone, asynchronize
from progressivis.table.table_facade import TableFacade
from progressivis.table.api import BasePTable
from typing import Any as AnyType, Dict, cast, Type, Tuple, TypeAlias
import json
import time
import os
from ipytablewidgets.source_adapter import SourceAdapter  # type: ignore

from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    label,
    dropdown,
    gridbox,
    hbox,
    text,
    _container_impl
)

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
            assert self.leaf is not None
            assert hasattr(self.leaf, "_proxy")
            assert self.leaf._proxy is not None
            vega_box = cast(ipw.Box, self.leaf._proxy.that.vega_box.widget)
            if not vega_box.children:
                return
            vega_box.children[0].update("data", remove="true", insert=data)  # type: ignore
        await asynchronize(_func)

@is_leaf
@no_progress_bar
@chaining_widget(label="Any Vega")
class AnyVegaW(VBox):
    def __init__(self, *args: AnyType, **kw: AnyType) -> None:
        super().__init__(*args, **kw)
        self.cols_mapping: dict[str, Tuple[ModuleOrFacade, str, str, str]] = {}
        self._updates_count: int = 0

    def initialize(self) -> None:
        self._proxy = anybox(
            self,
            dropdown("Schemas",
                     options=[""] + os.listdir(self.widget_dir),
                     value="",
                     rows=5,
            )
            .layout(width="60%")
            .uid("schemas")
            .observe(self._schemas_cb),
            json_editor().uid("editor"),
            hbox(
                button("Fetch info").on_click(self._btn_fetch_cols_cb),
                button("Save schema ...").on_click(self._save_schema_cb),
                text("File:",
                     value=time.strftime("vega%Y%m%d_%H%M%S"),
                     placeholder="",
                ).uid("base_name").layout(width="100%"),

            ),
            gridbox().uid("grid").layout(grid_template_columns="100px 200px 100px 100px"),
            button("Apply").on_click(self.starter_callback),
            hbox().uid("vega_box")
        )

    def _save_schema_cb(self, proxy: Proxy, btn: AnyType) -> None:
        assert self._proxy is not None
        pv_dir = dot_progressivis()
        assert pv_dir
        base_name = self._proxy.that.base_name.widget.value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name, "w") as f:
            json.dump(self._proxy.that.editor.widget.data, f, indent=4)  # type: ignore
        self._proxy.that.schemas.attrs(options = [""] + os.listdir(self.widget_dir))

    def _observe_keys(self, proxy: Proxy, change: Dict[str, AnyType]) -> None:
        assert self._proxy is not None
        uid = proxy._uid
        assert uid is not None
        prefix, col, what = uid.split("/")
        assert prefix == "cell"
        assert what == "mapping"
        m_key = proxy.widget.value
        k_widget = self._proxy.lookup(f"cell/{col}/key")
        facade = self.input_module
        if isinstance(facade, TableFacade):
            mod_cls = facade.registry[m_key].module_cls
            name = facade.registry[m_key].output_name
            for outp in mod_cls.outputs:
                if outp.name == name:
                    assert outp.datashape is not None
                    k_widget.attrs(options = list(outp.datashape.keys()), disabled = False)
                    break
            else:
                print(f"{name} not found")
                return
        else:
             k_widget.attrs(options = [m_key], value = m_key)

    def _btn_fetch_cols_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self._proxy is not None
        edit_val = self._proxy.that.editor.widget.data  # type: ignore
        en_ = edit_val.get("encoding")
        if en_ is None:
            return
        cols = [v["field"] for (k, v) in en_.items() if "field" in v]
        facade = self.input_module
        if isinstance(facade, TableFacade):
            members = [m for m in facade.members if "/" in m]  # only configured members
        else:
            members = getattr(self.input_module, self.input_slot).columns
        header = [label("Vega columns"),label("Mapping"),label("Key"),label("Processing")]
        lst = [
            [
                label(col),
                dropdown(
                    options=members + [""],
                )
                .layout(width="max-content")
                .uid(f"cell/{col}/mapping")
                .observe(self._observe_keys),
                dropdown(
                    options=[""],
                )
                .uid(f"cell/{col}/key"),
                dropdown(
                    options=["", "enumerate", "cbrt"],
                )
                .uid(f"cell/{col}/processing"),

            ] for col in cols
        ]
        grid = self._proxy.that.grid
        _container_impl(
            grid,
            *(header+list(chain.from_iterable(lst)))
        )
        self._proxy._registry.update(grid._registry)

    def _schemas_cb(self, proxy: Proxy, change: Dict[str, AnyType]) -> None:
        assert self._proxy is not None
        base_name = change["new"]
        if not base_name:
            self._proxy.that.editor.attrs(data=dict())
            return
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name) as f:
            self._proxy.that.editor.attrs(data=json.load(f))

    def get_mapping_dict(self) -> dict[str, dict[str, str]]:
        assert self._proxy is not None
        grid = self._proxy.that.grid
        lst = grid._children
        assert lst is not None
        rows = list(batched(lst, 4))[1:]
        mapping_dict = dict()
        for col, map_, key, proc in rows:
            mapping_dict[col.widget.value] = dict(
                Mapping=map_.widget.value,
                Key=key.widget.value,
                Processing=proc.widget.value
            )
        return mapping_dict

    def starter_callback(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self._proxy is not None
        mapping_dict = self.get_mapping_dict()
        js_val = self._proxy.that.editor.widget.data.copy()  # type: ignore
        self.record = self._proxy.dump()
        self.init_modules(mapping_dict=mapping_dict, vega_schema=js_val)

    def init_modules(
        self, mapping_dict: dict[str, dict[str, str]], vega_schema: AnyType
    ) -> None:
        assert self._proxy is not None
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
        vegabox = self._proxy.that.vega_box.widget
        assert hasattr(vegabox, "children")
        if not vegabox.children:
            vegabox.children = [VegaWidget(spec=vega_schema)]

    def run(self) -> None:
        mapping_dict = self.get_mapping_dict()
        js_val = self._proxy.that.editor.widget.data.copy()  # type: ignore
        self.init_modules(mapping_dict=mapping_dict, vega_schema=js_val)

