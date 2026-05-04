from .utils import (VBox, chaining_widget, runner, needs_dtypes,
                    modules_producer, starter_callback)
import ipywidgets as ipw
from progressivis.table.api import Aggregate
from progressivis.core.api import Sink
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    label,
    restore,
    select,
    gridbox,
    checkbox,
    _container_impl
)
from typing import Any as AnyType

WidgetType = AnyType

RECORD = "__RECORD__"
ALL_FNC_SET = set(Aggregate.registry.keys())

type_op_mismatches: dict[str, set[str]] = dict(
    string=ALL_FNC_SET-{"set", "nunique", "hide"},
    _=ALL_FNC_SET-{"count", "hide"}
)


def is_disabled(dt: str, op: str) -> bool:
    # return dt in ("", "string", "datetime64")  # op in type_op_mismatches.get(dt, set())
    return op in type_op_mismatches.get(dt, {"count"})

@chaining_widget(label="Aggregate")
class AggregateW(VBox):
    @needs_dtypes
    def initialize(self) -> None:
        fncs = ["hide"] + list(Aggregate.registry.keys())
        self.all_functions = dict(zip(fncs, fncs))
        self.all_columns: list[str] = [RECORD] + list(self.dtypes.keys())
        self.grid_header: list[Proxy] = [label("")] + [
            label(s) for s in self.all_functions.values()
        ]
        self._proxy = None  # hack
        self._proxy = anybox(
            self,
            select(
                options=[""],
                value="",
                rows=5,
                description="❎"
            ).uid("hidden_sel").observe(self._sel_obs_cb),
            gridbox(
                *self.get_checkboxes()
            ).layout(grid_template_columns=f"200px repeat({len(self.all_functions)}, 70px)").uid("grid"),
            button("Start",
                   disabled=True
                   ).uid("start_btn").on_click(self._start_btn_cb)
        )
    @property
    def hidden_cols(self) -> list[str]:
        if self._proxy is None:
            return []
        wg = self._proxy.that.hidden_sel.widget
        assert hasattr(wg, "options")
        return [opt for opt in wg.options if opt]
    @property
    def visible_cols(self) -> list[str]:
        return [col for col in self.all_columns if col not in self.hidden_cols]
    @modules_producer
    def init_modules(self, compute: AnyType) -> Aggregate:
        s = self.input_module.scheduler
        with s:
            aggr = Aggregate(compute=compute, scheduler=s)
            aggr.input.table = self.input_module.output[self.input_slot]
            sink = Sink(scheduler=s)
            sink.input.inp = aggr.output.result
            return aggr

    def get_checkboxes(self) -> list[Proxy]:
        reuse: dict[str, Proxy] = dict()
        if self._proxy is not None:
            reuse = self._proxy._registry.copy()
            cbx_list = list(self._proxy._registry.keys())
            for k in cbx_list:
                if k.startswith("cbx/"):
                    del self._proxy._registry[k]
        lst: list[WidgetType] = self.grid_header[:]
        for col in sorted(self.visible_cols, key=str.lower):  # __RECORD__ first
            if col == RECORD:
                col_type = "_"
            else:
                col_type = self.dtypes[col]
            lst.append(label(f"{col}:{col_type}"))
            for func in self.all_functions.keys():
                key = f"cbx/{col}/{func}"
                lst.append(reuse.get(key, self.checkbox_proxy(key, is_disabled(col_type, func))))
        return lst

    def checkbox_proxy(self, key: str, dis: bool) -> Proxy:
        wgt = checkbox(
            value=False,
            description="",
            disabled=dis,
            indent=False).uid(key).observe(self.cbx_observer)
        return wgt

    def info_cbx_dict(self) -> dict[tuple[str, str], Proxy]:
        def _fnc(key: str) -> tuple[str, str]:
            return tuple(key.split("/")[1:])  # type: ignore
        assert self._proxy is not None
        return {_fnc(key): pr for (key, pr) in self._proxy._registry.items() if key.startswith("cbx/")}

    @starter_callback
    def _start_btn_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        compute = [
            ("" if col == RECORD else col, fnc)
            for ((col, fnc), ck) in self.info_cbx_dict().items()
            if fnc != "hide" and ck.widget.value
        ]
        assert self._proxy is not None
        self.record = self._proxy.dump()
        self.output_module = self.init_modules(compute)
        self.output_slot = "result"

    def init_ui(self) -> None:
        content = self.record
        self._proxy = restore(content, globals(), obj=self)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
        # refreshing column list (e.g. if new columns were added)
        _container_impl(self._proxy.that.grid, *self.get_checkboxes())
        self._proxy._registry.update(self._proxy.that.grid._registry)


    @runner
    def run(self) -> AnyType:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        self.children = self._proxy.widget.children  # type: ignore
        # get compute
        compute = [
            ("" if col == RECORD else col, fnc)
            for ((col, fnc), ck) in self.info_cbx_dict().items()
            if fnc != "hide" and ck.widget.value
        ]
        self.output_module = self.init_modules(compute)
        self.output_slot = "result"

    def _sel_obs_cb(self, proxy: Proxy, change: AnyType) -> None:
        self.obs_flag = True
        col = change["new"]
        if not col:
            return
        hidden = list(self.hidden_cols)
        hidden.remove(col)
        assert self._proxy is not None
        self._proxy.that.hidden_sel.attrs(value="")
        self._proxy.that.hidden_sel.attrs(options=[""]+sorted(hidden))
        _container_impl(proxy.that.grid, *self.get_checkboxes())
        self._proxy._registry.update(proxy.that.grid._registry)
        for func in self.all_functions.keys():
            uid = f"cbx/{col}/{func}"
            self._proxy.lookup(uid).attrs(value=False)

    def cbx_observer(self, proxy: Proxy, change: AnyType) -> None:
        key = proxy._uid
        assert key is not None
        assert self._proxy is not None
        _, col, func = key.split("/")
        if func == "hide" and change["new"]:
            proxy.that.start_btn.attrs(disabled=True)
            hidden = list(self.hidden_cols) + [col]
            proxy.that.hidden_sel.attrs(options=[""]+sorted(hidden))
            _container_impl(proxy.that.grid, *self.get_checkboxes())
            self._proxy._registry.update(proxy.that.grid._registry)
        else:
            proxy.that.start_btn.attrs(disabled=False)
