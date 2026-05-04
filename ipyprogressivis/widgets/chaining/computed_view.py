from .utils import (
    starter_callback,
    chaining_widget,
    VBox,
    runner,
    needs_dtypes,
    modules_producer
)
import numpy as np
from itertools import chain, batched
from inspect import signature
from progressivis.table.repeater import Repeater, Computed
from progressivis.core.api import Sink
from progressivis.table.compute import (
    week_day,
    # UNCHANGED,
    # make_if_else,
    ymd_string,
    is_weekend,
    year,
    month,
    day,
    hour,
    year_day_int,
    add_,
    mul_,
    true_div,
    div_percent,
    floor_div,

)
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    vbox,
    anybox,
    hbox,
    label,
    dropdown,
    select_multiple,
    select,
    stack,
    html,
    text,
    restore,
    gridbox,
    checkbox,
    _container_impl
)

from typing import Any as AnyType, Callable

WidgetType = AnyType
N_BOXES = 30

# NB: `np.sctypes` was removed in the NumPy 2.0 release. Access dtypes explicitly instead.
np_sctypes = [[np.int8, np.int16, np.int32, np.int64],
              [np.uint8, np.uint16, np.uint32, np.uint64],
              [np.float16, np.float32, np.float64, np.longdouble],
              [np.complex64, np.complex128, np.clongdouble],
              [bool, object, bytes, str, np.void]]

DTYPES = [
    np.dtype(e).name for lst in np_sctypes for e in lst  # type: ignore
] + [
    "datetime64"
]
UFUNCS: dict[str, Callable[..., AnyType]] = {
    k: v for (k, v) in np.__dict__.items() if isinstance(v, np.ufunc) and v.nin == 1
}

ALL_FUNCS: dict[str, Callable[..., AnyType]] = dict()  # UFUNCS.copy()

ALL_FUNCS.update(
    {"week_day": week_day, "is_weekend": is_weekend,
     "ymd_string": ymd_string, "year": year,
     "month": month, "day": day, "hour": hour, "year_day": year_day_int,
     "+": add_,
     "*": mul_,
     "/": true_div,
     "//": floor_div,
     "%": div_percent
     }
)

def _s(tpl: tuple[str, ...] | list[str]) -> str:
    assert isinstance(tpl, (tuple, list))
    return ",".join(tpl)

def func_view(main: "ComputedViewW", abox: Proxy, colnames: list[str], fname: str) -> None:
    fnc = ALL_FUNCS[fname]
    if isinstance(fnc, np.ufunc):
        type_ = main.dtypes[colnames[0]]  # TODO: improve it using ufunc.types information
    elif isinstance(fnc, np.vectorize):
        type_ = fnc.pyfunc.__annotations__.get("return", object).__name__
    else:
        type_ = fnc.__annotations__.get("return", object).__name__
    if type_ == "int":
        type_ = "int64"
    elif type_ == "float":
        type_ = "float64"
    elif type_ == "str":
        type_ = "object"
    if type_ not in DTYPES:
        type_ = "object"
    if len(colnames) > 1:
        vars = signature(fnc.pyfunc if isinstance(fnc, np.vectorize) else fnc).parameters.keys()
    _container_impl(
        abox,
        hbox(
            label("Name:"),
            text(
                value=f"{'_'.join(colnames)}_{fnc.__name__}",
                placeholder="mandatory",
                layout={"width": "initial"}
            ).uid(f"gname/{_s(colnames)}/{fname}"),  # given name
            html().uid(f"col_var_map/{_s(colnames)}/{fname}")
            if len(colnames) <= 1 else
            gridbox(
                label(""), label("Variable"),  # header
                *chain.from_iterable([(label(col), dropdown(
                    placeholder="var",
                    options=[""]+list(vars),
                    value="",
                    ensure_option=True,
                    layout={"width": "initial"},
                )) for col in colnames]))
            .layout(
                grid_template_columns="150px 70px",
                border="solid")
            .uid(f"col_var_map/{_s(colnames)}/{fname}"),
            hbox(
                label("Output dtype:"),
                dropdown(
                    placeholder="dtype",
                    options=DTYPES,
                    value=type_,
                    ensure_option=True,
                    layout={"width": "initial"}
                ).uid(f"dtype/{_s(colnames)}/{fname}")
            ),
            checkbox("Use",
                     value=False
            ).uid(f"use/{_s(colnames)}/{fname}").observe(main.update_func_list)
        )
    )



@chaining_widget(label="Computed view")
class ComputedViewW(VBox):
    @needs_dtypes
    def initialize(self) -> None:
        cols_t = [f"{c}:{t}" for (c, t) in self.dtypes.items()]
        col_list = list(zip(cols_t, self.dtypes.keys()))
        from .custom import CUSTOMER_FNC
        ALL_FUNCS.update(CUSTOMER_FNC)
        self._proxy = anybox(
            self,
            hbox(  # opts bar
                checkbox("Show Numpy universal functions", indent=False)
                .uid("numpy_ufuncs")
                .observe(self._numpy_ufuncs_cb),
                button(icon="refresh")
                .uid("refresh_btn")
                .on_click(self._refresh_funcs_cb)
                .layout(width='30px', height='30px'),
                label("Refresh custom function list")
            ),
            hbox(  # cols funcs
                select_multiple(
                    options=[("", "")] + col_list,
                    rows=7
                ).uid("cols").observe(self._columns_cb),
                select(
                    disabled=True,
                    options=[""] + list(ALL_FUNCS.keys()),
                    rows=7
                ).uid("funcs").observe(self._functions_cb),
                stack(
                    vbox(),
                    *[vbox(local_index=i+1).uid(f"free_{i+1}") for i in range(N_BOXES)],
                    selected_index=0
                ).uid("computed")
            ),
            gridbox().uid("func_table"),
            hbox(  # keep stored
                select_multiple(
                    "Keep also:",
                    options=col_list,
                    value=[],
                    rows=5,
                ).uid("stored_cols"),
                checkbox("Select all").uid("keep_all").observe(self._keep_all_cb)
            ),
            button("Apply").uid("apply_btn").on_click(self._apply_btn_cb)

        )


    def _keep_all_cb(self, proxy: Proxy, change: AnyType) -> None:
        val = change["new"]
        self._proxy.that.stored_cols.attrs(value=list(self.dtypes.keys()) if val else [])

    def _refresh_funcs_cb(self, proxy: Proxy, change: AnyType) -> None:
        from .custom import CUSTOMER_FNC
        ALL_FUNCS.update(CUSTOMER_FNC)
        self._proxy.that.funcs.attrs(options = [""] + list(ALL_FUNCS.keys()))

    def _numpy_ufuncs_cb(self, proxy: Proxy, change: AnyType) -> None:
        if change["new"]:
            ALL_FUNCS.update(UFUNCS)
        else:
            for k in UFUNCS.keys():
                del ALL_FUNCS[k]
        self._proxy.that.funcs.attrs(options = [""] + list(ALL_FUNCS.keys()))

    def _columns_cb(self, proxy: Proxy, change: AnyType) -> None:
        val = change["new"]
        self._proxy.that.funcs.attrs(disabled=False)
        if not val:
            self._proxy.that.funcs.attrs(disabled=True, value="")
            self._proxy.that.computed.attrs(selected_index=0)  # hide
        elif self._proxy.that.funcs.widget.value:
            self.set_selection()
        else:
            self._proxy.that.computed.attrs(selected_index=0)  # hide

    def _functions_cb(self, proxy: Proxy, change: AnyType) -> None:
        val = change["new"]
        if not val:
            self._proxy.that.computed.attrs(selected_index=0)  # hide
        else:
            self.set_selection()

    def set_selection(self) -> None:
        cols_v = self._proxy.that.cols.widget.value
        funcs_v = self._proxy.that.funcs.widget.value
        key = f"grid/{_s(cols_v)}/{funcs_v}"
        if key not in self._proxy._registry:
            for uid in self._proxy._registry.keys():
                if not uid.startswith("free_"):
                    continue
                break
            else:
                raise ValueError("no more free entries")  # TODO: extend
        abox = self._proxy._registry[uid]  # uid == free_xx
        del self._proxy._registry[uid]
        func_view(main=self, abox=abox, colnames=cols_v, fname=funcs_v)
        abox.uid(key)
        self._proxy._registry.update(abox._registry)
        self._proxy._registry[key] = abox
        assert hasattr(abox.widget, "local_index")
        self._proxy.that.computed.attrs(selected_index=abox.widget.local_index)

    def _make_computed_list(self) -> list[dict[str, str]]:
        res = []
        for uid in self._proxy._registry.keys():
            if not uid.startswith("grid/"):
                continue
            _, s_cols, func = uid.split("/")
            use_uid = f"use/{s_cols}/{func}"
            if not self._proxy.lookup(use_uid).widget.value:
                continue
            gname_uid = f"gname/{s_cols}/{func}"
            wg_name = self._proxy.lookup(gname_uid).widget.value
            dtype_uid = f"dtype/{s_cols}/{func}"
            wg_dtype = self._proxy.lookup(dtype_uid).widget.value
            map_uid = f"col_var_map/{s_cols}/{func}"
            map_wg = self._proxy.lookup(map_uid)
            if hasattr(map_wg.widget, "children"):  # gridbox case
                lst = map_wg._children
                assert lst is not None
                rows = list(batched(lst, 2))
                map_ = {var.widget.value: col.widget.value for (col, var) in rows[1:] if var.widget.value}
            else:  # html dongle case
                map_ = {}
            res.append(dict(cols=s_cols.split(","),
                            fname=func,
                            map=map_,
                            wg_name=wg_name,
                            wg_dtype=wg_dtype)
                       )
        return res

    @starter_callback
    def _apply_btn_cb(self, proxy: Proxy, btn: AnyType) -> None:
        comp_list = self._make_computed_list()
        cols = list(self._proxy.that.stored_cols.widget.value)
        self.record = self._proxy.dump()
        self.output_module = self.init_modules(comp_list, columns=cols)

    def init_ui(self) -> None:
        from .custom import CUSTOMER_FNC
        ALL_FUNCS.update(UFUNCS)
        ALL_FUNCS.update(CUSTOMER_FNC)
        content = self.record
        self._proxy = restore(content, globals(), obj=self)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children

    @runner
    def run(self) -> None:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        self.children = self._proxy.widget.children  # type: ignore
        comp_list = self._make_computed_list()
        cols = list(self._proxy.that.stored_cols.widget.value)
        self.output_module = self.init_modules(comp_list, columns=cols)
        self.output_slot = "result"

    @modules_producer
    def init_modules(self, comp_list: list[dict[str, list[str]]],
                    columns: list[str]) -> Repeater:
        comp = Computed()
        from .custom import CUSTOMER_FNC
        ALL_FUNCS.update(UFUNCS)
        ALL_FUNCS.update(CUSTOMER_FNC)
        for d_ in comp_list:
            func = ALL_FUNCS[d_["fname"]]  # type: ignore
            cols = d_["cols"]
            if len(cols) == 1:
                comp.add_ufunc_column(
                    d_["wg_name"], cols[0], func, np.dtype(d_["wg_dtype"])  # type: ignore
                )
            else:
                assert len(cols) > 1
                comp.add_multi_col_func(
                    name=d_["wg_name"], cols=cols, func=func,  # type: ignore
                    col_var_map=d_["map"],  # type: ignore
                    dtype=np.dtype(d_["wg_dtype"]),

                )
        s = self.input_module.scheduler
        with s:
            rep = Repeater(computed=comp, scheduler=s)
            rep.input.table = self.input_module.output[self.input_slot][tuple(columns)]
            sink = Sink(scheduler=s)
            sink.input.inp = rep.output.result
            return rep

    def _func_btn_cb(self, proxy: Proxy, b: AnyType) -> None:
        assert proxy._uid is not None
        btn, s_cols, func = proxy._uid.split("/")
        assert btn == "btn"
        cols = s_cols.split(",")
        self._proxy.that.cols.attrs(value=cols)
        self._proxy.that.funcs.attrs(value=func)

    def update_func_list(self, proxy: Proxy, change: AnyType) -> None:
        table_width = 4
        seld = []
        for uid in self._proxy._registry.keys():
            if not uid.startswith("grid/"):
                continue
            _, s_cols, func = uid.split("/")
            use_uid = f"use/{s_cols}/{func}"
            if not self._proxy.lookup(use_uid).widget.value:
                continue
            gname_uid = f"gname/{s_cols}/{func}"
            wg_name = self._proxy.lookup(gname_uid).widget.value
            seld.append(dict(s_cols=s_cols,
                             fname=func,
                             wg_name=wg_name))
        lst = [button(d["wg_name"])
               .uid(f"btn/{d['s_cols']}/{d['fname']}")
               .on_click(self._func_btn_cb) for d in seld]
        resume = table_width - len(lst) % table_width
        lst2 = [label()] * resume
        grid = self._proxy.that.func_table
        _container_impl(grid, *(lst + lst2)).layout(grid_template_columns=f"repeat({table_width}, 200px)")
        self._proxy._registry.update(grid._registry)
