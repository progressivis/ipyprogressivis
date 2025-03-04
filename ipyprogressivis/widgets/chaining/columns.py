from .utils import (
    make_button,
    stage_register,
    dongle_widget,
    VBoxTyped,
    IpyHBoxTyped,
    TypedBase,
    is_recording,
    amend_last_record,
    runner,
    needs_dtypes
)
import ipywidgets as ipw
import numpy as np
import operator as op
import weakref
from progressivis.table.repeater import Repeater, Computed
from progressivis.core.api import Sink, Module
from progressivis.table.compute import (
    week_day,
    UNCHANGED,
    make_if_else,
    ymd_string,
    is_weekend,
    year,
    month,
    day,
    hour,
    year_day_int
)

from typing import Any as AnyType, Optional, Tuple, List, Dict, Callable, Union, cast

WidgetType = AnyType


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
UFUNCS: Dict[str, Callable[..., AnyType]] = {
    k: v for (k, v) in np.__dict__.items() if isinstance(v, np.ufunc) and v.nin == 1
}

ALL_FUNCS = UFUNCS.copy()

ALL_FUNCS.update(
    {"week_day": week_day, "is_weekend": is_weekend,
     "ymd_string": ymd_string, "year": year,
     "month": month, "day": day, "hour": hour, "year_day": year_day_int}
)

class FuncW(ipw.VBox):
    def __init__(self, main: "PColumnsW", colname: str, fname: str) -> None:
        self._colname = colname
        self._fname = fname
        self._name = ipw.Text(
            value=f"{colname}_{fname}",
            placeholder="mandatory",
            description="Name:",
            disabled=False,
        )
        fnc = ALL_FUNCS[fname]
        if isinstance(fnc, np.ufunc):
            type_ = main.dtypes[colname]  # TODO: improve it using ufunc.types information
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
        self._dtype = ipw.Dropdown(
            value=type_,
            placeholder="dtype",
            options=DTYPES,
            description="Out dtype",
            ensure_option=True,
            disabled=False,
        )
        self._use = ipw.Checkbox(value=False, description="Use", disabled=False)
        self._use.observe(main.update_func_list, names="value")
        super().__init__([self._name, self._dtype, self._use])


class IfElseW(ipw.VBox):
    def __init__(self, main: "PColumnsW") -> None:
        self._main = weakref.ref(main)
        self._name = ipw.Text(
            value="", placeholder="mandatory", description="Name:", disabled=False
        )
        self._name.observe(self._name_cb, names="value")
        self._type = ipw.Dropdown(
            options=[("object", lambda x: x), ("integer", int), ("floating", float)],
            description="Type:",
            ensure_option=True,
            disabled=False,
        )
        self._name_box = ipw.HBox([self._name, self._type])
        self._is = ipw.Dropdown(
            options=[
                ("", None),
                (">", op.gt),
                ("<", op.lt),
                (">=", op.ge),
                ("<=", op.le),
                ("==", op.eq),
                ("NaN", np.isnan),
            ],
            description="Is",
            ensure_option=True,
            disabled=False,
        )
        self._is.observe(self._is_cb, names="value")
        self._than = ipw.Text(
            value="", placeholder="", description="Than:", disabled=False
        )
        self._than.observe(self._name_cb, names="value")
        self._cond_box = ipw.HBox([self._is, self._than])

        self._if_true_val = ipw.Text(
            value="", placeholder="mandatory", description="If True:", disabled=False
        )
        self._if_true_val.observe(self._name_cb, names="value")
        self._if_true_ck = ipw.Checkbox(
            value=False, description="Unchanged", disabled=False
        )
        self._if_true_ck.observe(self._if_true_ck_cb, names="value")
        self._if_true_box = ipw.HBox([self._if_true_val, self._if_true_ck])
        self._if_false_val = ipw.Text(
            value="", placeholder="mandatory", description="If False:", disabled=False
        )
        self._if_false_val.observe(self._name_cb, names="value")
        self._if_false_ck = ipw.Checkbox(
            value=False, description="Unchanged", disabled=False
        )
        self._if_false_ck.observe(self._if_false_ck_cb, names="value")
        self._if_false_box = ipw.HBox([self._if_false_val, self._if_false_ck])
        self._create_fnc = make_button("Create", disabled=True, cb=self._create_fnc_cb)
        super().__init__(
            [
                self._name_box,
                self._cond_box,
                self._if_true_box,
                self._if_false_box,
                self._create_fnc,
            ]
        )

    @property
    def main(self) -> Optional["PColumnsW"]:
        return self._main()

    def _name_cb(self, change: AnyType) -> None:
        self._check_all()

    def _is_cb(self, change: AnyType) -> None:
        if change["new"] is np.isnan:
            self._than.value = ""
            self._than.disabled = True
        else:
            self._than.disabled = False
        self._check_all()

    def _if_true_ck_cb(self, change: AnyType) -> None:
        if change["new"]:
            self._if_true_val.value = ""
            self._if_true_val.disabled = True
        else:
            self._if_true_val.disabled = False
        self._check_all()

    def _if_false_ck_cb(self, change: AnyType) -> None:
        if change["new"]:
            self._if_false_val.value = ""
            self._if_false_val.disabled = True
        else:
            self._if_false_val.disabled = False
        self._check_all()

    def _check_all(self) -> None:
        if not self._name.value:
            self._create_fnc.disabled = True
            return
        if not self._is.value:
            self._create_fnc.disabled = True
            return
        if not (self._if_true_val.value or self._if_true_ck.value):
            self._create_fnc.disabled = True
            return
        if not (self._if_false_val.value or self._if_false_ck.value):
            self._create_fnc.disabled = True
            return
        if self._if_true_ck.value and self._if_false_ck.value:
            self._create_fnc.disabled = True
            return
        if self._is.value is not np.isnan and not self._than.value:
            self._create_fnc.disabled = True
            return
        self._create_fnc.disabled = False

    def _create_fnc_cb(self, btn: AnyType) -> None:
        name = self._name.value
        assert name
        op_ = self._is.value
        assert op_ is not None
        conv_ = self._type.value
        than_ = None if op_ is np.isnan else conv_(self._than.value)
        if_true = (
            UNCHANGED if self._if_true_ck.value else conv_(self._if_true_val.value)
        )
        if_false = (
            UNCHANGED if self._if_false_ck.value else conv_(self._if_false_val.value)
        )
        func = make_if_else(op_=op_, test_val=than_, if_true=if_true, if_false=if_false)
        ALL_FUNCS.update({name: np.vectorize(func)})
        assert self.main is not None
        self.main.c_.cols_funcs.c_.funcs.options = [""] + list(ALL_FUNCS.keys())

layout_refresh = ipw.Layout(width='30px', height='30px')

class ColsFuncs(IpyHBoxTyped):
    class Typed(TypedBase):
        cols: ipw.Select
        funcs: ipw.Select
        computed: Optional[FuncW]


class KeepStored(IpyHBoxTyped):
    class Typed(TypedBase):
        stored_cols: ipw.SelectMultiple
        keep_all: ipw.Checkbox

class OptsBar(IpyHBoxTyped):
    class Typed(TypedBase):
        numpy_ufuncs: ipw.Checkbox
        refresh_btn: ipw.Button
        label: ipw.Label

class PColumnsW(VBoxTyped):
    class Typed(TypedBase):
        opts: OptsBar
        custom_funcs: ipw.Accordion
        cols_funcs: ColsFuncs
        func_table: Optional[Union[ipw.Label, ipw.GridBox]]
        keep_stored: KeepStored
        btn_apply: ipw.Button

    @needs_dtypes
    def initialize(self) -> None:
        self._col_widgets: Dict[Tuple[str, str], FuncW] = {}
        self._computed: List[Optional[FuncW]] = []
        self.c_.opts = OptsBar()
        wg: AnyType
        wg = self.c_.opts.c_.numpy_ufuncs = ipw.Checkbox(
            value=True, description="Show Numpy universal functions", disabled=False, indent=False
        )
        wg.observe(self._numpy_ufuncs_cb, names="value")
        wg = self.c_.opts.c_.refresh_btn = make_button(
            "", cb=self._refresh_funcs_cb, disabled=False, icon="refresh",
            layout=layout_refresh
        )
        wg.observe(self._refresh_funcs_cb, names="value")
        self.c_.opts.c_.label = ipw.Label("Refresh custom function list")
        self._if_else = IfElseW(self)
        self.c_.custom_funcs = ipw.Accordion(
            children=[self._if_else], selected_index=None
        )
        self.c_.custom_funcs.set_title(0, "Add If-Else expressions")
        cols_t = [f"{c}:{t}" for (c, t) in self.dtypes.items()]
        col_list = list(zip(cols_t, self.dtypes.keys()))
        cols_funcs = ColsFuncs()
        cols_funcs.c_.cols = ipw.Select(
            disabled=False, options=[("", "")] + col_list, rows=7
        )
        cols_funcs.c_.cols.observe(self._columns_cb, names="value")
        from .custom import CUSTOMER_FNC
        ALL_FUNCS.update(CUSTOMER_FNC)
        cols_funcs.c_.funcs = ipw.Select(
            disabled=True, options=[""] + list(ALL_FUNCS.keys()), rows=7
        )
        cols_funcs.c_.funcs.observe(self._functions_cb, names="value")
        self.c_.cols_funcs = cols_funcs
        keep_stored = KeepStored()
        keep_stored.c_.stored_cols = ipw.SelectMultiple(
            options=col_list,
            value=[],
            rows=5,
            description="Keep also:",
            disabled=False,
        )
        keep_stored.c_.keep_all = ipw.Checkbox(
            value=False, description="Select all", disabled=False
        )
        keep_stored.c_.keep_all.observe(self._keep_all_cb, names="value")
        self.c_.keep_stored = keep_stored
        self.c_.btn_apply = make_button("Apply", disabled=False, cb=self._btn_apply_cb)


    def _keep_all_cb(self, change: AnyType) -> None:
        val = change["new"]
        self.c_.keep_stored.c_.stored_cols.value = (
            list(self.dtypes.keys()) if val else []
        )

    def _refresh_funcs_cb(self, change: AnyType) -> None:
        from .custom import CUSTOMER_FNC
        ALL_FUNCS.update(CUSTOMER_FNC)
        self.c_.cols_funcs.c_.funcs.options = list(ALL_FUNCS.keys())

    def _numpy_ufuncs_cb(self, change: AnyType) -> None:
        if change["new"]:
            ALL_FUNCS.update(UFUNCS)
        else:
            for k in UFUNCS.keys():
                del ALL_FUNCS[k]
        self.c_.cols_funcs.c_.funcs.options = [""] + list(ALL_FUNCS.keys())

    def _columns_cb(self, change: AnyType) -> None:
        val = change["new"]
        self.c_.cols_funcs.c_.funcs.disabled = False
        if not val:
            self.c_.cols_funcs.c_.funcs.value = ""
            self.c_.cols_funcs.c_.funcs.disabled = True
            self.c_.cols_funcs.c_.computed = None
        elif self.c_.cols_funcs.c_.funcs.value:
            self.set_selection()
        else:
            self.c_.cols_funcs.c_.computed = None

    def _functions_cb(self, change: AnyType) -> None:
        val = change["new"]
        if not val:
            self.c_.cols_funcs.c_.computed = None
        else:
            self.set_selection()

    def set_selection(self) -> None:
        key = (
            self.c_.cols_funcs.c_.cols.value,
            self.c_.cols_funcs.c_.funcs.value,
        )
        if key not in self._col_widgets:
            self._col_widgets[key] = FuncW(self, *key)
        self.c_.cols_funcs.c_.computed = self._col_widgets[key]

    def _make_computed_list(self) -> list[dict[str, str]]:
        return [
            dict(col=col, fname=fname, wg_name=wg._name.value, wg_dtype=wg._dtype.value)
            for (col, fname), wg in self._col_widgets.items()
            if wg._use.value
        ]

    def _btn_apply_cb(self, btn: AnyType) -> None:
        comp_list = self._make_computed_list()
        cols = list(self.c_.keep_stored.c_.stored_cols.value)
        if is_recording():
            amend_last_record({"frozen": dict(comp_list=comp_list, columns=cols)})
        self.output_module = self.init_module(comp_list, columns=cols)
        self.make_chaining_box()
        self.dag_running()

    @runner
    def run(self) -> None:
        content = self.frozen_kw
        self.output_module = self.init_module(**content)
        self.output_slot = "result"

    def init_module(self, comp_list: list[dict[str, str]],
                    columns: List[str]) -> Repeater:
        comp = Computed()
        from .custom import CUSTOMER_FNC
        ALL_FUNCS.update(CUSTOMER_FNC)
        for d_ in comp_list:
            func = ALL_FUNCS[d_["fname"]]
            comp.add_ufunc_column(
                d_["wg_name"], d_["col"], func, np.dtype(d_["wg_dtype"])
            )
        s = self.input_module.scheduler()
        with s:
            rep = Repeater(computed=comp, scheduler=s)
            rep.input.table = self.input_module.output[self.input_slot][tuple(columns)]
            sink = Sink(scheduler=s)
            sink.input.inp = rep.output.result
            return rep

    def make_func_button(self, key: Tuple[str, str], wg: FuncW) -> ipw.Button:
        kcol, kfun = key

        def _cb(btn: AnyType) -> None:
            self.c_.cols_funcs.c_.cols.value = kcol
            self.c_.cols_funcs.c_.funcs.value = kfun

        btn = make_button(wg._name.value, cb=_cb)
        btn.layout = ipw.Layout(width="auto", height="40px")
        return btn

    def update_func_list(self, wg: FuncW) -> None:
        table_width = 4
        seld = {k: wg for (k, wg) in self._col_widgets.items() if wg._use.value}
        if not seld:
            self.c_.func_table = None
            return
        lst = [self.make_func_button(key, wg) for (key, wg) in seld.items()]
        resume = table_width - len(lst) % table_width
        lst2 = [dongle_widget()] * resume
        self.c_.func_table = ipw.GridBox(
            lst + lst2,
            layout=ipw.Layout(grid_template_columns=f"repeat({table_width}, 200px)"),
        )

    def get_underlying_modules(self) -> List[Module]:
        return [cast(Module, self.output_module)]


stage_register["View"] = PColumnsW
