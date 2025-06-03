from .utils import (
    make_button,
    stage_register,
    append_child, VBox, amend_last_record, is_recording, disable_all, runner, needs_dtypes,
    modules_producer
)
import ipywidgets as ipw
from progressivis.table.group_by import UTIME_SHORT_D
from progressivis.table.api import Join
from progressivis.core.api import Sink, Module
from typing import (
    Any as AnyType,
    Any,
    Optional,
    Dict,
    Callable,
    List,
    Union,
    Literal,
    cast
)

WidgetType = AnyType
_l = ipw.Label


def _ck(name: str) -> ipw.Checkbox:
    return ipw.Checkbox(value=True,
                        description=name,
                        disabled=False,
                        indent=False)


class MaskWidget(ipw.HBox):
    def __init__(self) -> None:
        self.year = _ck("Y")
        self.month = _ck("M")
        self.day = _ck("D")
        self.hour = _ck("h")
        self.min_ = _ck("m")
        self.sec = _ck("s")
        self._ck_tpl = tuple([self.year, self.month, self.day,
                              self.hour, self.min_, self.sec])
        super().__init__([])

    def show(self) -> None:
        self.children = self._ck_tpl

    def hide(self) -> None:
        for ck in self._ck_tpl:
            ck.value = True
        self.children = tuple([])

    def get_values(self) -> List[bool]:
        return [ck.value for ck in self._ck_tpl]

    def get_dt(self) -> str:
        return "".join([sym*ck.value for (sym, ck) in zip(UTIME_SHORT_D, self._ck_tpl)])


class JoinW(VBox):
    def __init__(self) -> None:
        super().__init__()

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = None  # type: ignore
        dd_list = [(f"{k}[{n}]" if n
                    else k, (k, n)) for (k, n) in self.current_widget_keys]
        self._input_1 = _l(self.parent.title)
        self._role_1 = _l("primary")
        self._input_2 = ipw.Dropdown(
            options=dd_list,
            description="",
            disabled=False,
            style={"description_width": "initial"},
        )
        self._role_2 = ipw.Dropdown(
            options=["primary", "related"],
            value="related",
            description="",
            disabled=False,
            style={"description_width": "initial"},
        )
        self._cols_setup = ipw.Tab()
        self._primary_cols_dict: Dict[str, AnyType] = {}
        self._related_cols_dict: Dict[str, AnyType] = {}
        self._primary_wg: Optional[VBox] = None
        self._related_wg: Optional[VBox] = None
        self._role_2.observe(self._role_2_cb, "value")
        lst = [_l("Inputs"), _l("Roles"), self._input_1, self._role_1,
               self._input_2, self._role_2]
        gb = ipw.GridBox(
            lst,
            layout=ipw.Layout(grid_template_columns="50% 50%"),
        )
        gb.layout.border = "1px solid"
        self._btn_ok = make_button("OK", cb=self._btn_ok_cb)
        self._how = ipw.Dropdown(
            options=["inner", "outer"],
            value="inner",
            description="How",
            disabled=False,
            style={"description_width": "initial"},
        )
        self._btn_start = make_button("Start", disabled=True, cb=self._btn_start_cb)
        self.children = (gb, self._btn_ok,
                         self._cols_setup,
                         ipw.HBox([self._how,
                                   self._btn_start]))

    def _btn_start_cb(self, btn: Any) -> None:
        primary_cols = [k for (k, (ck, _, _)) in
                        self._primary_cols_dict.items() if ck.value]
        related_cols = [k for (k, ck) in
                        self._related_cols_dict.items() if ck.value]
        primary_on: Union[str, List[str]] = [k for (k, (_, dd, _)) in
                                             self._primary_cols_dict.items() if dd.value]
        related_on: Union[str, List[str]] = [dd.value for (_, dd, _) in
                                             self._primary_cols_dict.values() if dd.value]
        assert primary_on
        assert related_on
        assert len(primary_on) == len(related_on)
        primary_on = primary_on[0] if len(primary_on) == 1 else primary_on
        related_on = related_on[0] if len(related_on) == 1 else related_on
        inv_mask = None
        assert self._primary_wg is not None
        assert self._primary_wg.output_dtypes is not None
        if (isinstance(primary_on,
                       str) and self._primary_wg.output_dtypes[
                           primary_on] == "datetime64"):
            _, _, mw = self._primary_cols_dict[primary_on]
            msk = mw.get_dt()
            if msk != "YMDhms":
                inv_mask = msk
        join_kw = dict(
            primary_cols=primary_cols,
            related_cols=related_cols,
            primary_on=primary_on,
            related_on=related_on,
            primary_inp=self._primary_wg_frozen,
            related_inp=self._related_wg_frozen,
            inv_mask=inv_mask,
            how=self._how.value,
            )
        if is_recording():
            amend_last_record({'frozen': join_kw})
        self.output_module = self.init_join(**join_kw)

        self.make_chaining_box()
        self.dag_running()
        disable_all(self)

    @runner
    def run(self) -> None:
        content = self.frozen_kw
        if (key := content["primary_inp"]) != "parent":
            primary_wg = self.get_widget_by_key(key)
            self.dag.add_parent(self.title,  primary_wg.title)
        if (key := content["related_inp"]) != "parent":
            related_wg = self.get_widget_by_key(key)
            self.dag.add_parent(self.title,  related_wg.title)
        self.output_module = self.init_join(**content)
        self.output_slot = "result"

    @modules_producer
    def init_join(self, primary_cols: list[str], related_cols: list[str],
                  primary_on: str | list[str], related_on: str | list[str],
                  primary_inp: str | tuple[str, int], related_inp: tuple[str, int],
                  inv_mask: str,
                  how: Literal['inner', 'outer']
                  ) -> Join:
        if primary_inp == "parent":
            primary_wg = self.parent
            related_wg = self.get_widget_by_key(tuple(related_inp))  # type: ignore
            # second_wg = related_wg
        else:
            primary_wg = self.get_widget_by_key(tuple(primary_inp))  # type: ignore
            related_wg = self.parent
        s = self.input_module.scheduler()
        with s:
            assert primary_wg is not None
            assert related_wg is not None
            join = Join(how=how, inv_mask=inv_mask, scheduler=s)
            join.create_dependent_modules(
                related_module=cast(Module, related_wg.output_module),
                primary_module=cast(Module, primary_wg.output_module),
                related_on=related_on,
                primary_on=primary_on,
                related_cols=related_cols,
                primary_cols=primary_cols
            )
            sink = Sink(scheduler=s)
            sink.input.inp = join.output.result
        return join

    def _btn_ok_cb(self, *args: Any, **kw: Any) -> None:
        self._input_2.disabled = True
        self._role_2.disabled = True
        widget_1 = self.parent
        widget_1_frozen = "parent"
        widget_2 = self.get_widget_by_key(self._input_2.value)
        widget_2_frozen = self._input_2.value
        if widget_2.output_dtypes is None:
            widget_2.compute_dtypes_then_call(self._btn_ok_cb, args, kw)
            return
        self.dag.add_parent(self.title,  widget_2.title)
        if self._role_1.value == "primary":
            primary_wg = widget_1
            primary_wg_frozen = widget_1_frozen
            related_wg = widget_2
            related_wg_frozen = widget_2_frozen
        else:
            primary_wg = widget_2
            primary_wg_frozen = widget_2_frozen
            related_wg = widget_1
            related_wg_frozen = widget_1_frozen
        self._primary_wg = primary_wg
        self._related_wg = related_wg
        self._primary_wg_frozen = primary_wg_frozen
        self._related_wg_frozen = related_wg_frozen
        # primary cols
        ck_all = ipw.Checkbox(value=True,
                              description="",
                              disabled=False,
                              indent=False)
        lst: List[WidgetType] = [_l(""), _l("Keep"), _l("Join on"), _l("Subcolumns"),
                                 _l("*"), ck_all, _l(""), _l("")]
        assert primary_wg.output_dtypes is not None
        assert related_wg.output_dtypes is not None
        for col, ty in primary_wg.output_dtypes.items():
            on_list = [""] + [c for (c, t) in related_wg.output_dtypes.items() if t == ty]
            ck = ipw.Checkbox(value=True,
                              description="",
                              disabled=False,
                              indent=False)
            dd = ipw.Dropdown(options=on_list,
                              description="",
                              disabled=False,
                              layout={"width": "initial"})
            dtw = MaskWidget()
            dd.observe(self._make_dd_cb(col), "value")
            self._primary_cols_dict[col] = (ck, dd, dtw)
            lst.extend([_l(col), ck, dd, dtw])
        ck_all.observe(self._ck_all_cb, "value")
        p_gb = ipw.GridBox(
            lst,
            layout=ipw.Layout(grid_template_columns="20% 10% 40% 30%"),
        )
        append_child(self._cols_setup, p_gb, title="Primary")
        # related cols
        ck_all = ipw.Checkbox(value=True,
                              description="",
                              disabled=False,
                              indent=False)
        lst = [_l(""), _l("Keep"), _l("*"), ck_all]
        for col in related_wg.output_dtypes.keys():
            ck = ipw.Checkbox(value=True,
                              description="",
                              disabled=False,
                              indent=False)
            self._related_cols_dict[col] = ck
            lst.extend([_l(col), ck])
        ck_all.observe(self._ck_all_cb2, "value")
        r_gb = ipw.GridBox(
            lst,
            layout=ipw.Layout(grid_template_columns="80% 20%"),
        )
        append_child(self._cols_setup, r_gb, title="Related")

    def _ck_all_cb(self, change: AnyType) -> None:
        val = change["new"]
        if val:
            for ck, dd, _ in self._primary_cols_dict.values():
                if not dd.value:
                    ck.value = True
        else:
            for ck, dd, _ in self._primary_cols_dict.values():
                ck.value = False

    def _ck_all_cb2(self, change: AnyType) -> None:
        val = change["new"]
        for ck in self._related_cols_dict.values():
            ck.value = val

    def _role_2_cb(self, change: Dict[str, AnyType]) -> None:
        role = change["new"]
        self._role_1.value = "primary" if role == "related" else "related"

    def _make_dd_cb(self, col: str) -> Callable[..., None]:
        def _cbk(change: AnyType) -> None:
            val = change["new"]
            ck, dd, mw = self._primary_cols_dict[col]
            if val:
                ck.value = False
                ck.disabled = True
                self._btn_start.disabled = False
                assert self._primary_wg is not None
                assert self._primary_wg.output_dtypes is not None
                if self._primary_wg.output_dtypes[col] == "datetime64":
                    mw.show()
                else:
                    mw.hide()
            else:
                mw.hide()
                ck.disabled = False
                j = [dd for (_, dd, _) in self._primary_cols_dict.values() if dd.value]
                if not j:
                    self._btn_start.disabled = True
        return _cbk


stage_register["Join"] = JoinW
