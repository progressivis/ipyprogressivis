from .utils import (
    chaining_widget,
    starter_callback,
    VBox,
    runner,
    needs_dtypes,
    modules_producer,
)
import ipywidgets as ipw
import json
from progressivis.table.group_by import UTIME_SHORT_D
from progressivis.table.api import Join
from progressivis.core.api import Sink, Module
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    hbox,
    label,
    dropdown,
    stack,
    html,
    text,
    restore,
    gridbox,
    checkbox,
    tab,
    _container_impl,
)
from typing import Any as AnyType, Any, Literal, cast

WidgetType = AnyType
_l = ipw.Label


def get_dt(proxy: Proxy, col: str) -> str:
    return "".join(
        [sym * proxy.lookup(f"{sym}/{col}").widget.value for sym in UTIME_SHORT_D]
    )


def mask_widget(col: str) -> Proxy:
    return stack(
        html(),
        hbox(
            checkbox("Y", indent=False).uid(f"Y/{col}"),
            checkbox("M", indent=False).uid(f"M/{col}"),
            checkbox("D", indent=False).uid(f"D/{col}"),
            checkbox("h", indent=False).uid(f"h/{col}"),
            checkbox("m", indent=False).uid(f"m/{col}"),
            checkbox("s", indent=False).uid(f"s/{col}"),
        ),
        selected_index=0,
    ).uid(f"datetime/{col}")


@chaining_widget(label="Join")
class JoinW(VBox):
    def __init__(self) -> None:
        super().__init__()
        self._proxy: Proxy | None = None

    @needs_dtypes
    def initialize(self) -> None:
        self.output_dtypes = None  # type: ignore
        dd_list = [
            (f"{k}[{n}]" if n else k, (k, n)) for (k, n) in self.current_widget_keys
        ]
        self._proxy = anybox(
            self,
            gridbox(
                *[
                    label("Inputs"),
                    label("Roles"),
                    label(self.parent.title).uid("input_1"),
                    label("primary").uid("role_1"),
                    dropdown(
                        options=dd_list, style={"description_width": "initial"}
                    ).uid("input_2"),
                    dropdown(
                        options=["primary", "related"],
                        value="related",
                        style={"description_width": "initial"},
                    )
                    .uid("role_2")
                    .observe(self._role_2_cb),
                ]
            ).layout(grid_template_columns="50% 50%", border="1px solid"),
            stack(
                html(),
                text().uid("primary_wg_frozen"),
                text().uid("related_wg_frozen"),
                selected_index=0,
            ),
            button("OK").on_click(self._ok_btn_cb),
            tab(
                gridbox().uid("p_gb").layout(grid_template_columns="20% 10% 40% 30%"),
                gridbox().uid("r_gb").layout(grid_template_columns="80% 20%"),
                titles=["Primary", "Related"],
            ).uid("cols_setup"),
            hbox(
                dropdown(
                    "How",
                    options=["inner", "outer"],
                    value="inner",
                    style={"description_width": "initial"},
                ).uid("how_dd"),
                button("Start").uid("start_btn").on_click(self._start_btn_cb),
            ),
        )
        self._primary_wg: VBox | None = None
        self._related_wg: VBox | None = None

    def filter_cols(
        self, start_str: str, set_only: bool = True, cols_only: bool = True
    ) -> list[tuple[str, Proxy]] | list[str]:
        res: list[tuple[str, Proxy]] | list[str] = []
        if self._proxy is None:
            return res
        start = len(start_str)
        for uid, px in self._proxy._registry.items():
            if not uid.startswith(start_str):
                continue
            if set_only and not self._proxy.lookup(uid).widget.value:
                continue
            res.append(uid[start:] if cols_only else (uid[start:], px))  # type: ignore
        return res

    def primary_cols_ck(
        self, set_only: bool = True, cols_only: bool = True
    ) -> list[tuple[str, Proxy]] | list[str]:
        return self.filter_cols("p_ck/", set_only, cols_only)

    def related_cols_ck(
        self, set_only: bool = True, cols_only: bool = True
    ) -> list[tuple[str, Proxy]] | list[str]:
        return self.filter_cols("r_ck/", set_only, cols_only)

    def primary_cols_dd(
        self, set_only: bool = True, cols_only: bool = True
    ) -> list[tuple[str, Proxy]] | list[str]:
        return self.filter_cols("p_dd/", set_only, cols_only)

    def related_cols_dd(
        self, set_only: bool = True, cols_only: bool = True
    ) -> list[tuple[str, Proxy]] | list[str]:
        return self.filter_cols("r_dd/", set_only, cols_only)

    def get_join_parameters(self) -> dict[str, AnyType]:
        primary_cols = self.primary_cols_ck()
        related_cols = self.related_cols_ck()
        primary_on: str | list[str] = cast(list[str], self.primary_cols_dd())
        related_on: str | list[str] = [
            dd.widget.value
            for (_, dd) in cast(
                list[tuple[str, Proxy]], self.primary_cols_dd(cols_only=False)
            )
        ]
        assert primary_on
        assert related_on
        assert len(primary_on) == len(related_on)
        primary_on = primary_on[0] if len(primary_on) == 1 else primary_on
        related_on = related_on[0] if len(related_on) == 1 else related_on
        inv_mask = None
        assert self._primary_wg is not None
        assert self._primary_wg.output_dtypes is not None
        assert self._proxy is not None
        if (
            isinstance(primary_on, str)
            and self._primary_wg.output_dtypes[primary_on] == "datetime64"
        ):
            msk = get_dt(self._proxy, primary_on)
            if msk != "YMDhms":
                inv_mask = msk
        return dict(
            primary_cols=primary_cols,
            related_cols=related_cols,
            primary_on=primary_on,
            related_on=related_on,
            primary_inp=json.loads(self._proxy.that.primary_wg_frozen.widget.value),
            related_inp=json.loads(self._proxy.that.related_wg_frozen.widget.value),
            inv_mask=inv_mask,
            how=self._proxy.that.how_dd.widget.value,
        )

    @starter_callback
    def _start_btn_cb(self, proxy: Proxy, btn: Any) -> None:
        assert self._proxy is not None
        self.record = self._proxy.dump()
        join_kw = self.get_join_parameters()
        self.output_module = self.init_modules(**join_kw)

    def init_ui(self) -> None:
        content = self.record
        self._proxy = restore(content, globals(), obj=self)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
        primary_inp = json.loads(self._proxy.that.primary_wg_frozen.widget.value)
        related_inp = json.loads(self._proxy.that.related_wg_frozen.widget.value)
        if (key := primary_inp) != "parent":
            self._primary_wg = self.get_widget_by_key(key)
        else:
            self._primary_wg = self.parent
        self.dag.add_parent(self.title, self._primary_wg.title)
        if (key := related_inp) != "parent":
            self._related_wg = self.get_widget_by_key(key)
        else:
            self._related_wg = self.parent
        self.dag.add_parent(self.title, self._related_wg.title)

    @runner
    def run(self) -> None:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        self.children = self._proxy.widget.children  # type: ignore
        primary_inp = json.loads(self._proxy.that.primary_wg_frozen.widget.value)
        related_inp = json.loads(self._proxy.that.related_wg_frozen.widget.value)
        if (key := primary_inp) != "parent":
            self._primary_wg = self.get_widget_by_key(key)
        else:
            self._primary_wg = self.parent
        self.dag.add_parent(self.title, self._primary_wg.title)
        if (key := related_inp) != "parent":
            self._related_wg = self.get_widget_by_key(key)
        else:
            self._related_wg = self.parent
        self.dag.add_parent(self.title, self._related_wg.title)
        content = self.get_join_parameters()
        self.output_module = self.init_modules(**content)
        self.output_slot = "result"

    @modules_producer
    def init_modules(
        self,
        primary_cols: list[str],
        related_cols: list[str],
        primary_on: str | list[str],
        related_on: str | list[str],
        primary_inp: str | tuple[str, int],
        related_inp: tuple[str, int],
        inv_mask: str,
        how: Literal["inner", "outer"],
    ) -> Join:
        if primary_inp == "parent":
            primary_wg = self.parent
            related_wg = self.get_widget_by_key(tuple(related_inp))  # type: ignore
            # second_wg = related_wg
        else:
            primary_wg = self.get_widget_by_key(tuple(primary_inp))  # type: ignore
            related_wg = self.parent
        s = self.input_module.scheduler
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
                primary_cols=primary_cols,
            )
            sink = Sink(scheduler=s)
            sink.input.inp = join.output.result
        return join

    def _ok_btn_cb(self, proxy: Proxy, b: Any) -> None:
        assert self._proxy is not None
        input_2 = proxy.that.input_2
        role_2 = proxy.that.role_2
        input_2.attrs(disabled=True)
        role_2.attrs(disabled=True)
        widget_1 = self.parent
        widget_1_frozen = "parent"
        widget_2 = self.get_widget_by_key(input_2.widget.value)
        widget_2_frozen = input_2.widget.value
        if widget_2.output_dtypes is None:
            widget_2.compute_dtypes_then_call(self._ok_btn_cb, [proxy, b])
            return
        self.dag.add_parent(
            self.title, widget_2.title
        )  # TODO: find a cleaner way to add a second parent
        assert self.carrier not in widget_2.carrier.subwidgets
        widget_2.carrier.subwidgets.append(self.carrier)
        if self._proxy.that.role_1.widget.value == "primary":
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
        self._proxy.that.primary_wg_frozen.attrs(value=json.dumps(primary_wg_frozen))
        self._proxy.that.related_wg_frozen.attrs(value=json.dumps(related_wg_frozen))
        # primary cols
        lst: list[Proxy] = [
            label(""),
            label("Keep"),
            label("Join on"),
            label("Subcolumns"),
            label("*"),
            checkbox(value=True, indent=False).uid("ck_all").observe(self._ck_all_cb),
            label(""),
            label(""),
        ]
        assert primary_wg.output_dtypes is not None
        assert related_wg.output_dtypes is not None
        for col, ty in primary_wg.output_dtypes.items():
            on_list = [""] + [
                c for (c, t) in related_wg.output_dtypes.items() if t == ty
            ]
            ck = checkbox(value=True, indent=False).uid(f"p_ck/{col}")
            dd = (
                dropdown(options=on_list, layout={"width": "initial"})
                .uid(f"p_dd/{col}")
                .observe(self._dd_cb)
            )
            dtw = mask_widget(col) if ty == "datetime64" else html()
            lst.extend([label(col), ck, dd, dtw])
        _container_impl(proxy.that.p_gb, *lst)
        self._proxy._registry.update(proxy.that.p_gb._registry)
        # related cols
        lst = [
            label(""),
            label("Keep"),
            label("*"),
            checkbox(value=True, indent=False)
            .uid("ck_all_r")
            .observe(self._ck_all_cb2),
        ]
        for col in related_wg.output_dtypes.keys():
            lst.extend(
                [label(col), checkbox(value=True, indent=False).uid(f"r_ck/{col}")]
            )
        _container_impl(proxy.that.r_gb, *lst)
        self._proxy._registry.update(proxy.that.r_gb._registry)

    def _ck_all_cb(self, proxy: Proxy, change: AnyType) -> None:
        val = change["new"]
        if val:
            for col, dd in cast(
                list[tuple[str, Proxy]],
                self.primary_cols_dd(set_only=False, cols_only=False),
            ):
                if not dd.widget.value:
                    proxy.lookup(f"p_ck/{col}").attrs(value=True)
        else:
            for _, ck in cast(
                list[tuple[str, Proxy]], self.primary_cols_ck(cols_only=False)
            ):
                ck.attrs(value=False)

    def _ck_all_cb2(self, proxy: Proxy, change: AnyType) -> None:
        val = change["new"]
        for _, ck in cast(
            list[tuple[str, Proxy]],
            self.related_cols_ck(set_only=False, cols_only=False),
        ):
            ck.attrs(value=val)

    def _role_2_cb(self, proxy: Proxy, change: dict[str, AnyType]) -> None:
        role = change["new"]
        proxy.that.role_1.attrs(value=("primary" if role == "related" else "related"))

    def _dd_cb(self, proxy: Proxy, change: AnyType) -> None:
        assert proxy._uid is not None
        _, col = proxy._uid.split("/")
        ck = proxy.lookup(f"p_ck/{col}")
        mw = proxy.lookup(f"datetime/{col}")
        val = change["new"]
        if val:
            ck.attrs(value=False, disabled=True)
            proxy.that.start_btn.attrs(disabled=False)
            assert self._primary_wg is not None
            assert self._primary_wg.output_dtypes is not None
            mw.attrs(
                selected_index=(self._primary_wg.output_dtypes[col] == "datetime64")
            )
        else:
            mw.attrs(selected_index=0)  # hide
            ck.attrs(disabled=False)
            if not self.primary_cols_dd():
                proxy.that.start_btn.attrs(disabled=True)
