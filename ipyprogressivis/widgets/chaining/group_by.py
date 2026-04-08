from .utils import (VBox, chaining_widget,
                    starter_callback,
                    runner, needs_dtypes,
                    modules_producer)
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    select_multiple,
    stack,
    radiobuttons,
    restore,
    html,
    hbox,
    dropdown,
)
import ipywidgets as ipw
from progressivis.core.api import Sink
from progressivis.table.group_by import (
    GroupBy,
    UTIME,
    DT_MAX,
    SubPColumn as SC,
    UTIME_SHORT_D,
)

from typing import Any as AnyType


@chaining_widget(label="Group by")
class GroupByW(VBox):
    @needs_dtypes
    def initialize(self) -> None:
        self._proxy = anybox(
            self,
            stack(
                radiobuttons("Grouping mode:",
                             options=["columns",
                                      "datetime subcolumn",
                                      "multi index subcolumn"],
                             style={"description_width": "initial"},
                             ).uid("grouping_mode_radio").observe(self._on_grouping_cb),
                html("columns"),
                selected_index="datetime64" not in self.input_dtypes.values()
            ).uid("grouping_mode_stack"),
            stack(
                select_multiple("By",
                                options=[(f"{col}:{t}", col) for (col, t) in self.dtypes.items()],
                                value=[],
                                rows=5,
                                ).uid("by_box_selm").observe(self.selm_cb),
                hbox(
                    dropdown("Datetime column:",
                             options=[("", "")]
                             + [
                                 (f"{col}:{t}", col)
                                 for (col, t) in self.dtypes.items()
                                 if t == "datetime64"
                             ],
                             style={"description_width": "initial"},
                             ).uid("by_box_dd").observe(self.dd_cb),
                    select_multiple(
                        options=list(zip(UTIME, UTIME_SHORT_D.keys())),
                        value=[],
                        rows=DT_MAX,
                        description="==>",
                    ).uid("by_box_time").observe(self.sel_cb),
                ),
                selected_index=0
            ).uid("by_box"),
            button("Start",
                   disabled=True
                   ).uid("start_btn").on_click(self._add_group_by_cb)
            )

    @modules_producer
    def init_modules(self, by: AnyType) -> GroupBy:
        print("by", by)
        if isinstance(by, dict):
            by = SC(by["col"]).dt[by["subcols"]]
        s = self.input_module.scheduler
        with s:
            grby = GroupBy(by=by, keepdims=True, scheduler=s)
            grby.input.table = self.input_module.output[self.input_slot]
            sink = Sink(scheduler=s)
            sink.input.inp = grby.output.result
            return grby

    @starter_callback
    def _add_group_by_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        #proxy.that.grouping_mode.attrs(disabled=True)
        #proxy.that.by_box.attrs(disabled=True)
        if proxy.that.grouping_mode_stack.widget.selected_index == 1:  # type: ignore
            by = proxy.that.by_box_selm.widget.value
            assert by
            if len(by) == 1:
                by = by[0]
            #proxy.that.by_box.attrs(disabled=True)
        else:
            #by_box = cast(ipw.HBox, self.child.by_box)
            dd = proxy.that.by_box_dd.widget.value
            sel = proxy.that.by_box_time.widget.value
            col = dd
            by = SC(col).dt["".join(sel)]
            by = dict(col=col, subcols="".join(sel))
            proxy.that.by_box_dd.attrs(disabled = True)
            proxy.that.by_box_time.attrs(disabled = True)
        #if is_recording():
        #    amend_last_record({'frozen': dict(by=by)})
        self.record = self._proxy.dump()
        self.output_module = self.init_modules(by)
        self.output_slot = "result"

    @runner
    def run(self) -> AnyType:
        ui_dumped = self.record
        self._proxy = restore(ui_dumped, globals(), obj=self)
        self.children = self._proxy.widget.children  # type: ignore
        if self._proxy.that.grouping_mode_stack.widget.selected_index == 1:  # type: ignore
            by = self._proxy.that.by_box_selm.widget.value
            assert by
            if len(by) == 1:
                by = by[0]
        else:
            dd = self._proxy.that.by_box_dd.widget.value
            sel = self._proxy.that.by_box_time.widget.value
            col = dd
            by = SC(col).dt["".join(sel)]
            by = dict(col=col, subcols="".join(sel))
        self.output_module = self.init_modules(by)
        self.output_slot = "result"

    def _on_grouping_cb(self, proxy: Proxy, val: AnyType) -> None:
        proxy.that.by_box.attrs(selected_index=val["new"] != "columns")
        #if val["new"] == "columns":
        #    self.child.by_box = self.make_sel_multiple()
        #else:
        #    self.child.by_box = self.make_subcolumn_box()


    def selm_cb(self, proxy: Proxy, change: dict[str, AnyType]) -> None:
        proxy.that.start_btn.attrs(disabled=not change["new"])

    def dd_cb(self, proxy: Proxy, val: AnyType) -> None:
        if val["new"]:
           proxy.that.by_box_time.attrs(disabled=False)
        else:
           proxy.that.by_box_time.attrs(disabled=True)
           proxy.that.start_btn.attrs(disabled=True)

    def sel_cb(self, proxy: Proxy, val: AnyType) -> None:
        proxy.that.start_btn.attrs(disabled=not val["new"])

