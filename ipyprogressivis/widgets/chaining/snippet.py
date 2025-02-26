from .utils import (make_button, stage_register, dongle_widget, VBoxTyped,
                    TypedBase, amend_last_record, GuestWidget,
                    is_recording, disable_all, runner, needs_dtypes)
from ..df_grid import DataFrameGrid
import pandas as pd
import ipywidgets as ipw
from progressivis.core.api import Module, Sink

from typing import Any as AnyType, List




class SnippetW(VBoxTyped):
    class Typed(TypedBase):
        snippet: ipw.Dropdown
        cols_mode: ipw.RadioButtons
        columns: DataFrameGrid
        start_btn: ipw.Button
        widget: ipw.DOMWidget

    @needs_dtypes
    def initialize(self) -> None:
        from .custom import CUSTOMER_SNIPPET
        inp_module = self.input_module
        self.child.snippet = ipw.Dropdown(description="Snippet:",
                                          options=[""] + list(CUSTOMER_SNIPPET.keys()),
                                          value = ""
                                          )
        self.child.snippet.observe(self._snippet_cb, names="value")
        if not isinstance(inp_module, Sink):  # i.e. not a custom loader
            self.child.cols_mode = ipw.RadioButtons(
                options=[("All", "all"),
                         ("Selection as list", "aslist"),
                         ("Selection as dict", "asdict")],
                value="all",
                description="Columns to process:",
                disabled=False,
                style={"description_width": "initial"},
                )
            self.child.cols_mode.observe(self._cols_mode_cb, names="value")
        else:
            self.child.cols_mode = dongle_widget()
        self.child.start_btn = make_button(
            "Start", cb=self._start_btn_cb, disabled=True
        )
    def _cols_mode_cb(self, val: AnyType) -> None:
        if val["new"] == "all":
            self.child.columns = dongle_widget()
        elif val["new"] == "aslist":
            self.child.columns = ipw.SelectMultiple(
                options=[(f"{col}:{t}", col) for (col, t) in self.dtypes.items()],
                value=[],
                rows=5,
                description="",
                disabled=False,
            )
        else:
            assert val["new"] == "asdict"
            # col_dtypes = [(f"{col}:{t}", col) for (col, t) in self.dtypes.items()]
            df = pd.DataFrame(
                index=list(self.dtypes.keys()),
                columns=["Key"],
                dtype=object,
            )
            df.loc[:, "Key"] = lambda: ipw.Text(value="", description="",  # type: ignore
                                                placeholder="enter a key name to select",
                                                disabled=False)
            self.child.columns = DataFrameGrid(df)

    def _snippet_cb(self, val: AnyType) -> None:
        self.child.start_btn.disabled = not val["new"]

    def get_underlying_modules(self) -> List[str]:
        assert isinstance(self.output_module, Module)
        return [self.output_module.name]

    def _start_btn_cb(self, btn: ipw.Button) -> None:
        from .custom import CUSTOMER_SNIPPET
        snippet = CUSTOMER_SNIPPET[self.child.snippet.value]
        mode = self.child.cols_mode.value
        columns: list[str] | dict[str, str] = []
        if mode == "aslist":
            columns = self.child.columns.value
        elif mode == "asdict":
            columns = {row["Key"].value: cname for (cname, row) in
                       self.child.columns.df.iterrows()
                       if row["Key"].value}
        else:
            assert mode in ("all", "")
        if is_recording():
            amend_last_record({'frozen': dict(snippet=self.child.snippet.value, columns=columns)})
        res = snippet(self.input_module, self.input_slot, columns)
        self.output_module = res.output_module
        self.output_slot = res.output_slot
        if res.widget is not None:
            self.child.widget = res.widget
        self.make_chaining_box()
        self.dag_running()
        disable_all(self, exceptions=(res.widget,))
        self.manage_replay()

    def provide_surrogate(self, title: str) -> GuestWidget:
        # disable_all(self)
        return self

    @runner
    def run(self) -> AnyType:
        content = self.frozen_kw
        print("snippet content", content)
        from .custom import CUSTOMER_SNIPPET
        snippet = CUSTOMER_SNIPPET[content["snippet"]]
        res = snippet(self.input_module, self.input_slot, content["columns"])
        self.output_module = res.output_module
        self.output_slot = res.output_slot
        if res.widget is not None:
            self.child.widget = res.widget

stage_register["Snippet"] = SnippetW
