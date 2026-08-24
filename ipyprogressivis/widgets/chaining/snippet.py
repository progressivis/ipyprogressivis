from .utils import (VBox, chaining_widget, starter_callback,
                    disable_all, runner, labcommand, modules_producer, restore_on_replay)

import ipywidgets as ipw
from itertools import chain, batched
from progressivis.core.api import Sink
from .custom import register_snippet, SnippetResult
from typing import Any as AnyType, Callable
from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    label,
    dropdown,
    radiobuttons,
    file_upload,
    select_multiple,
    gridbox,
    text,
    html,
    stack,
    hbox,
    box,
)


layout_refresh = ipw.Layout(width='30px', height='30px')
_ = register_snippet, SnippetResult

@chaining_widget(label="Snippet")
class SnippetW(VBox):
    @restore_on_replay
    def initialize(self) -> None:
        from .custom import CUSTOMER_SNIPPET
        inp_module = self.input_module
        self._proxy = anybox(
            self,
            hbox(label("Upload snippets:"), file_upload(accept='.py').observe(self._upload_cb)),
            hbox(dropdown("Snippet:",
                          options=[""] + list(CUSTOMER_SNIPPET.keys()),
                          ).uid("choice").observe(self._snippet_cb),
                 button(icon="refresh")
                 .layout(width='30px', height='30px')
                 .on_click(self._refresh_btn_cb)
            ),
            html().uid("cols_mode") if isinstance(inp_module, Sink) else radiobuttons(
                options=[("All", "all"),
                         ("Selection as list", "aslist"),
                         ("Selection as dict", "asdict")],
                value="all",
                description="Columns to process:",
                disabled=False,
                style={"description_width": "initial"},
            ).uid("cols_mode").observe(self._cols_mode_cb),
            stack(
                html(),
                select_multiple(
                    options=[(f"{col}:{t}", col) for (col, t) in self.dtypes.items()] if self.dtypes is not None else [],
                    rows=5,
                ).uid("aslist_mode"),
                gridbox(
                    label(""), label("Key"),
                    *list(
                        chain.from_iterable(
                            [
                                [label(col), text(placeholder="enter a key name to select")]
                                for col in self.dtypes.keys()
                            ] if self.dtypes is not None else []
                        )

                    )
                ).layout(width="100%", grid_template_columns="100px 100px").uid("asdict_mode"),
                selected_index=0
            ).uid("columns"),
            button("Start", disabled=True)
            .uid("start_btn")
            .on_click(self._start_btn_cb),
            box().uid("custom_widget")
        )

    def _upload_cb(self, p: Proxy, change: dict[str, AnyType]) -> None:
        from .custom import CUSTOMER_SNIPPET
        _ = CUSTOMER_SNIPPET
        for item in change["new"]:
            code = item.content.tobytes().decode()
            exec(code, globals(),  globals())
            labcommand("progressivis:create_code_cell",
                       code=code,
                       index=2,  # i.e. insert it after #root & co
                       run=False)
        self._refresh_btn_cb()

    def _cols_mode_cb(self, p: Proxy, val: AnyType) -> None:
        assert self._proxy is not None
        columns = self._proxy.that.columns

        if val["new"] == "all":
            columns.attrs(selected_index=0)
        elif val["new"] == "aslist":
            columns.attrs(selected_index=1)
        else:
            assert val["new"] == "asdict"
            columns.attrs(selected_index=2)

    def _snippet_cb(self, p: Proxy, val: AnyType) -> None:
        assert self._proxy is not None
        self._proxy.that.start_btn.attrs(disabled = not val["new"])

    def _refresh_btn_cb(self, p: Proxy | None = None, btn: ipw.Button | None = None) -> None:
        assert self._proxy is not None
        from .custom import CUSTOMER_SNIPPET
        self._proxy.that.choice.attrs(options = [""] + list(CUSTOMER_SNIPPET.keys()))

    def get_params(self, mode: str) -> list[str] | dict[str, str]:
        assert self._proxy is not None
        if mode == "aslist":
            wg = self._proxy.that.aslist_mode.widget
            assert isinstance(wg, ipw.SelectMultiple)
            return wg.value
        elif mode == "asdict":
            gbox = self._proxy.that.asdict_mode.widget
            assert hasattr(gbox, "children")
            rows = list(batched([wg.value for wg in gbox.children], 2))
            return {key: col for (col, key) in rows[1:] if key}
        assert mode in ("all", "")
        return []

    @starter_callback(disable_ui=False)
    def _start_btn_cb(self, p: Proxy, btn: ipw.Button) -> None:
        assert self._proxy is not None
        from .custom import CUSTOMER_SNIPPET
        snippet = CUSTOMER_SNIPPET[self._proxy.that.choice.widget.value]
        mode = self._proxy.that.cols_mode.widget.value
        columns: list[str] | dict[str, str] = self.get_params(mode)
        self.record = self._proxy.dump()
        res = snippet(self.input_module, self.input_slot, columns)
        self.output_module = res.output_module
        self.output_slot = res.output_slot
        if res.widget is not None:
            custom_widget = self._proxy.that.custom_widget.widget
            assert isinstance(custom_widget, ipw.Box)
            custom_widget.children = [res.widget]
        disable_all(self, exceptions=(res.widget,))

    @runner
    def run(self) -> AnyType:
        assert self._proxy is not None
        mode = self._proxy.that.cols_mode.widget.value
        columns: list[str] | dict[str, str] = self.get_params(mode)
        from .custom import CUSTOMER_SNIPPET
        choice = self._proxy.that.choice.widget.value
        snippet = CUSTOMER_SNIPPET[choice]
        res = self.eval_snippet(snippet, columns)
        self.output_module = res.output_module
        self.output_slot = res.output_slot
        if res.widget is not None:
            custom_widget = self._proxy.that.custom_widget.widget
            assert isinstance(custom_widget, ipw.Box)
            custom_widget.children = [res.widget]

    @modules_producer
    def eval_snippet(self, snippet: Callable[..., AnyType], columns: list[str]) -> AnyType:
        return snippet(self.input_module, self.input_slot, columns)
