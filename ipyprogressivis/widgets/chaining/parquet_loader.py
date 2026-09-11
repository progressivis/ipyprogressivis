import ipywidgets as ipw
import numpy as np
import pandas as pd

from progressivis.table.dshape import dataframe_dshape, ExtensionDtype
from progressivis.core.api import Module, Sink
from progressivis.table.api import PTable, Constant
from progressivis.io.api import ParquetLoader
from .utils import (
    VBox,
    dot_progressivis,
    expand_urls,
    shuffle_urls,
    relative_urls,
    chaining_widget,
    customized_restore,
    METH_BACKWARD_COMPAT
)

from .parquet_sniffer import (
    sniffer,
    _sniffer,
    ParquetSniffer,

    get_dtypes
)

from ipyprogressivis.ipywel import (
    Proxy,
    button,
    anybox,
    vbox,
    hbox,
    stack,
    text,
    int_text,
    textarea,
    checkbox,
    select,
    select_multiple,
    label,
    restore_backends,
    restore,
    merge_trees
)
import os
import json
from typing import Any

_ = ParquetSniffer  # keeps ruff happy

def _ds(t: np.dtype[Any] | ExtensionDtype) -> str:
    ds = dataframe_dshape(t)
    return "datetime64" if ds == "6*uint16" else ds


def _cleanup(raw: str) -> list[str]:
    return [line for line in raw.strip().split("\n")[1:] if "<pyarrow" not in line]


ROWS = 20

@chaining_widget(label="PARQUET loader")
class ParquetLoaderW(VBox):
    def btn_bar(self) -> Proxy:
        return hbox(
            button("Sniff ...", disabled=True)
            .uid("sniff_btn")
            .on_click(self._sniffer_cb),
            stack(
                button("Start loading csv ...")
                .on_click(self.starter_callback)
                .uid("start_btn"),
                label(""),
                selected_index=1,
            ).uid("start_stack"),
            stack(
                button("Save settings ...", disabled=True)
                .uid("save_btn")
                .on_click(self._save_cb),
                label(""),
                selected_index=1,
            ).uid("save_stack"),
            stack(
                text("File:").observe(self._save_file_cb).uid("save_file_name"),
                label(""),
                selected_index=1,
            ).uid("save_file_stack"),
        )

    def loader_ui(self, bookmarks: list[str]) -> Proxy:
        lfiles = []
        if self.widget_dir:
            lfiles = os.listdir(self.widget_dir)
        return anybox(
            self,
            checkbox("Reuse previous settings ...", disabled=not lfiles)
            .uid("reuse_ck")
            .observe(self._reuse_ck_cb),
            stack(
                vbox(
                    select(
                        "Settings:",
                        options=[""] + lfiles,
                        rows=5,
                    )
                    .uid("reuse_file")
                    .observe(self._activate_reuse_cb),
                    button("Reuse", disabled=True)
                    .uid("reuse_btn")
                    .on_click(self._reuse_cb),
                ),
                vbox(
                    select_multiple(
                        "Bookmarks:", options=bookmarks, rows=5
                    )
                    .layout(width="60%")
                    .observe(self._to_sniff_cb)
                    .uid("bookmarks"),
                    textarea("New URLs:").observe(self._to_sniff_cb).uid("urls_wg"),
                    text("URL to sniff(optional):")
                    .observe(self._to_sniff_cb)
                    .uid("to_sniff"),
                    checkbox("Shuffle URLs", value=True).uid("shuffle_ck"),
                    int_text("Throttle:", value=0).uid("throttle"),
                    stack().uid("sniffer"),  # merged later
                    self.btn_bar(),
                ),
                selected_index=1,
            ).uid("global_stack"),
        )

    def __init__(self) -> None:
        super().__init__()
        self._to_sniff: str = ""

    def _activate_reuse_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        assert self._proxy is not None
        self._proxy.that.reuse_btn.attrs(disabled=not proxy.widget.value)

    @customized_restore
    def initialize(self) -> None:
        if self.is_replaying:
            return self.init_ui()
        pv_dir = dot_progressivis()
        bookmarks = [
            f"no '{pv_dir}/' directory found",
            f"no '{pv_dir}/bookmarks' file found",
        ]
        if os.path.isdir(pv_dir):
            bookmarks_file = f"{pv_dir}/bookmarks"
            if os.path.exists(bookmarks_file):
                try:
                    bookmarks = open(bookmarks_file).read().split("\n")
                except Exception:
                    bookmarks = [f"cannot read '{pv_dir}/bookmarks'"]
            else:
                bookmarks = [f"no '{pv_dir}/bookmarks' file found"]
        self._proxy = self.loader_ui(bookmarks)

    def _save_file_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        assert self._proxy is not None
        self._proxy.that.save_btn.attrs(disabled=not change["new"])

    def restore_ui(self, content: dict[str, Any]) -> None:
        backends = restore_backends(content, globals())
        stuff = _sniffer(backends["sniffer"])
        self._proxy = restore(
            content,
            globals(),
            obj=self,
            lambdas=stuff._lambda,
            meth_backward_compat=METH_BACKWARD_COMPAT
        )

    def init_ui(self) -> None:
        content = self.record
        self.restore_ui(content)
        assert self._proxy is not None
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children

    def _reuse_cb(self, proxy: Proxy, b: ipw.Button) -> None:
        assert self._proxy is not None
        base_name = self._proxy.that.reuse_file.widget.value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name) as f:
            content = json.load(f)
        self.restore_ui(content)
        assert self._proxy is not None
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
        self._to_sniff_cb(self._proxy, dict())  # sets self._urls self._to_sniff

    def _reuse_ck_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        assert self._proxy is not None
        self._proxy.that.global_stack.attrs(selected_index=not change["new"])

    def get_urls(self) -> list[str]:
        assert self._proxy is not None
        bookmarks = self._proxy.that.bookmarks
        urls_wg = self._proxy.that.urls_wg
        urls = list(bookmarks.widget.value) + urls_wg.widget.value.strip().split("\n")
        return [elt for elt in urls if elt]

    def _to_sniff_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        assert self._proxy is not None
        to_sniff = self._proxy.that.to_sniff
        if not (urls := self.get_urls()):
            return
        to_sniff_url = ""
        to_sniff_lst = expand_urls([to_sniff.widget.value.strip()])
        if to_sniff_lst:
            to_sniff_url = to_sniff_lst[0]
        if not to_sniff_url:
            to_sniff_url = urls[0]

        if not to_sniff_url:
            return
        self._to_sniff = to_sniff_url
        sniff_btn = self._proxy.that.sniff_btn
        sniff_btn.attrs(disabled=not self._to_sniff)

    def _sniffer_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self._proxy is not None
        self_proxy = self._proxy
        for uid in ("start_stack", "save_stack", "save_file_stack"):
            self_proxy.lookup(uid).attrs(selected_index=0)
        snf_proxy = sniffer(self._to_sniff)
        sniff_stack = self_proxy.that.sniffer
        if not sniff_stack._children:
            merge_trees(self._proxy, sniff_stack, snf_proxy)
        sniff_stack.attrs(selected_index=0)
        self_proxy.that.sniff_btn.attrs(disabled=True)

    def get_content(self) -> dict[str, Any]:
        assert self._proxy is not None
        self_proxy = self._proxy
        urls = relative_urls(self.get_urls())
        throttle = self_proxy.that.throttle.widget.value
        shuffle = self_proxy.that.shuffle_ck.widget.value
        dtypes = get_dtypes(self_proxy)
        return dict(urls=urls, throttle=throttle, shuffle=shuffle, dtypes=dtypes)

    def starter_callback(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self._proxy is not None
        self_proxy = self._proxy
        sniffer = self_proxy._backends["sniffer"]()
        assert sniffer is not None
        self.record = self._proxy.dump()
        kw = self.get_content()
        pq_module = self.init_modules(**kw)
        self.output_module = pq_module
        self.output_slot = "result"
        self.output_dtypes = kw["dtypes"]

    def _save_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        assert self._proxy is not None
        pv_dir = dot_progressivis()
        assert pv_dir
        base_name = self._proxy.that.save_file_name.widget.value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name, "w") as f:
            json.dump(self._proxy.dump(), f, indent=4)

    def run(self) -> Any:
        kw = self.get_content()
        pq_module = self.init_modules(**kw)
        self.output_module = pq_module
        self.output_slot = "result"
        self.output_dtypes = kw["dtypes"]

    def init_modules(
        self,
        urls: list[str] | None,
        throttle: int,
        dtypes: dict[str, str],
        shuffle: bool = False,
        **kw: Any,
    ) -> ParquetLoader:
        if urls is None:
            urls = expand_urls(self.get_urls())
            throttle = 0
        else:
            urls = expand_urls(urls)
        if shuffle:
            urls = shuffle_urls(urls)
        imodule = self.input_module
        assert isinstance(imodule, Module)
        s = imodule.scheduler
        with s:
            filenames = pd.DataFrame({"filename": urls})
            cst = Constant(PTable("filenames", data=filenames), scheduler=s)
            cols = list(dtypes.keys())
            pql = ParquetLoader(columns=cols, throttle=throttle, scheduler=s)
            pql.input.filenames = cst.output[0]
            sink = Sink(scheduler=s)
            sink.input.inp = pql.output.result
        return pql
