import ipywidgets as ipw
import numpy as np
import pandas as pd

from progressivis.table.dshape import dataframe_dshape, ExtensionDtype
from progressivis.core.api import Module, Sink
from progressivis.table.api import PTable, Constant
from progressivis.io.api import ParquetLoader
from .utils import (
    VBox,
    is_recording,
    starter_callback,
    amend_last_record,
    dot_progressivis,
    runner,
    expand_urls,
    shuffle_urls,
    relative_urls,
    modules_producer,
)

from .parquet_sniffer import (
    sniffer,
    _sniffer,
    ParquetSniffer,

    get_dtypes
)

from ipyprogressivis.ipywel import (
    Proxy,
    # Backend,
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


class ParquetLoaderW(VBox):

    def btn_bar(self) -> Proxy:
        return hbox(
            button("Sniff ...", disabled=True)
            .uid("sniff_btn")
            .on_click(self._sniffer_cb),
            stack(
                button("Start loading csv ...")
                .on_click(self._start_loader_cb)
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
        self._urls: list[str] = []
        self._to_sniff: str = ""

    def _activate_reuse_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        proxy.lookup("reuse_btn").attrs(disabled=not proxy.widget.value)

    def initialize(self) -> None:
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
        proxy.lookup("save_btn").attrs(disabled=not change["new"])

    def _reuse_cb(self, proxy: Proxy, b: ipw.Button) -> None:
        base_name = proxy.lookup("reuse_file").widget.value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name) as f:
            content = json.load(f)
        backends = restore_backends(content, globals())
        stuff = _sniffer(backends["_"])
        self._proxy = restored = restore(content, globals(), obj=self, lambdas=stuff._lambda)
        assert hasattr(restored.widget, "children")
        self.children = restored.widget.children
        self._to_sniff_cb(restored, dict())  # sets self._urls self._to_sniff

    def _reuse_ck_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        proxy.lookup("global_stack").attrs(selected_index=not change["new"])

    def _to_sniff_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        to_sniff = proxy.lookup("to_sniff")
        bookmarks = proxy.lookup("bookmarks")
        urls_wg = proxy.lookup("urls_wg")
        urls = list(bookmarks.widget.value) + urls_wg.widget.value.strip().split("\n")
        urls = [elt for elt in urls if elt]
        if not urls:
            return
        self._urls = urls
        to_sniff_url = ""
        to_sniff_lst = expand_urls([to_sniff.widget.value.strip()])
        if to_sniff_lst:
            to_sniff_url = to_sniff_lst[0]
        if not to_sniff_url:
            to_sniff_url = urls[0]

        if not to_sniff_url:
            return
        self._to_sniff = to_sniff_url
        sniff_btn = proxy.lookup("sniff_btn")
        sniff_btn.attrs(disabled=not self._to_sniff)

    def _sniffer_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        # to_sniff = proxy.lookup("to_sniff")
        # bookmarks = proxy.lookup("bookmarks")
        # urls_wg = proxy.lookup("urls_wg")
        for uid in ("start_stack", "save_stack", "save_file_stack"):
            proxy.lookup(uid).attrs(selected_index=0)
        snf_proxy = sniffer(self._to_sniff)
        sniff_stack = proxy.lookup("sniffer")
        if not sniff_stack._children:
            merge_trees(self._proxy, sniff_stack, snf_proxy)
        sniff_stack.attrs(selected_index=0)
        proxy.lookup("sniff_btn").attrs(disabled=True)

    @starter_callback
    def _start_loader_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        urls = relative_urls(self._urls)
        throttle = proxy.lookup("throttle").widget.value
        shuffle = proxy.lookup("shuffle_ck").widget.value
        dtypes = get_dtypes(proxy)
        kw = dict(urls=urls, throttle=throttle, shuffle=shuffle, dtypes=dtypes)
        if is_recording():
            amend_last_record({"frozen": kw})
        pq_module = self.init_modules(**kw)
        self.output_module = pq_module
        self.output_slot = "result"
        self.output_dtypes = dtypes

    def _save_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        pv_dir = dot_progressivis()
        assert pv_dir
        base_name = proxy.lookup("save_file_name").widget.value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name, "w") as f:
            json.dump(self._proxy.dump(), f, indent=4)

    @runner
    def run(self) -> Any:
        content = self.frozen_kw
        urls = content["urls"]
        throttle = content["throttle"]
        shuffle = content.get("shuffle", False)
        dtypes = content["dtypes"]
        pq_module = self.init_modules(
            urls=urls, throttle=throttle, shuffle=shuffle, dtypes=dtypes
        )
        self.output_module = pq_module
        self.output_slot = "result"
        self.output_dtypes = dtypes

    @modules_producer
    def init_modules(
        self,
        urls: list[str] | None,
        throttle: int,
        dtypes: dict[str, str],
        shuffle: bool = False,
        **kw: Any,
    ) -> ParquetLoader:
        if urls is None:
            urls = expand_urls(self._urls)
            throttle = 0 #self.c_.throttle.value
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
