import ipywidgets as ipw
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from progressivis.table.dshape import dataframe_dshape, ExtensionDtype
from progressivis.core.api import Module, Sink
from progressivis.table.api import PTable, Constant
from progressivis.io.api import ParquetLoader
from .loaders import JsonEditorW, BtnBar
from .utils import (make_button, VBoxTyped, TypedBase, is_recording,
                    amend_last_record,
                    disable_all,
                    dot_progressivis,
                    runner,
                    expand_urls,
                    shuffle_urls,
                    relative_urls,
                    modules_producer
                    )
import os
import time
import json
from typing import Any, Any as AnyType, Dict, Union


# https://www.mungingdata.com/pyarrow/parquet-metadata-min-max-statistics/

def _ds(t: Union[np.dtype[Any], ExtensionDtype]) -> str:
    ds = dataframe_dshape(t)
    return "datetime64" if ds == "6*uint16" else ds

def _cleanup(raw: str) -> list[str]:
    return [line for line in  raw.strip().split("\n")[1:] if "<pyarrow" not in line]

ROWS = 20

class ColInfo(ipw.VBox):
    def __init__(
            self,
            pqfile: pq.ParquetFile,
            ix: int,
            dtype: Union[np.dtype[Any], ExtensionDtype],
            sniffer: "Sniffer",
            *args: Any,
            **kw: Any,
    ) -> None:
        super().__init__(*args, **kw)
        col_schema = _cleanup(str(pqfile.schema.column(ix)))
        col_meta = _cleanup(str(pqfile.metadata.row_group(0).column(ix)))  # type: ignore
        col_text = "\n".join(col_schema + col_meta)
        self.info = ipw.Textarea(col_text, rows=ROWS)

        self.use = ipw.Checkbox(description="Use", value=True)
        self.use.observe(self._use_cb, names="value")
        self.dtype = dtype
        self.sniffer = sniffer
        self.children = [self.info, self.use]

    def _use_cb(self, change: Dict[str, AnyType]) -> None:
        self.sniffer.refresh_start()
        self.sniffer.kept_cols.options = [pair for pair in
                                          self.sniffer.columns.options
                                          if self.sniffer.info_cols[pair[1]].use.value]

class Sniffer(ipw.HBox):
    def __init__(self, url: str, start_btn: ipw.Button, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.pqfile = pq.ParquetFile(url)
        self.start_btn = start_btn  # TODO: use a weakref here
        self.schema = self.pqfile.schema.to_arrow_schema()
        names = self.schema.names
        self.names = names
        types = [t.to_pandas_dtype() for t in self.schema.types]
        decorated = [(f"{n}:{np.dtype(t).name}", n) for (n, t) in zip(names, types)]
        self.info_cols: Dict[str, ColInfo] = {
            n: ColInfo(self.pqfile, i, np.dtype(types[i]), self)
            for (i, n) in enumerate(names)
        }
        # PColumn selection
        self.columns = ipw.Select(disabled=False, rows=ROWS, options=decorated)
        self.columns.observe(self._columns_cb, names="value")
        # PColumn details
        self.select_all_ck = ipw.Checkbox(description="Select/Unselect all", value=True)
        self.select_all_ck.observe(self._select_all_cb, names="value")
        self.column: Dict[str, ColInfo] = {}
        self.no_detail = ipw.Label(value="All column Selected")
        self.details = ipw.Box([self.no_detail], label="Details")
        layout = ipw.Layout(border="solid")
        self.kept_cols = ipw.Select(
            options=decorated,
            rows=ROWS,
            description="",
            disabled=False,
        )
        self.kept_cols.observe(self._kept_cols_cb, names="value")
        # Toplevel Box
        self.children = (
            ipw.VBox([ipw.Label("Columns"), self.columns, self.select_all_ck], layout=layout),
            ipw.VBox([ipw.Label("Info"), self.details], layout=layout),
            ipw.VBox([ipw.Label("Selected"), self.kept_cols], layout=layout),
        )

    def refresh_start(self) -> None:
        for col in self.info_cols.values():
            if col.use.value:
                self.start_btn.disabled = False
                return
        self.start_btn.disabled = True
        self.no_detail.value = "No colums are selected"
        self.details.children = [self.no_detail]
        self.kept_cols.options = []

    def get_dtypes(self) -> Dict[str, str]:
        return {
            k: _ds(col.dtype) for (k, col) in self.info_cols.items() if col.use.value
        }

    def _select_all_cb(self, change: Dict[str, AnyType]) -> None:
        self.no_detail.value = "All column Selected" if change["new"] else "No colums are selected"
        for col in self.info_cols.values():
            col.use.value = change["new"]
        self.start_btn.disabled = not change["new"]
        if change["new"]:
            self.kept_cols.options = self.columns.options
        else:
            self.kept_cols.options = []

    def _kept_cols_cb(self, change: Dict[str, AnyType]) -> None:
        self.columns.value = change["new"]

    def _columns_cb(self, change: Dict[str, AnyType]) -> None:
        column = change["new"]
        self.show_column(column)

    def show_column(self, column: str) -> None:
        if column not in self.names:
            self.details.children = [self.no_detail]
            return
        col = self.info_cols[column]
        self.details.children = [col]


class ParquetLoaderW(VBoxTyped):
    class Typed(TypedBase):
        reuse_ck: ipw.Checkbox
        bookmarks: ipw.SelectMultiple
        urls_wg: ipw.Textarea
        to_sniff: ipw.Text
        n_lines: ipw.IntText
        shuffle_ck: ipw.Checkbox
        throttle: ipw.IntText
        sniffer: Sniffer | JsonEditorW
        start_save: BtnBar


    def __init__(self) -> None:
        super().__init__()
        self._urls: list[str] = []
        self.child.start_save = BtnBar()

    def initialize(self) -> None:
        self.child.start_save = BtnBar()
        if self.widget_dir and os.listdir(self.widget_dir):
            self.c_.reuse_ck = ipw.Checkbox(description="Reuse previous settings ...")
            self.c_.reuse_ck.observe(self._reuse_cb, names="value")
        else:
            self.c_.reuse_ck = None
        bmk_disabled = True
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
                    bmk_disabled = False
                except Exception:
                    bookmarks = [f"cannot read '{pv_dir}/bookmarks'"]
            else:
                bookmarks = [f"no '{pv_dir}/bookmarks' file found"]
        self.c_.bookmarks = ipw.SelectMultiple(
            options=bookmarks,
            value=[],
            rows=5,
            description="Bookmarks",
            disabled=bmk_disabled,
            layout=ipw.Layout(width="60%"),
        )
        self.c_.urls_wg = ipw.Textarea(
            value="",
            placeholder="",
            description="New URLs:",
            disabled=False,
            layout=ipw.Layout(width="60%"),
        )
        self.c_.to_sniff = ipw.Text(
            value="",
            placeholder="",
            description="URL to sniff(optional):",
            disabled=False,
            layout=ipw.Layout(width="60%"),
        )
        self.c_.shuffle_ck = ipw.Checkbox(
            description="Shuffle URLs", value=True, disabled=False
        )
        self.c_.throttle = ipw.IntText(value=0, description="Throttle:", disabled=False)
        self.c_.start_save.c_.sniff_btn = make_button("Sniff ...", cb=self._sniffer_cb)

    def _reuse_cb(self, change: Dict[str, Any]) -> None:
        if change["new"]:
            self.c_.to_sniff = None
            self.c_.sniffer = None
            self.c_.urls_wg = None
            self.c_.bookmarks = ipw.Select(
                options=[""] + os.listdir(self.widget_dir),
                value="",
                rows=5,
                description="Settings",
                disabled=False,
                layout=ipw.Layout(width="60%"),
            )
            self.c_.bookmarks.observe(self._enable_reuse_cb, names="value")
            self.c_.throttle = None
            self.c_.start_save.c_.sniff_btn = make_button(
                "Edit settings", cb=self._edit_settings_cb, disabled=True
            )
            self.c_.start_save.c_.start = make_button(
                "Start loading csv ...",
                cb=self._start_loader_reuse_cb,
                disabled=True,
            )
        else:
            self.initialize()

    def _enable_reuse_cb(self, change: Dict[str, Any]) -> None:
        self.c_.start_save.c_.sniff_btn.disabled = not change["new"]
        self.c_.start_save.c_.start.disabled = not change["new"]

    def _edit_settings_cb(self, btn: ipw.Button) -> None:
        self.c_.sniffer = JsonEditorW(self)
        self.c_.sniffer.initialize()
        self.c_.start_save.c_.start = make_button(
            "Start loading csv ...",
            cb=self._start_loader_reuse_cb)
        self.c_.start_save.c_.save = make_button(
            "Save settings ...",
            cb=self._save_settings_cb,
            disabled=False,
        )
        self.c_.start_save.c_.text = ipw.Text(
            value=self.c_.bookmarks.value,
            placeholder="",
            description="File:",
            disabled=False,
            layout=ipw.Layout(width="100%"),
        )

    def _sniffer_cb(self, btn: ipw.Button) -> None:
        if btn.description.startswith("Sniff"):
            urls = list(self.c_.bookmarks.value) + self.c_.urls_wg.value.strip().split(
                "\n"
            )
            urls = [elt for elt in urls if elt]
            assert urls
            self._urls = urls
            to_sniff = self.c_.to_sniff.value.strip()
            if not to_sniff:
                to_sniff = urls[0]
            to_sniff = expand_urls([to_sniff])[0]
            pv_dir = dot_progressivis()
            placeholder = (
                (
                    "'.progressivis' dir not found in your home dir. Create it"
                    " in order to enable settings saves"
                )
                if not pv_dir
                else ""
            )
            disabled = not pv_dir
            self.c_.start_save.c_.start = make_button(
                "Start loading ...",
                cb=self._start_loader_cb
            )
            self.child.sniffer = Sniffer(to_sniff, self.c_.start_save.c_.start)
            self.c_.start_save.c_.save = make_button(
                "Save settings ...",
                cb=self._save_settings_cb,
                disabled=disabled,
            )
            self.c_.start_save.c_.text = ipw.Text(
                value=time.strftime("w%Y%m%d_%H%M%S"),
                placeholder=placeholder,
                description="File:",
                disabled=disabled,
                layout=ipw.Layout(width="100%"),
            )
            self.c_.urls_wg.disabled = True
            btn.description = "Hide sniffer"
        elif btn.description.startswith("Hide"):
            self.c_.sniffer = None
            btn.description = "Show sniffer"
        else:
            assert btn.description.startswith("Show")
            btn.description = "Hide sniffer"

    def _start_loader_reuse_cb(self, btn: ipw.Button) -> None:
        if isinstance(self.c_.sniffer, JsonEditorW):
            content = self.c_.sniffer.c_.editor.data
        else:
            file_ = "/".join([self.widget_dir, self.c_.bookmarks.value])
            with open(file_) as f:
                content = json.load(f)
        urls = content["urls"]
        throttle = content["throttle"]
        shuffle = content.get("shuffle", False)
        dtypes = content["dtypes"]
        kw = dict(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            dtypes=dtypes
        )
        pq_module = self.init_modules(**kw)
        if is_recording():
            amend_last_record({"frozen": kw})
        self.output_module = pq_module
        self.output_slot = "result"
        self.output_dtypes = dtypes
        self.make_chaining_box()
        btn.disabled = True
        self.dag_running()
        disable_all(self)
        self.manage_replay()

    def _start_loader_cb(self, btn: ipw.Button) -> None:
        urls = relative_urls(self._urls)
        throttle = self.c_.throttle.value
        shuffle = self.c_.shuffle_ck.value
        dtypes = self.child.sniffer.get_dtypes()
        kw = dict(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            dtypes= dtypes
        )
        if is_recording():
            amend_last_record({"frozen": kw})
        pq_module = self.init_modules(**kw)
        self.output_module = pq_module
        self.output_slot = "result"
        self.output_dtypes = dtypes
        self.make_chaining_box()
        btn.disabled = True
        self.dag_running()
        disable_all(self)

    def _save_settings_cb(self, btn: ipw.Button) -> None:
        pv_dir = dot_progressivis()
        assert pv_dir
        base_name = self.c_.start_save.c_.text.value
        file_name = f"{self.widget_dir}/{base_name}"
        dtypes = self.child.sniffer.get_dtypes()
        res = dict(
            urls=self.c_.urls_wg.value,
            throttle=self.c_.throttle.value,
            dtypes=dtypes
        )
        with open(file_name, "w") as f:
            json.dump(res, f, indent=4)

    @runner
    def run(self) -> Any:
        content = self.frozen_kw
        urls = content["urls"]
        throttle = content["throttle"]
        shuffle = content.get("shuffle", False)
        dtypes = content["dtypes"]
        pq_module = self.init_modules(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            dtypes=dtypes
        )
        self.output_module = pq_module
        self.output_slot = "result"
        self.output_dtypes = dtypes

    @modules_producer
    def init_modules(self, urls: list[str] | None,
                     throttle: int, dtypes: dict[str, str], shuffle: bool = False, **kw: Any) -> ParquetLoader:
        if urls is None:
            urls = expand_urls(self._urls)
            throttle = self.c_.throttle.value
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
