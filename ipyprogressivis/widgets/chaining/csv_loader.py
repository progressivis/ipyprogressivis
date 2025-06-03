import ipywidgets as ipw
import pandas as pd
from .loaders import JsonEditorW, BtnBar
from ..csv_sniffer import CSVSniffer
from progressivis.io.api import SimpleCSVLoader
from progressivis.core.api import Module, Sink
from progressivis.table.api import PTable, Constant
from .utils import (
    make_button,
    get_schema,
    VBoxTyped,
    TypedBase,
    amend_last_record,
    is_recording,
    disable_all,
    runner,
    dot_progressivis,
    expand_urls,
    shuffle_urls,
    relative_urls,
    modules_producer
)
import os
import time
import json as js
import operator as op

from typing import List, Optional, Any, Dict, Callable, cast

HOME = os.getenv("HOME")
assert HOME is not None


def clean_nodefault(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for (k, v) in d.items() if type(v).__name__ != "_NoDefault"}


def make_filter(
    filter_dict: Dict[str, List[Any]]
) -> Callable[[pd.DataFrame], pd.DataFrame]:
    operators = dict(
        [
            (">", op.gt),
            ("<", op.lt),
            (">=", op.ge),
            ("<=", op.le),
            ("==", op.eq),
            ("!=", op.ne),
        ]
    )

    def filter_(df: pd.DataFrame) -> pd.DataFrame:
        res = None
        for col, pairs in filter_dict.items():
            for symb, cnst in pairs:
                res_ = operators[symb](df[col], cnst)
                res = res_ if res is None else op.and_(res, res_)
        assert res is not None
        return cast(pd.DataFrame, df[res])

    return filter_


def test_filter(df: pd.DataFrame) -> pd.DataFrame:
    pklon = df["pickup_longitude"]
    pklat = df["pickup_latitude"]
    return df[(pklon > -74.08) & (pklon < -73.5)
              & (pklat > 40.55) & (pklat < 41.00)]


class CsvLoaderW(VBoxTyped):
    class Typed(TypedBase):
        reuse_ck: ipw.Checkbox
        bookmarks: ipw.SelectMultiple
        urls_wg: ipw.Textarea
        to_sniff: ipw.Text
        n_lines: ipw.IntText
        shuffle_ck: ipw.Checkbox
        throttle: ipw.IntText
        sniffer: CSVSniffer | JsonEditorW | None
        start_save: BtnBar

    def __init__(self) -> None:
        super().__init__()
        self._sniffer: Optional[CSVSniffer] = None
        self._urls: List[str] = []
        self.child.start_save = BtnBar()

    def initialize(
        self, urls: List[str] = [], to_sniff: str = "", lines: int = 100
    ) -> None:
        if self.widget_dir and os.listdir(self.widget_dir):
            self.c_.reuse_ck = ipw.Checkbox(
                description="Reuse previous settings ..."
            )
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
            value=to_sniff,
            placeholder="",
            description="URL to sniff(optional):",
            disabled=False,
            layout=ipw.Layout(width="60%"),
        )
        self.c_.sniffer = None
        self.c_.n_lines = ipw.IntText(
            value=lines, description="Max rows to sniff:", disabled=False
        )
        self.c_.shuffle_ck = ipw.Checkbox(
            description="Shuffle URLs", value=True, disabled=False
        )
        self.c_.throttle = ipw.IntText(
            value=0,
            description="Throttle:",
            disabled=False
        )
        self.c_.start_save.c_.sniff_btn = make_button(
            "Sniff ...",
            cb=self._sniffer_cb
        )

    def _reuse_cb(self, change: Dict[str, Any]) -> None:
        if change["new"]:
            self.c_.to_sniff = None
            self.c_.n_lines = None
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

    def _enable_reuse_cb(self, change: Dict[str, Any]) -> None:
        self.c_.start_save.c_.sniff_btn.disabled = not change["new"]
        self.c_.start_save.c_.start.disabled = not change["new"]

    def _start_loader_reuse_cb(self, btn: ipw.Button) -> None:
        if isinstance(self.c_.sniffer, JsonEditorW):
            # content = self.c_.sniffer.json_editor.value
            content = self.c_.sniffer.c_.editor.data
        else:
            file_ = "/".join([self.widget_dir, self.c_.bookmarks.value])
            with open(file_) as f:
                content = js.load(f)
        urls = content["urls"]
        throttle = content["throttle"]
        shuffle = content.get("shuffle", False)
        sniffed_params = content["sniffed_params"]
        schema = content["schema"]
        filter_ = content["filter_"]
        kw = dict(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            sniffed_params=sniffed_params,
            filter_=filter_,
        )
        csv_module = self.init_modules(**kw)
        kw["schema"] = schema
        if is_recording():
            amend_last_record({"frozen": kw})
        self.output_module = csv_module
        self.output_slot = "result"
        self.output_dtypes = schema
        self.make_chaining_box()
        btn.disabled = True
        self.dag_running()
        disable_all(self)
        self.manage_replay()

    def _sniffer_cb(self, btn: ipw.Button) -> None:
        if btn.description.startswith("Sniff"):
            urls = (list(self.c_.bookmarks.value)
                    + self.c_.urls_wg.value.strip().split("\n"))
            urls = [elt for elt in urls if elt]
            assert urls
            self._urls = urls
            to_sniff = self.c_.to_sniff.value.strip()
            if not to_sniff:
                to_sniff = urls[0]
            to_sniff = expand_urls([to_sniff])[0]
            n_lines = self.c_.n_lines.value
            self._sniffer = CSVSniffer(path=to_sniff, lines=n_lines)
            self.c_.sniffer = self._sniffer.box
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
                "Start loading csv ...",
                cb=self._start_loader_cb
            )
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
            self.c_.to_sniff.disabled = True
            self.c_.n_lines.disabled = True
            btn.description = "Hide sniffer"
        elif btn.description.startswith("Hide"):
            self.c_.sniffer = None
            btn.description = "Show sniffer"
        else:
            assert btn.description.startswith("Show")
            assert self._sniffer
            self.c_.sniffer = self._sniffer.box
            btn.description = "Hide sniffer"

    def _start_loader_cb(self, btn: ipw.Button) -> None:
        urls = relative_urls(self._urls)
        assert self._sniffer is not None
        pv_params = self._sniffer.progressivis
        filter_ = pv_params.get("filter_values", {})
        throttle = self.c_.throttle.value
        shuffle = self.c_.shuffle_ck.value
        sniffed_params = clean_nodefault(self._sniffer.params)
        kw = dict(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            sniffed_params=sniffed_params,
            schema=get_schema(self._sniffer),
            filter_=filter_,
        )
        if is_recording():
            amend_last_record({"frozen": kw})
        csv_module = self.init_modules(**kw)
        self.output_module = csv_module
        self.output_slot = "result"
        assert self._sniffer is not None
        self.output_dtypes = get_schema(self._sniffer)
        self.make_chaining_box()
        btn.disabled = True
        self.dag_running()
        disable_all(self)

    def _save_settings_cb(self, btn: ipw.Button) -> None:
        pv_dir = dot_progressivis()
        assert pv_dir
        base_name = self.c_.start_save.c_.text.value
        file_name = f"{self.widget_dir}/{base_name}"
        if self._sniffer is not None:  # after sniffing
            pv_params = self._sniffer.progressivis
            schema = get_schema(self._sniffer)
            filter_ = pv_params.get("filter_values", {})
            res = dict(
                urls=relative_urls(self._urls),
                throttle=self.c_.throttle.value,
                sniffed_params=clean_nodefault(self._sniffer.params),
                schema=schema,
                filter_=filter_,
            )
        else:
            assert isinstance(self.c_.sniffer, JsonEditorW)
            res = self.c_.sniffer.c_.editor.data
        with open(file_name, "w") as f:
            js.dump(res, f, indent=4)

    @runner
    def run(self) -> Any:
        content = self.frozen_kw
        urls = content["urls"]
        throttle = content["throttle"]
        shuffle = content.get("shuffle", False)
        sniffed_params = content["sniffed_params"]
        schema = content["schema"]
        filter_ = content["filter_"]
        csv_module = self.init_modules(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            sniffed_params=sniffed_params,
            filter_=filter_,
        )
        self.output_module = csv_module
        self.output_slot = "result"
        self.output_dtypes = schema

    @modules_producer
    def init_modules(
        self,
        urls: List[str] | None = None,
        throttle: int | None = None,
        shuffle: bool = False,
        sniffed_params: Dict[str, Any] | None = None,
        filter_: Dict[str, Any] | None = None,
        **kw: Any
    ) -> SimpleCSVLoader:
        if urls is None:
            assert self._sniffer is not None
            urls = expand_urls(self._urls)
            urls = shuffle_urls(urls) if shuffle else urls
            params = self._sniffer.params.copy()
            pv_params = self._sniffer.progressivis
            if "filter_values" in pv_params:
                filter_fnc = make_filter(pv_params["filter_values"])
                params["filter_"] = filter_fnc
            params["throttle"] = self.c_.throttle.value
        else:
            urls = expand_urls(urls)
            if filter_:
                filter_ = dict(filter_=make_filter(filter_))
            else:
                filter_ = {}
            assert sniffed_params is not None
            params = dict(throttle=throttle, **sniffed_params, **filter_)
        if shuffle:
            urls = shuffle_urls(urls)
        imodule = self.input_module
        assert isinstance(imodule, Module)
        s = imodule.scheduler()
        with s:
            filenames = pd.DataFrame({"filename": urls})
            cst = Constant(PTable("filenames", data=filenames), scheduler=s)
            csv = SimpleCSVLoader(scheduler=s, **params)
            csv.input.filenames = cst.output[0]
            sink = Sink(scheduler=s)
            sink.input.inp = csv.output.result
            return csv

