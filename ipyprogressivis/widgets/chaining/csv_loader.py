import ipywidgets as ipw
import pandas as pd
from progressivis.io.api import SimpleCSVLoader
from progressivis.core.api import Module, Sink
from progressivis.table.api import PTable, Constant
from .custom import register_function
from .utils import (
    starter_callback,
    get_schema,
    VBox,
    runner,
    dot_progressivis,
    expand_urls,
    shuffle_urls,
    modules_producer,
    labcommand,
)
from ipyprogressivis.csv_sniffer.sniffer import sniffer, _sniffer
from ipyprogressivis.csv_sniffer.backend import CSVSniffer
import os
import json
import operator as op
from ipyprogressivis.ipywel import (
    Proxy,
    anybox,
    vbox,
    hbox,
    stack,
    button,
    text,
    int_text,
    textarea,
    checkbox,
    select,
    dropdown,
    select_multiple,
    label,
    file_upload,
    restore_backends,
    restore,
    merge_trees,
)
from typing import Any, Callable, cast

HOME = os.getenv("HOME")
assert HOME is not None
_0 = CSVSniffer  # keeps ruff happy

def clean_nodefault(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for (k, v) in d.items() if type(v).__name__ != "_NoDefault"}


def make_filter(
    filter_dict: dict[str, list[Any]],
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
    return df[(pklon > -74.08) & (pklon < -73.5) & (pklat > 40.55) & (pklat < 41.00)]


def combine_filters(
    fnc1: Callable[[pd.DataFrame], pd.DataFrame] | None,
    fnc2: Callable[[pd.DataFrame], pd.DataFrame] | None,
) -> Callable[[pd.DataFrame], pd.DataFrame] | None:
    if fnc1 and fnc2:
        return lambda x: fnc2(fnc1(x))
    if fnc1:
        return fnc1
    if fnc2:
        return fnc2
    return None


layout_refresh = ipw.Layout(width="30px", height="30px")
_ = register_function


class CsvLoaderW(VBox):
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
                .on_click(self._save_settings_cb),
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
        from .custom import CUSTOMER_FNC

        return anybox(
            self,
            checkbox("Reuse previous settings ...")
            .uid("reuse_ck")
            .observe(self._reuse_ck_cb),
            stack(
                vbox(
                    select(
                        "Settings",
                        options=[""] + os.listdir(self.widget_dir),
                        value="",
                        rows=5,
                    )
                    # .layout(width="60%")
                    .observe(self._activate_reuse_cb).uid("reuse_file"),
                    button("Reuse", disabled=True)
                    .uid("reuse_btn")
                    .on_click(self._reuse_cb),
                ),
                vbox(
                    select_multiple("Bookmarks:", options=bookmarks, rows=5)
                    .layout(width="60%")
                    .uid("bookmarks")
                    .observe(self._to_sniff_cb),
                    textarea("New URLs:").layout(width="60%").uid("urls_wg"),
                    text("URL to sniff(optional):")
                    .uid("to_sniff")
                    .observe(self._to_sniff_cb)
                    .layout(width="60%"),
                    int_text("Max rows to sniff:", value=100).uid("n_lines"),
                    checkbox("Shuffle URLs", value=True).uid("shuffle_ck"),
                    int_text("Throttle:", value=0).uid("throttle"),
                    stack().uid("sniffer"),  # merged later
                    int_text("Stop after:", value=0).uid("n_rows"),
                    hbox(  # upload bar
                        label("Upload filters:"),
                        file_upload(
                            accept=".py",
                            multiple=False,  # True to accept multiple files upload else False
                        ).observe(self._upload_cb),
                    ),
                    hbox(  # snippet bar
                        dropdown(
                            "Preprocessor:",
                            options=[""] + list(CUSTOMER_FNC.keys()),
                            value="",
                        ).uid("preprocessor"),
                        button(icon="refresh")
                        .layout(width="30px", height="30px")
                        .on_click(self._refresh_btn_cb),
                    ),
                    self.btn_bar(),
                ),
                selected_index=1,
            ).uid("global_stack"),
        )

    def initialize(
        self, urls: list[str] = [], to_sniff: str = "", lines: int = 100
    ) -> None:
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

    def _refresh_btn_cb(self, proxy: Proxy, btn: ipw.Button | None = None) -> None:
        from .custom import CUSTOMER_FNC

        proxy.that.preprocessor.attrs(options=[""] + list(CUSTOMER_FNC.keys()))

    def _upload_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        from .custom import CUSTOMER_FNC

        _ = CUSTOMER_FNC
        for item in change["new"]:
            code = item.content.tobytes().decode()
            exec(code, globals(), globals())
            labcommand(
                "progressivis:create_code_cell",
                code=code,
                index=2,  # i.e. insert it after #root & co
                run=False,
            )
        self._refresh_btn_cb(proxy)

    def _reuse_ck_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        proxy.lookup("global_stack").attrs(selected_index=not change["new"])

    def _activate_reuse_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        proxy.lookup("reuse_btn").attrs(disabled=not proxy.widget.value)

    def restore_ui(self, content: dict[str, Any]) -> None:
        backends = restore_backends(content, globals())
        # self.backends = backends
        stuff = _sniffer(backends["sniffer"])
        self._proxy = restore(content, globals(), obj=self, lambdas=stuff._lambda)

    def init_ui(self) -> None:
        content = self.record
        self.restore_ui(content)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children

    def _reuse_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        base_name = proxy.lookup("reuse_file").widget.value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name) as f:
            content = json.load(f)
        self.restore_ui(content)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
        self._to_sniff_cb(self._proxy, dict())  # sets self._urls self._to_sniff

    def get_all_urls(self, proxy: Proxy | None = None) -> list[str]:
        proxy = proxy or self._proxy
        bookmarks = proxy.that.bookmarks
        urls_wg = proxy.that.urls_wg
        urls = list(bookmarks.widget.value) + urls_wg.widget.value.strip().split("\n")
        urls = [elt for elt in urls if elt]
        return urls

    def _to_sniff_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        if to_sniff := proxy.that.to_sniff.widget.value:
            self._to_sniff = to_sniff
            proxy.that.sniff_btn.attrs(disabled=False)
            return
        if not (urls := self.get_all_urls(proxy)):
            return
        to_sniff_lst = expand_urls(urls)  # just in case when urls[0] contains *
        if not to_sniff_lst:
            return
        self._to_sniff = to_sniff_lst[0]
        sniff_btn = proxy.lookup("sniff_btn")
        sniff_btn.attrs(disabled=not self._to_sniff)

    def _save_file_cb(self, proxy: Proxy, change: dict[str, Any]) -> None:
        proxy.lookup("save_btn").attrs(disabled=not change["new"])

    def _sniffer_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        n_lines = proxy.that.n_lines.widget.value
        for uid in ("start_stack", "save_stack", "save_file_stack"):
            proxy.lookup(uid).attrs(selected_index=0)
        snf_proxy = sniffer(self._to_sniff, n_lines)
        sniff_stack = proxy.lookup("sniffer")
        if not sniff_stack._children:
            merge_trees(self._proxy, sniff_stack, snf_proxy)
        sniff_stack.attrs(selected_index=0)
        proxy.lookup("sniff_btn").attrs(disabled=True)

    def fetch_parameters(self) -> dict[str, Any]:
        proxy = self._proxy
        urls = self.get_all_urls(proxy)
        filter_: dict[str, str] = {}  # disabled
        filter_code = proxy.that.preprocessor.widget.value
        throttle = proxy.that.throttle.widget.value
        shuffle = proxy.that.shuffle_ck.widget.value
        sniffer = self._proxy._backends["sniffer"]()
        assert sniffer is not None
        sniffed_params = clean_nodefault(sniffer.params)
        schema = get_schema(sniffer)
        return dict(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            sniffed_params=sniffed_params,
            schema=schema,
            filter_=filter_,
            filter_code=filter_code,
        )

    @starter_callback
    def _start_loader_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        sniffer = self._proxy._backends["sniffer"]()
        assert sniffer is not None
        content = self._proxy.dump()
        self.record = content  # saved for replay
        kw = self.fetch_parameters()
        csv_module = self.init_modules(**kw)
        self.output_module = csv_module
        self.output_slot = "result"
        self.output_dtypes = kw["schema"]

    def _save_settings_cb(self, proxy: Proxy, btn: ipw.Button) -> None:
        pv_dir = dot_progressivis()
        assert pv_dir
        base_name = proxy.lookup("save_file_name").widget.value
        file_name = f"{self.widget_dir}/{base_name}"
        with open(file_name, "w") as f:
            json.dump(self._proxy.dump(), f, indent=4)

    @runner
    def run(self) -> Any:
        content = self.record
        self.restore_ui(content)
        assert hasattr(self._proxy.widget, "children")
        self.children = self._proxy.widget.children
        content = self.fetch_parameters()
        urls = content["urls"]
        throttle = content["throttle"]
        shuffle = content.get("shuffle", False)
        sniffed_params = content["sniffed_params"]
        schema = content["schema"]
        filter_ = content["filter_"]
        filter_code = content.get("filter_code", "")
        print("content", content)
        csv_module = self.init_modules(
            urls=urls,
            throttle=throttle,
            shuffle=shuffle,
            sniffed_params=sniffed_params,
            filter_=filter_,
            filter_code=filter_code,
        )
        self.output_module = csv_module
        self.output_slot = "result"
        self.output_dtypes = schema

    @modules_producer
    def init_modules(
        self,
        urls: list[str] = [],
        throttle: int | None = None,
        shuffle: bool = False,
        sniffed_params: dict[str, Any] = dict(),
        filter_: dict[str, Any] | None = None,
        filter_code: str = "",
        **kw: Any,
    ) -> SimpleCSVLoader:
        filter_fnc = None
        filter_fnc2 = None
        params = sniffed_params
        if filter_code:
            from .custom import CUSTOMER_FNC

            filter_fnc2 = CUSTOMER_FNC[filter_code]
        if filter_impl := combine_filters(filter_fnc, filter_fnc2):
            params["filter_"] = filter_impl
        if shuffle:
            urls = shuffle_urls(urls)
        imodule = self.input_module
        assert isinstance(imodule, Module)
        s = imodule.scheduler
        with s:
            filenames = pd.DataFrame({"filename": urls})
            cst = Constant(PTable("filenames", data=filenames), scheduler=s)
            # params["throttle"] = 100
            csv = SimpleCSVLoader(scheduler=s, **params)
            csv.input.filenames = cst.output[0]
            sink = Sink(scheduler=s)
            sink.input.inp = csv.output.result
            return csv
