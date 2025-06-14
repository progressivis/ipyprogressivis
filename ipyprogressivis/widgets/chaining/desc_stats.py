from functools import singledispatch
from collections.abc import Iterable
from itertools import product
from functools import wraps
import logging
import ipywidgets as ipw
import numpy as np
import pandas as pd
from progressivis.core.api import asynchronize, Sink, Scheduler, Module
import progressivis.core.aio as aio
from progressivis.utils.api import PDict
from progressivis.io.api import Variable
from progressivis.stats.api import (
    KLLSketch,
    Corr,
    Histogram1D,
    Histogram2D,
    Histogram1DCategorical,
)
from progressivis.vis import (
    StatsFactory,
    Histogram1DPattern,
    Histogram2DPattern,
)
from ..vega import VegaWidget
from .._hist1d_schema import hist1d_spec_no_data, kll_spec_no_data
from .._hist2d_schema import hist2d_spec_no_data
from .._corr_schema import corr_spec_no_data
from .._bar_schema import bar_spec_no_data
from .utils import TreeTab, make_button, stage_register, VBox, needs_dtypes
from ..utils import historized_widget, HistorizedBox

from typing import (
    Any as AnyType,
    Sequence,
    cast,
    Optional,
    Dict,
    Set,
    Type,
    Union,
    List,
    Tuple,
    Callable,
    Coroutine,
)

WidgetType = AnyType
MAIN_TAB_TITLE = "Main"
HIST1D_TAB_TITLE = "1D Histograms"
HIST2D_TAB_TITLE = "Heatmaps"
CORR_MX_TAB_TITLE = "Correlation Matrix"
GENERAL_TAB_TITLE = "General"
SETTINGS_TAB_TITLE = "Settings"
GENERAL_SET_TAB_TITLE = "General"
HEATMAPS_SET_TAB_TITLE = "Heatmaps"
SIMPLE_RESULTS_TAB_TITLE = "Simple results"
# https://stackoverflow.com/questions/56949504/how-to-lazify-output-in-tabbed-layout-in-jupyter-notebook

logger = logging.getLogger(__name__)


def bins_range_slider(desc: str, binning: int) -> ipw.IntRangeSlider:
    last_bin = binning - 1
    return ipw.IntRangeSlider(
        value=[0, last_bin],
        min=0,
        max=last_bin,
        step=1,
        description=desc,
        disabled=False,
        continuous_update=False,
        orientation="horizontal",
        readout=False,
        readout_format="d",
    )


def narrow_label(text: str, w: int = 70) -> ipw.Button:
    button = ipw.Button(
        description=text,
        disabled=False,
        button_style="",
        tooltip=text,
        icon="",
        layout=ipw.Layout(width=f"{w}px", height="40px"),
    )
    return button


def make_observer(
    hname: str, sk_mod: KLLSketch, lower_mod: Variable, upper_mod: Variable
) -> Callable[[AnyType], None]:
    def _observe_range(val: AnyType) -> None:
        async def _coro(v: AnyType) -> None:
            lo = v["new"][0]
            up = v["new"][1]
            res = sk_mod.result
            assert res is not None
            hist = res["pmf"]
            min_ = res["min"]
            max_ = res["max"]
            len_ = len(hist)
            bins_ = np.linspace(min_, max_, len_)
            lower = bins_[lo]
            upper = bins_[up]
            await lower_mod.from_input({hname: lower})
            await upper_mod.from_input({hname: upper})

        aio.create_task(_coro(val))

    return _observe_range


def corr_as_vega_dataset(
    mod: Corr, columns: Optional[Sequence[str]] = None
) -> Sequence[Dict[str, AnyType]]:
    """ """
    if columns is None:
        columns = mod.columns
        assert columns

    def _c(kx: str, ky: str) -> float:
        assert mod.result is not None
        return mod.result[frozenset([kx, ky])]  # type: ignore

    return [
        dict(corr=_c(kx, ky), corr_label=f"{_c(kx,ky):.2f}", var=kx, var2=ky)
        for (kx, ky) in product(columns, columns)
    ]


def categ_as_vega_dataset(categs: PDict) -> List[Dict[str, AnyType]]:
    return [{"category": k, "count": v} for (k, v) in categs.items()]


# TODO: use typing annotation below for python>=3.11 and remove "ignores"

@singledispatch
def format_label(arg):  # type: ignore
    return str(arg)


@format_label.register(float)
@format_label.register(np.floating)
def _np_floating_arg(arg):  # type: ignore
    return f"{arg:.4f}"


@format_label.register(str)
def _str_arg(arg):  # type: ignore
    return arg


@format_label.register(Iterable)
def _len_arg(arg):  # type: ignore
    return str(len(arg))


def asynchronized(func: Callable[..., None]) -> Callable[..., AnyType]:
    """
    decorator
    """

    @wraps(func)
    def asynchronizer(
        *args: AnyType,
    ) -> Callable[[AnyType, AnyType], Coroutine[AnyType, AnyType, None]]:
        async def _coro(_1: AnyType, _2: AnyType) -> None:
            _ = _1, _2
            await asynchronize(func, *args)

        return _coro

    return asynchronizer


def asynchronized_wg(func: Callable[..., None]) -> Callable[..., AnyType]:
    """
    decorator
    """

    @wraps(func)
    def asynchronizer(
        wg: AnyType, *args: AnyType
    ) -> Callable[[AnyType, AnyType], Coroutine[AnyType, AnyType, None]]:
        async def _coro(_1: AnyType, _2: AnyType) -> None:
            _ = _1, _2
            if not wg._selection_event:
                return
            await asynchronize(func, wg, *args)

        return _coro

    return asynchronizer


_VegaWidget: Type[HistorizedBox] = historized_widget(VegaWidget, "update")


@asynchronized
def refresh_info_sketch(
    hout: WidgetType, hmod: KLLSketch, name: str, tab: "TreeTab", main: "DynViewer"
) -> None:
    if not tab.is_visible(name) and hmod.updated_once:  # type: ignore
        return
    if not hmod.result:
        return
    res = hmod.result
    hist = res["pmf"]
    min_: float = res["min"]
    max_: float = res["max"]
    len_: int = len(hist)
    bins_ = np.linspace(min_, max_, len_)
    rule_lower = np.zeros(len_, dtype="int32")
    rule_upper = np.zeros(len_, dtype="int32")
    range_widget = main.range_widgets.get(hmod.column)
    if range_widget is not None:
        rule_lower[0] = range_widget.value[0]
        rule_upper[0] = range_widget.value[1]
    source = pd.DataFrame(
        {
            "xvals": bins_,
            "nbins": range(len(hist)),
            "rule_lower": rule_lower,
            "rule_upper": rule_upper,
            "level": hist,
        }
    )
    hout.children[0].children[0].update("data", remove="true", insert=source)
    # range slider, labels etc.
    if range_widget is None:
        return
    label_min = hout.children[0].children[1].children[1]
    label_max = hout.children[0].children[1].children[2]
    label_min.value = f"{bins_[range_widget.value[0]]:.2f}"
    label_max.value = f" -- {bins_[range_widget.value[1]]:.2f}"
    hmod.updated_once = True  # type: ignore


@asynchronized
def refresh_info_barplot(
    hout: WidgetType, hmod: Histogram1DCategorical, name: str, tab: "TreeTab"
) -> None:
    if not tab.is_visible(name) and hmod.updated_once:  # type: ignore
        return
    categs = hmod.result
    if not categs:
        return
    dataset = categ_as_vega_dataset(categs)
    hout.update("data", remove="true", insert=dataset)
    hmod.updated_once = True  # type: ignore


@asynchronized
def refresh_info_hist_1d(
    hout: WidgetType, h1d_mod: Histogram1D, name: str, tab: "TreeTab"
) -> None:
    if (not tab.is_visible(name)) and h1d_mod.updated_once:  # type: ignore
        return
    if not h1d_mod.result:
        return
    res = h1d_mod.result
    assert res
    hist = res["array"]
    min_ = res["min"]
    max_ = res["max"]
    bins_ = np.linspace(min_, max_, len(hist))
    source = pd.DataFrame({
        "xvals": bins_,
        "nbins": range(len(hist)),
        "level": hist
    })
    hout.children[1].update("data", remove="true", insert=source)
    h1d_mod.updated_once = True  # type: ignore


@asynchronized
def refresh_info_h2d(
    hout: WidgetType, h2d_mod: Histogram2D, name: str, tab: "TreeTab"
) -> None:
    if not tab.is_visible(name) and h2d_mod.updated_once:  # type: ignore
        return
    if not h2d_mod.result:
        return
    last = h2d_mod.result.last()
    assert last
    res = last.to_dict()
    arr = res["array"]
    maxa = arr.max()
    if maxa != 0:
        hist = np.cbrt(arr/maxa)
    else:
        hist = maxa
    hout.update("data", insert=hist, remove="true")
    h2d_mod.updated_once = True  # type: ignore


@asynchronized
def refresh_info_corr(cout: WidgetType, cmod: Corr, name: str, tab: "TreeTab") -> None:
    if not tab.is_visible(name) and cmod.updated_once:  # type: ignore
        return
    if not cmod.result:
        return
    cols = cmod.columns
    dataset = corr_as_vega_dataset(cmod, cols)
    cout.update("data", remove="true", insert=dataset)
    cmod.updated_once = True  # type: ignore


type_op_mismatches: Dict[str, Set[str]] = dict(
    string=set(["min", "max", "var", "corr", "hist2d"])
)


def get_flag_status(dt: str, op: str) -> bool:
    return op in type_op_mismatches.get(dt, set())


def make_tab_observer(tab: "TreeTab", sched: Scheduler) -> Callable[..., None]:
    def _tab_observer(wg: AnyType) -> None:
        key = tab.get_selected_title()
        if key is None:
            return
        sched._module_selection = tab.mod_dict.get(key)

    return _tab_observer


def make_tab_observer_2l(tab: "DynViewer", sched: Scheduler) -> Callable[..., None]:
    def _tab_observer(wg: AnyType) -> None:
        subtab = tab.get_selected_child()
        if isinstance(subtab, TreeTab):
            key = subtab.get_selected_title()
            if key is None:
                return
            sched._module_selection = subtab.mod_dict.get(key)
        else:
            key = tab.get_selected_title()
            if key is None:
                return
            sched._module_selection = tab.mod_dict.get(key)

    return _tab_observer


def _get_func_name(func: str) -> str:
    _dictionary = {"hide": "❌", "corr": "Corr. Mx", "distinct": "≠", "hist": "1D Hist"}
    return _dictionary.get(func, func.capitalize())


class DynViewer(TreeTab):
    save_for_cancel: Tuple[AnyType, ...]

    def __init__(
        self,
        dtypes: Dict[str, AnyType],
        input_module: Module,
        input_slot: str = "result",
    ):
        super().__init__(upper=None, known_as="")
        self._dtypes = dtypes
        self._input_module = input_module
        self._input_slot = input_slot
        self._modgroup = f"group_{id(self)}"
        self.hidden_cols: List[str] = []
        self._hidden_sel_wg: Optional[ipw.SelectMultiple] = None
        self.visible_cols: List[str] = []
        self._last_df: Optional[pd.DataFrame] = None
        self._last_h2d_df: Optional[pd.DataFrame] = None
        self.previous_visible_cols: List[str] = []
        self.info_labels: Dict[Tuple[str, str], ipw.Label] = {}
        self.info_cbx: Dict[Tuple[str, str], ipw.Checkbox] = {}
        self.h2d_cbx: Dict[Tuple[str, str], ipw.Checkbox] = {}
        self._hdict: Dict[
            str, Tuple[Union[Histogram1DPattern, Histogram1DCategorical], WidgetType]
        ] = {}
        self._h2d_dict: Dict[str, Tuple[Histogram2DPattern, WidgetType]] = {}
        self._hist_tab: Optional[TreeTab] = None
        self._hist_sel: Set[AnyType] = set()
        self._h2d_tab: Optional[TreeTab] = None
        self._h2d_sel: Set[AnyType] = set()
        self._corr_sel: List[str] = []
        self._registry_mod = self.init_factory(input_module, input_slot)
        self.all_functions = {
            dec: _get_func_name(dec) for dec in self._registry_mod.func_dict.keys()
        }
        self.scalar_functions = {
            k: v
            for (k, v) in self.all_functions.items()
            if k not in ("hide", "hist", "corr")
        }
        self.obs_flag = False
        self.range_widgets: Dict[str, ipw.IntRangeSlider] = {}
        self.updated_once = False
        self._selection_event = True
        self._registry_mod.scheduler().on_change(self.set_selection_event())
        self.observe(
            make_tab_observer_2l(self, self.get_scheduler()), names="selected_index"
        )
        input_module.scheduler().on_tick(DynViewer.refresh_info(self))

    def init_factory(self, input_module: Module, input_slot: str) -> StatsFactory:
        s = input_module.scheduler()
        with s:
            factory = StatsFactory(input_module=input_module, scheduler=s)
            factory.create_dependent_modules()
            factory.input.table = input_module.output[input_slot]
            sink = Sink(scheduler=s)
            sink.input.inp = factory.output.result
            return factory

    def get_scheduler(self) -> Scheduler:
        return self._registry_mod.scheduler()

    def draw_matrix(self, ext_df: Optional[pd.DataFrame] = None) -> ipw.GridBox:
        lst: List[WidgetType] = [ipw.Label("")] + [
            ipw.Label(s) for s in self.all_functions.values()
        ]
        width_ = len(lst)
        df = self.matrix_to_df() if ext_df is None else ext_df
        for col in sorted(self.visible_cols):
            col_type = self.col_types[col]
            lst.append(ipw.Label(f"{col}:{col_type}"))
            for k in self.all_functions.keys():
                lst.append(self._info_checkbox(col, k, get_flag_status(col_type, k)))
        gb = ipw.GridBox(
            lst,
            layout=ipw.Layout(grid_template_columns=f"200px repeat({width_-1}, 70px)"),
        )
        if df is not None:
            for i in df.index:
                for c in df.columns:
                    self.info_cbx[(i, c)].value = bool(df.loc[i, c])
        return gb

    def draw_h2d_matrix(self, ext_df: Optional[pd.DataFrame] = None) -> ipw.GridBox:
        num_cols = sorted(
            [col for col in self.visible_cols if self.col_types[col] != "string"]
        )
        lst: List[WidgetType] = [narrow_label("", 150)] + [
            narrow_label(s) for s in num_cols
        ]
        width_ = len(lst)
        df = self.matrix_to_h2d_df() if ext_df is None else ext_df
        for col in num_cols:
            lst.append(narrow_label(col, 150))
            for k in num_cols:
                lst.append(self._h2d_checkbox(col, k, col == k))
        gb = ipw.GridBox(
            lst,
            layout=ipw.Layout(grid_template_columns=f"150px repeat({width_-1}, 60px)"),
        )
        if df is not None:
            for k in df.index:
                for c in df.columns:
                    self.h2d_cbx[(k, c)].value = bool(df.loc[k, c])
        return gb

    def draw_matrices(
        self,
        ext_df: Optional[pd.DataFrame] = None,
        ext_h2d_df: Optional[pd.DataFrame] = None,
    ) -> TreeTab:
        gb = self.draw_matrix(ext_df)
        h2d_gb = self.draw_h2d_matrix(ext_h2d_df)
        settings_tab = TreeTab(upper=self, known_as=SETTINGS_TAB_TITLE)
        settings_tab.set_tab(GENERAL_SET_TAB_TITLE, gb)
        settings_tab.set_tab(HEATMAPS_SET_TAB_TITLE, h2d_gb)
        return settings_tab

    def lock_conf(self) -> None:
        assert self._hidden_sel_wg
        self._hidden_sel_wg.disabled = True
        for cbx in self.info_cbx.values():
            cbx.disabled = True
        for cbx in self.h2d_cbx.values():
            cbx.disabled = True

    def unlock_conf(self) -> None:
        assert self._hidden_sel_wg
        self._hidden_sel_wg.disabled = False
        for (key, func), cbx in self.info_cbx.items():
            dtype = self.col_types[key]
            cbx.disabled = get_flag_status(dtype, func)
        for (i, j), cbx in self.h2d_cbx.items():
            cbx.disabled = i == j

    def matrix_to_df(self) -> Optional[pd.DataFrame]:
        if not self.info_cbx:
            return None
        cols = self.visible_cols
        funcs = list(self.all_functions.keys())[1:]  # because 0 is "hide"
        arr = np.zeros((len(cols), len(funcs)), dtype=bool)
        for i, c in enumerate(self.visible_cols):
            for j, f in enumerate(funcs):
                arr[i, j] = bool(self.info_cbx[(c, f)].value)
        df = pd.DataFrame(arr, index=cols, columns=funcs)
        return df

    def matrix_to_h2d_df(self) -> Optional[pd.DataFrame]:
        if not self.h2d_cbx:
            return None
        cols = [col for col in self.visible_cols if self.col_types[col] != "string"]
        len_ = len(cols)
        arr = np.zeros((len_, len_), dtype=bool)
        for i, ci in enumerate(cols):
            for j, cj in enumerate(cols):
                arr[i, j] = bool(self.h2d_cbx[(ci, cj)].value)
                pass
        df = pd.DataFrame(arr, index=cols, columns=cols)
        return df

    def make_btn_bar(self) -> ipw.HBox:
        self._btn_edit = make_button("Edit", disabled=False, cb=self._btn_edit_cb)
        self._btn_cancel = make_button("Cancel", disabled=True, cb=self._btn_cancel_cb)
        self._btn_apply = make_button("Apply", disabled=True, cb=self._btn_apply_cb)
        self._btn_bar = ipw.HBox([self._btn_edit, self._btn_cancel, self._btn_apply])
        return self._btn_bar

    def set_histogram_widget(
        self, name: str, hist_mod: Union[Histogram1DPattern, Histogram1DCategorical]
    ) -> None:
        if name in self._hdict and self._hdict[name][0] is hist_mod:
            return  # self._hdict[name][1], None # None means selection unchanged
        type_ = self.col_types[name]
        hout: ipw.VBox
        if type_ == "string":
            hout = _VegaWidget(spec=bar_spec_no_data)
            bp_mod = cast(Histogram1DCategorical, hist_mod)
            bp_mod.updated_once = False  # type: ignore
            selection = bp_mod.path_to_origin()
            bp_mod.on_after_run(
                refresh_info_barplot(hout, bp_mod, name, self._hist_tab)
            )
        else:
            hist_mod = cast(Histogram1DPattern, hist_mod)
            hmod_1d = hist_mod.dep.histogram1d
            sk_mod = hist_mod.dep.kll
            lower_mod = hist_mod.dep.lower
            upper_mod = hist_mod.dep.upper
            range_slider = bins_range_slider("Range:", cast(int, sk_mod.params.binning))
            self.range_widgets[name] = range_slider
            vega_wg = _VegaWidget(spec=kll_spec_no_data)
            hout = ipw.VBox(
                [
                    ipw.VBox(
                        [
                            vega_wg,
                            ipw.HBox(
                                [range_slider, ipw.Label("Min:"), ipw.Label("Max:")]
                            ),
                        ]
                    ),
                    _VegaWidget(spec=hist1d_spec_no_data),
                ]
            )
            range_slider.observe(
                make_observer(name, sk_mod, lower_mod, upper_mod), "value"
            )
            selection1 = sk_mod.path_to_origin()
            selection2 = hmod_1d.path_to_origin()
            selection = selection1 | selection2
            sk_mod.updated_once = False
            sk_mod.on_after_run(
                refresh_info_sketch(hout, sk_mod, name, self._hist_tab, self)
            )
            hmod_1d.updated_once = False
            hmod_1d.on_after_run(
                refresh_info_hist_1d(hout, hmod_1d, name, self._hist_tab)
            )
        self._hdict[name] = (hist_mod, hout)
        # return hout, selection
        assert self._hist_tab
        self._hist_tab.set_tab(name, hout, overwrite=False)
        self._hist_tab.mod_dict[name] = selection

    def set_module_selection(self, sel: Optional[Set[str]]) -> None:
        self._registry_mod.scheduler()._module_selection = sel

    def set_h2d_widget(self, name: str, h2d_mod: Histogram2DPattern) -> None:
        if name in self._h2d_dict and self._h2d_dict[name][0] is h2d_mod:
            return
        hout = _VegaWidget(spec=hist2d_spec_no_data)
        _mod = h2d_mod.dep.histogram2d
        _mod.updated_once = False
        selection = _mod.path_to_origin()
        _mod.on_after_run(refresh_info_h2d(hout, _mod, name, self._h2d_tab))
        self._h2d_dict[name] = (h2d_mod, hout)
        assert self._h2d_tab
        self._h2d_tab.set_tab(name, hout, overwrite=False)
        self._h2d_tab.mod_dict[name] = selection

    def get_selection_set(self, func: str) -> Set[str]:
        assert self._last_df is not None
        return set(self._last_df.index[self._last_df.loc[:, func] != 0])

    @asynchronized_wg
    def refresh_info(self) -> None:
        if not self.children:
            selm = ipw.SelectMultiple(
                options=self.hidden_cols,
                value=[],
                rows=5,
                description="❎",
                disabled=False,
            )
            self._hidden_sel_wg = selm
            self.col_types = {k: str(t) for (k, t) in self._dtypes.items()}
            self.visible_cols = list(self.col_types.keys())
            selm.observe(self._selm_obs_cb, "value")
            gb = self.draw_matrices()
            self.conf_box = ipw.VBox([selm, gb, self.make_btn_bar()])
            self.lock_conf()
            self.set_tab(SETTINGS_TAB_TITLE, self.conf_box)
        if self._registry_mod._matrix is None:
            return
        mod_matrix = self._registry_mod._matrix
        mod_h2d_matrix = self._registry_mod._h2d_matrix
        if self.previous_visible_cols != self.visible_cols:
            # rebuild results grid cause cols list changes
            lst = [ipw.Label("")] + [
                ipw.Label(s) for s in self.scalar_functions.values()
            ]
            width_ = len(lst)
            for col in sorted(self.visible_cols):
                lst.append(ipw.Label(f"{col}:{self.col_types[col]}"))
                for k in self.scalar_functions.keys():
                    lst.append(self._info_label((col, k)))
            gb_res = ipw.GridBox(
                lst,
                layout=ipw.Layout(
                    grid_template_columns=f"200px repeat({width_-1}, 120px)"
                ),
            )
            self.previous_visible_cols = self.visible_cols[:]
            self.updated_once = False
            self.set_tab(SIMPLE_RESULTS_TAB_TITLE, gb_res)
        # refresh Simple results
        if self.is_visible(SIMPLE_RESULTS_TAB_TITLE) or not self.updated_once:
            self.set_module_selection(None)  # TODO : be more specific
            for col in self.visible_cols:
                col_name = col
                for k in self.scalar_functions.keys():
                    lab = self.info_labels[(col, k)]
                    if not self.info_cbx[(col, k)].value:
                        lab.value = ""
                        continue
                    subm = mod_matrix.loc[col, k]
                    if subm is None:
                        lab.value = "..."
                        continue
                    assert hasattr(subm, "result")
                    if subm.result is None:
                        lab.value = "..."
                        continue
                    res = subm.result.get(col_name, "")
                    res = format_label(res)
                    lab.value = res
            self.updated_once = True
        # histograms
        if self._last_df is not None and np.any(self._last_df.loc[:, "hist"]):
            if self._hist_tab is None:
                self._hist_tab = TreeTab(upper=self, known_as=HIST1D_TAB_TITLE)
                self._hist_tab.observe(
                    make_tab_observer(self._hist_tab, self.get_scheduler()),
                    names="selected_index",
                )
            self.set_tab(HIST1D_TAB_TITLE, self._hist_tab, overwrite=False)
            hist_sel = self.get_selection_set("hist")
            if hist_sel != self._hist_sel:
                self._hist_tab.children = tuple([])
                for attr in hist_sel:
                    hist_mod = mod_matrix.loc[attr, "hist"]
                    assert hist_mod
                    self.set_histogram_widget(
                        attr,
                        cast(
                            Union[Histogram1DPattern, Histogram1DCategorical], hist_mod
                        ),
                    )
                self._hist_sel = hist_sel
        else:
            self.remove_tab(HIST1D_TAB_TITLE)
            self._hist_sel = set()
        # heatmaps (2D histograms)
        if (
            self._last_h2d_df is not None
            and np.any(self._last_h2d_df.loc[:, :])
            and mod_h2d_matrix is not None
        ):
            if self._h2d_tab is None:
                self._h2d_tab = TreeTab(upper=self, known_as=HIST2D_TAB_TITLE)
                self._h2d_tab.observe(
                    make_tab_observer(self._h2d_tab, self.get_scheduler()),
                    names="selected_index",
                )
            self.set_tab(HIST2D_TAB_TITLE, self._h2d_tab, overwrite=False)
            h2d_sel = set(
                [
                    (ci, cj)
                    for (ci, cj) in product(self._last_h2d_df.columns, repeat=2)
                    if self._last_h2d_df.loc[ci, cj]
                ]
            )
            if h2d_sel != self._h2d_sel:
                self._h2d_tab.children = tuple([])
                for ci, cj in h2d_sel:
                    h2d_mod = mod_h2d_matrix.loc[ci, cj]
                    assert h2d_mod
                    title = f"{ci}/{cj}"
                    self.set_h2d_widget(title, h2d_mod)  # type: ignore
                self._h2d_sel = h2d_sel
        else:
            self.remove_tab(HIST2D_TAB_TITLE)
            self._h2d_sel = set()
        # corr
        if self._last_df is not None and np.any(self._last_df.loc[:, "corr"]):
            if "corr" not in self._registry_mod._multi_col_modules:
                return
            corr_mod = cast(Corr, self._registry_mod._multi_col_modules["corr"])
            assert corr_mod
            assert corr_mod.columns
            corr_sel = corr_mod.columns[:]
            if corr_sel != self._corr_sel:
                corr_out = _VegaWidget(spec=corr_spec_no_data)
                self.set_tab(CORR_MX_TAB_TITLE, corr_out)
                corr_mod.updated_once = False  # type: ignore
                selection = corr_mod.path_to_origin()
                corr_mod.on_after_run(
                    refresh_info_corr(corr_out, corr_mod, CORR_MX_TAB_TITLE, self)
                )
                assert isinstance(corr_sel, list)
                self._corr_sel = corr_sel.copy()
                self.mod_dict[CORR_MX_TAB_TITLE] = selection
        else:
            self.remove_tab(CORR_MX_TAB_TITLE)
            self._corr_sel = []

    def _info_label(self, k: Tuple[str, str]) -> ipw.Label:
        lab = ipw.Label()
        self.info_labels[k] = lab
        return lab

    def _info_checkbox(self, col: str, func: str, dis: bool) -> ipw.Checkbox:
        wgt = ipw.Checkbox(value=False, description="", disabled=dis, indent=False)
        self.info_cbx[(col, func)] = wgt
        wgt.observe(self._make_cbx_obs(col, func), "value")
        return wgt

    def _h2d_checkbox(self, col: str, func: str, dis: bool) -> ipw.Checkbox:
        wgt = ipw.Checkbox(value=False, description="", disabled=dis, indent=False)
        self.h2d_cbx[(col, func)] = wgt
        wgt.observe(self._h2d_cbx_obs_cb, "value")
        return wgt

    def get_underlying_modules(self) -> List[str]:
        s = self._registry_mod.scheduler()
        modules = s.group_modules(self._registry_mod.name)
        print("to delete", modules)
        return modules

    def _make_cbx_obs(self, col: str, func: str) -> Callable[..., None]:
        def _cbk(change: AnyType) -> None:
            self._btn_apply.disabled = False
            if func == "hide":
                self.hidden_cols.append(col)
                self.visible_cols.remove(col)
                assert self._hidden_sel_wg
                self._hidden_sel_wg.options = sorted(self.hidden_cols)
                gb = self.draw_matrices()
                self.unlock_conf()
                self.conf_box.children = (
                    self._hidden_sel_wg,
                    gb,
                    self._btn_bar,
                )

        return _cbk

    def _h2d_cbx_obs_cb(self, change: AnyType) -> None:
        self._btn_apply.disabled = False

    def _btn_edit_cb(self, btn: ipw.Button) -> None:
        btn.disabled = True
        self.save_for_cancel = (
            self.hidden_cols[:],
            self.visible_cols[:],
            self.matrix_to_df(),
            self.matrix_to_h2d_df(),
        )
        self._btn_cancel.disabled = False
        self._btn_apply.disabled = True
        self.unlock_conf()

    def _btn_cancel_cb(self, btn: ipw.Button) -> None:
        btn.disabled = True
        self._btn_edit.disabled = False
        hcols, vcols, df, h2d_df = self.save_for_cancel
        self.hidden_cols = hcols[:]
        self.visible_cols = vcols[:]
        assert self._hidden_sel_wg
        self._hidden_sel_wg.options = self.hidden_cols
        gb = self.draw_matrices(df, h2d_df)
        self.lock_conf()
        self.conf_box.children = (
            self._hidden_sel_wg,
            gb,
            self._btn_bar,
        )
        self.lock_conf()
        self._btn_apply.disabled = True

    def _btn_apply_cb(self, btn: ipw.Button) -> None:
        btn.disabled = True
        self._btn_edit.disabled = False
        self._btn_cancel.disabled = True
        self.lock_conf()

        async def _coro() -> None:
            self._last_df = self.matrix_to_df()
            self._last_h2d_df = self.matrix_to_h2d_df()
            self._selection_event = False
            await self._registry_mod.variable.from_input(
                {
                    "matrix": self._last_df,
                    "h2d_matrix": self._last_h2d_df,
                    "hidden_cols": self.hidden_cols[:],
                }
            )

        aio.create_task(_coro())

    def _selm_obs_cb(self, change: AnyType) -> None:
        if self.obs_flag:
            return
        try:
            self.obs_flag = True
            cols = change["new"]
            for col in cols:
                self.hidden_cols.remove(col)
                self.visible_cols.append(col)
            assert self._hidden_sel_wg
            self._hidden_sel_wg.options = sorted(self.hidden_cols)
            gb = self.draw_matrices()
            self.unlock_conf()
            self.conf_box.children = (
                self._hidden_sel_wg,
                gb,
                self._btn_bar,
            )
        finally:
            self.obs_flag = False

    def set_selection_event(
        self,
    ) -> Callable[
        [Scheduler, Set[Module], Set[Module]], Coroutine[AnyType, AnyType, None]
    ]:
        async def fun(a: AnyType, b: AnyType, c: AnyType) -> None:
            if not self._selection_event:
                self._selection_event = True

        return fun


class DescStatsW(VBox):
    def __init__(self) -> None:
        super().__init__()

    @needs_dtypes
    def initialize(self) -> None:
        assert isinstance(self.input_module, Module)
        self._dyn_viewer = DynViewer(self.dtypes, self.input_module, self.input_slot)
        self.dag.request_attention(self.title, "widget", "PROGRESS_NOTIFICATION", "0")
        self.children = (self._dyn_viewer,)

    def get_underlying_modules(self) -> List[str]:
        return self._dyn_viewer.get_underlying_modules()


stage_register["Descriptive_statistics"] = DescStatsW
