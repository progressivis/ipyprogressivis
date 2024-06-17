import logging
import ipywidgets as ipw
import pandas as pd
import numpy as np
from progressivis.core import Module, Sink, aio
from progressivis.stats.kll import KLLSketch
from progressivis.table.range_query import RangeQuery
from progressivis.stats import Histogram1D
from progressivis.table.table_facade import TableFacade
from ..df_grid import DataFrameGrid
from .utils import TreeTab, make_button, stage_register, VBox, TypedBase, IpyVBoxTyped
from progressivis.io import Variable
from ..vega import VegaWidget
from .._stacked_hist_schema import stacked_hist_spec_no_data
from typing import Any as AnyType, Optional, Dict, List, Tuple, cast

NDArray = np.ndarray[AnyType, AnyType]
WidgetType = AnyType

LBINS = 10
NBINS = 4096

SETTINGS_TAB_TITLE = "Settings"
FILTERS_TAB_TITLE = "Filters"
ALL_COLS_TAB_TITLE = "All columns"
NUM_COLS_TAB_TITLE = "Numerical columns"
STR_COLS_TAB_TITLE = "String columns"
DATETIME_COLS_TAB_TITLE = "DateTime columns"
CAT_COLS_TAB_TITLE = "Categorical columns"
LOWER_COL = "> "
UPPER_COL = " <"
RANGE_COL = "Range%"
QUERY_COL = "Filter"
HIST_COL = "Histograms"
PEEL_COL = "Peel"
# https://stackoverflow.com/questions/56949504/how-to-lazify-output-in-tabbed-layout-in-jupyter-notebook

logger = logging.getLogger(__name__)


def p100_range_slider(
    desc: str = "", min_: int = 0, max_: int = 100
) -> ipw.IntRangeSlider:
    return ipw.IntRangeSlider(
        value=[min_, max_],
        min=min_,
        max=max_,
        step=1,
        description=desc,
        disabled=False,
        continuous_update=False,
        orientation="horizontal",
        readout=False,
        readout_format="d",
        layout=ipw.Layout(height="5px", max_height="5px", width="150px"),
    )


class HistSlider(IpyVBoxTyped):
    sk_mod: KLLSketch | None = None
    var_mod: Variable | None = None
    raw_hist_1d: Histogram1D | None = None
    qry_hist_1d: Histogram1D | None = None
    lo_wg = None
    up_wg = None
    last_array = None
    grid: DataFrameGrid | None = None

    class Typed(TypedBase):
        hist: VegaWidget
        slider: ipw.IntRangeSlider

    def init(self, grid: DataFrameGrid) -> None:
        self.c_.hist = VegaWidget(stacked_hist_spec_no_data)
        self.c_.slider = p100_range_slider()
        self.c_.slider.observe(self.observe_range, "value")
        self.layout.border = "0px"
        self.layout.padding = "0px"
        self.children = tuple()
        self.grid = grid

    @property
    def disabled(self) -> bool:
        return not self.children

    @disabled.setter
    def disabled(self, val: AnyType) -> None:
        if not val:
            if not self.children:
                self.children = (self.c_.hist, self.c_.slider)
        else:
            self.children = tuple()

    def observe_range(self, val: AnyType) -> None:
        lo, up = val["new"]
        assert self.grid
        row, col = self.grid.get_coords(self)
        assert col == HIST_COL
        if not self.lo_wg:
            self.lo_wg = self.grid.df.loc[row, LOWER_COL]
        if not self.up_wg:
            self.up_wg = self.grid.df.loc[row, UPPER_COL]
        self.lo_wg.value = lo
        self.up_wg.value = up
        if self.sk_mod:
            assert self.var_mod is not None

            async def _coro(v: AnyType) -> None:
                assert self.sk_mod is not None
                if lo == 0 and up == 100:
                    lower = self.sk_mod._kll.get_min_value()
                    upper = self.sk_mod._kll.get_max_value()
                elif lo == 0:
                    lower = self.sk_mod._kll.get_min_value()
                    upper = self.sk_mod._kll.get_quantiles([up / 100])[0]
                elif up == 100:
                    upper = self.sk_mod._kll.get_max_value()
                    lower = self.sk_mod._kll.get_quantiles([lo / 100])[0]
                else:
                    assert self.sk_mod
                    assert self.sk_mod._kll
                    lower, upper = self.sk_mod._kll.get_quantiles([lo / 100, up / 100])
                assert self.var_mod
                await self.var_mod.from_input({"lower": lower, "upper": upper})

            aio.create_task(_coro(val))

    def peeling(
        self, raw_hist: NDArray, qry_hist: NDArray, threshold: int = 0
    ) -> tuple[NDArray, NDArray]:
        assert self.grid
        row, col = self.grid.get_coords(self)
        assert col == HIST_COL
        raw_res: list[AnyType] = []
        qry_res = []
        for raw, qry in zip(raw_hist, qry_hist):
            if raw <= threshold and raw_res and raw_res[-1] <= threshold:
                continue
            raw_res.append(raw)
            qry_res.append(qry)
        return np.array(raw_res), np.array(qry_res)

    def update(self, *args: AnyType) -> None:
        raw_hist_1d = self.raw_hist_1d
        qry_hist_1d = self.qry_hist_1d
        if raw_hist_1d is None or qry_hist_1d is None:
            return
        if raw_hist_1d.result is None or qry_hist_1d.result is None:
            return
        raw_res = raw_hist_1d.result
        qry_res = qry_hist_1d.result
        if not raw_res or not qry_res:
            return
        raw_hist = raw_res["array"]
        qry_hist = qry_res["array"]
        if (
            self.last_array is not None
            and self.last_array.shape == qry_hist.shape
            and np.all(self.last_array == qry_hist)
        ):
            return
        if not np.any(qry_hist):
            self.last_array = None
            return
        self.last_array = qry_hist.copy()
        raw_hist, qry_hist = self.peeling(raw_hist, qry_hist)
        raw_max = raw_hist.max()
        if raw_max != 0:
            raw_hist = np.cbrt(raw_hist / raw_max)
        qry_max = qry_hist.max()
        if qry_max != 0:
            qry_hist = np.cbrt(qry_hist / qry_max)
        hist_wg = self.c_.hist
        raw_df = pd.DataFrame(
            {
                "nbins": range(len(raw_hist)),
                "level": raw_hist,
                "Origin": ["raw"] * len(raw_hist),
            }
        )
        qry_df = pd.DataFrame(
            {
                "nbins": range(len(qry_hist)),
                "level": qry_hist,
                "Origin": ["qry"] * len(qry_hist),
            }
        )
        source = pd.concat([raw_df, qry_df], ignore_index=True)
        hist_wg._displayed = True
        hist_wg.update("data", remove="true", insert=source)


class DynViewer(TreeTab):
    save_for_cancel: Tuple[AnyType, ...]

    def __init__(
        self,
        dtypes: Dict[str, AnyType],
        input_module: Module,
        input_slot: str = "result",
    ):
        super().__init__(upper=None, known_as="", layout=ipw.Layout(width="90%"))
        self._dtypes = dtypes
        self._input_module = input_module
        self._input_slot = input_slot
        self.col_types = {k: str(t) for (k, t) in self._dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}
        self.num_functions = {
            "min": 0,
            "max": 0,
            "mean": 0,
            "var": 0,
            QUERY_COL: 0,
            LOWER_COL: 0,
            HIST_COL: 0,
            UPPER_COL: 0,
        }
        self.str_functions = {"min": 0, "max": 0, "filter_str": 0}
        self.dt_functions = {"min": 0, "max": 0, "filter_dt": 0}
        self.cat_functions = {"min": 0, "max": 0, "filter_cat": 0}
        self._hidden_set: set[str] = set()
        self._categorical_set: set[str] = set()
        self.gb_all: DataFrameGrid | None = None
        self.gb_num: DataFrameGrid | None = None
        self.gb_str: DataFrameGrid | None = None
        self.gb_dt: DataFrameGrid | None = None
        self.gb_cat: DataFrameGrid | None = None
        tabs = self.draw_matrices()
        self.conf_box = ipw.VBox([tabs])
        self.set_tab(SETTINGS_TAB_TITLE, self.conf_box)
        self.filter_box = ipw.VBox([ipw.Label("...")])
        # self.set_tab(FILTERS_TAB_TITLE, self.filter_box) see if still necessary
        tabs.observe(self.change_tab, names="selected_index")
        self.observe(self.change_upper_tab, names="selected_index")

    def change_tab(self, bunch: AnyType) -> None:
        tab = bunch.owner
        key = tab.get_selected_title()
        if key == ALL_COLS_TAB_TITLE:
            return
        if key == NUM_COLS_TAB_TITLE:
            gb = self.draw_matrix_num()
        elif key == STR_COLS_TAB_TITLE:
            gb = self.draw_matrix_str()
        elif key == DATETIME_COLS_TAB_TITLE:
            gb = self.draw_matrix_dt()
        elif key == CAT_COLS_TAB_TITLE:
            gb = self.draw_matrix_cat()
        else:
            raise ValueError(f"Unknown tab {key}")
        tab.set_tab(key, gb)

    def change_upper_tab(self, bunch: AnyType) -> None:
        tab = bunch.owner
        key = tab.get_selected_title()
        if key == SETTINGS_TAB_TITLE:
            return
        assert key == FILTERS_TAB_TITLE
        assert self.gb_num
        cols = [i for (i, row) in self.gb_num.df.iterrows() if row[-1].value]
        df = pd.DataFrame(index=cols, columns=["Lower", "Upper"], dtype=object)
        df.loc[:, :] = lambda: ipw.Checkbox(  # type: ignore
            value=False, description="", disabled=False, indent=False
        )
        grid = DataFrameGrid(df, first="200px")
        tab.set_tab(key, grid)

    def draw_matrix_all(self, ext_df: Optional[pd.DataFrame] = None) -> ipw.GridBox:
        df = pd.DataFrame(
            index=list(self.col_typed_names.keys()),
            columns=["Ignore", "Categorical"],
            dtype=object,
        )
        df.loc[:, :] = lambda: ipw.Checkbox(  # type: ignore
            value=False, description="", disabled=False, indent=False
        )
        grid = DataFrameGrid(df, first="200px")

        def observer(change: Dict[str, AnyType]) -> None:
            obj = change["owner"]
            row, col = grid.get_coords(obj)
            if col == "Categorical":
                if change["new"]:
                    self._categorical_set.add(row)
                else:
                    if row in self._categorical_set:
                        self._categorical_set.remove(row)
                return
            assert col == "Ignore"
            other = grid.df.loc[row, "Categorical"]
            if change["new"]:
                other.value = False
                other.disabled = True
                self._hidden_set.add(row)
            else:
                other.disabled = False
                if row in self._hidden_set:
                    self._hidden_set.remove(row)

        self.gb_all = grid
        grid.observe_all(observer)

        return grid

    @property
    def visible_cols(self) -> list[str]:
        assert self.gb_all
        df = self.gb_all.df
        return [cast(str, i) for (i, row) in df.iterrows() if not row[0].value]

    def draw_matrix_num(self, ext_df: Optional[pd.DataFrame] = None) -> ipw.GridBox:
        num_cols = [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if col not in self._hidden_set | self._categorical_set
            and (t.startswith("float") or t.startswith("int"))
        ]
        df = pd.DataFrame(  # type: ignore
            index=num_cols, columns=self.num_functions.keys(), dtype=object
        )
        df.loc[:, :QUERY_COL] = lambda: ipw.Checkbox(  # type: ignore
            value=False, description="", disabled=False, indent=False
        )
        df.loc[:, HIST_COL] = HistSlider

        def _lower() -> ipw.BoundedIntText:
            return ipw.BoundedIntText(
                min=0,
                max=25,
                step=5,
                value=0,
                disabled=True,
                layout=ipw.Layout(width="50px"),
            )

        df.loc[:, LOWER_COL] = _lower

        def _upper() -> ipw.BoundedIntText:
            return ipw.BoundedIntText(
                min=75,
                max=100,
                step=5,
                value=100,
                disabled=True,
                layout=ipw.Layout(width="50px"),
            )

        df.loc[:, UPPER_COL] = _upper
        if self.gb_num is not None:
            for i, row in self.gb_num.df.iterrows():
                if i in df.index:
                    df.loc[i] = row
        grid = DataFrameGrid(df, repeat="50px", sizes={HIST_COL: "200px"})

        def _observe_lo(change: Dict[str, AnyType]) -> None:
            obj = change["owner"]
            row, col = grid.get_coords(obj)
            rge = grid.df.loc[row, HIST_COL].c_.slider
            old = rge.value
            rge.value = (obj.value, old[1])

        grid.observe_col(LOWER_COL, _observe_lo)

        def _observe_up(change: Dict[str, AnyType]) -> None:
            obj = change["owner"]
            row, col = grid.get_coords(obj)
            rge = grid.df.loc[row, HIST_COL].c_.slider
            old = rge.value
            rge.value = (old[0], obj.value)

        grid.observe_col(UPPER_COL, _observe_up)

        def _observe_range(change: Dict[str, AnyType]) -> None:
            obj = change["owner"]
            row, col = grid.get_coords(obj)

        def _observe_query(change: Dict[str, AnyType]) -> None:
            obj = change["owner"]
            row, col = grid.get_coords(obj)
            if change["new"]:
                grid.df.loc[row, LOWER_COL].disabled = False
                grid.df.loc[row, UPPER_COL].disabled = False
                grid.df.loc[row, HIST_COL].disabled = False
            else:
                grid.df.loc[row, LOWER_COL].disabled = True
                grid.df.loc[row, UPPER_COL].disabled = True
                grid.df.loc[row, HIST_COL].c_.slider.value = (0, 100)
                grid.df.loc[row, HIST_COL].disabled = True

        grid.observe_col(QUERY_COL, _observe_query)
        grid.broadcast_col(HIST_COL, lambda obj: obj.init(grid))
        self.gb_num = grid
        return grid

    def draw_matrix_str(self, ext_df: Optional[pd.DataFrame] = None) -> ipw.GridBox:
        str_cols = [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if col not in self._hidden_set | self._categorical_set
            and (t.startswith("string") or t.startswith("object"))
        ]
        df = pd.DataFrame(  # type: ignore
            index=str_cols, columns=self.str_functions.keys(), dtype=object
        )
        df.loc[:, :] = lambda: ipw.Checkbox(
            value=False, description="", disabled=False, indent=False
        )
        if self.gb_str is not None:
            for i, row in self.gb_str.df.iterrows():
                if i in df.index:
                    df.loc[i] = row
        grid = DataFrameGrid(df)
        self.gb_str = grid
        return grid

    def draw_matrix_dt(self, ext_df: Optional[pd.DataFrame] = None) -> ipw.GridBox:
        dt_cols = [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if col not in self._hidden_set | self._categorical_set
            and t.startswith("datetime")
        ]
        df = pd.DataFrame(
            index=dt_cols, columns=self.dt_functions.keys(), dtype=object
        )  # type: ignore
        df.loc[:, :] = lambda: ipw.Checkbox(
            value=False, description="", disabled=False, indent=False
        )
        if self.gb_dt is not None:
            for i, row in self.gb_dt.df.iterrows():
                if i in df.index:
                    df.loc[i] = row
        grid = DataFrameGrid(df)
        self.gb_dt = grid
        return grid

    def draw_matrix_cat(self, ext_df: Optional[pd.DataFrame] = None) -> ipw.GridBox:
        cat_cols = [
            col
            for (col, (c, t)) in self.col_typed_names.items()
            if col in self._categorical_set
        ]
        df = pd.DataFrame(  # type: ignore
            index=cat_cols, columns=self.cat_functions.keys(), dtype=object
        )
        df.loc[:, :] = lambda: ipw.Checkbox(
            value=False, description="", disabled=False, indent=False
        )
        if self.gb_cat is not None:
            for i, row in self.gb_cat.df.iterrows():
                if i in df.index:
                    df.loc[i] = row
        grid = DataFrameGrid(df)
        self.gb_cat = grid
        return grid

    def draw_matrices(
        self,
    ) -> TreeTab:
        self.draw_matrix_all()
        self.draw_matrix_num()
        self.draw_matrix_str()
        self.draw_matrix_dt()
        self.draw_matrix_cat()
        settings_tab = TreeTab(upper=self, known_as=SETTINGS_TAB_TITLE)
        settings_tab.set_tab(ALL_COLS_TAB_TITLE, self.gb_all)
        settings_tab.set_tab(NUM_COLS_TAB_TITLE, self.gb_num)
        settings_tab.set_tab(STR_COLS_TAB_TITLE, self.gb_str)
        settings_tab.set_tab(DATETIME_COLS_TAB_TITLE, self.gb_dt)
        settings_tab.set_tab(CAT_COLS_TAB_TITLE, self.gb_cat)
        return settings_tab

    def get_num_bounds(self) -> dict[str, tuple[int, int]]:
        assert self.gb_num
        return {
            cast(str, i): row[1].c_.slider.value
            for (i, row) in self.gb_num.df.loc[:, [QUERY_COL, HIST_COL]].iterrows()
            if row[0].value
        }

    def get_checked_num(self, fnc: str) -> list[str]:
        assert self.gb_num
        return [
            self.untyped(cast(str, i))
            for (i, cell) in self.gb_num.df.loc[:, fnc].items()
            if cell.value
        ]

    def untyped(self, tcol: str) -> str:
        return self.col_typed_names[tcol][0]

    def run(self, carrier: "FacadeCreatorW") -> None:
        num_bounds = self.get_num_bounds()
        max_num_cols = self.get_checked_num("max")
        min_num_cols = self.get_checked_num("min")
        s = carrier.input_module.scheduler()
        with s:
            inp = carrier.input_module
            raw_hist_1d = None
            raw_hist_index = None
            for tcol, (lo, up) in num_bounds.items():
                col = self.col_typed_names[tcol][0]
                kll = KLLSketch(column=col, k=10000, scheduler=s)
                kll.input[0] = inp.output[carrier.input_slot]
                range_qry = RangeQuery(column=col, scheduler=s)
                range_qry.params.watched_key_lower = "lower"
                range_qry.params.watched_key_upper = "upper"
                variable = Variable({"lower": "*", "upper": "*"}, scheduler=s)
                range_qry.create_dependent_modules(
                    inp, "result", min_value=variable, max_value=variable
                )
                sink = Sink(scheduler=s)
                sink.input.inp = kll.output.result
                sink.input.inp = range_qry.output.result
                self.gb_num.df.loc[tcol, HIST_COL].sk_mod = kll  # type: ignore
                self.gb_num.df.loc[tcol, HIST_COL].var_mod = variable  # type: ignore
                inp = range_qry
                raw_hist_1d = Histogram1D(column=col, scheduler=s)
                raw_hist_1d.input.table = carrier.input_module.output[
                    carrier.input_slot
                ]
                raw_hist_index = range_qry.dep.hist_index
                raw_hist_1d.input.min = raw_hist_index.output.min_out
                raw_hist_1d.input.max = raw_hist_index.output.max_out
                # kll.input[0] = raw_hist_index.output.result
                raw_hist_1d.params.bins = NBINS
                sink.input.inp = raw_hist_1d.output.result
                qry_hist_1d = Histogram1D(column=col, scheduler=s)
                qry_hist_1d.input.table = inp.output[carrier.input_slot]
                qry_hist_1d.input.min = raw_hist_index.output.min_out
                qry_hist_1d.input.max = raw_hist_index.output.max_out
                qry_hist_1d.params.bins = NBINS
                sink.input.inp = qry_hist_1d.output.result
                assert self.gb_num
                viz_obj = self.gb_num.df.loc[tcol, HIST_COL]
                viz_obj.raw_hist_1d = raw_hist_1d  # type: ignore
                viz_obj.qry_hist_1d = qry_hist_1d  # type: ignore
                qry_hist_1d.on_after_run(viz_obj.update)  # type: ignore
            facade = TableFacade.get_or_create(inp, "result")
            if max_num_cols:
                facade.configure(base="max", hints=max_num_cols, name="max_num")
            if min_num_cols:
                facade.configure(base="min", hints=min_num_cols, name="min_num")
            carrier.output_module = facade  # type: ignore
            carrier.output_slot = "result"
            carrier.output_dtypes = carrier.input_dtypes


class FacadeCreatorW(VBox):
    def __init__(self) -> None:
        super().__init__()

    def init(self) -> None:
        self._dyn_viewer = DynViewer(self.dtypes, self.input_module, self.input_slot)
        self.dag.request_attention(self.title, "widget", "PROGRESS_NOTIFICATION", "0")
        btn = make_button("Start", cb=self._start_cb)
        self.children = (self._dyn_viewer, btn)

    def get_underlying_modules(self) -> List[str]:
        return ["TODO"]

    def _start_cb(self, btn: AnyType) -> None:
        self._dyn_viewer.run(self)
        self.make_chaining_box()
        self.dag_running()


stage_register["Facade"] = FacadeCreatorW
