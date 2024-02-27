import logging
import ipywidgets as ipw
import pandas as pd
from progressivis.core import Module, Sink
from progressivis.stats.kll import KLLSketch
from progressivis.table.range_query import RangeQuery
from progressivis.table.table_facade import TableFacade
from ..df_grid import DataFrameGrid
from .utils import TreeTab, make_button, stage_register, VBox

from typing import (
    Any as AnyType,
    Optional,
    Dict,
    List,
    Tuple,
    cast
)

WidgetType = AnyType

SETTINGS_TAB_TITLE = "Settings"
FILTERS_TAB_TITLE = "Filters"
ALL_COLS_TAB_TITLE = "All columns"
NUM_COLS_TAB_TITLE = "Numerical columns"
STR_COLS_TAB_TITLE = "String columns"
DATETIME_COLS_TAB_TITLE = "DateTime columns"
CAT_COLS_TAB_TITLE = "Categorical columns"
LOWER_COL = "[ lower,"
UPPER_COL = "upper ]"

# https://stackoverflow.com/questions/56949504/how-to-lazify-output-in-tabbed-layout-in-jupyter-notebook

logger = logging.getLogger(__name__)


# TODO: use typing annotation below for python>=3.11 and remove "ignores"


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
        self.col_types = {k: str(t) for (k, t) in self._dtypes.items()}
        self.col_typed_names = {f"{n}:{t}": (n, t) for (n, t) in self.col_types.items()}
        self.num_functions = {
            "min": 0,
            "max": 0,
            "mean": 0,
            "var": 0,
            LOWER_COL: 0,
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
        self.set_tab(FILTERS_TAB_TITLE, self.filter_box)
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
        df.loc[:, :"var"] = lambda: ipw.Checkbox(  # type: ignore
            value=False, description="", disabled=False, indent=False
        )

        def _lower() -> ipw.Dropdown:
            return ipw.Dropdown(
                options=[
                    ("", 0),
                    ("5%", 5),
                    ("10%", 10),
                    ("15%", 15),
                    ("20%", 20),
                    ("25%", 25),
                ],
                value=0,
                layout=ipw.Layout(width="60px"),
            )

        df.loc[:, LOWER_COL] = _lower

        def _upper() -> ipw.Dropdown:
            return ipw.Dropdown(
                options=[
                    ("75%", 75),
                    ("80%", 80),
                    ("85%", 85),
                    ("90%", 90),
                    ("95%", 95),
                    ("", 100),
                ],
                value=100,
                layout=ipw.Layout(width="60px"),
            )

        df.loc[:, UPPER_COL] = _upper
        if self.gb_num is not None:
            for i, row in self.gb_num.df.iterrows():
                if i in df.index:
                    df.loc[i] = row
        grid = DataFrameGrid(df)
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
        df = pd.DataFrame(index=dt_cols, columns=self.dt_functions.keys(),
                          dtype=object)  # type: ignore
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
            cast(str, i): (row[0].value, row[1].value)
            for (i, row) in self.gb_num.df.loc[:, [LOWER_COL, UPPER_COL]].iterrows()
        }

    def get_checked_num(self, fnc: str) -> list[str]:
        assert self.gb_num
        return [self.untyped(cast(str, i))
                for (i, cell) in self.gb_num.df.loc[:, fnc].items() if cell.value]

    def untyped(self, tcol: str) -> str:
        return self.col_typed_names[tcol][0]

    def run(self, carrier: "FacadeCreatorW") -> None:
        num_bounds = self.get_num_bounds()
        max_num_cols = self.get_checked_num("max")
        min_num_cols = self.get_checked_num("min")
        s = carrier.input_module.scheduler()
        with s:
            inp = carrier.input_module
            for tcol, (lo, up) in num_bounds.items():
                if (lo, up) == (0, 100):
                    continue
                col = self.col_typed_names[tcol][0]
                kll = KLLSketch(column=col, scheduler=s)
                kll.params.quantiles = [lo / 100, up / 100]
                kll.params.named_quantiles = ["lower", "upper"]
                kll.input[0] = carrier.input_module.output[carrier.input_slot]
                range_qry = RangeQuery(column=col, quantiles=True, scheduler=s)
                range_qry.params.watched_key_lower = "lower"
                range_qry.params.watched_key_upper = "upper"
                range_qry.create_dependent_modules(
                    inp, "result", min_value=kll, max_value=kll
                )
                sink = Sink(scheduler=s)
                sink.input.inp = range_qry.output.result
                inp = range_qry
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
