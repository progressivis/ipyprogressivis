"""
Sniffer for Pandas csv_read, allows interactive specification of data types,
names, and various parameters before loading the whole file.
"""
from __future__ import annotations

import csv
import inspect
import io
import logging

# import pprint

import pandas as pd
import fsspec  # type: ignore
from ipywidgets import widgets  # type: ignore

# from traitlets import HasTraits, observe, Instance

from typing import Dict, Any, Union, List, Optional, cast

logger = logging.getLogger(__name__)


def set_child(box: widgets.Box, i: int, child: Any) -> None:
    lst = list(box.children)
    lst[i] = child
    box.children = lst


def quote_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


_parser_defaults: Dict[str, Any] = {
    key: val.default
    for key, val in inspect.signature(pd.read_csv).parameters.items()
    if val.default is not inspect._empty and key not in ("mangle_dupe_cols", "index_col")
}

# Borrowed from pandas
MANDATORY_DIALECT_ATTRS = (
    "delimiter",
    "doublequote",
    "escapechar",
    "skipinitialspace",
    "quotechar",
    "quoting"
)


def _merge_with_dialect_properties(
    dialect: Optional[csv.Dialect], defaults: Dict[str, Any]
) -> Dict[str, Any]:
    if not dialect:
        return defaults
    kwds = defaults.copy()

    for param in MANDATORY_DIALECT_ATTRS:
        dialect_val = getattr(dialect, param)

        parser_default = _parser_defaults[param]
        provided = kwds.get(param, parser_default)

        # Messages for conflicting values between the dialect
        # instance and the actual parameters provided.
        conflict_msgs = []

        # Don't warn if the default parameter was passed in,
        # even if it conflicts with the dialect (gh-23761).
        if provided != parser_default and provided != dialect_val:
            msg = (
                f"Conflicting values for '{param}': '{provided}' was "
                f"provided, but the dialect specifies '{dialect_val}'. "
                "Using the dialect-specified value."
            )

            # Annoying corner case for not warning about
            # conflicts between dialect and delimiter parameter.
            # Refer to the outer "_read_" function for more info.
            if not (param == "delimiter" and kwds.pop("sep_override", False)):
                conflict_msgs.append(msg)

        if conflict_msgs:
            print("\n\n".join(conflict_msgs))
        kwds[param] = dialect_val
    return kwds


class CSVSniffer:
    """
    Non progressive class to assist in specifying parameters
    to a CSV module
    """

    signature = inspect.signature(pd.read_csv)
    delimiters = [",", ";", "<TAB>", "<SPACE>", ":", "skip initial space"]
    del_values = [",", ";", "\t", " ", ":", "skip"]

    def __init__(self, path: str, lines: int = 100, **args: Any) -> None:
        self.path = path
        self._args = args
        self.lines = widgets.BoundedIntText(
            value=lines, min=10, max=1000, continuous_update=False, description="Lines:"
        )
        self.lines.observe(self._lines_cb, names="value")
        self._head: str = ""
        self._dialect: Optional[csv.Dialect] = None
        self.params: Dict[str, Any] = {}
        self.progressivis: Dict[str, Any] = {}
        self._df: Optional[pd.DataFrame] = None
        self._df2: Optional[pd.DataFrame] = None
        self._rename: Optional[List[str]] = None
        self._types: Optional[Dict[str, str]] = None
        # Widgets
        layout = widgets.Layout(border="solid")
        self.head_text = widgets.HTML()
        self.df_text = widgets.HTML()
        self.df2_text = widgets.HTML()
        self.error_msg = widgets.Textarea(description="Error:")
        self.tab = widgets.Tab(
            [self.head_text, self.df_text, self.df2_text, widgets.Label()],
            layout=widgets.Layout(max_height="1024px"),
        )
        for i, title in enumerate(["Head", "DataFrame", "DataFrame2", "Hide"]):
            self.tab.set_title(i, title)
        # Delimiters
        self.delimiter = widgets.RadioButtons(
            orientation="vertical",
            options=list(zip(self.delimiters, self.del_values)),
        )
        self.delimiter.observe(self._delimiter_cb, names="value")
        self.delim_other = widgets.Text()
        self.delim_other.observe(self._delimiter_cb, names="value")
        self.delimiter = widgets.VBox(
            [
                # widgets.Label("Delimiter"),
                self.delim_other,
                self.delimiter,
            ],
            layout=layout,
        )
        # Dates
        # TODO
        self.dayfirst = widgets.Checkbox(description="Dayfirst", value=False)
        self.date_format = widgets.Text(description="Date format:",
                                        value="mixed",
                                        disabled=(pd.__version__[0] < '2'))
        self.date = widgets.VBox(
            [self.dayfirst, self.date_format], layout=layout
        )
        # Header
        self.header = widgets.BoundedIntText(
            value=-1, min=-1, max=1000, continuous_update=False, description="Header:"
        )
        self.skiprows = widgets.BoundedIntText(
            value=0, min=0, max=1000, continuous_update=False, description="Skip rows:"
        )
        self.skiprows.observe(self._skiprows_cb, names="value")
        # Special values
        self.true_values = widgets.Text(
            description="True values", continuous_update=False
        )
        self.true_values.observe(self._true_values_cb, names="value")
        self.false_values = widgets.Text(
            description="False values", continuous_update=False
        )
        self.false_values.observe(self._false_values_cb, names="value")
        self.na_values = widgets.Text(description="NA values", continuous_update=False)
        self.na_values.observe(self._na_values_cb, names="value")
        self.special_values = widgets.VBox(
            [self.true_values, self.false_values, self.na_values], layout=layout
        )

        # Global tab with Delimiters and Dates
        self.global_tab = widgets.Tab(
            [
                self.delimiter,
                self.date,
                widgets.VBox([self.lines, self.header, self.skiprows], layout=layout),
                self.special_values,
            ]
        )
        for i, title in enumerate(["Delimiters", "Dates", "Header", "Special values"]):
            self.global_tab.set_title(i, title)

        # PColumn selection
        self.columns = widgets.Select(disabled=True, rows=9)
        self.columns.observe(self._columns_cb, names="value")
        self.enable_all = widgets.Checkbox(description="Enable/disable all", value=True)
        self.enable_all.observe(self._enable_all_cb, names="value")
        # PColumn details
        self.column: Dict[str, PColumnInfo] = {}
        self.no_detail = widgets.Label(value="No PColumn Selected")
        self.details = widgets.Box([self.no_detail], label="Details")
        # Toplevel Box
        self.top = widgets.HBox(
            [
                self.global_tab,
                widgets.VBox(
                    [widgets.Label("PColumns"), self.columns, self.enable_all],
                    layout=layout,
                ),
                widgets.VBox(
                    [widgets.Label("Selected PColumn"), self.details],
                    layout=layout
                ),
            ]
        )
        self.cmdline = widgets.Textarea(layout=widgets.Layout(width="100%"),
                                        rows=3)
        self.testBtn = widgets.Button(description="Test")
        self.box = widgets.VBox(
            [
                self.top,
                widgets.HBox(
                    [self.testBtn, widgets.Label(value="CmdLine:"),
                     self.cmdline]
                ),
                self.tab,
            ]
        )
        self.testBtn.on_click(self.test_cmd)
        self.column_info: List[PColumnInfo] = []
        self.clear()
        self.dataframe()

    def _parse_list(self, key: str, values: str) -> None:
        split = [s for s in values.split(",") if s]
        if split:
            self.params[key] = split
        else:
            self.params.pop(key, None)
        self.set_cmdline()

    def _enable_all_cb(self, change: Dict[str, Any]) -> None:
        for col in self.column.values():
            col.use.value = change["new"]
            col.retype.disabled = not change["new"]

    def _true_values_cb(self, change: Dict[str, Any]) -> None:
        self._parse_list("true_values", change["new"])

    def _false_values_cb(self, change: Dict[str, Any]) -> None:
        self._parse_list("false_values", change["new"])

    def _na_values_cb(self, change: Dict[str, Any]) -> None:
        self._parse_list("na_values", change["new"])

    def _skiprows_cb(self, change: Dict[str, Any]) -> None:
        skip = change["new"]
        self._head = ""
        self.params["skiprows"] = skip
        self.dataframe(force=True)

    def _lines_cb(self, change: Dict[str, Any]) -> None:
        self._head = ""
        self.dataframe(force=True)

    def _delimiter_cb(self, change: Dict[str, Any]) -> None:
        delim = change["new"]
        # print(f"Delimiter: '{delim}'")
        self.set_delimiter(delim)

    def _columns_cb(self, change: Dict[str, Any]) -> None:
        column = change["new"]
        # print(f"PColumn: '{column}'")
        self.show_column(column)

    def set_delimiter(self, delim: str) -> None:
        if delim == "skip":
            delim = " "
            if self.params.get("skipinitialspace"):
                return
            self.params["skipinitialspace"] = True
        self.dialect()
        assert self._dialect is not None
        self._dialect.delimiter = delim  # TODO check valid delim
        self.delim_other.value = delim
        self.delimiter.value = delim
        self.tab.selected_index = 1
        if self._df is not None:
            self._reset()
        else:
            self.params = _merge_with_dialect_properties(self._dialect, self.params)
        self.dataframe(force=True)

    def _reset(self) -> None:
        args = self._args.copy()
        self.params = {}
        for name, param in self.signature.parameters.items():
            if name in ("index_col", "mangle_dupe_cols"):
                continue
            if name != "sep" and param.default is not inspect._empty:
                self.params[name] = args.pop(name, param.default)
        self.params = _merge_with_dialect_properties(self._dialect, self.params)
        self.set_cmdline()
        if args:
            raise ValueError(f"extra keywords arguments {args}")

    def kwargs(self) -> Dict[str, Any]:
        "Return the arguments to pass to pandas.csv_read"
        params: Dict[str, Any] = {}
        for key, val in self.params.items():
            default = _parser_defaults[key]
            if val == default:
                continue
            params[key] = val
        return params

    def set_cmdline(self) -> None:
        params = self.kwargs()
        self.cmdline.value = str(params)
        if self.progressivis:
            self.cmdline.value += "\nFilters: " + str(self.progressivis)

    def clear(self) -> None:
        self.lines.value = 100
        self._head = ""
        self.head_text.value = '<pre style="white-space: pre"></pre>'
        self.df_text.value = ""
        self._dialect = None
        self._reset()

    def _format_head(self) -> None:
        self.head_text.value = (
            '<pre style="white-space: pre">' + quote_html(self._head) + "</pre>"
        )

    def head(self) -> str:
        if self._head:
            return self._head
        with fsspec.open(self.path, mode="rt", compression="infer") as inp:
            lineno = 0
            # TODO assumes that newline is correctly specified to fsspec
            for line in inp:
                if line and lineno < self.lines.value:
                    self._head += line
                    lineno += 1
                else:
                    break
        self._format_head()
        return self._head

    def dialect(self, force: bool = False) -> csv.Dialect:
        if not force and self._dialect:
            return self._dialect
        sniffer = csv.Sniffer()
        head = self.head()
        self._dialect = sniffer.sniff(head)  # type: ignore
        # self.params['dialect'] = self._dialect
        assert self._dialect is not None
        self.set_delimiter(self._dialect.delimiter)
        if self.params["header"] == "infer":
            if sniffer.has_header(head):
                self.params["header"] = 0
                self.header.value = 0
        else:
            self.header.value = self.params["header"]
        return self._dialect

    def dataframe(self, force: bool = False) -> Optional[pd.DataFrame]:
        if not force and self._df is not None:
            return self._df
        self.dialect()
        strin = io.StringIO(self.head())
        try:
            # print(f"read_csv params: {self.params}")
            self._df = cast(pd.DataFrame, pd.read_csv(strin, **self.params))
            self.column = {}
        except ValueError as e:
            self._df = None
            self.df_text.value = f"""
<pre style="white-space: pre">Error {quote_html(repr(e))}</pre>
"""
        else:
            with pd.option_context(
                "display.max_rows", self.lines.value, "display.max_columns", 0
            ):
                self.df_text.value = self._df._repr_html_()  # type: ignore
        self.dataframe_to_columns()
        self.dataframe_to_params()
        return self._df

    def test_cmd(self, button: widgets.Button) -> None:
        strin = io.StringIO(self.head())
        try:
            self._df2 = cast(pd.DataFrame, pd.read_csv(strin, **self.params))
        except ValueError as e:
            self._df2 = None
            self.df2_text.value = f"""
<pre style="white-space: pre">Error {quote_html(repr(e))}</pre>
"""
        else:
            with pd.option_context(
                "display.max_rows", self.lines.value, "display.max_columns", 0
            ):
                self.df2_text.value = self._df2._repr_html_()  # type: ignore
        self.tab.selected_index = 2

    def dataframe_to_params(self) -> None:
        df = self._df
        if df is None:
            return
        # if self.params['names'] is None:
        #     self.params['names'] = list(df.columns)
        # # TODO test for existence?
        self.set_cmdline()

    def dataframe_to_columns(self) -> None:
        df = self._df
        col: Union[pd.Series[Any], PColumnInfo]
        if df is None:
            self.columns.options = []
            self.columns.disabled = True
            return
        for column in df.columns:
            col = df[column]
            if self.column.get(column) is None:
                col = PColumnInfo(self, col)
                self.column[column] = col
        for column in list(self.column):
            if column not in df.columns:
                col = self.column[column]
                col.box.close()
                del self.column[column]
        self.columns.options = list(df.columns)
        self.columns.disabled = False
        self.show_column(df.columns[0])

    def show_column(self, column: str) -> None:
        if column not in self.column:
            # print(f"Not in columns: '{column}'")
            self.details.children = [self.no_detail]
            return
        col = self.column[column]
        self.details.children = [col.box]

    def rename_columns(self) -> None:
        assert self._df is not None
        names = [self.column[col].rename.value for col in self._df.columns]
        self._rename = names
        # print(f"Renames: {names}")

    def usecols_columns(self) -> None:
        assert self._df is not None
        names = [col for col in self._df.columns if self.column[col].use.value]
        if names == list(self._df.columns):
            del self.params["usecols"]
        else:
            self.params["usecols"] = names
        self.set_cmdline()

    def retype_columns(self) -> None:
        types: Dict[str, str] = {}
        parse_dates: List[str] = []
        assert self._df is not None
        for name in list(self._df.columns):
            col = self.column[name]
            if col.use.value and col.default_type != col.retype.value:
                type = col.retype.value
                if type == "datetime":
                    types[name] = "str"
                    parse_dates.append(name)
                else:
                    types[name] = type
        if types:
            self._types = {col: typ for (col, typ) in types.items() if col not in parse_dates}
            if not self._types:
                self._types = None
            else:
                self.params["dtype"] = self._types
        else:
            self._types = None
            if "dtype" in self.params:
                del self.params["dtype"]
        if parse_dates:
            self.params["parse_dates"] = parse_dates
            self.params["dayfirst"] = self.dayfirst.value
            if pd.__version__[0] >= '2':
                self.params["date_format"] = self.date_format.value
        else:
            self.params["parse_dates"] = None
            self.params["dayfirst"] = False
            if pd.__version__[0] >= '2':
                self.params["date_format"] = None
        self.set_cmdline()

    def na_values_columns(self) -> None:
        assert self._df is not None
        na_values: Dict[str, Any] = {}
        for name in list(self._df.columns):
            col = self.column[name]
            if not col.na_values_ck.value:
                continue
            if col.na_values_sep.value:
                val = col.na_values_.value.split(col.na_values_sep.value)
            else:
                val = col.na_values_.value
            na_values[name] = val
        if na_values:
            self.params["na_values"] = na_values
        else:
            self.params["na_values"] = None
        self.set_cmdline()

    def filtering_columns(self) -> None:
        assert self._df is not None
        f_values: Dict[str, Any] = {}
        for name in list(self._df.columns):
            col = self.column[name]
            if not col.filtering_ck.value:
                continue
            val = []
            for pred in col.filtering_group.children:
                if pred.children[0].value:
                    val.append([v.value for v in pred.children])
            if val:
                f_values[name] = val
        if f_values:
            self.progressivis["filter_values"] = f_values
        else:
            self.progressivis["filter_values"] = None
        self.set_cmdline()

    def load_dataframe(self) -> pd.DataFrame:
        "Full load the DataFrame with the GUI parameters"
        return cast(pd.DataFrame, pd.read_csv(self.path, **self.params))


class PColumnInfo:
    numeric_types = [
        "int8",
        "uint8",
        "int16",
        "uint16",
        "int32",
        "int64",
        "uint32",
        "uint64",
        "float32",
        "float64",
        "str",
    ]
    object_types = ["object", "str", "category", "datetime"]

    def __init__(self, sniffer: CSVSniffer, series: pd.Series[Any]):
        self.sniffer = sniffer
        self.series = series
        self.default_type = series.dtype.name
        self.name = widgets.Text(
            description="Name:",
            value=series.name,
            continuous_update=False,
            disabled=True,
        )
        self.type = widgets.Text(
            description="Type:", value=series.dtype.name, disabled=True
        )
        self.use = widgets.Checkbox(description="Use", value=True)
        self.use.observe(self.usecols_column, names="value")
        self.rename = widgets.Text(description="Rename:", value=series.name)
        self.rename.observe(self.rename_column, names="value")
        self.retype = widgets.Dropdown(
            description="Retype:", options=self.retype_values(), value=series.dtype.name
        )
        self.retype.observe(self.retype_column, names="value")
        self.nunique = widgets.Text(
            description="Unique vals:",
            value=f"{series.nunique()}/{len(series)}",
            disabled=True,
        )
        self.na_values_ck = widgets.Checkbox(
            description="NA values",
            indent=True,
            value=False,
            disabled=True,
        )
        self.na_values_ck.observe(self.na_values_ck_cb, "value")
        self.na_values_ = widgets.Text(description="NA values:", value="")
        self.na_values_.observe(self.na_values_cb, "value")
        self.na_values_sep = widgets.Text(
            description="Separator:",
            value="",
            placeholder="if many values",
            disabled=True,
        )
        self.na_values_sep.observe(self.na_values_sep_cb, "value")
        self.filtering_ck = widgets.Checkbox(
            description="Filtering", indent=True, value=False
        )
        operators = [
            "",
            ">",
            "<",
            ">=",
            "<=",
            "==",
            "!=",
        ]
        fit = widgets.Layout(max_width="fit-content")
        wft = widgets.Layout(max_width="100px")
        self.filtering_group = widgets.VBox([
            widgets.HBox([widgets.Dropdown(description="Filter",
                                           options=operators, layout=fit),
                          widgets.FloatText(layout=wft)], layout=fit),
            widgets.HBox([widgets.Dropdown(description="& ",
                                           options=operators, layout=fit),
                          widgets.FloatText(layout=wft)], layout=fit)
            ])
        self.filtering_ck.observe(self.filtering_ck_cb, "value")
        for hbox in self.filtering_group.children:
            for wg in hbox.children:
                wg.observe(self.filtering_value_cb, "value")
        self.box = widgets.VBox()
        self.box.children = [
            self.name,
            self.rename,
            self.type,
            self.retype,
            self.use,
            self.nunique,
            self.na_values_ck,
            self.filtering_ck,
        ]

    def retype_values(self) -> List[str]:
        type = self.series.dtype.name
        if type in self.numeric_types:
            return self.numeric_types
        elif type == "object":
            return self.object_types + self.numeric_types
        return [type]

    def rename_column(self, change: Dict[str, Any]) -> None:
        self.sniffer.rename_columns()

    def usecols_column(self, change: Dict[str, Any]) -> None:
        self.sniffer.usecols_columns()
        if change["new"]:
            for wg in self.box.children:
                if wg in [self.use, self.type, self.name, self.nunique]:
                    continue
                wg.disabled = False
            self.na_values_ck.disabled = False
            self.filtering_ck.disabled = False
        else:
            for wg in self.box.children:
                if wg in [self.use, self.type, self.name, self.nunique]:
                    continue
                wg.disabled = True
            self.na_values_ck.value = False
            self.na_values_ck.disabled = True
            self.filtering_ck.value = False
            self.filtering_ck.disabled = True

    def na_values_ck_cb(self, change: Dict[str, Any]) -> None:
        if change["new"]:
            set_child(self.box, 6, widgets.VBox([
                self.na_values_ck,
                self.na_values_,
                self.na_values_sep
                ]))
        else:
            self.na_values_.value = ""
            self.na_values_sep.value = ""
            set_child(self.box, 6, self.na_values_ck)

    def filtering_ck_cb(self, change: Dict[str, Any]) -> None:
        if change["new"]:
            set_child(self.box, 7, widgets.VBox([
                self.filtering_ck,
                self.filtering_group
                ]))
        else:
            set_child(self.box, 7, self.filtering_ck)
            for hbox in self.filtering_group.children:
                hbox.children[0].value = None
                hbox.children[1].value = 0.0

    def na_values_cb(self, change: Dict[str, Any]) -> None:
        if change["new"]:
            self.na_values_sep.disabled = False
        else:
            self.na_values_sep.value = ""
            self.na_values_sep.disabled = True
        self.sniffer.na_values_columns()

    def na_values_sep_cb(self, change: Dict[str, Any]) -> None:
        self.sniffer.na_values_columns()

    def filtering_value_cb(self, change: Dict[str, Any]) -> None:
        self.sniffer.filtering_columns()

    def _test_column_type(self, newtype: Any) -> Optional[ValueError]:
        try:
            self.series.as_type(newtype)
        except ValueError as e:
            return e
        return None

    def retype_column(self, change: Dict[str, Any]) -> None:
        self.sniffer.retype_columns()
