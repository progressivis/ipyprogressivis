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

# from traitlets import HasTraits, observe, Instance

from typing import Dict, Any, Union, List, Optional, cast

logger = logging.getLogger(__name__)

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
        self._head: str = ""
        self._dialect: Optional[csv.Dialect] = None
        self.params: Dict[str, Any] = {}
        self.progressivis: Dict[str, Any] = {}
        self._df: Optional[pd.DataFrame] = None
        self._df2: Optional[pd.DataFrame] = None
        self._rename: Optional[List[str]] = None
        self._types: Optional[Dict[str, str]] = None
        #self.column_info: List[PColumnInfo] = []
        ## No widgets
        self.column: dict[str, PColumnInfo] = dict()
        self.delim_other: str = ""
        self.delimiter: str = ""
        self.tab_selected_index = 0
        self.cmdline: str = ""
        self.lines: int = 100
        self.head_text: str = ""
        self.df_text: str = ""
        self.df2_text: str = ""
        self._rename: list[str] | None = None
        self.dayfirst = False
        self.date_format = "mixed"
        self.header = -1
        self.skiprows = 0
        self.true_values = ""
        self.false_values = ""
        self.na_values = ""
        self.clear()
        self.dataframe()

    def _parse_list(self, key: str, values: str) -> None:
        split = [s for s in values.split(",") if s]
        if split:
            self.params[key] = split
        else:
            self.params.pop(key, None)
        self.set_cmdline()
        return self

    def set_delimiter(self, delim: str) -> None:
        if delim == "skip":
            delim = " "
            if self.params.get("skipinitialspace"):
                return
            self.params["skipinitialspace"] = True
        self.dialect()
        assert self._dialect is not None
        self._dialect.delimiter = delim  # TODO check valid delim
        self.delim_other = delim
        self.delimiter = delim
        self.tab_selected_index = 1
        if self._df is not None:
            self._reset()
        else:
            self.params = _merge_with_dialect_properties(self._dialect, self.params)
        self.dataframe(force=True)
        return self

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
        params: dict[str, Any] = {}
        for key, val in self.params.items():
            default = _parser_defaults[key]
            if val == default:
                continue
            params[key] = val
        return params
    
    def set_cmdline(self) -> None:
        params = self.kwargs()
        self.cmdline = str(params)
        if self.progressivis:
            self.cmdline += "\nFilters: " + str(self.progressivis)

    def clear(self) -> None:
        self.lines = 100
        self._head = ""
        self.head_text = '<pre style="white-space: pre"></pre>'
        self.df_text = ""
        self._dialect = None
        self._reset()

    def _format_head(self) -> None:
        self.head_text = (
            '<pre style="white-space: pre">' + quote_html(self._head) + "</pre>"
        )

    def head(self) -> str:
        if self._head:
            return self._head
        with fsspec.open(self.path, mode="rt", compression="infer") as inp:
            lineno = 0
            # TODO assumes that newline is correctly specified to fsspec
            for line in inp:
                if line and lineno < self.lines:
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
                self.header = 0
        else:
            self.header = self.params["header"]
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
            self.df_text = f"""
<pre style="white-space: pre">Error {quote_html(repr(e))}</pre>
"""
        else:
            with pd.option_context(
                "display.max_rows", self.lines, "display.max_columns", 0
            ):
                self.df_text = self._df._repr_html_()  # type: ignore
        self.dataframe_to_columns()
        self.dataframe_to_params()
        return self # ._df

    def test_cmd(self) -> None:
        strin = io.StringIO(self.head())
        try:
            self._df2 = cast(pd.DataFrame, pd.read_csv(strin, **self.params))
        except ValueError as e:
            self._df2 = None
            self.df2_text = f"""
<pre style="white-space: pre">Error {quote_html(repr(e))}</pre>
"""
        else:
            with pd.option_context(
                "display.max_rows", self.lines, "display.max_columns", 0
            ):
                self.df2_text = self._df2._repr_html_()  # type: ignore
        #self.tab.selected_index = 2
        return self

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
            #self.columns.options = []
            #self.columns.disabled = True
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
        #self.columns = list(df.columns)
        # self.show_column(df.columns[0])

    def rename_columns(self) -> None:
        assert self._df is not None
        names = [self.column[col].rename for col in self._df.columns]
        self._rename = names
        self.params["names"] = names
        # print(f"Renames: {names}")
        self.set_cmdline()
        return self
    
    def usecols_columns(self) -> None:
        assert self._df is not None
        names = [col for col in self._df.columns if self.column[col].use]
        if names == list(self._df.columns):
            if "usecols" in self.params:
                del self.params["usecols"]
        else:
            self.params["usecols"] = names
        self.set_cmdline()
        return self
    
    def retype_columns(self) -> None:
        types: Dict[str, str] = {}
        parse_dates: List[str] = []
        assert self._df is not None
        for name in list(self._df.columns):
            col = self.column[name]
            if col.use and col.default_type != col.retype:
                type = col.retype
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
            self.params["dayfirst"] = self.dayfirst
            self.params["date_format"] = self.date_format
        self.set_cmdline()
        return self

    def na_values_columns(self) -> None:
        assert self._df is not None
        na_values: Dict[str, Any] = {}
        for name in list(self._df.columns):
            col = self.column[name]
            raw = col.na_values_
            if raw:
                na_values[name] = raw.split(",")
            else:
                na_values.pop(name, None)
        if na_values:
            self.params["na_values"] = na_values
        else:
            self.params.pop("na_values", None)
        self.set_cmdline()
        return self

    def filtering_columns(self) -> None:
        assert self._df is not None
        f_values: Dict[str, Any] = {}
        for name in list(self._df.columns):
            col = self.column[name]
            if not col.filtering_ck:
                continue
            val = []
            for pred in col.filtering_group.children:
                if pred.children[0]:
                    val.append([v for v in pred.children])
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

    def update_backend(self, proxy, force=False):
        col_options = proxy.that.columns.widget.options
        for col, _ in col_options:
            c_use = proxy.lookup(f"c_use_{col}").widget.value
            col_info = self.column[col]
            col_info.use = c_use
            if not c_use:
                continue
            col_info.retype = proxy.lookup(f"c_retype_{col}").widget.value
            col_info.rename = proxy.lookup(f"c_rename_{col}").widget.value
        self.usecols_columns()
        self.delimiter = proxy.that.delimiter.widget.value
        self.dayfirst = proxy.that.dayfirst.widget.value
        self.date_format = proxy.that.date_format.widget.value
        self.header = proxy.that.header.widget.value
        self.skiprows = proxy.that.skiprows.widget.value
        self.true_values = proxy.that.true_values.widget.value
        self.false_values = proxy.that.false_values.widget.value
        self.na_values = proxy.that.na_values.widget.value
        if force:
            self.dataframe(force=True)
        return self

    def sync_cmdline(self, proxy):
        self.update_backend(proxy)
        proxy.that.cmdline.attrs(value=self.cmdline)
        return self

    def this(self, proxy):
        return proxy

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
        self.name = series.name
        self.type = series.dtype.name
        self.use = True
        self.rename = series.name
        self.retype = series.dtype.name
        self.nunique = f"{series.nunique()}/{len(series)}"
        self.na_values_ck = False
        self.na_values_ = ""
        self.na_values_sep = ""
        self.filtering_ck = False
        operators = [
            "",
            ">",
            "<",
            ">=",
            "<=",
            "==",
            "!=",
        ]

    def retype_values(self) -> List[str]:
        type = self.series.dtype.name
        if type in self.numeric_types:
            return self.numeric_types
        elif type == "object":
            return self.object_types + self.numeric_types
        return [type]

    def retype_column(self, change: Dict[str, Any]) -> None:
        self.sniffer.retype_columns()

    def set_attributes(self, **kw):
        for k, w in kw.items():
            setattr(self, k, w)
        return self
