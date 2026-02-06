from ipyprogressivis.ipyfuncs import Proxy

from typing import Any


def sync_cmdline(proxy):
    backend = proxy.back("sniffer")
    update_backend(proxy)
    proxy.that.cmdline.attrs(value=backend.cmdline)
    

def _enable_all(proxy, change: dict[str, Any]) -> None:
    for col, _, in proxy.that.columns.widget.options:
        (
            proxy.lookup(f"c_use_{col}")
            .attrs(value=change["new"])
            .lookup(f"c_retype_{col}")
            .attrs(disabled=not change["new"])
            .lookup(f"c_rename_{col}")
            .attrs(disabled=not change["new"])
        )
        #proxy.lookup(f"c_retype_{col}").attrs(disabled=not change["new"])
    backend = proxy.back("sniffer")
    update_backend(proxy)
    proxy.that.cmdline.attrs(value=backend.cmdline)

def true_values(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    backend._parse_list("true_values", change["new"])
    proxy.that.cmdline.attrs(value=backend.cmdline)
    
def false_values(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    backend._parse_list("false_values", change["new"])
    proxy.that.cmdline.attrs(value=backend.cmdline)

def na_values(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    backend._parse_list("na_values", change["new"])
    proxy.that.cmdline.attrs(value=backend.cmdline)
    
def skiprows(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")    
    skip = change["new"]
    backend._head = ""
    backend.params["skiprows"] = skip
    backend.dataframe(force=True)

def lines(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    backend._head = ""
    backend.dataframe(force=True)

def delimiter(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    delim = change["new"]
    # print(f"Delimiter: '{delim}'")
    backend.set_delimiter(delim)
    backend.dataframe(force=True)
    proxy.that.columns.attrs(options=[(col, i) for (i, col) in enumerate(backend.column.keys())])
    proxy.that.cmdline.attrs(value=backend.cmdline)
    
def columns(proxy, change: dict[str, Any]) -> None:
    column = change["new"]
    proxy.that.sel_column_stk.attrs(selected_index=column)
    

# Col info

def update_backend(proxy, force=False):
    backend = proxy.back("sniffer")
    col_options = proxy.that.columns.widget.options
    for col, _ in col_options:
        c_use = proxy.lookup(f"c_use_{col}").widget.value
        col_info = backend.column[col]
        col_info.use = c_use
        if not c_use:
            continue
        col_info.retype = proxy.lookup(f"c_retype_{col}").widget.value
    backend.usecols_columns()
    backend.delimiter = proxy.that.delimiter.widget.value
    backend.dayfirst = proxy.that.dayfirst.widget.value
    backend.date_format = proxy.that.date_format.widget.value
    backend.header = proxy.that.header.widget.value
    backend.skiprows = proxy.that.skiprows.widget.value
    backend.true_values = proxy.that.true_values.widget.value
    backend.false_values = proxy.that.false_values.widget.value
    backend.na_values = proxy.that.na_values.widget.value
    if force:
        backend.dataframe(force=True)

def __retype_column(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    update_backend(proxy)
    backend.retype_columns()
    proxy.that.cmdline.attrs(value=backend.cmdline)
    

        
def retype_values(proxy) -> List[str]:
    backend = proxy.back("sniffer")
    col_name = proxy.hint.col
    col_info = backend.column[col_name]
    type = col_info.series.dtype.name
    if type in col_info.numeric_types:
        return col_info.numeric_types
    elif type == "object":
        return col_info.object_types + col_info.numeric_types
    return [type]

def rename_column(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    col_name = proxy.hint.col
    col_info = backend.column[col_name]
    col_info.rename = change["new"]
    backend.rename_columns()
    backend.sync_cmdline(proxy)

def per_column_na(proxy, change: dict[str, Any]) -> None:
    if change["new"]:
        proxy.that.na_values.attrs(value="")
    proxy.that.na_values.attrs(disabled=change["new"])
    col_options = proxy.that.columns.widget.options
    for col, _ in col_options:
        c_use = proxy.lookup(f"c_use_{col}").widget.value
        if not c_use:
            continue
        proxy.lookup(f"c_na_{col}").attrs(
            disabled=not change["new"],
            value="")

def __usecols_column(proxy, change: dict[str, Any]) -> None:
    new_val = change["new"]
    backend = proxy.back("sniffer")
    col_name = proxy.hint.col
    col_info = backend.column[col_name]
    col_info.use = new_val
    col_options = proxy.that.columns.widget.options
    update_backend(proxy)
    backend.usecols_columns()
    for temp in "c_rename_{}", "c_retype_{}", "c_na_{}":
        uid = temp.format(col_name)
        proxy.lookup(uid).attrs(disabled=not new_val)
    if not proxy.that.per_col_na.widget.value:
        proxy.lookup(f"c_na_{col_name}").attrs(disabled=True)
    cmdline = proxy.that.cmdline
    cmdline.attrs(value=backend.cmdline)
        


def __na_values_col(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    col_name = proxy.hint.col
    backend.column[col_name].na_values_ = change["new"]
    backend.na_values_columns()
    cmdline = proxy.that.cmdline
    cmdline.attrs(value=backend.cmdline)


def test_column_type(proxy, newtype: Any) -> ValueError | None:
    """try:
        proxy.series.as_type(newtype)
    except ValueError as e:
        return e
    return None"""


def test_cmd(proxy, button: widgets.Button) -> None:
    backend = proxy.back("sniffer")
    update_backend(proxy)
    backend.test_cmd()
    proxy.that.df2_text.attrs(value=backend.df2_text)
    proxy.that.tab.attrs(selected_index=2)

    
