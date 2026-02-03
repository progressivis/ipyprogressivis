from ipyprogressivis.ipyfuncs import Proxy

from typing import Any

def enable_all(proxy, change: dict[str, Any]) -> None:
    for col, _, in proxy.that.columns.widget.options:
        proxy.lookup(f"c_use_{col}").attrs(value=change["new"])
        proxy.lookup(f"c_retype_{col}").attrs(disabled=not change["new"])
    backend = proxy.back("sniffer")
    update_backend(proxy)

def true_values(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    backend._parse_list("true_values", change["new"])

def false_values(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    backend._parse_list("false_values", change["new"])

def na_values(proxy, change: dict[str, Any]) -> None:
    backend = proxy.back("sniffer")
    backend._parse_list("na_values", change["new"])

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

def columns(proxy, change: dict[str, Any]) -> None:
    column = change["new"]
    proxy.that.sel_column_stk.attrs(selected_index=column)

def set_delimiter(proxy, delim: str) -> None:
    backend = proxy.back("sniffer")    
    if delim == "skip":
        delim = " "
        if backend.params.get("skipinitialspace"):
            return
        backend.params["skipinitialspace"] = True
    backend.dialect()
    assert backend._dialect is not None
    backend._dialect.delimiter = delim  # TODO check valid delim
    backend.delim_other.value = delim
    backend.delimiter.value = delim
    backend.tab.selected_index = 1
    if backend._df is not None:
        backend._reset()
    else:
        backend.params = _merge_with_dialect_properties(backend._dialect, backend.params)
    backend.dataframe(force=True)

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

def retype_column(proxy, change: dict[str, Any]) -> None:
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
    backend.rename_columns()


    
def usecols_column(proxy, change: dict[str, Any]) -> None:
    new_val = change["new"]
    backend = proxy.back("sniffer")
    col_name = proxy.hint.col
    col_info = backend.column[col_name]
    col_info.use = new_val
    col_options = proxy.that.columns.widget.options
    update_backend(proxy)
    backend.usecols_columns()
    for temp in "c_rename_{}", "c_retype_{}", "c_na_ck_{}":
        uid = temp.format(col_name)
        proxy.lookup(uid).attrs(disabled=not new_val)
    cmdline = proxy.that.cmdline
    cmdline.attrs(value=backend.cmdline)
        

def na_values_ck(proxy, change: dict[str, Any]) -> None:
    """if change["new"]:
        set_child(proxy.box, 6, widgets.VBox([
            proxy.na_values_ck,
            proxy.na_values_,
            proxy.na_values_sep
            ]))
    else:
        proxy.na_values_.value = ""
        proxy.na_values_sep.value = ""
        set_child(proxy.box, 6, proxy.na_values_ck)"""
    new_val = change["new"]
    backend = proxy.back("sniffer")
    col_name = proxy.hint.col
    col_info = backend.column[col_name]    

def filtering_ck(proxy, change: dict[str, Any]) -> None:
    """if change["new"]:
        set_child(proxy.box, 7, widgets.VBox([
            proxy.filtering_ck,
            proxy.filtering_group
            ]))
    else:
        set_child(proxy.box, 7, proxy.filtering_ck)
        for hbox in proxy.filtering_group.children:
            hbox.children[0].value = None
            hbox.children[1].value = 0.0"""

def na_values(proxy, change: dict[str, Any]) -> None:
    """if change["new"]:
        proxy.na_values_sep.disabled = False
    else:
        proxy.na_values_sep.value = ""
        proxy.na_values_sep.disabled = True
    proxy.sniffer.na_values_columns()"""

def na_values_sep(proxy, change: dict[str, Any]) -> None:
    #proxy.sniffer.na_values_columns()
    pass

def filtering_value(proxy, change: dict[str, Any]) -> None:
    #proxy.sniffer.filtering_columns()
    pass

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
