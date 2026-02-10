import ipywidgets as ipw
from ipyprogressivis.ipyfuncs import *
import ipywidgets as ipw
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROWS = 20


def _cleanup(raw: str) -> list[str]:
    return [line for line in raw.strip().split("\n")[1:] if "<pyarrow" not in line]


layout = ipw.Layout(width="60%")
layout_solid = ipw.Layout(border="solid")


class ColInfo:
    def __init__(
        self,
        pqfile: pq.ParquetFile,
        ix: int,
        dtype: Union[np.dtype[Any], ExtensionDtype],
    ) -> None:
        col_schema = _cleanup(str(pqfile.schema.column(ix)))
        col_meta = _cleanup(str(pqfile.metadata.row_group(0).column(ix)))  # type: ignore
        self.col_text = "\n".join(col_schema + col_meta)
        self.dtype = dtype
        self.use = True


def sniffer(url):
    pqfile = pq.ParquetFile(url)
    schema = pqfile.schema.to_arrow_schema()
    names = schema.names
    types = [t.to_pandas_dtype() for t in schema.types]
    info_cols: Dict[str, ColInfo] = {
        n: ColInfo(pqfile, i, np.dtype(types[i])) for (i, n) in enumerate(names)
    }

    types = [t.to_pandas_dtype() for t in schema.types]
    decorated = [(f"{n}:{np.dtype(t).name}", n) for (n, t) in zip(names, types)]
    return hbox(
        vbox(
            label("Columns"),
            select(rows=ROWS, options=decorated).uid("columns").observe(_columns_cb),
            checkbox(description="Select/Unselect all", value=True).observe(
                _select_all_cb
            ),
            layout=layout_solid,
        ).observe(_select_all_cb),
        vbox(
            label("Info"),
            stack(
                box(label("All column Selected").uid("details")),
                stack(
                    *[
                        vbox(
                            textarea(rows=ROWS, value=info_cols[nm].col_text).uid(
                                f"col_text_{nm}"
                            ),
                            checkbox(description="Use", value=info_cols[nm].use)
                            .uid(f"use_it_ck_{nm}")
                            .observe(_use_cb),
                        )
                        for nm in names
                    ]
                ).uid("col_stack"),
                selected_index=0,
            ).uid("details_stack"),
            layout=layout_solid,
        ),
        vbox(
            label("Selected"),
            select(rows=ROWS, options=decorated)
            .uid("kept_cols")
            .observe(_kept_cols_cb),
            layout=layout_solid,
        ),
        layout=layout_solid,
    )


def _select_all_cb(proxy, change: dict[str, AnyType]) -> None:
    proxy.that.details.attrs(
        value="All column Selected" if change["new"] else "No colums are selected"
    )
    col_stack = proxy.that.col_stack
    kept_cols = proxy.that.kept_cols
    columns = proxy.that.columns
    if change["new"]:
        kept_cols.attrs(options=columns.widget.options)
    else:
        kept_cols.attrs(options=[])
    for _, col in columns.widget.options:
        proxy.lookup(f"use_it_ck_{col}").attrs(value=change["new"])


def _kept_cols_cb(proxy, change: dict[str, AnyType]) -> None:
    proxy.that.columns.attrs(value=change["new"])


def _columns_cb(proxy, change: dict[str, AnyType]) -> None:
    col = change["new"]
    names = [n for (_, n) in proxy.widget.options]
    pos = names.index(col)
    col_stack = proxy.that.col_stack
    col_stack.attrs(selected_index=pos)
    details = proxy.that.details_stack
    details.attrs(selected_index=1)  # stack: label|box => choose box


def _use_cb(proxy, change: Dict[str, AnyType]) -> None:
    kept_cols = proxy.that.kept_cols
    columns = proxy.that.columns
    options = [
        (t, col)
        for (t, col) in columns.widget.options
        if proxy.lookup(f"use_it_ck_{col}").widget.value
    ]
    kept_cols.attrs(options=options)


def get_dtypes(proxy) -> Dict[str, str]:
    kept_cols = proxy.that.kept_cols
    return {col: tcol.split(":")[1] for (tcol, col) in kept_cols.widget.options}
