import ipywidgets as ipw
from ipyprogressivis.ipywel import (
    Proxy,
    Backend,
    box,
    vbox,
    hbox,
    stack,
    textarea,
    checkbox,
    select,
    label,
)
from progressivis.table.dshape import ExtensionDtype
import numpy as np
import pyarrow.parquet as pq
from typing import Any


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
        dtype: np.dtype[Any] | ExtensionDtype,
    ) -> None:
        col_schema = _cleanup(str(pqfile.schema.column(ix)))
        col_meta = _cleanup(str(pqfile.metadata.row_group(0).column(ix)))  # type: ignore
        self.col_text = "\n".join(col_schema + col_meta)
        self.dtype = dtype
        self.use = True


class ParquetSniffer:
    def __init__(self, url: str) -> None:
        pqfile = pq.ParquetFile(url)
        schema = pqfile.schema.to_arrow_schema()
        self.names = names = schema.names
        types = [t.to_pandas_dtype() for t in schema.types]
        self.info_cols: dict[str, ColInfo] = {
            n: ColInfo(pqfile, i, np.dtype(types[i])) for (i, n) in enumerate(names)
        }

        types = [t.to_pandas_dtype() for t in schema.types]
        self.decorated = [
            (f"{n}:{np.dtype(t).name}", n) for (n, t) in zip(names, types)
        ]


def _sniffer(bk: Backend) -> Proxy:
    return (
        hbox(
            vbox(
                label("Columns"),
                select(rows=ROWS, options=bk().decorated)
                .uid("columns")
                .observe(
                    lambda proxy, change: proxy.that.col_stack.attrs(
                        selected_index=[n for (_, n) in proxy.widget.options].index(
                            change["new"]
                        )
                    ).that.details_stack.attrs(
                        selected_index=1
                    )  # stack: label|box => choose box
                    # _columns_cb
                ),
                checkbox(description="Select/Unselect all", value=True)
                .uid("select_ck")
                .observe(
                    lambda proxy, change:
                    proxy.that.details.attrs(
                        value=(
                            "All column Selected"
                            if change["new"]
                            else "No colums are selected"
                        )
                    )
                    .proc(
                        proxy.that.kept_cols.attrs(options=proxy.that.columns.widget.options)
                        if change["new"]
                        else proxy.that.kept_cols.attrs(options=[])
                    )
                    .proc(
                        [
                            proxy.lookup(f"use_it_ck_{col}").attrs(value=change["new"])
                            for (_, col) in proxy.that.columns.widget.options
                        ]
                    ),
                    # _select_all_cb
                ),
                layout=layout_solid,
            ),
            vbox(
                label("Info"),
                stack(
                    box(label("All column Selected").uid("details")),
                    stack(
                        *[
                            vbox(
                                textarea(
                                    rows=ROWS, value=bk().info_cols[nm].col_text
                                ).uid(f"col_text_{nm}"),
                                checkbox(
                                    description="Use", value=bk().info_cols[nm].use
                                )
                                .uid(f"use_it_ck_{nm}")
                                .observe(
                                    lambda proxy, change: proxy.that.kept_cols.attrs(
                                        options=[
                                            (t, col)
                                            for (
                                                t,
                                                col,
                                            ) in proxy.that.columns.widget.options
                                            if proxy.lookup(
                                                f"use_it_ck_{col}"
                                            ).widget.value
                                        ]
                                    )
                                    # _use_cb
                                ),
                            )
                            for nm in bk().names
                        ]
                    ).uid("col_stack"),
                    selected_index=0,
                ).uid("details_stack"),
                layout=layout_solid,
            ),
            vbox(
                label("Selected"),
                select(rows=ROWS, options=bk().decorated)
                .uid("kept_cols")
                .observe(
                    lambda proxy, change: proxy.that.columns.attrs(value=change["new"])
                    # _kept_cols_cb
                ),
                layout=layout_solid,
            ),
            layout=layout_solid,
        )
        .uid("main")
        .backend(bk)
    )


def sniffer(url: str) -> Proxy:
    bk = Backend(ParquetSniffer, url)  # type: ignore
    return _sniffer(bk)


def get_dtypes(proxy: Proxy) -> dict[str, str]:
    kept_cols = proxy.that.kept_cols
    assert hasattr(kept_cols.widget, "options")
    return {col: tcol.split(":")[1] for (tcol, col) in kept_cols.widget.options}
