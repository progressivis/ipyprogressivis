from ipyprogressivis.ipywel import *
from ipyprogressivis.csv_sniffer.backend import CSVSniffer

import numpy as np
import pandas as pd
import csv
import inspect
import io
import logging


def column_box(data):
    cname = data.name
    return (
        vbox(
            text("Name:", value=cname, continuous_update=False, disabled=True).uid(
                f"c_name_{cname}"
            ),
            text("Rename:", value=cname, continuous_update=False, disabled=True)
            .uid(f"c_rename_{cname}")
            .observe(
                lambda proxy, change: proxy.back("sniffer")
                .update_backend(proxy)
                .rename_columns()
                .sync_cmdline(proxy)
                # cb.rename_column
            )
            .hints(col=cname),
            text("Type:", value=data.type, continuous_update=False, disabled=True).uid(
                f"c_type_{cname}"
            ),
            checkbox("Use", value=True)
            .uid(f"c_use_{cname}")
            .observe(
                lambda proxy, change: proxy.back("sniffer")
                .column[proxy.hint.col]
                .set_attributes(use=change["new"])
                .sniffer.update_backend(proxy)
                .usecols_columns()
                .retype_columns()
                .this(proxy)
                .proc(
                    [
                        proxy.lookup(temp.format(proxy.hint.col)).attrs(
                            disabled=not change["new"]
                        )
                        for temp in ("c_rename_{}", "c_retype_{}", "c_na_{}")
                    ]
                )
                .proc(
                    proxy.lookup(f"c_na_{proxy.hint.col}").attrs(disabled=True)
                    if not proxy.that.per_col_na.widget.value
                    else proxy
                )
                .back("sniffer")
                .sync_cmdline(proxy),
                # cb.usecols_column
            )
            .hints(col=cname),
            dropdown(
                "Retype:", options=data.retype_values(), value=data.type, disabled=True
            )
            .uid(f"c_retype_{cname}")
            .observe(
                lambda proxy, change: proxy.back("sniffer")
                .update_backend(proxy)
                .retype_columns()
                .sync_cmdline(proxy)
                # cb.retype_column
            )
            .hints(col=cname),
            text("Unique vals:", value=data.nunique, disabled=True).uid(
                f"c_nunique_{cname}"
            ),
            text(
                "NA values:",
                value="",
                disabled=True,
                # placeholder="sep. first if many(ex: ,A,B)",
            )
            .uid(f"c_na_{cname}")
            .observe(
                lambda proxy, change: proxy.back("sniffer")
                .column[proxy.hint.col]
                .set_attributes(na_values_=change["new"])
                .sniffer.na_values_columns()
                .sync_cmdline(proxy)
                # cb.na_values_col
            )
            .hints(col=cname),
        )
        .uid(f"c_box_{cname}")
        .hints(col=cname)
    )


def _sniffer(csv_s):
    return (
        vbox(
            hbox(
                tab(
                    vbox(
                        text().uid("delimiter_other"),
                        radiobuttons(
                            orientation="vertical",
                            options=list(zip(csv_s().delimiters, csv_s().del_values)),
                        )
                        .uid("delimiter")
                        .observe(
                            lambda proxy, change: proxy.back("sniffer")
                            .set_delimiter(change["new"])
                            .dataframe(force=True)
                            .this(proxy)
                            .that.columns.attrs(
                                options=[
                                    (col, i)
                                    for (i, col) in enumerate(
                                        proxy.back("sniffer").column.keys()
                                    )
                                ]
                            )
                            .that.cmdline.attrs(value=proxy.back("sniffer").cmdline)
                            # cb.delimiter
                        ),
                    ),
                    vbox(
                        checkbox("Dayfirst").uid("dayfirst"),
                        text("Date format:", value="mixed").uid("date_format"),
                    ).uid("date"),
                    vbox(
                        bounded_int_text(
                            "Lines:",
                            value=100,
                            min=10,
                            max=1000,
                            continuous_update=False,
                        )
                        .uid("lines")
                        .observe(
                            lambda proxy, change: proxy.back("sniffer")
                            .set_attributes(_head="")
                            .dataframe(force=True)
                            # cb.lines
                        ),
                        bounded_int_text(
                            "Header:",
                            value=-1,
                            min=-1,
                            max=1000,
                            continuous_update=False,
                        ).uid("header"),
                        bounded_int_text(
                            "Skip rows:",
                            value=0,
                            min=0,
                            max=1000,
                            continuous_update=False,
                        )
                        .uid("skiprows")
                        .observe(
                            lambda proxy, change: proxy.back("sniffer")
                            .set_attributes(_head="")
                            .set_param("skiprows", change["new"])
                            .dataframe(force=True)
                            # cb.skiprows
                        ),
                    ),
                    vbox(
                        text("True values")
                        .uid("true_values")
                        .observe(
                            lambda proxy, change: proxy.back("sniffer")
                            ._parse_list("true_values", change["new"])
                            .this(proxy)
                            .that.cmdline.attrs(value=proxy.back("sniffer").cmdline)
                            # cb.true_values
                        ),
                        text("False values")
                        .uid("false_values")
                        .observe(
                            lambda proxy, change: proxy.back("sniffer")
                            ._parse_list("true_values", change["new"])
                            .this(proxy)
                            .that.cmdline.attrs(value=proxy.back("sniffer").cmdline)
                            # cb.false_values
                        ),
                        text("NA values")
                        .uid("na_values")
                        .observe(
                            lambda proxy, change: proxy.back("sniffer")
                            ._parse_list("na_values", change["new"])
                            .this(proxy)
                            .that.cmdline.attrs(value=proxy.back("sniffer").cmdline)
                            # cb.na_values
                        ),
                        checkbox("Per-column NA values")
                        .uid("per_col_na")
                        .observe(
                            lambda proxy, change: expr(
                                proxy.that.na_values.attrs(value="")
                                if change["new"]
                                else proxy.that.na_values.attrs(disabled=change["new"])
                            ).proc(
                                [
                                    (
                                        col,
                                        proxy.lookup(f"c_na_{col}").attrs(
                                            disabled=not change["new"], value=""
                                        ),
                                    )
                                    for (col, _) in proxy.that.columns.widget.options
                                    if proxy.lookup(f"c_use_{col}").widget.value
                                ]
                            )
                            # cb.per_column_na
                        ),
                    )
                    .uid("special_values")
                    .layout(border="solid"),
                )
                .uid("global_tab")
                .titles("Delimiters", "Dates", "Header", "Special values"),
                vbox(
                    label("PColumns"),
                    select(
                        options=[
                            (col, i) for (i, col) in enumerate(csv_s().column.keys())
                        ],
                        rows=9,
                    )
                    .observe(
                        lambda proxy, change: proxy.that.sel_column_stk.attrs(
                            selected_index=change["new"]
                        )
                        # cb.columns
                    )
                    .uid("columns"),
                    checkbox("Enable/disable all", value=False)
                    .uid("enable_all")
                    .observe(
                        lambda proxy, change: proxy.proc(
                            [
                                proxy.lookup(f"c_use_{col}")
                                .attrs(value=change["new"])
                                .lookup(f"c_retype_{col}")
                                .attrs(disabled=not change["new"])
                                .lookup(f"c_rename_{col}")
                                .attrs(disabled=not change["new"])
                                for (col, _) in proxy.that.columns.widget.options
                            ]
                        )
                        .back("sniffer")
                        .sync_cmdline(proxy),
                    ),
                ).layout(border="solid"),
                vbox(
                    label("Selected PColumn"),
                    stack(
                        box(
                            label(value="No PColumn Selected").uid("no_detail"),
                        ).uid("details"),
                        stack(
                            *[column_box(val) for val in csv_s().column.values()],
                            selected_index=0,
                        ).uid("sel_column_stk"),
                        selected_index=1,
                    ),
                ).layout(border="solid"),
            ).uid("top"),
            hbox(
                button("Test")
                .uid("test_btn")
                .on_click(
                    lambda proxy, btn: proxy.back("sniffer")
                    .update_backend(proxy)
                    .test_cmd()
                    .this(proxy)
                    .that.df2_text.attrs(value=proxy.back("sniffer").df2_text)
                    .that.tab.attrs(selected_index=2)
                    # cb.test_cmd
                ),
                label("CmdLine"),
                textarea(row=3).layout(width="100%").uid("cmdline"),
            ),
            tab(
                html(value=csv_s().head_text).uid("head_text"),
                html(value=csv_s().df_text).uid("df_text"),
                html(value=csv_s().df2_text).uid("df2_text"),
                label(),
            )
            .layout(max_height="1024px")
            .uid("tab")
            .layout(max_height="1024px")
            .titles("Head", "DataFrame", "DataFrame2", "Hide")
            .attrs(selected_index=1),
        )
        .uid("main")
        .backend(csv_s, name="sniffer")
    )


def sniffer(url, lines=100):
    csv_s = Backend(CSVSniffer, url, lines)
    proxy = _sniffer(csv_s)
    proxy.that.enable_all.attrs(value=True)
    return proxy
