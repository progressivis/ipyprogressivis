from ipyprogressivis.ipyfuncs import *
from ipyprogressivis.csv_sniffer.backend import CSVSnifferDF

import numpy as np
import pandas as pd
import csv
import inspect
import io
import logging


def column_box(data):
    import ipyprogressivis.csv_sniffer.callbacks as cb
    cname = data.name
    return vbox(
        text("Name:",
             value=cname,
             continuous_update=False,
             disabled=True
             ).uid(f"c_name_{cname}"),
        text("Rename:",
             value=cname,
             continuous_update=False,
             disabled=True
             ).uid(f"c_rename_{cname}").observe(cb.rename_column).hints(col=cname),
        text("Type:",
             value=data.type,
             continuous_update=False,
             disabled=True
             ).uid(f"c_type_{cname}"),
        checkbox("Use",
                 value=True
                 ).uid(f"c_use_{cname}").observe(cb.usecols_column).hints(col=cname),
        dropdown("Retype:",
                 options=data.retype_values(),
                 value=data.type
                 ).uid(f"c_retype_{cname}").observe(cb.retype_column).hints(col=cname),
        text("Unique vals:", value=data.nunique, disabled=True).uid(f"c_nunique_{cname}"),
        checkbox("NA values",
                 indent=True,
                 disabled=True
                 ).uid(f"c_na_ck_{cname}").observe(cb.na_values_ck).hints(col=cname),
        stack(
            html(""),
            vbox(
                text("NA values:",
                     value=""
                     ).uid(f"c_na_{cname}").observe(cb.na_values).hints(col=cname),
                text("Separator:",
                     value="",
                     placeholder="if many values",
                     disabled=True,
                     ).uid(f"c_na_sep_{cname}").observe(cb.na_values_sep).hints(col=cname),
                ).uid(f"c_na_box_{cname}")
        ).uid(f"c_na_stack_{cname}"),
        checkbox("Filtering",
                 indent=True,
                 disabled=True
                 ).uid(f"c_filter_{cname}").observe(cb.filtering_ck).hints(col=cname),
        ).uid(f"c_box_{cname}").hints(col=cname)





def sniffer(url, lines=100):
    import ipyprogressivis.csv_sniffer.callbacks as cb
    
    #csv_s = CSVSnifferDF(url, lines)
    csv_s = Backend(CSVSnifferDF, url, lines)
    return vbox(
        hbox(
            tab(
                vbox(
                    text().uid("delimiter_other"),
                    radiobuttons(
                        orientation="vertical",
                        options=list(zip(csv_s().delimiters, csv_s().del_values))
                    ).uid("delimiter").observe(cb.delimiter),
                ),
                vbox(
                    checkbox("Dayfirst").uid("dayfirst"),
                    text("Date format:", value="mixed").uid("date_format"),
                ).uid("date"),
                vbox(
                    bounded_int_text(
                        "Lines:", value=100, min=10, max=1000, continuous_update=False
                    ).uid("lines").observe(cb.lines),
                    bounded_int_text(
                        "Header:", value=-1, min=-1, max=1000, continuous_update=False
                    ).uid("header"),
                    bounded_int_text(
                        "Skip rows:",
                        value=0,
                        min=0,
                        max=1000,
                        continuous_update=False,
                    ).uid("skiprows").observe(cb.skiprows),
                ),
                vbox(
                    text("True values").uid("true_values").observe(cb.true_values),
                    text("False values").uid("false_values").observe(cb.false_values),
                    text("NA values").uid("na_values").observe(cb.na_values),
                ).uid("special_values").layout(border="solid"),
            )
            .uid("global_tab")
            .titles("Delimiters", "Dates", "Header", "Special values"),
            vbox(
                label("PColumns"),
                select(options=[(col, i) for (i, col) in enumerate(csv_s().column.keys())],
                       rows=9
                       ).observe(cb.columns).uid("columns"),
                checkbox("Enable/disable all", value=True).uid("enable_all").observe(cb.enable_all),
            ).layout(border="solid"),
            vbox(
                label("Selected PColumn"),
                stack(
                    box(
                        label(value="No PColumn Selected").uid("no_detail"),
                    ).uid("details"),
                    stack(
                        *[column_box(val) for val in csv_s().column.values()],
                        selected_index=0
                        ).uid("sel_column_stk"), selected_index=1
                    )
            ).layout(border="solid"),
        ).uid("top"),
        hbox(button("Test").on_click(cb.test_cmd),
             label("CmdLine"),
             textarea(row=3).layout(width="100%").uid("cmdline")
             ),
        tab(
            html(value=csv_s().head_text).uid("head_text"),
            html(value=csv_s().df_text).uid("df_text"),
            html(value=csv_s().df2_text).uid("df2_text"),
            label(),
        ).layout(max_height="1024px")
        .uid("tab").layout(max_height="1024px")
        .titles("Head", "DataFrame", "DataFrame2", "Hide")
        .attrs(selected_index=1)
    ).uid("main").backend("sniffer", csv_s)

