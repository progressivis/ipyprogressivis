import ipywidgets as ipw
import pandas as pd

from typing import Any, Callable

WidgetType = Any


class DataFrameGrid(ipw.GridBox):
    def __init__(
        self, df: pd.DataFrame, first: str = "200px", repeat: str = "70px",
            sizes: dict[str, str] | None = None,
            index_title: str = "",
            **kw: Any
    ) -> None:
        self.df = pd.DataFrame(index=df.index, columns=df.columns, dtype=object)
        self.inv_index: dict[int, tuple[Any, Any]] = {}
        lst: list[WidgetType] = [ipw.Label(index_title)] + [ipw.Label(s) for s in df.columns]
        for row in df.index:
            lst.append(ipw.Label(row))
            for col in df.columns:
                widget = df.loc[row, col]
                if callable(widget):
                    widget = widget()
                self.df.loc[row, col] = widget
                lst.append(widget)
                self.inv_index[id(widget)] = (row, col)
        if "layout" not in kw:
            kw = kw.copy()
            gtc = kw.pop("grid_template_columns", None)
            if gtc is None:
                if not sizes:
                    gtc = f"{first} repeat({len(df.columns)}, {repeat})"
                else:
                    others = " ".join([sizes.get(col, repeat) for col in df.columns])
                    gtc = f"{first} {others}"
            print("grid_template_columns:", gtc, sizes, df.columns)
            kw["layout"] = ipw.Layout(grid_template_columns=gtc)

        super().__init__(lst, **kw)

    def get_coords(self, obj: Any) -> tuple[Any, Any]:
        return self.inv_index[id(obj)]

    def broadcast_col(self, col: str, fnc: Callable[..., Any]) -> None:
        for row in self.df.index:
            fnc(self.df.loc[row, col])

    def broadcast(self, fnc: Callable[..., Any]) -> None:
        for row in self.df.index:
            for col in self.df.columns:
                fnc(self.df.loc[row, col])

    def observe_all(
        self, fnc: Callable[..., Any], names: str | list[str] = "value"
    ) -> None:
        for row in self.df.index:
            for col in self.df.columns:
                self.df.loc[row, col].observe(fnc, names=names)

    def observe_col(
            self, col: str,
            fnc: Callable[..., Any], names: str | list[str] = "value"
    ) -> None:
        for row in self.df.index:
            self.df.loc[row, col].observe(fnc, names=names)
