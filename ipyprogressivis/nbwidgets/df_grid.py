import ipywidgets as ipw
import pandas as pd

from typing import Any, Callable

WidgetType = Any


class DataFrameGrid(ipw.GridBox):
    def __init__(
        self, df: pd.DataFrame, first: str = "200px", repeat: str = "70px", **kw: Any
    ) -> None:
        self.df = pd.DataFrame(index=df.index, columns=df.columns, dtype=object)
        self.inv_index: dict[int, tuple[Any, Any]] = {}
        lst: list[WidgetType] = [ipw.Label("")] + [ipw.Label(s) for s in df.columns]
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
            gtc = kw.pop(
                "grid_template_columns", f"{first} repeat({len(df.columns)}, {repeat})"
            )
            kw["layout"] = ipw.Layout(grid_template_columns=gtc)

        super().__init__(lst, **kw)

    def get_coords(self, obj: Any) -> tuple[Any, Any]:
        return self.inv_index[id(obj)]

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
