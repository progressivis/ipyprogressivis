from progressivis.core.api import Module
from typing import Any
import ipywidgets as ipw
from ..widgets.chaining.utils import (
    Coro,
)


class PView(Coro):
    pview_objects: dict[str, "PView"] = dict()
    def __init__(self, name: str, widget: ipw.DOMWidget, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self._name = name
        self._widget = widget
        PView.pview_objects[name] = self

    def connect_module(self, m: Module) -> None:
        m.on_after_run(self)



