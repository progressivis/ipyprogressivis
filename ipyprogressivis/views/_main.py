import ipywidgets as ipw
from ..widgets.chaining.utils import (
    TypedBase,
    IpyVBoxTyped,
    IpyHBoxTyped,
    make_button,
)
from .util import PView
from IPython.display import display
from progressivis.core.api import Scheduler, Module
from typing import Any as AnyType

has_graphviz = False
try:
    import graphviz  # type: ignore

    has_graphviz = True
except ImportError:
    pass


class SelectionBar(IpyHBoxTyped):
    class Typed(TypedBase):
        module: ipw.Dropdown
        view: ipw.Dropdown
        show: ipw.Button
        graph: ipw.Button


modules_views = {
    "Corr": ["Corr"],
    "RangeQuery2D": ["Sliders"],
    "Histogram2D": ["HeatmapVega"],
}


class MainBox(IpyVBoxTyped):
    class Typed(TypedBase):
        sel_bar: SelectionBar
        widget: ipw.DOMWidget

    def __init__(self, scheduler: Scheduler, *args: AnyType, **kw: AnyType) -> None:
        super().__init__(*args, **kw)
        self._scheduler = scheduler
        self._module: Module | None = None
        self._view: PView | None = None
        self._box: ipw.Box | None = None
        modules = self._scheduler.modules()
        vis_mod = [
            mn for (mn, mo) in modules.items() if mo.__class__.__name__ in modules_views
        ]
        self.child.sel_bar = SelectionBar()
        self.c_.sel_bar.c_.module = ipw.Dropdown(
            options=[""] + vis_mod,
            value="",
            description="Modules:",
            disabled=False,
            style={"description_width": "initial"},
            # layout={"width": "initial"},
        )
        self.c_.sel_bar.c_.module.observe(self.obs_modules, "value")
        self.c_.sel_bar.c_.view = ipw.Dropdown(
            options=[""],
            value="",
            description="Views:",
            disabled=True,
            style={"description_width": "initial"},
            # layout={"width": "initial"},
        )
        self.c_.sel_bar.c_.view.observe(self.obs_views, "value")
        self.c_.sel_bar.c_.show = make_button(
            "Show", cb=self._show_btn_cb, disabled=True
        )
        self.c_.sel_bar.c_.graph = make_button(
            "Graph",
            cb=self._graph_btn_cb,
            disabled=not has_graphviz,
            tooltip="Needs graphviz",
        )
        self.c_.widget = ipw.Label("Widget")

    def obs_modules(self, change: dict[str, AnyType]) -> None:
        if m_name := self.c_.sel_bar.c_.module.value:
            modules = self._scheduler.modules()
            self._module = m = modules[m_name]
            cname = m.__class__.__name__
            views = modules_views[cname]
            self.c_.sel_bar.c_.view.disabled = False
            self.c_.sel_bar.c_.view.options = [""] + views[:]
            self.c_.sel_bar.c_.view.value = ""
        else:
            self.c_.sel_bar.c_.view.options = [""]
            self.c_.sel_bar.c_.view.value = ""
            self.c_.sel_bar.c_.view.disabled = True

    def obs_views(self, change: dict[str, AnyType]) -> None:
        if view_name := self.c_.sel_bar.c_.view.value:
            from .util import PView

            self._view = PView.pview_objects[view_name]
            self.c_.sel_bar.c_.show.disabled = False
        else:
            self.c_.sel_bar.c_.show.disabled = True
            self._view = None

    def _show_btn_cb(self, btn: ipw.Button) -> None:
        if btn.description == "Show" and self._box is None:
            assert self._view is not None
            bar = (
                self._view.bar if "action" in type(self._view).__dict__ else ipw.Label()
            )
            self._box = ipw.VBox([self._view._widget, bar])
            # self._module.on_after_run(self._view)
            assert self._module is not None
            self._view.connect_module(self._module)
            self.c_.widget = self._box
            btn.description = "Hide"
        elif btn.description == "Hide":
            assert self._box is not None
            self.c_.widget = ipw.Label("...")
            btn.description = "Show"
        else:
            assert btn.description == "Show" and self._box is not None
            self.c_.widget = self._box
            btn.description = "Hide"

    def _graph_btn_cb(self, btn: ipw.Button) -> None:
        self.c_.sel_bar.c_.show.description = "Show"
        src = self._scheduler.to_graphviz()
        gvz = graphviz.Source(src)
        self.c_.widget = ipw.Output()
        with self.c_.widget:
            display(gvz)  # type: ignore


def show(scheduler: Scheduler) -> MainBox:
    return MainBox(scheduler)
