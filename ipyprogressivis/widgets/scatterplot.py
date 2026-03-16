from __future__ import annotations


import ipywidgets as widgets
from ipytablewidgets import (serialization,  # type: ignore
                             NumpyAdapter, TableType)
from traitlets import Unicode, Any, Bool  # type: ignore
from progressivis.core.api import JSONEncoderNp as JS, asynchronize
import progressivis.core.aio as aio
from .. _frontend import NPM_PACKAGE, NPM_PACKAGE_RANGE
from typing import Any as AnyType, TYPE_CHECKING, cast, Callable

if TYPE_CHECKING:
    from progressivis import Module
    from progressivis.vis.mcscatterplot import MCScatterPlot
WidgetType = AnyType

DISPLAY_RATE = 2



@widgets.register
class Scatterplot(widgets.DOMWidget):
    """Progressivis Scatterplot widget."""

    # Name of the widget view class in front-end
    _view_name = Unicode("ScatterplotView").tag(sync=True)

    # Name of the widget model class in front-end
    _model_name = Unicode("ScatterplotModel").tag(sync=True)

    # Name of the front-end module containing widget view
    _view_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Name of the front-end module containing widget model
    _model_module = Unicode(NPM_PACKAGE).tag(sync=True)

    # Version of the front-end module containing widget view
    _view_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)
    # Version of the front-end module containing widget model
    _model_module_version = Unicode(NPM_PACKAGE_RANGE).tag(sync=True)

    compression = None
    histograms = TableType(None).tag(sync=True, **serialization)
    samples = TableType(None).tag(sync=True, **serialization)
    data = Unicode("{}").tag(sync=True)
    _img_url = Unicode('null').tag(sync=True)
    value = Any("{}").tag(sync=True)
    move_point = Any("{}").tag(sync=True)
    modal = Bool(False).tag(sync=True)
    to_hide = Any([]).tag(sync=True)
    display_counter = 0

    def __init__(self, *, enable_centroids: bool = False, **kw: AnyType) -> None:
        super().__init__(**kw)
        if not enable_centroids:
            self.to_hide = cast(Any, ["init_centroids_view_"])


    def link_module(
        self, module: MCScatterPlot, refresh: bool = True
    ) -> Callable[[], None]:  # -> List[Coroutine[Any, Any, None]]:
        def _feed_widget(wg: WidgetType, m: MCScatterPlot) -> None:
            val = m.to_json()
            data_ = {
                k: v
                for (k, v) in val.items()
                if k not in ("hist_tensor", "sample_tensor")
            }
            ht = val.get("hist_tensor")
            arrays = dict()
            if ht is not None:
                for i, arr in enumerate(ht):
                    arrays[f"hist_{i}"] = arr
                wg.histograms = NumpyAdapter(arrays, touch_mode=False)
            st = val.get("sample_tensor", None)
            if st is not None:
                vectors = {f"v{i}": vec for (i, vec) in enumerate(st)}
                wg.samples = NumpyAdapter(vectors, touch_mode=False)

            wg.data = JS.dumps(data_)

        async def _after_run(
            m: Module, run_number: int
        ) -> None:  # pylint: disable=unused-argument
            if self.display_counter < DISPLAY_RATE:
                self.display_counter += 1
                return
            self.display_counter = 0
            if not self.modal:
                await asynchronize(_feed_widget, self, m)
        if refresh:
            module.on_after_run(_after_run)

        def from_input_value(_val: Any) -> None:
            bounds = self.value

            async def _cbk() -> None:
                assert module.dep.min_value is not None
                assert module.dep.max_value is not None
                assert isinstance(bounds, dict)
                await module.dep.min_value.from_input(bounds["min"])
                await module.dep.max_value.from_input(bounds["max"])

            aio.create_task(_cbk())

        self.observe(from_input_value, "value")

        def from_input_move_point(_val: Any) -> None:
            aio.create_task(module.move_point.from_input(self.move_point))  # type: ignore

        self.observe(from_input_move_point, "move_point")
        def feed() -> None:
            aio.create_task(asynchronize(_feed_widget, self, module))

        def awake(_val: Any) -> None:
            if module._json_cache is None or self.modal:
                return
            dummy = module._json_cache.get("dummy", 555)
            module._json_cache["dummy"] = -dummy
            aio.create_task(asynchronize(_feed_widget, self, module))  # TODO: improve

        self.observe(awake, "modal")
        return feed
